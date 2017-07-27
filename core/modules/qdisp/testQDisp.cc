// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// System headers
#include <string>
#include <unistd.h>

// Third-party headers
#include "boost/lexical_cast.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE Qdisp_1
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/MergingHandler.h"
#include "global/ResourceUnit.h"
#include "global/MsgReceiver.h"
#include "qdisp/Executive.h"
#include "qdisp/JobQuery.h"
#include "qdisp/LargeResultMgr.h"
#include "qdisp/MessageStore.h"
#include "qdisp/XrdSsiMocks.h"
#include "util/threadSafe.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

typedef util::Sequential<int> SequentialInt;
typedef std::vector<qdisp::ResponseHandler::Ptr> RequesterVector;

//+++++++++++++++++++++++++
#undef LOGS_DEBUG
#define LOGS_DEBUG(x) std::cerr <<x <<'\n' <<std::flush

class ChunkMsgReceiverMock : public MsgReceiver {
public:
    virtual void operator()(int code, std::string const& msg) {
        LOGS_DEBUG("Mock::operator() chunkId=" << _chunkId
                   << ", code=" << code << ", msg=" << msg);
    }
    static std::shared_ptr<ChunkMsgReceiverMock> newInstance(int chunkId) {
        std::shared_ptr<ChunkMsgReceiverMock> r = std::make_shared<ChunkMsgReceiverMock>();
        r->_chunkId = chunkId;
        return r;
    }
    int _chunkId;
};

/** Simple functor for testing that _retryfunc has been called.
 */
class JobQueryTest : public qdisp::JobQuery {
public:
    typedef std::shared_ptr<JobQueryTest> Ptr;
    JobQueryTest(qdisp::Executive::Ptr const& executive,
                 qdisp::JobDescription const& jobDescription,
                 qdisp::JobStatus::Ptr jobStatus,
                 qdisp::MarkCompleteFunc::Ptr markCompleteFunc)
        : qdisp::JobQuery{executive, jobDescription, jobStatus, markCompleteFunc, 12345} {}

    virtual ~JobQueryTest() {}
    virtual bool runJob() override {
        retryCalled = true;
        LOGS_DEBUG("_retryCalled=" << retryCalled);
        return true;
    }
    bool retryCalled {false};

    // Create a fresh JobQueryTest instance.
    // Special ResponseHandlers need to be defined in JobDescription.
    static JobQueryTest::Ptr getJobQueryTest(
            qdisp::Executive::Ptr const& executive, qdisp::JobDescription jobDesc,
            qdisp::MarkCompleteFunc::Ptr markCompleteFunc) {
        qdisp::JobStatus::Ptr status(new qdisp::JobStatus());
        std::shared_ptr<JobQueryTest> jqTest(new JobQueryTest(executive, jobDesc, status, markCompleteFunc));
        jqTest->_setup(); // Must call _setup() by hand as bypassing newJobQuery().
        jqTest->_queryRequestPtr.reset(new qdisp::QueryRequest(jqTest));
        return jqTest;
    }
};

/** Simple functor for testing _finishfunc.
 */
class FinishTest : public qdisp::MarkCompleteFunc {
public:
    typedef std::shared_ptr<FinishTest> Ptr;
    FinishTest() : MarkCompleteFunc(0, -1) {}
    virtual ~FinishTest() {}
    virtual void operator()(bool val) {
        finishCalled = true;
        LOGS_DEBUG("_finishCalled=" << finishCalled);
    }
    bool finishCalled {false};
};

/** Simple ResponseHandler for testing.
 */
class ResponseHandlerTest : public qdisp::ResponseHandler {
public:
    ResponseHandlerTest() : _code(0), _finished(false), _processCancelCalled(false) {}
    virtual std::vector<char>& nextBuffer() {
        return _vect;
    }
    virtual bool flush(int bLen, bool& last, bool& largeResult) {
        return bLen == magic();
    }
    virtual void errorFlush(std::string const& msg, int code) {
        _msg = msg;
        _code = code;
    }
    virtual bool finished() const {
        return _finished;
    }
    virtual bool reset() {
        return true;
    }
    virtual qdisp::ResponseHandler::Error getError() const {
        return qdisp::ResponseHandler::Error(-1, "testQDisp Error");
    }
    virtual std::ostream& print(std::ostream& os) const {
        return os;
    }
    virtual void processCancel() {
        _processCancelCalled = true;
    }

    static int magic() {return 8;}
    std::vector<char> _vect;
    std::string _msg;
    int _code;
    bool _finished;
    bool _processCancelCalled;
};

/** Add dummy requests to an executive corresponding to the requesters
 */
void addFakeRequests(qdisp::Executive::Ptr const& ex, SequentialInt &sequence, std::string const& millisecs, RequesterVector& rv) {
    ResourceUnit ru;
    int copies = rv.size();
    std::vector<std::shared_ptr<qdisp::JobDescription>> s(copies);
    for(int j=0; j < copies; ++j) {
        // The job copies the JobDescription.
        qdisp::JobDescription job(sequence.incr(),
                ru,        // dummy
                millisecs, // Request = stringified milliseconds
                rv[j]);
        auto jobQuery = ex->add(job); // ex->add() is not thread safe.
        std::cerr <<"Add jq=" <<std::hex <<jobQuery <<std::dec<<'\n'<<std::flush;
    }
}

/** Start adds 'copies' number of test requests that each sleep for 'millisecs' time
 * before signaling to 'ex' that they are done.
 * Returns time to complete in seconds.
 */
void executiveTest(qdisp::Executive::Ptr const& ex, SequentialInt &sequence,
                   SequentialInt &chunkId, std::string const& millisecs, int copies) {
    // Test class Executive::add
    // Modeled after ccontrol::UserQuery::submit()
    ResourceUnit ru;
    std::string chunkResultName = "mock";
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId.get());
    ccontrol::MergingHandler::Ptr mh = std::make_shared<ccontrol::MergingHandler>(cmr, infileMerger, chunkResultName);
    std::string msg = millisecs;
    RequesterVector rv;
    for (int j=0; j < copies; ++j) {
        rv.push_back(mh);
    }
    addFakeRequests(ex, sequence, millisecs, rv);
}


/** This function is run in a separate thread to fail the test if it takes too long
 * for the jobs to complete.
 */
void timeoutFunc(util::Flag<bool>& flagDone, int millisecs) {
    LOGS_DEBUG("timeoutFunc");
//  usleep(1000*millisecs);
//  bool done = flagDone.get();
//  LOGS_DEBUG("timeoutFunc sleep over millisecs=" << millisecs << " done=" << done);
//  BOOST_REQUIRE(done == true);
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(Executive) {
    util::Flag<bool> done(false);
    int millisInt = 200;
    std::thread timeoutT(&timeoutFunc, std::ref(done), millisInt*10);
    std::string millis(boost::lexical_cast<std::string>(millisInt));
    int jobs = 0;

   {LOGS_DEBUG("Executive test 1");
    // Modeled after ccontrol::UserQuery::submit()
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    // test single instance
    ++jobs;
    executiveTest(ex, sequence, chunkId, millis, 1);
    LOGS_DEBUG("jobs=" << jobs);
    ex->join();
    BOOST_CHECK(ex->getEmpty() == true);
   }

    // test adding 4 jobs
   {LOGS_DEBUG("Executive test 2");
    util::Flag<bool> done(false);
    // Modeled after ccontrol::UserQuery::submit()
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    executiveTest(ex, sequence, chunkId, millis, 4);
    jobs += 4;
    ex->join();
    BOOST_CHECK(ex->getEmpty() == true);
   }

    // Test that we can detect ex._empty == false.
    LOGS_DEBUG("Executive test 3");
   {util::Flag<bool> done(false);
    // Modeled after ccontrol::UserQuery::submit()
    std::string str = qdisp::Executive::Config::getMockStr();
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    SequentialInt sequence(0);
    SequentialInt chunkId(1234);
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(false);
    executiveTest(ex, sequence, chunkId, millis, 5);
    jobs += 5;
    while (qdisp::XrdSsiServiceMock::_count.get() < jobs) {
        LOGS_DEBUG("waiting for _count(" << qdisp::XrdSsiServiceMock::_count.get()
                   << ") == jobs(" << jobs << ")");
        usleep(10000);
    }
    BOOST_CHECK(ex->getEmpty() == false);
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(true);
    ex->join();
    LOGS_DEBUG("ex->join() joined");
    BOOST_CHECK(ex->getEmpty() == true);
    done.exchange(true);
   }
    timeoutT.join();
    LOGS_DEBUG("Executive test end");
}

BOOST_AUTO_TEST_CASE(MessageStore) {
    LOGS_DEBUG("MessageStore test start");
    qdisp::MessageStore ms;
    BOOST_CHECK(ms.messageCount() == 0);
    ms.addMessage(123, 456, "test1");
    std::string str("test2");
    ms.addMessage(124, -12, str);
    ms.addMessage(86, -12, "test3");
    BOOST_CHECK(ms.messageCount() == 3);
    BOOST_CHECK(ms.messageCount(-12) == 2);
    qdisp::QueryMessage qm = ms.getMessage(1);
    BOOST_CHECK(qm.chunkId == 124 && qm.code == -12 && str.compare(qm.description) == 0);
    LOGS_DEBUG("MessageStore test end");
}

BOOST_AUTO_TEST_CASE(QueryRequest) {
    LOGS_DEBUG("QueryRequest test");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and RetryTest (JobQuery child)
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    int jobId = 93;
    int chunkId = 14;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();

    JobQueryTest::Ptr jqTest =
        JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 1");
    // Test that ProcessResponse detects !isOk and retries.
    qdisp::QueryRequest::Ptr qrq = jqTest->getQueryRequest();
    XrdSsiRespInfo rInfo;
    rInfo.rType = XrdSsiRespInfo::isError;
    rInfo.eNum = 123;
    rInfo.eMsg = "test_msg";
    XrdSsiErrInfo eInfo;
    eInfo.Set("test_msg", 123);
    qrq->ProcessResponse(eInfo, rInfo);
    BOOST_CHECK(respReq->_code == -1);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(jqTest->retryCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 2");
    // Test that ProcessResponse detects XrdSsiRespInfo::isError.
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    int magicErrNum = 5678;
    rInfo.rType = XrdSsiRespInfo::isError;
    rInfo.eNum = magicErrNum;
    eInfo.Set("magicErrNum", magicErrNum);
    finishTest->finishCalled = false;
    qrq->ProcessResponse(eInfo, rInfo);
    LOGS_DEBUG("respReq->_code=" << respReq->_code);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_ERROR);
    BOOST_CHECK(respReq->_code == magicErrNum);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponse test 3");
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    rInfo.rType = XrdSsiRespInfo::isStream;
    eInfo.Clr();
    finishTest->finishCalled = false;
    qrq->ProcessResponse(eInfo, rInfo);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_ERROR_CORRUPT);
    BOOST_CHECK(finishTest->finishCalled);
    // The success case for ProcessResponse is probably best tested with integration testing.
    // Getting it work in a unit test requires replacing inline bool XrdSsiRequest::GetResponseData
    // or coding around that function call for the test. Failure of the path will have high visibility.
    LOGS_DEBUG("QueryRequest::ProcessResponseData test 1");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    qrq = jqTest->getQueryRequest();
    qrq->doNotRetry();
    const char* ts="abcdefghijklmnop";
    char dataBuf[50];
    strcpy(dataBuf, ts);
    eInfo.Set("mockError", 7);
    qrq->ProcessResponseData(eInfo, dataBuf, -7, true); // qrq deleted
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::RESPONSE_DATA_NACK);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponseData test 2");
    finishTest->finishCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    eInfo.Clr();
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(eInfo, dataBuf, ResponseHandlerTest::magic()+1, true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::MERGE_ERROR);
    BOOST_CHECK(finishTest->finishCalled);

    LOGS_DEBUG("QueryRequest::ProcessResponseData test 3");
    finishTest->finishCalled = false;
    jqTest->retryCalled = false;
    jqTest = JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    eInfo.Clr();
    qrq = jqTest->getQueryRequest();
    qrq->ProcessResponseData(eInfo, dataBuf, ResponseHandlerTest::magic(), true);
    BOOST_CHECK(jqTest->getStatus()->getInfo().state == qdisp::JobStatus::COMPLETE);
    BOOST_CHECK(finishTest->finishCalled);
    BOOST_CHECK(!jqTest->retryCalled);
}

BOOST_AUTO_TEST_CASE(ExecutiveCancel) {
    // Test that all JobQueries are cancelled.
    LOGS_DEBUG("Check that executive squash");
    std::string str = qdisp::Executive::Config::getMockStr();
    // Setup Executive and JobQueryTest child
    qdisp::Executive::Config::Ptr conf = std::make_shared<qdisp::Executive::Config>(str);
    std::shared_ptr<qdisp::MessageStore> ms = std::make_shared<qdisp::MessageStore>();
    qdisp::LargeResultMgr::Ptr lgResMgr = std::make_shared<qdisp::LargeResultMgr>();
    qdisp::Executive::Ptr ex = qdisp::Executive::newExecutive(conf, ms, lgResMgr);
    int chunkId = 14;
    int first = 1;
    int last = 20;
    std::string chunkResultName = "mock"; //ttn.make(cs.chunkId);
    std::shared_ptr<rproc::InfileMerger> infileMerger;
    std::shared_ptr<ChunkMsgReceiverMock> cmr = ChunkMsgReceiverMock::newInstance(chunkId);
    ResourceUnit ru;
    std::shared_ptr<ResponseHandlerTest> respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobQuery::Ptr jq;
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(false); // Can't let jobs run or they are untracked before squash
    for (int jobId=first; jobId<=last; ++jobId) {
        qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);
        auto jQuery = ex->add(jobDesc);
        jq = ex->getJobQuery(jobId);
        auto qRequest = jq->getQueryRequest();
        BOOST_CHECK(jq->isQueryCancelled() == false);
    }
    ex->squash();
    ex->squash(); // check that squashing twice doesn't cause issues.
    for (int jobId=first; jobId<=last; ++jobId) {
        jq = ex->getJobQuery(jobId);
        BOOST_CHECK(jq->isQueryCancelled() == true);
    }
    qdisp::XrdSsiServiceMock::_go.exchangeNotify(true);
    usleep(250000); // Give mock threads a quarter second to complete.

    LOGS_DEBUG("Check that QueryResource and QueryRequest detect the cancellation of a job.");
    std::shared_ptr<FinishTest> finishTest = std::make_shared<FinishTest>();
    int jobId = 7;
    respReq = std::make_shared<ResponseHandlerTest>();
    qdisp::JobDescription jobDesc(jobId, ru, "a message", respReq);

    qdisp::JobQuery::Ptr jqTest =
        JobQueryTest::getJobQueryTest(ex, jobDesc, finishTest);
    auto request = jqTest->getQueryRequest();
    BOOST_CHECK(request->isQueryRequestCancelled() == false);
    BOOST_CHECK(respReq->_processCancelCalled == false);
    jqTest->cancel();
    BOOST_CHECK(request->isQueryCancelled() == true);
    BOOST_CHECK(request->isQueryRequestCancelled() == true);
    BOOST_CHECK(respReq->_processCancelCalled == true);

}

BOOST_AUTO_TEST_SUITE_END()



