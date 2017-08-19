#include <iostream>
#include <stdexcept>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/Controller.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/ServiceProvider.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_admin");

const char* usage = "Usage: <config> {SERVICE_SUSPEND | SERVICE_RESUME | SERVICE_STATUS | SERVICE_REQUESTS} <worker>";

/**
 * Print a status of the service
 */
void printRequest (const rc::ServiceManagementRequestBase::pointer &request) {
    LOGS(_log, LOG_LVL_INFO, request->id() << request->getServiceState ());
    LOGS(_log, LOG_LVL_INFO, request->id() << request->performance ());
}

/**
 * Run the test
 */
void test (const std::string &configFileName,
           const std::string &operation,
           const std::string &worker) {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Start the controller in its own thread before injecting any requests

        controller->run();

        rc::ServiceManagementRequestBase::pointer request;

        if      ("SERVICE_SUSPEND" == operation) request = controller->suspendWorkerService (
                worker,
                [] (rc::ServiceSuspendRequest::pointer request) {
                    printRequest (request);
                });

        else if ("SERVICE_RESUME"  == operation) request = controller->resumeWorkerService (
                worker,
                [] (rc::ServiceResumeRequest::pointer request) {
                    printRequest (request);
                });

        else if ("SERVICE_STATUS"  == operation) request = controller->statusOfWorkerService (
                worker,
                [] (rc::ServiceStatusRequest::pointer request) {
                    printRequest (request);
                });
        else if ("SERVICE_REQUESTS"  == operation) request = controller->requestsOfWorkerService (
                worker,
                [] (rc::ServiceRequestsRequest::pointer request) {
                    printRequest (request);
                });
        else {
            std::cerr << usage << std::endl;
            return;
        }

        // Wait before the request is finished. Then stop the master controller

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between iterations

        while (request->state() != rc::Request::State::FINISHED) {
            blockPost.wait();
        }
        controller->stop();

        // Block the current thread indefinitively or untill the controller is cancelled.

        LOGS(_log, LOG_LVL_INFO, "waiting for: controller->join()");
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
}

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 4) {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    const std::string configFileName = argv[1];
    const std::string operation      = argv[2];
    const std::string workerName     = argv[3];
    
    ::test (configFileName, operation, workerName);

    return 0;
}