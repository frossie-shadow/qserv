/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

// Class header
#include "replica_core/FindAllRequest.h"

// System headers

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/ProtocolBuffer.h"
#include "replica_core/ServiceProvider.h"

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.FindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

FindAllRequest::pointer
FindAllRequest::create (ServiceProvider         &serviceProvider,
                        boost::asio::io_service &io_service,
                        const std::string       &worker,
                        const std::string       &database,
                        callback_type            onFinish,
                        int                      priority) {

    return FindAllRequest::pointer (
        new FindAllRequest (
            serviceProvider,
            io_service,
            worker,
            database,
            onFinish,
            priority));
}

FindAllRequest::FindAllRequest (ServiceProvider         &serviceProvider,
                                boost::asio::io_service &io_service,
                                const std::string       &worker,
                                const std::string       &database,
                                callback_type            onFinish,
                                int                      priority)
    :   Request(serviceProvider,
                io_service,
                "REPLICA_FIND_ALL",
                worker,
                priority),
        _database (database),
        _onFinish (onFinish),
        _replicaInfoCollection () {
}

FindAllRequest::~FindAllRequest () {
}

const ReplicaInfoCollection&
FindAllRequest::responseData () const {
    if ((state() == FINISHED) && (extendedState() == SUCCESS)) return _replicaInfoCollection;
    throw std::logic_error("operation is only allowed in state " + state2string(FINISHED, SUCCESS)  +
                           " in FindAllRequest::replicaInfoCollection()");
}

void
FindAllRequest::beginProtocol () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "beginProtocol");

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_type        (proto::ReplicationRequestHeader::REPLICA);
    hdr.set_replica_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestFindAll message;
    message.set_priority (priority());
    message.set_id       (id());
    message.set_database (database());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &FindAllRequest::requestSent,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequest::requestSent (const boost::system::error_code &ec,
                             size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "requestSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
FindAllRequest::receiveResponse () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveResponse");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &FindAllRequest::responseReceived,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequest::responseReceived (const boost::system::error_code &ec,
                                  size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "responseReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // Get the length of the message and try reading the message itself
    // from the socket.
        
    const uint32_t bytes = _bufferPtr->parseLength();

    _bufferPtr->resize(bytes);      // make sure the buffer has enough space to accomodate
                                    // the data of the message.

    boost::system::error_code error_code;
    boost::asio::read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        error_code
    );
    if (error_code) restart();
    else {
    
        // Parse the response to see what should be done next.
    
        proto::ReplicationResponseFindAll message;
        _bufferPtr->parse(message, bytes);
    
        analyze(message);
    }
}

void
FindAllRequest::wait () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "wait");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &FindAllRequest::awaken,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error
        )
    );
}

void
FindAllRequest::awaken (const boost::system::error_code &ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awaken");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    sendStatus();
}

void
FindAllRequest::sendStatus () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "sendStatus");

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_type           (proto::ReplicationRequestHeader::REQUEST);
    hdr.set_management_type(proto::ReplicationManagementRequestType::REQUEST_STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(id());
    message.set_type(proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL);

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &FindAllRequest::statusSent,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequest::statusSent (const boost::system::error_code &ec,
                            size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusSent");

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveStatus();
}

void
FindAllRequest::receiveStatus () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "receiveStatus");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
    // the message itsef) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &FindAllRequest::statusReceived,
            shared_from_base<FindAllRequest>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
FindAllRequest::statusReceived (const boost::system::error_code &ec,
                                size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "statusReceived");

    if (isAborted(ec)) return;

    if (ec) {
        restart();
        return;
    }

    // Get the length of the message and try reading the message itself
    // from the socket.

    const uint32_t bytes = _bufferPtr->parseLength();

    _bufferPtr->resize(bytes);      // make sure the buffer has enough space to accomodate
                                    // the data of the message.

    boost::system::error_code error_code;
    boost::asio::read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        error_code
    );
    if (error_code) restart();
    else {
    
        // Parse the response to see what should be done next.
    
        proto::ReplicationResponseFindAll message;
        _bufferPtr->parse(message, bytes);
    
        analyze(message);
    }
}

void
FindAllRequest::analyze (const proto::ReplicationResponseFindAll &message) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "analyze  remote status: "
         << proto::ReplicationStatus_Name(message.status()));

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    _performance.update(message.performance());

    for (int num = message.replica_info_many_size(), idx = 0; idx < num; ++idx)
        _replicaInfoCollection.emplace_back(&(message.replica_info_many(idx)));

    switch (message.status()) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish (SUCCESS);
            break;

        case proto::ReplicationStatus::QUEUED:
        case proto::ReplicationStatus::IN_PROGRESS:
        case proto::ReplicationStatus::IS_CANCELLING:

            // Go wait until a definitive response from the worker is received.

            wait();
            return;

        case proto::ReplicationStatus::BAD:
            finish (SERVER_BAD);
            break;

        case proto::ReplicationStatus::FAILED:
            finish (SERVER_ERROR);
            break;

        case proto::ReplicationStatus::CANCELLED:
            finish (SERVER_CANCELLED);
            break;

        default:
            throw std::logic_error("FindAllRequest::analyze() unknown status '" + proto::ReplicationStatus_Name(message.status()) +
                                   "' received from server");

    }
}

void
FindAllRequest::endProtocol () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "endProtocol");

    if (_onFinish != nullptr) {
        _onFinish(shared_from_base<FindAllRequest>());
    }
}

}}} // namespace lsst::qserv::replica_core
