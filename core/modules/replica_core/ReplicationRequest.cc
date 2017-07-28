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
#include "replica_core/ReplicationRequest.h"

// System headers

#include <arpa/inet.h>  // htonl, ntohl

#include <chrono>
#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers

#include "replica_core/ProtocolBuffer.h"
#include "replica_core/WorkerInfo.h"

namespace proto = lsst::qserv::proto;

namespace lsst {
namespace qserv {
namespace replica_core {

ReplicationRequest::pointer
ReplicationRequest::create (ServiceProvider::pointer serviceProvider,
                            const std::string        &database,
                            unsigned int             chunk,
                            const std::string        &sourceWorker,
                            const std::string        &destinationWorker,
                            boost::asio::io_service  &io_service,
                            callback_type            onFinish) {

    return pointer (
        new ReplicationRequest (
            serviceProvider,
            database,
            chunk,
            sourceWorker,
            destinationWorker,
            io_service,
            onFinish));
}

ReplicationRequest::ReplicationRequest (ServiceProvider::pointer serviceProvider,
                                        const std::string        &database,
                                        unsigned int             chunk,
                                        const std::string        &sourceWorker,
                                        const std::string        &destinationWorker,
                                        boost::asio::io_service  &io_service,
                                        callback_type            onFinish)
    :   Request(serviceProvider,
                "REPLICATE",
                destinationWorker,
                io_service),
 
        _database            (database),
        _chunk               (chunk),
        _sourceWorker        (sourceWorker),
        _sourceWorkerInfoPtr (serviceProvider->workerInfo(sourceWorker)),
        _onFinish            (onFinish)
{}

ReplicationRequest::~ReplicationRequest ()
{
}

std::shared_ptr<Request>
ReplicationRequest::final_shared_from_this () {
    return shared_from_this () ;
}

void
ReplicationRequest::beginProtocol () {

    std::cout << context() << "beginProtocol()" << std::endl;

    // Serialize the Request message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_type(proto::ReplicationRequestHeader::REPLICATE);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestReplicate message;
    message.set_database(_database);
    message.set_chunk(_chunk);
    message.set_id(id());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &ReplicationRequest::requestSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::requestSent (const boost::system::error_code &ec,
                                 size_t                           bytes_transferred) {

    std::cout << context() << "requestSent()" << std::endl;

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveResponse();
}

void
ReplicationRequest::receiveResponse () {

    std::cout << context() << "receiveResponse()" << std::endl;

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
            &ReplicationRequest::responseReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::responseReceived (const boost::system::error_code &ec,
                                      size_t                           bytes_transferred) {

    std::cout << context() << "responseReceived()" << std::endl;

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
    
        proto::ReplicationResponseReplicate message;
        _bufferPtr->parse(message, bytes);
    
        analyze(message.status());
    }
}

void
ReplicationRequest::wait () {

    std::cout << context() << "wait()" << std::endl;

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &ReplicationRequest::awaken,
            shared_from_this(),
            boost::asio::placeholders::error
        )
    );
}

void
ReplicationRequest::awaken (const boost::system::error_code &ec) {

    std::cout << context() << "awaken()" << std::endl;

    if (isAborted(ec)) return;

    sendStatus();
}

void
ReplicationRequest::sendStatus () {

    std::cout << context() << "sendStatus()" << std::endl;

    // Serialize the Status message header and the request itself into
    // the network buffer.

    _bufferPtr->resize();

    proto::ReplicationRequestHeader hdr;
    hdr.set_type(proto::ReplicationRequestHeader::STATUS);

    _bufferPtr->serialize(hdr);

    proto::ReplicationRequestStatus message;
    message.set_id(id());

    _bufferPtr->serialize(message);

    // Send the message

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &ReplicationRequest::statusSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::statusSent (const boost::system::error_code &ec,
                                size_t                           bytes_transferred) {

    std::cout << context() << "statusSent()" << std::endl;

    if (isAborted(ec)) return;

    if (ec) restart();
    else    receiveStatus();
}

void
ReplicationRequest::receiveStatus () {

    std::cout << context() << "receiveStatus()" << std::endl;

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
            &ReplicationRequest::statusReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
ReplicationRequest::statusReceived (const boost::system::error_code &ec,
                                   size_t                            bytes_transferred) {

    std::cout << context() << "statusReceived()" << std::endl;

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
    
        proto::ReplicationResponseStatus message;
        _bufferPtr->parse(message, bytes);
    
        analyze(message.status());
    }
}

void
ReplicationRequest::analyze (proto::ReplicationStatus status) {

    std::cout << context() << "analyze()  remote status: " << proto::ReplicationStatus_Name(status) << std::endl;

    switch (status) {
 
        case proto::ReplicationStatus::SUCCESS:
            finish (SUCCESS);
            break;

        case proto::ReplicationStatus::QUEUED:
        case proto::ReplicationStatus::IN_PROGRESS:
        case proto::ReplicationStatus::SUSPENDED:

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
            throw std::logic_error("ReplicationRequest::analyze() unknown status '" + proto::ReplicationStatus_Name(status) + "' received from server");

    }
}

void
ReplicationRequest::endProtocol () {

    std::cout << context() << "endProtocol()" << std::endl;

    if (_onFinish != nullptr) {
        _onFinish(shared_from_this());
    }
}

}}} // namespace lsst::qserv::replica_core
