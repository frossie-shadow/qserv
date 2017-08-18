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

#include "replica_core/WorkerServerConnection.h"

// System headers

#include <boost/bind.hpp>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessor.h"

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerServerConnection");

} /// namespace

namespace {
    
typedef std::shared_ptr<lsst::qserv::replica_core::ProtocolBuffer> ProtocolBufferPtr;

/// The context for diagnostic & debug printouts
const std::string context = "CONNECTION  ";

bool isErrorCode (boost::system::error_code ec,
                  const std::string         &scope) {

    if (ec) {
        if (ec == boost::asio::error::eof)
            LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** closed **");
        else
            LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** failed: " << ec << " **");
        return true;
    }
    return false;
}

bool readIntoBuffer (boost::asio::ip::tcp::socket &socket,
                     const ProtocolBufferPtr      &ptr,
                     size_t                        bytes) {

    ptr->resize(bytes);     // make sure the buffer has enough space to accomodate
                            // the data of the message.

    boost::system::error_code ec;
    boost::asio::read (
        socket,
        boost::asio::buffer (
            ptr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    return ! ::isErrorCode (ec, "readIntoBuffer");
}

template <class T>
bool readMessage (boost::asio::ip::tcp::socket  &socket,
                  const ProtocolBufferPtr       &ptr,
                  size_t                         bytes,
                  T                             &message) {
    
    if (!readIntoBuffer (socket,
                         ptr,
                         bytes)) return false;

    // Parse the response to see what should be done next.

    ptr->parse(message, bytes);
    return true;
}

bool readLength (boost::asio::ip::tcp::socket  &socket,
                 const ProtocolBufferPtr       &ptr,
                 uint32_t                      &bytes) {

    if (!readIntoBuffer (socket,
                         ptr,
                         sizeof(uint32_t))) return false;
    
    bytes = ptr->parseLength();
    return true;
} 
}   // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

WorkerServerConnection::pointer
WorkerServerConnection::create (ServiceProvider         &serviceProvider,
                                WorkerProcessor         &processor,
                                boost::asio::io_service &io_service) {

    return WorkerServerConnection::pointer (
        new WorkerServerConnection (
            serviceProvider,
            processor,
            io_service));
}

WorkerServerConnection::WorkerServerConnection (ServiceProvider         &serviceProvider,
                                                WorkerProcessor         &processor,
                                                boost::asio::io_service &io_service)

    :   _serviceProvider (serviceProvider),
        _processor       (processor),
        _socket          (io_service),

        _bufferPtr (
            std::make_shared<ProtocolBuffer>(
                serviceProvider.config().requestBufferSizeBytes()))
{}

WorkerServerConnection::~WorkerServerConnection () {
}

void
WorkerServerConnection::beginProtocol () {
    receive();
}

void
WorkerServerConnection::receive () {

    LOGS(_log, LOG_LVL_DEBUG, context << "receive");

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
            &WorkerServerConnection::received,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerServerConnection::received (const boost::system::error_code &ec,
                                  size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "received");

    if ( ::isErrorCode (ec, "received")) return;

    // Now read the request header

    proto::ReplicationRequestHeader hdr;
    if (!::readMessage (_socket, _bufferPtr, _bufferPtr->parseLength(), hdr)) return;
   
    // Analyse the header of the request. Note that th eheader message defined
    // requests in two layers:
    //
    // - first goes the class of requests as defined by member 'type'
    // - themn goes a choice of a specific request witin its class. Those specific
    //   request codes are obtained from the corresponding members

    switch (hdr.type()) {

        case proto::ReplicationRequestHeader::REPLICA:
            processRequest(hdr.replica_type());
            break;

        case proto::ReplicationRequestHeader::REQUEST:
            processRequest(hdr.management_type());
            break;

        case proto::ReplicationRequestHeader::SERVICE:
            processRequest(hdr.service_type());
            break;
 
        default:
            throw std::logic_error("WorkerServerConnection::received() unhandled request class '" +
                                   proto::ReplicationRequestHeader::RequestType_Name(hdr.type()));
    }
}

void
WorkerServerConnection::processRequest (proto::ReplicationReplicaRequestType type) {

    // Read the request length
    uint32_t bytes;
    if (!::readLength (_socket, _bufferPtr, bytes)) return;

    switch (type) {

        case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {

            // Read the request body
            proto::ReplicationRequestReplicate request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseReplicate response;
            _processor.enqueueForReplication (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {

            // Read the request body
            proto::ReplicationRequestDelete request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseDelete response;
            _processor.enqueueForDeletion (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_FIND: {

            // Read the request body
            proto::ReplicationRequestFind request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseFind response;
            _processor.enqueueForFind (request, response);
            reply(response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {

            // Read the request body
            proto::ReplicationRequestFindAll request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            proto::ReplicationResponseFindAll response;
            _processor.enqueueForFindAll (request, response);
            reply(response);

            break;
        }
        default:
            throw std::logic_error("WorkerServerConnection::processRequest() unhandled replica request '" +
                                   proto::ReplicationReplicaRequestType_Name(type));
    }
}

void
WorkerServerConnection::processRequest (proto::ReplicationManagementRequestType type) {

    // Read the request length
    uint32_t bytes;
    if (!::readLength (_socket, _bufferPtr, bytes)) return;

    switch (type) {

        case proto::ReplicationManagementRequestType::REQUEST_STOP: {

            // Read the request body
            proto::ReplicationRequestStop request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            switch (request.type()) {

                case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {
                    proto::ReplicationResponseReplicate response;
                    _processor.dequeueOrCancel (request, response);
                    reply(response);
                    break;
                }

                case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {
                    proto::ReplicationResponseDelete response;
                    _processor.dequeueOrCancel (request, response);
                    reply(response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                    proto::ReplicationResponseFind response;
                    _processor.dequeueOrCancel (request, response);
                    reply(response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {
                    proto::ReplicationResponseFindAll response;
                    _processor.dequeueOrCancel (request, response);
                    reply(response);
                    break;
                }
            }
            break;
        }
        case proto::ReplicationManagementRequestType::REQUEST_STATUS : {

            // Read the request body
            proto::ReplicationRequestStatus request;
            if (!::readMessage (_socket, _bufferPtr, bytes, request)) return;

            switch (request.type()) {

                case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {
                    proto::ReplicationResponseReplicate response;
                    _processor.checkStatus (request, response);
                    reply(response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {
                    proto::ReplicationResponseDelete response;
                    _processor.checkStatus (request, response);
                    reply(response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                    proto::ReplicationResponseFind response;
                    _processor.checkStatus (request, response);
                    reply(response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {
                    proto::ReplicationResponseFindAll response;
                    _processor.checkStatus (request, response);
                    reply(response);
                    break;
                }
            }
            break;
        }
        default:
            throw std::logic_error("WorkerServerConnection::processRequest() unhandled management request '" +
                                   proto::ReplicationManagementRequestType_Name(type));
    }
}

void
WorkerServerConnection::processRequest (proto::ReplicationServiceRequestType type) {

    switch (type) {

        case proto::ReplicationServiceRequestType::SERVICE_SUSPEND: {

            // This operation is allowed to be asynchronious as it may take
            // extra time for the processor's threads to finish on-going processing

            _processor.stop ();

            proto::ReplicationServiceResponse response;
            _processor.setServiceResponse(response,
                                          _processor.state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                                              proto::ReplicationServiceResponse::FAILED :
                                              proto::ReplicationServiceResponse::SUCCESS);
            reply(response);

            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_RESUME: {
  
            // This is a synchronus operation. The state transition request should happen
            // (or be denied) instantaneously.
      
            _processor.run ();

            proto::ReplicationServiceResponse response;
            _processor.setServiceResponse (response,
                                           _processor.state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                                               proto::ReplicationServiceResponse::SUCCESS :
                                               proto::ReplicationServiceResponse::FAILED);
            reply(response);

            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_STATUS: {

            proto::ReplicationServiceResponse response;
            _processor.setServiceResponse (response,
                                           proto::ReplicationServiceResponse::SUCCESS);
            reply(response);

            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_REQUESTS: {

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests

            proto::ReplicationServiceResponse response;
            _processor.setServiceResponse (response,
                                           proto::ReplicationServiceResponse::SUCCESS,
                                           extendedReport);
            reply(response);

            break;
        }
        default:
            throw std::logic_error("WorkerServerConnection::processRequest() unhandled service request '" +
                                    proto::ReplicationServiceRequestType_Name(type));
    }
}

void
WorkerServerConnection::send () {

    LOGS(_log, LOG_LVL_DEBUG, context << "send");

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &WorkerServerConnection::sent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void
WorkerServerConnection::sent (const boost::system::error_code &ec,
                              size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "sent");

    if ( ::isErrorCode (ec, "sent")) return;

    // Go wait for another request

    receive();
}

}}} // namespace lsst::qserv::replica_core
