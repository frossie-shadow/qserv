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
#ifndef LSST_QSERV_REPLICA_CORE_STATUSREQUEST_H
#define LSST_QSERV_REPLICA_CORE_STATUSREQUEST_H

/// StatusRequest.h declares:
///
/// class StatusRequestBase
/// class StatusRequest
/// class ReplicateStatusRequest
/// class DeleteStatusRequest
/// class FindStatusRequest
/// class FindAllStatusRequest
///
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr
#include <string>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/ProtocolBuffer.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class StatusRequestBase represents teh base class for a family of requests
  * pulling a status of on-going operationd.
  */
class StatusRequestBase
    :   public Request {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestBase> pointer;

    // Default construction and copy semantics are proxibited

    StatusRequestBase () = delete;
    StatusRequestBase (StatusRequestBase const&) = delete;
    StatusRequestBase & operator= (StatusRequestBase const&) = delete;

    /// Destructor
    ~StatusRequestBase () override;

    /// Return an identifier of the target request
    const std::string& targetRequestId () const { return _targetRequestId; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StatusRequestBase (ServiceProvider                                   &serviceProvider,
                       const char                                        *requestTypeName,
                       const std::string                                 &worker,
                       boost::asio::io_service                           &io_service,
                       const std::string                                 &targetRequestId,
                       lsst::qserv::proto::ReplicationReplicaRequestType  requestType);

    /**
      * This method is called when a connection is established and
      * the stack is ready to begin implementing an actual protocol
      * with the worker server.
      *
      * The first step of the protocol will be to send the replication
      * request to the destination worker.
      */
    void beginProtocol () final;
    
    /// Callback handler for the asynchronious operation
    void requestSent (const boost::system::error_code &ec,
                      size_t                           bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (const boost::system::error_code &ec,
                           size_t                           bytes_transferred);
    /**
     * Parse request-specific reply
     *
     * This method must be implemented by subclasses.
     */
    virtual lsst::qserv::proto::ReplicationStatus parseResponse ()=0;

    /// Process the completion of the requested operation
    void analyze (lsst::qserv::proto::ReplicationStatus status);

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    lsst::qserv::proto::ReplicationReplicaRequestType  _requestType;
};


/**
  * Generic class StatusRequest extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class StatusRequest
    :   public StatusRequestBase {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequest<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are proxibited

    StatusRequest () = delete;
    StatusRequest (StatusRequest const&) = delete;
    StatusRequest & operator= (StatusRequest const&) = delete;

    /// Destructor
    ~StatusRequest () final {
    }

    /// Return request-specific extended data reported upon completion of the request
    const typename POLICY::responseDataType& responseData () const {
        return _responseData;
    }

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider  - a host of services for various communications
     * @param worker           - the identifier of a worker node (the one to be affectd by the request)
     * @param io_service       - network communication service
     * @param targetRequestId  - an identifier of the target request whose remote status
     *                           is going to be inspected
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     */
    static pointer create (ServiceProvider         &serviceProvider,
                           const std::string       &worker,
                           boost::asio::io_service &io_service,
                           const std::string       &targetRequestId,
                           callback_type            onFinish) {

        return StatusRequest<POLICY>::pointer (
            new StatusRequest<POLICY> (
                serviceProvider,
                POLICY::requestTypeName(),
                worker,
                io_service,
                targetRequestId,
                POLICY::requestType(),
                onFinish));
    }

private:

    /**
     * Construct the request
     */
    StatusRequest (ServiceProvider                                   &serviceProvider,
                   const char                                        *requestTypeName,
                   const std::string                                 &worker,
                   boost::asio::io_service                           &io_service,
                   const std::string                                 &targetRequestId,
                   lsst::qserv::proto::ReplicationReplicaRequestType  requestType,
                   callback_type                                      onFinish)

        :   StatusRequestBase (serviceProvider,
                               requestTypeName,
                               worker,
                               io_service,
                               targetRequestId,
                               requestType),
            _onFinish (onFinish)
    {}

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    void endProtocol () final {
        if (_onFinish != nullptr) {
            StatusRequest<POLICY>::pointer self = shared_from_base<StatusRequest<POLICY>>();
            _onFinish(self);
        }
    }

    /**
     * Parse request-specific reply
     *
     * This method implements the corresponing virtual method defined
     * by the base class.
     */
    lsst::qserv::proto::ReplicationStatus parseResponse () final {

        typename POLICY::responseMessageType message;
        _bufferPtr->parse(message, _bufferPtr->size());

        // Extract request-specific data from the response even if they mame no
        // sense before returning from thsi method
        //
        // TODO: consider optimizing this method to do teh parsing only
        //       if the status is 'SUCCESS".

        POLICY::parseResponseMessage(message, _responseData);
        
        // NOTE: field 'message' of a type returned by the current method
        //       must always be defined in all types of request-specific
        //       responses.

        return message.status();
    }

private:

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;
    
    /// Request-specific data
    typename POLICY::responseDataType _responseData;
};


// Customizations for specific request types require dedicated policies

struct StatusReplicationRequestPolicy {

    static const char* requestTypeName () { return "STATUS::REPLICA_CREATE"; } 

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_CREATE; }

    using responseMessageType = lsst::qserv::proto::ReplicationResponseReplicate;

    struct responseDataType {};

    static void parseResponseMessage (const responseMessageType& msg, responseDataType& data) {}
};
typedef StatusRequest<StatusReplicationRequestPolicy> StatusReplicationRequest;


struct StatusDeleteRequestPolicy {

    static const char* requestTypeName () { return "STATUS::REPLICA_DELETE"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_DELETE; }

    using responseMessageType = lsst::qserv::proto::ReplicationResponseDelete;

    struct responseDataType {};

    static void parseResponseMessage (const responseMessageType& msg, responseDataType& data) {}
};
typedef StatusRequest<StatusDeleteRequestPolicy> StatusDeleteRequest;

struct StatusFindRequestPolicy {

    static const char* requestTypeName () { return "STATUS::REPLICA_FIND"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_FIND; }

    using responseMessageType = lsst::qserv::proto::ReplicationResponseFind;

    struct responseDataType {};

    static void parseResponseMessage (const responseMessageType& msg, responseDataType& data) {}
};
typedef StatusRequest<StatusFindRequestPolicy> StatusFindRequest;

struct StatusFindAllRequestPolicy {

    static const char* requestTypeName () { return "STATUS::REPLICA_FIND_ALL"; }

    static lsst::qserv::proto::ReplicationReplicaRequestType requestType () {
        return lsst::qserv::proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL; }

    using responseMessageType = lsst::qserv::proto::ReplicationResponseFindAll;

    struct responseDataType {};

    static void parseResponseMessage (const responseMessageType& msg, responseDataType& data) {}
};
typedef StatusRequest<StatusFindAllRequestPolicy> StatusFindAllRequest;


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_STATUSREQUEST_H