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
#ifndef LSST_QSERV_REPLICA_CORE_SERVICEMANAGEMENTREQUEST_H
#define LSST_QSERV_REPLICA_CORE_SERVICEMANAGEMENTREQUEST_H

/// ServiceManagementRequest.h declares:
///
/// class ServiceManagementRequestBase
/// class ServiceManagementRequest
/// class ServiceSuspendRequest
/// class ServiceResumeRequest
/// class ServiceStatusRequest
//
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr
#include <ostream>
#include <string>
#include <vector>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class ServiceManagementRequestBase is the base class for a family of requests
  * managing worker-side replication service. The only variable parameter of this
  * class is a specific type of the managemenyt request.
  *
  * Note that this class can't be instantiate directly. It serves as an implementation
  * of the protocol. All final customizations and type-specific operations are
  * provided via a generic subclass.
  */
class ServiceManagementRequestBase
    :   public Request {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequestBase> pointer;

    // Default construction and copy semantics are proxibited

    ServiceManagementRequestBase () = delete;
    ServiceManagementRequestBase (ServiceManagementRequestBase const&) = delete;
    ServiceManagementRequestBase & operator= (ServiceManagementRequestBase const&) = delete;

    /// Destructor
    virtual ~ServiceManagementRequestBase ();

    /// Various parameters representing the state of the service which
    /// are available upon the completion of the request.
    struct ServiceState {

        // Its state
        enum State {
            SUSPEND_IN_PROGRESS = 0,
            SUSPENDED           = 1,
            RUNNING             = 2
        };
        State state;

        /// Return string representation of the state
        std::string state2string () const {
            switch (state) {
                case SUSPEND_IN_PROGRESS: return "SUSPEND_IN_PROGRESS";
                case SUSPENDED:           return "SUSPENDED";
                case RUNNING:             return "RUNNING";
            }
            return "";
        }

        // Counters for requests known to the service since its last start

        uint32_t numNewRequests;
        uint32_t numInProgressRequests;
        uint32_t numFinishedRequests;
        
        std::vector<lsst::qserv::proto::ReplicationServiceRequestInfo> newRequests;
        std::vector<lsst::qserv::proto::ReplicationServiceRequestInfo> inProgressRequests;
        std::vector<lsst::qserv::proto::ReplicationServiceRequestInfo> finishedRequests;
    };

    /**
     * Get the state of the worker-side service
     *
     * This method will throw exception std::logic_error if the request's primary state
     * is not 'FINISHED' and its extended state is neither 'SUCCESS" or 'SERVER_ERROR'.
     */
    const ServiceState& getServiceState () const;

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    ServiceManagementRequestBase (ServiceProvider                                   &serviceProvider,
                                  boost::asio::io_service                           &io_service,
                                  const char                                        *requestTypeName,
                                  const std::string                                 &worker,
                                  lsst::qserv::proto::ReplicationServiceRequestType  requestType);
private:

    /**
      * This method is called when a connection  with the worker server is established
      * and the communication stack is ready to begin implementing the actual protocol.
      *
      * The first step in the protocol will be to send the replication
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

    /// Process the worker response to the requested operation
    void analyze (const lsst::qserv::proto::ReplicationServiceResponse &message);

private:

    /// Request type
    lsst::qserv::proto::ReplicationServiceRequestType _requestType;

    /// Detailed status of the worker-side service obtained upon completion of
    /// the management request.
    ServiceState _serviceState;
};

/// Overloaded streaming operator for type ServiceManagementRequestBase::ServiceState
std::ostream& operator<< (std::ostream &os, const ServiceManagementRequestBase::ServiceState &ss);


/**
  * Generic class ServiceManagementRequest extends its base class
  * to allow further policy-based customization of specific requests.
  */
template <typename POLICY>
class ServiceManagementRequest
    :   public ServiceManagementRequestBase {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ServiceManagementRequest<POLICY>> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are proxibited

    ServiceManagementRequest () = delete;
    ServiceManagementRequest (ServiceManagementRequest const&) = delete;
    ServiceManagementRequest & operator= (ServiceManagementRequest const&) = delete;

    /// Destructor
    ~ServiceManagementRequest () final {
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
     * @param onFinish         - an optional callback function to be called upon a completion of
     *                           the request.
     */
    static pointer create (ServiceProvider                 &serviceProvider,
                           boost::asio::io_service         &io_service,
                           const std::string               &worker,
                           callback_type                    onFinish) {

        return ServiceManagementRequest<POLICY>::pointer (
            new ServiceManagementRequest<POLICY> (
                serviceProvider,
                io_service,
                POLICY::requestTypeName(),
                worker,
                POLICY::requestType(),
                onFinish));
    }

private:

    /**
     * Construct the request
     */
    ServiceManagementRequest (ServiceProvider                                   &serviceProvider,
                              boost::asio::io_service                           &io_service,
                              const char                                        *requestTypeName,
                              const std::string                                 &worker,
                              lsst::qserv::proto::ReplicationServiceRequestType  requestType,
                              callback_type                                      onFinish)

        :   ServiceManagementRequestBase (serviceProvider,
                                          io_service,
                                          requestTypeName,
                                          worker,
                                          requestType),
            _onFinish (onFinish)
    {}

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void endProtocol () final {
        if (_onFinish != nullptr) {
            ServiceManagementRequest<POLICY>::pointer self = shared_from_base<ServiceManagementRequest<POLICY>>();
            _onFinish(self);
        }
    }

private:

    /// Registered callback to be called when the operation finishes
    callback_type _onFinish;
};

// Customizations for specific request types require dedicated policies

struct ServiceSuspendRequestPolicy {
    static const char* requestTypeName () {
        return "SERVICE_SUSPEND";
    } 
    static lsst::qserv::proto::ReplicationServiceRequestType requestType () {
        return lsst::qserv::proto::ReplicationServiceRequestType::SERVICE_SUSPEND;
    }
};
typedef ServiceManagementRequest<ServiceSuspendRequestPolicy> ServiceSuspendRequest;

struct ServiceResumeRequestPolicy {
    static const char* requestTypeName () {
        return "SERVICE_RESUME";
    }   
    static lsst::qserv::proto::ReplicationServiceRequestType requestType () {
        return lsst::qserv::proto::ReplicationServiceRequestType::SERVICE_RESUME;
    }
};
typedef ServiceManagementRequest<ServiceResumeRequestPolicy> ServiceResumeRequest;

struct ServiceStatusRequestPolicy {
    static const char* requestTypeName () {
        return "SERVICE_STATUS";
    } 
    static lsst::qserv::proto::ReplicationServiceRequestType requestType () {
        return lsst::qserv::proto::ReplicationServiceRequestType::SERVICE_STATUS;
    }
};
typedef ServiceManagementRequest<ServiceStatusRequestPolicy> ServiceStatusRequest;

struct ServiceRequestsRequestPolicy {
    static const char* requestTypeName () {
        return "SERVICE_REQUESTS";
    } 
    static lsst::qserv::proto::ReplicationServiceRequestType requestType () {
        return lsst::qserv::proto::ReplicationServiceRequestType::SERVICE_REQUESTS;
    }
};
typedef ServiceManagementRequest<ServiceRequestsRequestPolicy> ServiceRequestsRequest;


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_SERVICEMANAGEMENTREQUEST_H