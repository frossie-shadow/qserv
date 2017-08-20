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
#include "replica_core/Controller.h"

// System headers

#include <iostream>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/DeleteRequest.h"
#include "replica_core/FindRequest.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceManagementRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_requestPocessingMtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.Controller");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

//////////////////////////////////////////////////////////////////////
//////////////////////////  RequestWrapper  //////////////////////////
//////////////////////////////////////////////////////////////////////

/**
 * The base class for implementing a polymorphic collection of active requests.
 */
struct RequestWrapper {

    /// The pointer type for instances of the class
    typedef std::shared_ptr<RequestWrapper> pointer;

    /// Destructor
    virtual ~RequestWrapper() {}

    /// This method will be called upon a completion of a request
    /// to notify a subscriber on the event.
    virtual void notify ()=0;

    /// Return a pointer to the stored request object
    virtual std::shared_ptr<Request> request () const=0;
};


//////////////////////////////////////////////////////////////////////////
//////////////////////////  RequestWrapperImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////////

/**
 * Request-type specific wrappers
 */
template <class  T>
struct RequestWrapperImpl
    :   RequestWrapper {

    /// The implementation of the vurtual method defined in the base class
    virtual void notify () {
        if (_onFinish == nullptr) return;
        _onFinish(_request);
    }

    RequestWrapperImpl(const typename T::pointer &request,
                       typename T::callback_type  onFinish)

        :   RequestWrapper(),

            _request  (request),
            _onFinish (onFinish) {
    }

    /// Destructor
    virtual ~RequestWrapperImpl() {}

    virtual std::shared_ptr<Request> request () const { return _request; }

private:

    // The context of the operation
    
    typename T::pointer       _request;
    typename T::callback_type _onFinish;
};


//////////////////////////////////////////////////////////////////////
//////////////////////////  ControllerImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////


/**
 * The utiliy class implementing operations on behalf of certain
 * methods of class Controller.
 * 
 * THREAD SAFETY NOTE: Methods implemented witin the class are NOT thread-safe.
 *                     They must be called from the thread-safe code only.
 */
class ControllerImpl {

public:

    /// Default constructor
    ControllerImpl () {}

    // Default copy semantics is proxibited

    ControllerImpl (ControllerImpl const&) = delete;
    ControllerImpl & operator= (ControllerImpl const&) = delete;

    /// Destructor
    ~ControllerImpl () {}

    /**
     * Generic method for managing requests such as stopping an outstanding
     * request or inquering a status of a request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be affected
     * @param onFinish        - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer requestManagementOperation (
            Controller::pointer                   controller, 
            const std::string                    &workerName,
            const std::string                    &targetRequestId,
            typename REQUEST_TYPE::callback_type  onFinish,
            bool                                  keepTracking) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                controller->_serviceProvider,
                controller->_io_service,
                workerName,
                targetRequestId,
                [controller] (typename REQUEST_TYPE::pointer request) {
                    controller->finish(request->id());
                },
                keepTracking
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
    
        (controller->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);  
    
        // Initiate the request

        request->start ();

        return request;
    }

   /**
     * Generic method for launching worker service management requests such as suspending,
     * resyming or inspecting a status of the worker-side replication service.
     *
     * @param workerName - the name of a worker node where the service is run
     * @param onFinish   - a callback function to be called upon completion of the operation
     */
    template <class REQUEST_TYPE>
    static
    typename REQUEST_TYPE::pointer serviceManagementOperation (
            Controller::pointer                   controller, 
            const std::string                    &workerName,
            typename REQUEST_TYPE::callback_type  onFinish) {

        controller->assertIsRunning();

        typename REQUEST_TYPE::pointer request =
            REQUEST_TYPE::create (
                controller->_serviceProvider,
                controller->_io_service,
                workerName,
                [controller] (typename REQUEST_TYPE::pointer request) {
                    controller->finish(request->id());
                }
            );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.

        (controller->_registry)[request->id()] =
            std::make_shared<RequestWrapperImpl<REQUEST_TYPE>> (request, onFinish);

        // Initiate the request

        request->start ();

        return request;
    }

    /**
     * Return a collection of requests filtered by type.
     */
    template <class REQUEST_TYPE>
    static
    std::vector<typename REQUEST_TYPE::pointer> requestsByType (Controller::pointer controller) {
    
        std::vector<typename REQUEST_TYPE::pointer> result;
    
        for (auto itr : controller->_registry) {
        
            typename REQUEST_TYPE::pointer request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request());
    
            if (request) result.push_back(request);
        }
        return result;
    }
    
    /**
     * Return the number of of requests filtered by type.
     */
    template <class REQUEST_TYPE>
    static
    size_t numRequestsByType (Controller::pointer controller) {
    
        size_t result{0};
    
        for (auto itr : controller->_registry) {
        
            typename REQUEST_TYPE::pointer request =
                std::dynamic_pointer_cast<REQUEST_TYPE>(itr.second->request());

            if (request) ++result;
        }
        return result;
    }
};


//////////////////////////////////////////////////////////////////
//////////////////////////  Controller  //////////////////////////
//////////////////////////////////////////////////////////////////


Controller::pointer
Controller::create (ServiceProvider &serviceProvider) {
    return Controller::pointer (
        new Controller(serviceProvider));
}

Controller::Controller (ServiceProvider &serviceProvider)
    :   _serviceProvider (serviceProvider),
        _io_service      (),
        _work     (nullptr),
        _thread   (nullptr),
        _registry () {
}

Controller::~Controller () {
}

void
Controller::run () {

    LOCK_GUARD;

    if (!isRunning()) {

        Controller::pointer controller = shared_from_this();
     
        _work.reset (
            new boost::asio::io_service::work(_io_service)
        );
        _thread.reset (
            new std::thread (
                [controller] () {
        
                    // This will prevent the I/O service from existing the .run()
                    // method event when it will run out of any requess to process.
                    // Unless the service will be explicitly stopped.
    
                    controller->_io_service.run();
                    
                    // Always reset the object in a preparation for its further
                    // usage.
    
                    controller->_io_service.reset();
                }
            )
        );
    }
}

bool
Controller::isRunning () const {
    return _thread.get() != nullptr;
}

void
Controller::stop () {

    if (!isRunning()) return;

    // IMPORTANT:
    //
    //   Never attempt running these operations within LOCK_GUARD
    //   due to a possibile deadlock when asynchronous handlers will be
    //   calling the thread-safe methods. A problem is that until they finish
    //   in a clean way (as per the _work.reset()) the thread will never finish,
    //   and the application will hang on _thread->join().

    // LOCK_GUARD  (disabled)

    // Destoring this object will let the I/O service to (eventually) finish
    // all on-going work and shut down the thread. In that case there is no need
    // to stop the service explicitly (which is not a good idea anyway because
    // there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictanle state.)

    _work.reset();

    // Join with the thread before clearning up the pointer

    _thread->join();

    _thread.reset(nullptr);
    
    // Double check that the collection of requests is empty.
    
    if (!_registry.empty())
        throw std::logic_error ("Controller::stop() the collection of outstanding requests is not empty");
}

void
Controller::join () {
    if (_thread) _thread->join();
}

ReplicationRequest::pointer
Controller::replicate (const std::string                 &workerName,
                       const std::string                 &sourceWorkerName,
                       const std::string                 &database,
                       unsigned int                      chunk,
                       ReplicationRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    ReplicationRequest::pointer request =
        ReplicationRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            sourceWorkerName,
            database,
            chunk,
            [controller] (ReplicationRequest::pointer request) {
                controller->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<ReplicationRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

DeleteRequest::pointer
Controller::deleteReplica (const std::string            &workerName,
                           const std::string            &database,
                           unsigned int                  chunk,
                           DeleteRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    DeleteRequest::pointer request =
        DeleteRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (DeleteRequest::pointer request) {
                controller->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<DeleteRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

FindRequest::pointer
Controller::findReplica (const std::string          &workerName,
                         const std::string          &database,
                         unsigned int                chunk,
                         FindRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    FindRequest::pointer request =
        FindRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            chunk,
            [controller] (FindRequest::pointer request) {
                controller->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

FindAllRequest::pointer
Controller::findAllReplicas (const std::string             &workerName,
                             const std::string             &database,
                             FindAllRequest::callback_type  onFinish) {
    LOCK_GUARD;

    assertIsRunning();

    Controller::pointer controller = shared_from_this();

    FindAllRequest::pointer request =
        FindAllRequest::create (
            _serviceProvider,
            _io_service,
            workerName,
            database,
            [controller] (FindAllRequest::pointer request) {
                controller->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.

    _registry[request->id()] =
        std::make_shared<RequestWrapperImpl<FindAllRequest>> (request, onFinish);

    // Initiate the request

    request->start ();

    return request;
}

StopReplicationRequest::pointer
Controller::stopReplication (const std::string                     &workerName,
                             const std::string                     &targetRequestId,
                             StopReplicationRequest::callback_type  onFinish,
                             bool                                   keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplication  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopReplicationRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StopDeleteRequest::pointer
Controller::stopReplicaDelete (const std::string                &workerName,
                               const std::string                &targetRequestId,
                               StopDeleteRequest::callback_type  onFinish,
                               bool                              keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaDelete  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopDeleteRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StopFindRequest::pointer
Controller::stopReplicaFind (const std::string              &workerName,
                             const std::string              &targetRequestId,
                             StopFindRequest::callback_type  onFinish,
                             bool                            keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFind  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopFindRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StopFindAllRequest::pointer
Controller::stopReplicaFindAll (const std::string                 &workerName,
                                const std::string                 &targetRequestId,
                                StopFindAllRequest::callback_type  onFinish,
                                bool                               keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "stopReplicaFindAll  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StopFindAllRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StatusReplicationRequest::pointer
Controller::statusOfReplication (const std::string                       &workerName,
                                 const std::string                       &targetRequestId,
                                 StatusReplicationRequest::callback_type  onFinish,
                                 bool                                     keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfReplication  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusReplicationRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StatusDeleteRequest::pointer
Controller::statusOfDelete (const std::string                  &workerName,
                            const std::string                  &targetRequestId,
                            StatusDeleteRequest::callback_type  onFinish,
                            bool                                keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfDelete  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusDeleteRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StatusFindRequest::pointer
Controller::statusOfFind (const std::string                &workerName,
                          const std::string                &targetRequestId,
                          StatusFindRequest::callback_type  onFinish,
                          bool                              keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFind  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusFindRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

StatusFindAllRequest::pointer
Controller::statusOfFindAll (const std::string                   &workerName,
                             const std::string                   &targetRequestId,
                             StatusFindAllRequest::callback_type  onFinish,
                             bool                                 keepTracking) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfFindAll  targetRequestId = " << targetRequestId);

    return ControllerImpl::requestManagementOperation<StatusFindAllRequest> (
        shared_from_this(),
        workerName,
        targetRequestId,
        onFinish,
        keepTracking);
}

ServiceSuspendRequest::pointer
Controller::suspendWorkerService (const std::string                    &workerName,
                                  ServiceSuspendRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "suspendWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceSuspendRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}

ServiceResumeRequest::pointer
Controller::resumeWorkerService (const std::string                   &workerName,
                                 ServiceResumeRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "resumeWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceResumeRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}

ServiceStatusRequest::pointer
Controller::statusOfWorkerService (const std::string                   &workerName,
                                   ServiceStatusRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceStatusRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}

ServiceRequestsRequest::pointer
Controller::requestsOfWorkerService (const std::string                     &workerName,
                                     ServiceRequestsRequest::callback_type  onFinish) {
    LOCK_GUARD;

    LOGS(_log, LOG_LVL_DEBUG, "statusOfWorkerService  workerName: " << workerName);

    return ControllerImpl::serviceManagementOperation<ServiceRequestsRequest> (
        shared_from_this(),
        workerName,
        onFinish);
}


std::vector<ReplicationRequest::pointer>
Controller::activeReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<ReplicationRequest>(shared_from_this());
}

std::vector<DeleteRequest::pointer>
Controller::activeDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<DeleteRequest>(shared_from_this());
}
std::vector<FindRequest::pointer>
Controller::activeFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<FindRequest>(shared_from_this());
}

std::vector<FindAllRequest::pointer>
Controller::activeFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<FindAllRequest>(shared_from_this());
}

std::vector<StopReplicationRequest::pointer>
Controller::activeStopReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StopReplicationRequest>(shared_from_this());       
}

std::vector<StopDeleteRequest::pointer>
Controller::activeStopDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StopDeleteRequest>(shared_from_this());       
}

std::vector<StopFindRequest::pointer>
Controller::activeStopFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StopFindRequest>(shared_from_this());       
}

std::vector<StopFindAllRequest::pointer>
Controller::activeStopFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StopFindAllRequest>(shared_from_this());       
}

std::vector<StatusReplicationRequest::pointer>
Controller::activeStatusReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StatusReplicationRequest>(shared_from_this());
}

std::vector<StatusDeleteRequest::pointer>
Controller::activeStatusDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StatusDeleteRequest>(shared_from_this());
}

std::vector<StatusFindRequest::pointer>
Controller::activeStatusFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StatusFindRequest>(shared_from_this());
}

std::vector<StatusFindAllRequest::pointer>
Controller::activeStatusFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<StatusFindAllRequest>(shared_from_this());
}

std::vector<ServiceSuspendRequest::pointer>
Controller::activeServiceSuspendRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<ServiceSuspendRequest>(shared_from_this());
}


std::vector<ServiceResumeRequest::pointer>
Controller::activeServiceResumeRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<ServiceResumeRequest>(shared_from_this());
}


std::vector<ServiceStatusRequest::pointer>
Controller::activeServiceStatusRequests () {
    LOCK_GUARD;
    return ControllerImpl::requestsByType<ServiceStatusRequest>(shared_from_this());
}

size_t
Controller::numActiveRequests () {
    LOCK_GUARD;
    return _registry.size();
}


size_t
Controller::numActiveReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<ReplicationRequest>(shared_from_this());
}


size_t
Controller::numActiveDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<DeleteRequest>(shared_from_this());
}

size_t
Controller::numActiveFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<FindRequest>(shared_from_this());
}

size_t
Controller::numActiveFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<FindAllRequest>(shared_from_this());
}

size_t
Controller::numActiveStopReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StopReplicationRequest>(shared_from_this());
}

size_t
Controller::numActiveStopDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StopDeleteRequest>(shared_from_this());
}

size_t
Controller::numActiveStopFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StopFindRequest>(shared_from_this());
}

size_t
Controller::numActiveStopFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StopFindAllRequest>(shared_from_this());
}

size_t
Controller::numActiveStatusReplicationRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StatusReplicationRequest>(shared_from_this());
}

size_t
Controller::numActiveStatusDeleteRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StatusDeleteRequest>(shared_from_this());
}

size_t
Controller::numActiveStatusFindRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StatusFindRequest>(shared_from_this());
}

size_t
Controller::numActiveStatusFindAllRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<StatusFindAllRequest>(shared_from_this());
}

size_t
Controller::numActiveServiceSuspendRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<ServiceSuspendRequest>(shared_from_this());
}

size_t
Controller::numActiveServiceResumeRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<ServiceResumeRequest>(shared_from_this());
}

size_t
Controller::numActiveServiceStatusRequests () {
    LOCK_GUARD;
    return ControllerImpl::numRequestsByType<ServiceStatusRequest>(shared_from_this());
}

void
Controller::finish (const std::string &id) {

    // IMPORTANT:
    //
    //   Make sure the notification is complete before removing
    //   the request from the registry. This has two reasons:
    //
    //   - it will avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the controller.
    //
    //   - it will reduce the controller API dead-time due to a prolonged
    //     execution time of of the callback function.

    RequestWrapper::pointer request;
    {
        LOCK_GUARD;
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}

void
Controller::assertIsRunning () const {
    if (!isRunning())
        throw std::runtime_error("the replication service is not running");
}

}}} // namespace lsst::qserv::replica_core
