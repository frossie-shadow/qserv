// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_CORE_CONTROLLER_H
#define LSST_QSERV_REPLICA_CORE_CONTROLLER_H

/// Controller.h declares:
///
/// class Controller
/// (see individual class documentation for more information)

// System headers

#include <boost/asio.hpp>

#include <map>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>        // std::mutex, std::lock_guard
#include <thread>
#include <vector>

// Qserv headers

#include "replica_core/Request.h"
#include "replica_core/RequestTypesFwd.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class ControllerImpl;
class ServiceProvider;
class RequestWrapper;

/**
  * Class Controller is used for pushing replication (etc.) requests
  * to the worker replication services. Only one instance of this class is
  * allowed per a thread.
  *
  * NOTES:
  *
  * - all methods launching, stopping or checking status of requests
  *   require that the server was runinng. Otherwise it will throw
  *   std::runtime_error. The current implementation of the server
  *   doesn't support (yet?) amn operation queuing mechanism.
  *
  * - methods which take worker names as parameters will throw exception
  *   std::invalid_argument if the specified worker names were not found
  *   in the configuration.
  */
class Controller
    :   public std::enable_shared_from_this<Controller>  {

public:

    /// Friend class behind this implementation must have access to
    /// the private methods
    friend class ControllerImpl;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Controller> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, other services
     */
    static pointer create (ServiceProvider &serviceProvider);

    // Default construction and copy semantics are proxibited

    Controller () = delete;
    Controller (Controller const&) = delete;
    Controller & operator= (Controller const&) = delete;
 
    /// Destructor
    virtual ~Controller ();

    /// Return the Service Provider used by the server
    ServiceProvider& serviceProvider () { return _serviceProvider; }

    /**
     * Run the server in a dedicated thread unless it's already running.
     * It's safe to call this method multiple times from any thread.
     */
    void run ();

    /**
     * Check if the service is running.
     *
     * @return true if the service is running.
     */
    bool isRunning () const;

    /**
     * Stop the server. This method will guarantee that all outstanding
     * opeations will finish and not aborted.
     *
     * This operation will also result in stopping the internal thread
     * in which the server is being run.
     */
    void stop ();

    /**
     * Join with a thread in which the service is being run (if any).
     * If the service was not started or if it's stopped the the method
     * will return immediattely.
     *
     * This method is meant to be used for integration of the service into
     * a larger multi-threaded application which may require a proper
     * synchronization between threads.
     */
    void join ();

    /**
     * Initiate a new replication request.
     *
     * The method will throw exception std::invalid_argument if the worker
     * names are equal.
     *
     * @param workerName            - the name of a worker node from which to copy the chunk
     * @param sourceWorkerName      - the name of a worker node where the replica will be created
     * @param database              - database name
     * @param chunk                 - the chunk number
     * @param onFinish              - an optional callback function to be called upon
     *                                the completion of the request
     *
     * @return a pointer to the replication request
     */
    ReplicationRequest_pointer replicate (const std::string                &workerName,
                                          const std::string                &sourceWorkerName,
                                          const std::string                &database,
                                          unsigned int                      chunk,
                                          ReplicationRequest_callback_type  onFinish=nullptr);

    /**
     * Initiate a new replica deletion request.
     *
     * @param workerName - the name of a worker node where the replica will be deleted
     * @param database   - database name
     * @param chunk      - the chunk number
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    DeleteRequest_pointer deleteReplica (const std::string           &workerName,
                                         const std::string           &database,
                                         unsigned int                 chunk,
                                         DeleteRequest_callback_type  onFinish=nullptr);

    /**
     * Initiate a new replica lookup request.
     *
     * @param workerName - the name of a worker node where the replica is located
     * @param database   - database name
     * @param chunk      - the chunk number
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    FindRequest_pointer findReplica (const std::string         &workerName,
                                     const std::string         &database,
                                     unsigned int               chunk,
                                     FindRequest_callback_type  onFinish=nullptr);

    /**
     * Initiate a new replicas lookup request.
     *
     * @param workerName - the name of a worker node where the replicas are located
     * @param database   - database name
     * @param onFinish   - an optional callback function to be called upon
     *                     the completion of the request
     *
     * @return a pointer to the replication request
     */
    FindAllRequest_pointer findAllReplicas (const std::string            &workerName,
                                            const std::string            &database,
                                            FindAllRequest_callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopReplicationRequest_pointer stopReplication (const std::string                    &workerName,
                                                    const std::string                    &targetRequestId,
                                                    StopReplicationRequest_callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopDeleteRequest_pointer stopReplicaDelete (const std::string               &workerName,
                                                 const std::string               &targetRequestId,
                                                 StopDeleteRequest_callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopFindRequest_pointer stopReplicaFind (const std::string             &workerName,
                                             const std::string             &targetRequestId,
                                             StopFindRequest_callback_type  onFinish=nullptr);

    /**
     * Stop an outstanding replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be stopped
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the stop request
     */
    StopFindAllRequest_pointer stopReplicaFindAll (const std::string                &workerName,
                                                   const std::string                &targetRequestId,
                                                   StopFindAllRequest_callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding replication request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusReplicationRequest_pointer statusOfReplication (const std::string                      &workerName,
                                                          const std::string                      &targetRequestId,
                                                          StatusReplicationRequest_callback_type  onFinish=nullptr);
 
    /**
     * Check the on-going status of an outstanding replica deletion request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusDeleteRequest_pointer statusOfDelete (const std::string                 &workerName,
                                                const std::string                 &targetRequestId,
                                                StatusDeleteRequest_callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding replica lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusFindRequest_pointer statusOfFind (const std::string               &workerName,
                                            const std::string               &targetRequestId,
                                            StatusFindRequest_callback_type  onFinish=nullptr);

    /**
     * Check the on-going status of an outstanding (multiple) replicas lookup request.
     *
     * @param workerName      - the name of a worker node where the request was launched
     * @param targetRequestId - an identifier of a request to be inspected
     * @param onFinish        - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the status inquery request
     */
    StatusFindAllRequest_pointer statusOfFindAll (const std::string                  &workerName,
                                                  const std::string                  &targetRequestId,
                                                  StatusFindAllRequest_callback_type  onFinish=nullptr);

    /**
     * Tell the worker-side service to temporarily suspend processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceSuspendRequest_pointer suspendWorkerService (const std::string                   &workerName,
                                                        ServiceSuspendRequest_callback_type  onFinish=nullptr);

    /**
     * Tell the worker-side service to resume processing requests
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceResumeRequest_pointer resumeWorkerService (const std::string                  &workerName,
                                                      ServiceResumeRequest_callback_type  onFinish=nullptr);
    /**
     * Request the current status of the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceStatusRequest_pointer statusOfWorkerService (const std::string                  &workerName,
                                                        ServiceStatusRequest_callback_type  onFinish=nullptr);
                                                         
    /**
     * Request detailed info on which replication-related requests are known
     * to the worker-side service
     *
     * @param workerName - the name of a worker node where the service runs
     * @param onFinish   - a callback function to be called upon completion of the operation
     *
     * @return a pointer to the request
     */
    ServiceRequestsRequest_pointer requestsOfWorkerService (const std::string                    &workerName,
                                                            ServiceRequestsRequest_callback_type  onFinish=nullptr);
                                                         
    // Filters for active requests

    std::vector<ReplicationRequest_pointer>       activeReplicationRequests ();
    std::vector<DeleteRequest_pointer>            activeDeleteRequests ();
    std::vector<FindRequest_pointer>              activeFindRequests ();
    std::vector<FindAllRequest_pointer>           activeFindAllRequests ();

    std::vector<StopReplicationRequest_pointer>   activeStopReplicationRequests ();
    std::vector<StopDeleteRequest_pointer>        activeStopDeleteRequests ();
    std::vector<StopFindRequest_pointer>          activeStopFindRequests ();
    std::vector<StopFindAllRequest_pointer>       activeStopFindAllRequests ();

    std::vector<StatusReplicationRequest_pointer> activeStatusReplicationRequests ();
    std::vector<StatusDeleteRequest_pointer>      activeStatusDeleteRequests ();
    std::vector<StatusFindRequest_pointer>        activeStatusFindRequests ();
    std::vector<StatusFindAllRequest_pointer>     activeStatusFindAllRequests ();

    std::vector<ServiceSuspendRequest_pointer>    activeServiceSuspendRequests ();
    std::vector<ServiceResumeRequest_pointer>     activeServiceResumeRequests ();
    std::vector<ServiceStatusRequest_pointer>     activeServiceStatusRequests ();

    // Counters of active requests

    /// Total number of request sof all kinds
    size_t numActiveRequests ();

    size_t numActiveReplicationRequests ();
    size_t numActiveDeleteRequests ();
    size_t numActiveFindRequests ();
    size_t numActiveFindAllRequests ();

    size_t numActiveStopReplicationRequests ();
    size_t numActiveStopDeleteRequests ();
    size_t numActiveStopFindRequests ();
    size_t numActiveStopFindAllRequests ();

    size_t numActiveStatusReplicationRequests ();
    size_t numActiveStatusDeleteRequests ();
    size_t numActiveStatusFindRequests ();
    size_t numActiveStatusFindAllRequests ();

    size_t numActiveServiceSuspendRequests ();
    size_t numActiveServiceResumeRequests ();
    size_t numActiveServiceStatusRequests ();

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, other services
     */
    explicit Controller (ServiceProvider &serviceProvider);

    /**
     * Finalize the completion of the request. This method will notify
     * a requestor on the completion of the operation and it will also
     * remove the request from the server's registry.
     */
    void finish (const std::string &id);

    /**
     * Make sure the server is runnning. Otherwise throw std::runtime_error.
     */
    void assertIsRunning () const;

private:

    // Parameters of the object

    ServiceProvider &_serviceProvider;

    // Communication and synchronization context

    boost::asio::io_service _io_service;

    std::unique_ptr<boost::asio::io_service::work> _work;

    std::unique_ptr<std::thread> _thread;   // this thread will run the asynchronous prosessing
                                            // of the requests.

    std::mutex _requestPocessingMtx;        // for thread safety of the implementation of
                                            // this class's public API and internal operations.

    /// The registry of the on-going requests.
    std::map<std::string,std::shared_ptr<RequestWrapper>> _registry;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_CONTROLLER_H