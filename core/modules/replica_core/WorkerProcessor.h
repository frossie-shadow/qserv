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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H
#define LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H

/// WorkerProcessor.h declares:
///
/// class WorkerProcessor
/// (see individual class documentation for more information)

// System headers

#include <chrono>
#include <queue>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <mutex>
#include <thread>
#include <vector>

// Qserv headers

#include "proto/replication.pb.h"

#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessorThread.h"
#include "replica_core/WorkerReplicationRequest.h"

namespace proto = lsst::qserv::proto;

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class WorkerProcessor is a front-end interface for processing
  * requests fro connected clients.
  */
class WorkerProcessor
    : public std::enable_shared_from_this<WorkerProcessor> {

public:

    // The thread-based processor class is allowed to access the internal API
    friend class WorkerProcessorThread;

    /// The pointer type for self
    typedef std::shared_ptr<WorkerProcessor> pointer;

    /// The priority queue for pointers
    typedef std::priority_queue<WorkerReplicationRequest::pointer,
                                std::vector<WorkerReplicationRequest::pointer>,
                                WorkerReplicationRequestCompare> QueueType;

    /// Current state of the request processing engine
    enum State {
        STATE_IS_STOPPED,    // not started
        STATE_IS_RUNNING,    // all threads are running
        STATE_IS_STOPPING    // stopping all threads
    };

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (ServiceProvider::pointer serviceProvider);

    // Default construction and copy semantics are proxibited

    WorkerProcessor () = delete;
    WorkerProcessor (WorkerProcessor const&) = delete;
    WorkerProcessor & operator= (WorkerProcessor const&) = delete;

    /// Destructor
    virtual ~WorkerProcessor ();

    /// @return the state of the processor
    State state () const { return _state; }

    /// Begin processing requests
    void run ();

    /// Stop processing all requests, and stop all threads
    void stop ();

    /**
     * Enqueue the replication request for processing
     *
     * @param request  - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void enqueueForReplication (const proto::ReplicationRequestReplicate &request,
                                proto::ReplicationResponseReplicate      &response);

    /**
     * Dequeue replication request
     *
     * If the request is not being processed yet then it wil be simply removed
     * from the ready-to-be-processed queue. If it's being processed an attempt
     * to cancel processing will be made. If it has already processed this will
     * be reported.
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void dequeueOrCancel (const proto::ReplicationRequestStop &request,
                          proto::ReplicationResponseStop      &response);

    /**
     * Return the status of an on-going replication request
     *
     * @param request - the protobuf object received from a client
     * @param response - the protobuf object to be initialized and sent back to the client
     */
    void checkStatus (const proto::ReplicationRequestStatus &request,
                      proto::ReplicationResponseStatus      &response);

private:

    /**
     * The constructor of the class.
     */
    explicit WorkerProcessor (ServiceProvider::pointer serviceProvider);

    /**
     * Return the next replication request which is ready to be pocessed
     * and if then one found assign it to the specified thread. The request
     * will be removed from the ready-to-be-processed queue.
     * 
     * If the one is available witin the specified timeout then such request
     * will be moved into the in-progress queue, assigned to the processor thread
     * and returned to a caller. Otherwise an empty pointer (pointing to nullptr)
     * will be returned.
     *
     * This method is supposed to be called by one of the processing threads
     * when it becomes available.
     * 
     * ATTENTION: this method will block for a duration of time not exceeding
     * the client-specified timeout.
     */
    WorkerReplicationRequest::pointer fetchNextForProcessing (
            const WorkerProcessorThread::pointer &processorThread,
            std::chrono::milliseconds             timeoutMilliseconds);

    /**
     * Report a decision not to process a request
     *
     * This method ia supposed to be called by one of the processing threads
     * after it fetches the next ready-to-process request and then decided
     * not to proceed with processing. Normally this should happen when
     * the thread was asked to stop. In that case the request will be put
     * back into the ready-to-be processed request and be picked up later
     * by some other thread.
     */
    void processingRefused (const WorkerReplicationRequest::pointer &request);

    /**
     * Report the cancellation of the request processing
     *
     * This method ia supposed to be called by one of the processing threads
     * when it was asked to cancel processing a specific request. Requests which
     * are reported through this mechanism will be removed from any queues.
     */
    void processingCancelled (const WorkerReplicationRequest::pointer &request);

    /**
     * Report a request which has been processed.
     *
     * The metghod is called by a thread which was processing th erequest.
     * The request wil be moved into the corresponding queue. The completion
     * status of the operation will be stored witin the request.
     */
    void processingFinished (const WorkerReplicationRequest::pointer    &request,
                             WorkerReplicationRequest::CompletionStatus  completionStatus);

    /**
     * For threads reporting their completion
     *
     * This method is used by threads to report a change in their state.
     * It's meant to be used during the gradual and asynchronous state transition
     * of this processor from the combined State::STATE_IS_STOPPING to
     * State::STATE_IS_STOPPED. The later is achieved when all threads are stopped.
     */
    void processorThreadStopped (const WorkerProcessorThread::pointer &processorThread);

private:

    /// Services used by the processor
    ServiceProvider::pointer _serviceProvider;

    /// Current state of the processor
    State _state;

    /// A pool of threads for processing requests
    std::vector<WorkerProcessorThread::pointer> _threads;
    
    /// Mutex guarding the queues
    std::mutex _mtx;

    /// New unprocessed requests
    QueueType _newRequests;

    /// Requests which are being processed
    QueueType _inProgressRequests;

    /// Completed (succeeded or otherwise) requests
    QueueType _finishedRequests;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERPROCESSOR_H