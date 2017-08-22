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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H
#define LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H

/// WorkerRequest.h declares:
///
/// class WorkerRequest
/// (see individual class documentation for more information)

// System headers

#include <exception>    // std::exception
#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>

// Qserv headers

#include "replica_core/Performance.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


// Forward declarations
class ServiceProvider;

/// Exception thrown when a replication request is cancelled
struct WorkerRequestCancelled
    :   std::exception {

    /// Return the short description of the exception
    virtual const char* what () const noexcept {
        return "cancelled";
    }
};

/**
  * Class WorkerRequest is the base class for a family of the worker-side
  * requsts which require non-deterministic interactions with the server's
  * environment (network, disk I/O, etc.). Generally speaking, all requests
  * which can't be implemented instanteneously fall into this category.
  */
class WorkerRequest
    :   public std::enable_shared_from_this<WorkerRequest> {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerRequest> pointer;

    /// Completion status of the request processing operation
    enum CompletionStatus {
        STATUS_NONE,           /// no processing has been attempted
        STATUS_IN_PROGRESS,
        STATUS_IS_CANCELLING,
        STATUS_CANCELLED,
        STATUS_SUCCEEDED,
        STATUS_FAILED
    };

    /// Return the string representation of the status
    static std::string status2string (CompletionStatus status);

    // Default construction and copy semantics are proxibited

    WorkerRequest () = delete;
    WorkerRequest (WorkerRequest const&) = delete;
    WorkerRequest & operator= (WorkerRequest const&) = delete;

    /// Destructor
    virtual ~WorkerRequest ();

    // Trivial accessors

    ServiceProvider& serviceProvider () { return _serviceProvider; }

    const std::string& type () const { return _type; }
    const std::string& id   () const { return _id; }

    int priority () const { return _priority; }

    CompletionStatus  status () const { return _status; }

    /// Return the performance info
    const WorkerPerformance& performance () const { return _performance; }

    /** Set the status
     *
     * ATTENTION: this method needs to be called witin a thread-safe context
     * when moving requests between different queues.
     */
    void setStatus (CompletionStatus status);

    /**
     * This method should be invoked (repeatedly) to execute the request until
     * it returns 'true' or throws an exception. Note that returning 'true'
     * may mean both success or failure, depeniding on the completion status
     * of the request.
     *
     * The default (and preferred) mode of operation (see parameter 'incremental') is
     * to let the method do its work in progressive steps returning 'false' after
     * each increment while the work is still being done. This prevents a calling thread
     * from being blocked for the whole duration of the request execution and be gracefully
     * stopped if needed.
     *
     * The method will throw custom exception WorkerRequestCancelled when
     * it detects a cancellation request.
     *
     * The default implementation of the method will do nothing, just simulate
     * processing. This can be serve as a foundation for various tests
     * of this framework.
     * 
     * @param incremental - setting it to 'false' will disable the incremental mode
     */
    virtual bool execute (bool incremental=true);

    /**
     * Cancel execution of the request.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     * 
     *   STATUS_NONE or STATUS_CANCELLED             - transition to state STATUS_CANCELLED
     *   STATUS_IN_PROGRESS or STATUS_IS_CANCELLING  - transition to state STATUS_IS_CANCELLING
     *   other                                       - throwing std::logic_error

     */
    virtual void cancel ();

    /**
     * Roll back the request into its initial state and cleanup partial results
     * if possible.
     *
     * The effect of the operation varies depending on the current state of
     * the request. The default (the base class's implementation) assumes
     * the following transitions:
     * 
     *   STATUS_NONE or STATUS_IN_PROGRESS - transition to STATUS_NONE
     *   STATUS_IS_CANCELLING              - transition to STATUS_CANCELLED and throwing WorkerRequestCancelled
     *   other                             - throwing std::logic_error
     */
    virtual void rollback ();

    /// Return the context string
    std::string context () const {
        return id() + "  " + type() + "  " + status2string(status()) + "  ";
    }

protected:

    /**
     * The normal constructor of the class.
     */
    WorkerRequest (ServiceProvider   &serviceProvider,
                   const std::string &worker,
                   const std::string &type,
                   const std::string &id,
                   int                priority);

protected:

    ServiceProvider  &_serviceProvider;

    std::string _worker;
    std::string _type;
    std::string _id;

    int _priority;
    
    CompletionStatus _status;

    /// Performance counters
    WorkerPerformance _performance;

    /// The number of milliseconds since the beginning of the request processing.
    /// This members is used by the default implementation of method execute()
    /// to simulate request processing.
    unsigned int _durationMillisec;
};


/// Comparision type for strict weak ordering reaquired by std::priority_queue
struct WorkerRequestCompare {

    /// Order requests by their priorities
    bool operator() (const WorkerRequest::pointer& lhs,
                     const WorkerRequest::pointer& rhs) const {

        return lhs->priority() < rhs->priority();
    }
};


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERREQUEST_H