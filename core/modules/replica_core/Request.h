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
#ifndef LSST_QSERV_REPLICA_CORE_REQUEST_H
#define LSST_QSERV_REPLICA_CORE_REQUEST_H

/// Request.h declares:
///
/// class Request
/// (see individual class documentation for more information)

// System headers

#include <chrono>
#include <memory>       // shared_ptr, enable_shared_from_this
#include <ostream>
#include <string>

#include <boost/asio.hpp>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

class ProtocolBuffer;
class ServiceProvider;
class WorkerInfo;

/**
  * Class Request is a base class for a family of requests within
  * the master server.
  */
class Request
    :   public std::enable_shared_from_this<Request>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Request> pointer;

    /// Primary public state of the request
    enum State {

        /// The request has been constructed, and no attempt to execute it has
        /// been made.
        CREATED,
        
        /// The request is in a progress
        IN_PROGRESS,
        
        /// The request is finihed. See extended status for more details
        /// (the completion status, etc.)
        FINISHED
    };

    /// Return the string representation of the primary state
    static std::string state2string (State state) ;

    /// Refined public sub-state of the requiest once it's FINISHED as per
    /// the above defined primary state.
    enum ExtendedState {

        /// No extended state exists at this time        
        NONE,

        /// The request has been fully implemented
        SUCCESS,
        
        /// The request could not be implemented due to an unrecoverable
        /// cliend-side error.
        CLIENT_ERROR,

        /// Server reports that the request can not be implemented due to incorrect parameters, etc.
        SERVER_BAD,

        /// The request could not be implemented due to an unrecoverable
        /// server-side error.
        SERVER_ERROR,

        /// The request is queued for processing by the server
        SERVER_QUEUED,

        /// The request is being processed by the server
        SERVER_IN_PROGRESS,

        /// The request is being cancelled by the server
        SERVER_IS_CANCELLING,

        /// The request is found as cancelled on the server
        SERVER_CANCELLED,

        /// Expired due to a timeout (as per the Configuration)
        EXPIRED,
        
        /// Explicitly cancelled on the client-side (similar to EXPIRED)
        CANCELLED
    };

    /// Return the string representation of the extended state
    static std::string state2string (ExtendedState state) ;

    /// Return the string representation of the compbined state
    static std::string state2string (State state, ExtendedState extendedState) {
        return state2string(state) + "::" +state2string(extendedState);
    }

    /**
     * Performance counters of a request to track its execution over time
     *
     * All time counters are expressed in milliseconds since Epoch.
     * Undefined values are set to 0.
     */
    class Performance {

    public:

        /// Return the current time in milliseconds since Epoch
        static uint64_t now ();

        /**
         * The default constructor
         *
         * All (but the request creation one) timestamps will be initialized wih 0.
         */
        Performance ();
        
        /// Copy c-tor
        Performance (const Performance &p);

        /// Assigned operator
        Performance & operator= (const Performance &p);  

        /// Destructor
        virtual ~Performance ();

    private:

        /// Set internal state from the one of another object
        void setFrom (const Performance &p);

    public:

        /// Created by the Controller
        uint64_t c_create_time;

        /// Started by the Controller
        uint64_t c_start_time;

        /// Received by a worker service
        uint64_t w_receive_time;

        /// Execution started by a worker service
        uint64_t w_start_time;

        /// Execution fiished by a worker service
        uint64_t w_finish_time;

        /// A subscriber notified by the Controller
        uint64_t c_finish_time;
    };

    // Default construction and copy semantics are proxibited

    Request () = delete;
    Request (Request const&) = delete;
    Request & operator= (Request const&) = delete;

    /// Destructor
    virtual ~Request ();

    /// Return a reference to the service provider,
    ServiceProvider& serviceProvider() { return _serviceProvider; }

    /// Return a string representing a type of a request.
    const std::string& type () const { return _type; }

    /// Return a unique identifier of the request
    const std::string& id () const { return _id; }

    /// Return the priority level of the request
    int priority () const { return _priority; }

    /// Return a unique identifier of the request
    const std::string& worker () const { return _worker; }

    /// Return the primary status of the request
    State state () const { return _state; }
    
    /// Return the extended state of the request when it finished.
    ExtendedState extendedState () const { return _extendedState; }

    /// Return the performance info
    const Performance& performance () const { return _performance; }

    /**
     * Reset the state (if needed) and begin processing the request.
     *
     * This is supposed to be the first operation to be called upon a creation
     * of the request.
     */
    void start ();

    /**
     * Explicitly cancel any asynchronous operation(s) and put the object into
     * the FINISHED::CANCELLED state. This operation is very similar to the
     * timeout-based request expiration, except it's requested explicitly.
     *
     * ATTENTION: this operation won't affect the remote (server-side) state
     * of the operation in case if the request was queued.
     */
    void cancel ();

    /// Return the context string for debugging and diagnostic printouts
    std::string context () const {
        return id() + "  " + type() + "  " + state2string(state(), extendedState()) + "  ";
    }

protected:

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base () {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /**
     * Generate a unique identifier of a request which can also be persisted.
     *
     * @return an identifier
     */
    static std::string generateId ();

    /**
     * Construct the request with the pointer to the services provider.
     *
     * @param serviceProvider - a provider of various services
     * @param type            - its type name (used informally for debugging)
     * @param worker          - the name of a worker
     * @io_service            - BOOST ASIO service
     * @priority              - may affect an execution order of the request by
     *                          the worker service. Higher number means higher
     *                          priority.
     */
    Request (ServiceProvider         &serviceProvider,
             boost::asio::io_service &io_service,
             const std::string       &type,
             const std::string       &worker,
             int                      priority=0);

    /**
     * Request expiration timer's handler. The expiration interval (if any)
     * is configured via the configuraton service. When the request expires
     * it finishes with completion status FINISHED::EXPIRED.
     */
    void expired (const boost::system::error_code &ec);

    /**
     * Restart the whole operation from scratch.
     *
     * Cancel any asynchronous operation(s) if not in the initial state
     * w/o notifying a subscriber.
     * 
     * NOTE: This method is called internally when there is a doubt that
     * it's possible to do a clean recovery from a failure.
     */
    void restart ();

    /// Start resolving the destination worker host & port
    void resolve ();

    /// Callback handler for the asynchronious operation
    void resolved (const boost::system::error_code &ec,
                   boost::asio::ip::tcp::resolver::iterator iter);

    /// Start resolving the destination worker host & port
    void connect (boost::asio::ip::tcp::resolver::iterator iter);

    /**
     * Callback handler for the asynchronious operation upon its
     * successfull completion will trigger a request-specific
     * protocol sequence.
     */
    void connected (const boost::system::error_code &ec,
                    boost::asio::ip::tcp::resolver::iterator iter);

    /// Start a timeout before attempting to restart the connection
    void waitBeforeRestart ();

    /// Callback handler fired for restarting the connection
    void awakenForRestart (const boost::system::error_code &ec);

    /**
     * Finalize request processing (as reported by subclasses)
     *
     * This is supposed to be the last operation to be called by subclasses
     * upon a completion of the request.
     */
    void finish (ExtendedState extendedState);

    /**
      * This method is supposed to be provided by subclasses to begin
      * an actual protocol as required by the subclass.
      */
    virtual void beginProtocol ()=0;

    /**
     * This method is supposed to be provided by subclasses to handle
     * request completion steps, such as notifying a party which initiated
     * the request, etc.
     */
    virtual void endProtocol ()=0;

    /**
     * Return 'true' if the operation was aborted.
     *
     * USAGE NOTES:
     *
     *    Nomally this method is supposed to be called as the first action
     *    witin asynchronous handlers to figure out if an on-going aynchronous
     *    operation was cancelled for some reason. Should this be the case
     *    the caller is supposed to quit right away. It will be up to a code
     *    which initiated the abort to take care of putting the object into
     *    a proper state.
     */
    bool isAborted (const boost::system::error_code &ec) const;

    /**
     * Ensure the object is in the deseride internal state. Throw an
     * exception otherwise.
     *
     * NOTES: normally this condition should never been seen unless
     *        there is a problem with the application implementation
     *        or the underlying run-time system.
     * 
     * @throws std::logic_error
     */
    void assertState (State desiredState) const;

    /**
     * Set the desired primary and extended state.
     *
     * The change of the state is done via a method to allow extra actions
     * at this step, such as:
     *
     * - reporting change state in a debug stream
     * - verifying the correctness of the state transition
     */
    void setState (State         state,
                   ExtendedState extendedStat);

protected:

    // Parameters of the object

    ServiceProvider &_serviceProvider;

    std::string _type;
    std::string _id;
    std::string _worker;

    int _priority;

    // Primary and extended states of the request

    State         _state;
    ExtendedState _extendedState;

    /// Performance counters
    Performance _performance;

    /// Buffers for data moved over the network
    std::auto_ptr<ProtocolBuffer> _bufferPtr;

    // To be initialized from a configuration via the ServiceProvider
    // then cached for the lifespan of the object.

    std::shared_ptr<WorkerInfo> _workerInfoPtr;

    unsigned int _timerIvalSec;

    // Mutable state for network communication

    boost::asio::ip::tcp::resolver _resolver;
    boost::asio::ip::tcp::socket   _socket;
    boost::asio::deadline_timer    _timer;

    // This timer is used (if configured) to limit the total run time
    // of a request. The timer (re-)starts each time method send() is called.
    // Upon a suvcessfull expiration of the timer the request would finish
    // with status: FINISHED::EXPIRED.
    
    unsigned int  _requestExpirationIvalSec;        // initialized from a configuration

    boost::asio::deadline_timer _requestExpirationTimer;
};

/// Overloaded streaming operator for class Request::Performance
std::ostream& operator<< (std::ostream& os, const Request::Performance &p);

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REQUEST_H