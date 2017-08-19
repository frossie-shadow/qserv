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
#include "replica_core/Request.h"

// System headers

#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "boost/uuid/uuid.hpp"
#include "boost/uuid/uuid_generators.hpp"
#include "boost/uuid/uuid_io.hpp"

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ProtocolBuffer.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerInfo.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.Request");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


std::string
Request::state2string (State state) {
    switch (state) {
        case CREATED:     return "CREATED";
        case IN_PROGRESS: return "IN_PROGRESS";
        case FINISHED:    return "FINISHED";
    }
    throw std::logic_error("incomplete implementation of method Request::state2string(State)");
}

std::string
Request::state2string (ExtendedState state) {
    switch (state) {
        case NONE:                 return "NONE";
        case SUCCESS:              return "SUCCESS";
        case CLIENT_ERROR:         return "CLIENT_ERROR";
        case SERVER_BAD:           return "SERVER_BAD";
        case SERVER_ERROR:         return "SERVER_ERROR";
        case SERVER_QUEUED:        return "SERVER_QUEUED";
        case SERVER_IN_PROGRESS:   return "SERVER_IN_PROGRESS";
        case SERVER_IS_CANCELLING: return "SERVER_IS_CANCELLING";
        case SERVER_CANCELLED:     return "SERVER_CANCELLED";
        case EXPIRED:              return "EXPIRED";
        case CANCELLED:            return "CANCELLED";
    }
    throw std::logic_error("incomplete implementation of method Request::state2string(ExtendedState)");
}

std::string
Request::generateId () {
    boost::uuids::uuid id = boost::uuids::random_generator()();
    return boost::uuids::to_string(id);
}

Request::Request (ServiceProvider         &serviceProvider,
                  boost::asio::io_service &io_service,
                  const std::string       &type,
                  const std::string       &worker,
                  int                      priority)

    :   _serviceProvider (serviceProvider),

        _type   (type),
        _id     (generateId()),
        _worker (worker),

        _priority (priority),

        _state         (CREATED),
        _extendedState (NONE),
        _performance   (),

        _bufferPtr     (new ProtocolBuffer(serviceProvider.config().requestBufferSizeBytes())),
        _workerInfoPtr (serviceProvider.workerInfo(worker)),
        _timerIvalSec  (serviceProvider.config().defaultRetryTimeoutSec()),

        _resolver (io_service),
        _socket   (io_service),
        _timer    (io_service),

        _requestExpirationIvalSec (serviceProvider.config().controllerRequestTimeoutSec()),
        _requestExpirationTimer   (io_service) {

        _serviceProvider.assertWorkerIsValid(worker);
}

Request::~Request () { 
}

void
Request::start () {

    assertState(CREATED);

    LOGS(_log, LOG_LVL_DEBUG, context() << "start  _requestExpirationIvalSec: " << _requestExpirationIvalSec);

    _performance.setUpdateStart();

    if (_requestExpirationIvalSec) {
        _requestExpirationTimer.cancel();
        _requestExpirationTimer.expires_from_now(boost::posix_time::seconds(_requestExpirationIvalSec));
        _requestExpirationTimer.async_wait (
            boost::bind (
                &Request::expired,
                shared_from_this(),
                boost::asio::placeholders::error
            )
        );
    }
    resolve();
}

void
Request::expired (const boost::system::error_code &ec) {

    // Ignore this event if the timer was aborted
    if (ec == boost::asio::error::operation_aborted) return;

    // Also ignore this event if the request is over
    if (_state == State::FINISHED) return;

    LOGS(_log, LOG_LVL_DEBUG, context() << "expired");

    finish(EXPIRED);
}

void
Request::cancel () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancel");

    finish(CANCELLED);
}

void
Request::finish (ExtendedState extendedState) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finish");

    // Check if it's not too late for tis operation

    if (_state == FINISHED) return;

    // Set new state to make sure all event handlers will recognize tis scenario
    // and avoid making any modifications to the request's state.

    State previouState = _state;    // remember this in case if extra actions will
                                    // need to be taken later.

    setState(FINISHED, extendedState);

    // Close all sockets if needed

    if (previouState == IN_PROGRESS) {
        _resolver.cancel();
        _socket.cancel();
        _socket.close();
        _timer.cancel();
        _requestExpirationTimer.cancel();
    }

    // This will invoke user-defined notifiers (if any)
    //
    // NOTE: We have to updat the timestamp before invoking a user
    //       handler on teh completion of the operation.

    _performance.setUpdateFinish();

    endProtocol();
}

void
Request::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    // Cancel any asynchronous operation(s) if not in the initial state

    switch (_state) {

        case CREATED:
            break;

        case IN_PROGRESS:
            _resolver.cancel();
            _socket.cancel();
            _socket.close();
            _timer.cancel();
            _requestExpirationTimer.cancel();
            break;

        default:
            break;
    }

    // Reset the state so that we could begin all over again

    setState(CREATED, NONE);
    
    resolve();
}

void
Request::resolve () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolve");

    boost::asio::ip::tcp::resolver::query query (
        _workerInfoPtr->svcHost(),
        _workerInfoPtr->svcPort()
    );
    _resolver.async_resolve (
        query,
        boost::bind (
            &Request::resolved,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
    setState(IN_PROGRESS, NONE);
}

void
Request::resolved (const boost::system::error_code          &ec,
                   boost::asio::ip::tcp::resolver::iterator  iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolved");

    if (isAborted(ec)) return;

    if (ec) waitBeforeRestart();
    else    connect(iter);
}

void
Request::connect (boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connect");

    boost::asio::async_connect (
        _socket,
        iter,
        boost::bind (
            &Request::connected,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
}

void
Request::connected (const boost::system::error_code          &ec,
                    boost::asio::ip::tcp::resolver::iterator  iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connected");

    if (isAborted(ec)) return;

    if (ec) waitBeforeRestart();
    else    beginProtocol();
}

void
Request::waitBeforeRestart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "waitBeforeRestart");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &Request::awakenForRestart,
            shared_from_this(),
            boost::asio::placeholders::error
        )
    );
}

void
Request::awakenForRestart (const boost::system::error_code &ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awakenForRestart");

    if (isAborted(ec)) return;

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) return;

    restart();
}

bool
Request::isAborted (const boost::system::error_code &ec) const {

    if (ec == boost::asio::error::operation_aborted) {
        LOGS(_log, LOG_LVL_DEBUG, context() << "isAborted  ** ABORTED **");
        return true;
    }
    return false;
}

void
Request::assertState (State state) const {
    if (state != _state)
        throw std::logic_error (
            "wrong state " + state2string(state) + " instead of " + state2string(_state));
}

void
Request::setState (State         state,
                   ExtendedState extendedState)
{
    LOGS(_log, LOG_LVL_DEBUG, context() << "setState  " << state2string(state, extendedState));

    _state         = state;
    _extendedState = extendedState;
}

}}} // namespace lsst::qserv::replica_core
