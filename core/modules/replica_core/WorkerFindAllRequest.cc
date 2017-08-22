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

#include "replica_core/WorkerFindAllRequest.h"

// System headers

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerFindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequest ////////////////////
///////////////////////////////////////////////////////////////

WorkerFindAllRequest::pointer
WorkerFindAllRequest::create (ServiceProvider   &serviceProvider,
                              const std::string &worker,
                              const std::string &id,
                              int                priority,
                              const std::string &database) {

    return WorkerFindAllRequest::pointer (
        new WorkerFindAllRequest (serviceProvider,
                                  worker,
                                  id,
                                  priority,
                                  database));
}

WorkerFindAllRequest::WorkerFindAllRequest (ServiceProvider   &serviceProvider,
                                            const std::string &worker,
                                            const std::string &id,
                                            int                priority,
                                            const std::string &database)
    :   WorkerRequest (serviceProvider,
                       worker,
                       "FIND-ALL",
                       id,
                       priority),

        _database              (database),
        _replicaInfoCollection () {
}


WorkerFindAllRequest::~WorkerFindAllRequest () {
}


const ReplicaInfoCollection&
WorkerFindAllRequest::replicaInfoCollection () const {
    return _replicaInfoCollection;
}


bool
WorkerFindAllRequest::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  worker: "   << _worker
         << "  database: " << database());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute(incremental);
    if (completed)
        for (unsigned int chunk=0; chunk<8; ++chunk)
            _replicaInfoCollection.emplace_back (
                ReplicaInfo::COMPLETE,
                _worker,
                database(),
                chunk);

    return completed;
}


////////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequestX ////////////////////
////////////////////////////////////////////////////////////////

WorkerFindAllRequestX::pointer
WorkerFindAllRequestX::create (ServiceProvider   &serviceProvider,
                               const std::string &worker,
                               const std::string &id,
                               int                priority,
                               const std::string &database) {

    return WorkerFindAllRequestX::pointer (
        new WorkerFindAllRequestX (serviceProvider,
                                   worker,
                                   id,
                                   priority,
                                   database));
}

WorkerFindAllRequestX::WorkerFindAllRequestX (ServiceProvider   &serviceProvider,
                                              const std::string &worker,
                                              const std::string &id,
                                              int                priority,
                                              const std::string &database)
    :   WorkerFindAllRequest (serviceProvider,
                              worker,
                              id,
                              priority,
                              database) {
}

WorkerFindAllRequestX::~WorkerFindAllRequestX () {
}

bool
WorkerFindAllRequestX::execute (bool incremental) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  worker: " << _worker
         << "  database: " << database());

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerRequest::execute(incremental);
}


}}} // namespace lsst::qserv::replica_core

