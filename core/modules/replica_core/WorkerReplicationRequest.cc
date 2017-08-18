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

#include "replica_core/WorkerReplicationRequest.h"

// System headers

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequest ////////////////////
///////////////////////////////////////////////////////////////////

WorkerReplicationRequest::pointer
WorkerReplicationRequest::create (ServiceProvider   &serviceProvider,
                                  const std::string &id,
                                  int                priority,
                                  const std::string &database,
                                  unsigned int       chunk,
                                  const std::string &worker) {

    return WorkerReplicationRequest::pointer (
        new WorkerReplicationRequest (serviceProvider,
                                      id,
                                      priority,
                                      database,
                                      chunk,
                                      worker));
}

WorkerReplicationRequest::WorkerReplicationRequest (ServiceProvider   &serviceProvider,
                                                    const std::string &id,
                                                    int                priority,
                                                    const std::string &database,
                                                    unsigned int       chunk,
                                                    const std::string &worker)
    :   WorkerRequest (serviceProvider,
                       "REPLICATE",
                       id,
                       priority),

        _database        (database),
        _chunk           (chunk),
        _worker          (worker),
        _replicationInfo () {

    _serviceProvider.assertWorkerIsValid       (worker);
    _serviceProvider.assertWorkersAreDifferent (_serviceProvider.config().workerName(), worker);
}


WorkerReplicationRequest::~WorkerReplicationRequest () {
}

bool
WorkerReplicationRequest::execute (bool incremental) {

   LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: "     << database()
         << "  chunk: "  << chunk()
         << "  worker: " << worker());

    // TODO: provide the actual implementation instead of the dummy one.

    const bool complete = WorkerRequest::execute(incremental);
    if (complete) {
        _replicationInfo = ReplicaCreateInfo(100.);     // simulate 100% completed
    }
    return complete;
}

////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestX ////////////////////
////////////////////////////////////////////////////////////////////

WorkerReplicationRequestX::pointer
WorkerReplicationRequestX::create (ServiceProvider   &serviceProvider,
                                   const std::string &id,
                                   int                priority,
                                   const std::string &database,
                                   unsigned int       chunk,
                                   const std::string &worker) {

    return WorkerReplicationRequestX::pointer (
        new WorkerReplicationRequestX (serviceProvider,
                                       id,
                                       priority,
                                       database,
                                       chunk,
                                       worker));
}

WorkerReplicationRequestX::WorkerReplicationRequestX (ServiceProvider   &serviceProvider,
                                                      const std::string &id,
                                                      int                priority,
                                                      const std::string &database,
                                                      unsigned int       chunk,
                                                      const std::string &worker)
    :   WorkerReplicationRequest (serviceProvider,
                                  id,
                                  priority,
                                  database,
                                  chunk,
                                  worker) {
}

WorkerReplicationRequestX::~WorkerReplicationRequestX () {
}

bool
WorkerReplicationRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerReplicationRequest::execute(incremental);
}

}}} // namespace lsst::qserv::replica_core

