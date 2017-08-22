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
                                  const std::string &worker,
                                  const std::string &id,
                                  int                priority,
                                  const std::string &database,
                                  unsigned int       chunk,
                                  const std::string &sourceWorker) {

    return WorkerReplicationRequest::pointer (
        new WorkerReplicationRequest (serviceProvider,
                                      worker,
                                      id,
                                      priority,
                                      database,
                                      chunk,
                                      sourceWorker));
}

WorkerReplicationRequest::WorkerReplicationRequest (ServiceProvider   &serviceProvider,
                                                    const std::string &worker,
                                                    const std::string &id,
                                                    int                priority,
                                                    const std::string &database,
                                                    unsigned int       chunk,
                                                    const std::string &sourceWorker)
    :   WorkerRequest (serviceProvider,
                       worker,
                       "REPLICATE",
                       id,
                       priority),

        _database        (database),
        _chunk           (chunk),
        _sourceWorker    (sourceWorker),
        _replicationInfo () {

    _serviceProvider.assertWorkerIsValid       (sourceWorker);
    _serviceProvider.assertWorkersAreDifferent (worker, sourceWorker);
}


WorkerReplicationRequest::~WorkerReplicationRequest () {
}

bool
WorkerReplicationRequest::execute (bool incremental) {

   LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: " << database()
         << "  chunk: " << chunk()
         << "  sourceWorker: " << sourceWorker());

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
                                   const std::string &worker,
                                   const std::string &id,
                                   int                priority,
                                   const std::string &database,
                                   unsigned int       chunk,
                                   const std::string &sourceWorker) {

    return WorkerReplicationRequestX::pointer (
        new WorkerReplicationRequestX (serviceProvider,
                                       worker,
                                       id,
                                       priority,
                                       database,
                                       chunk,
                                       sourceWorker));
}

WorkerReplicationRequestX::WorkerReplicationRequestX (ServiceProvider   &serviceProvider,
                                                      const std::string &worker,
                                                      const std::string &id,
                                                      int                priority,
                                                      const std::string &database,
                                                      unsigned int       chunk,
                                                      const std::string &sourceWorker)
    :   WorkerReplicationRequest (serviceProvider,
                                  worker,
                                  id,
                                  priority,
                                  database,
                                  chunk,
                                  sourceWorker) {
}

WorkerReplicationRequestX::~WorkerReplicationRequestX () {
}

bool
WorkerReplicationRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerReplicationRequest::execute(incremental);
}

}}} // namespace lsst::qserv::replica_core

