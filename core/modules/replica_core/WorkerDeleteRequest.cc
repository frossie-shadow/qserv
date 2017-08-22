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

#include "replica_core/WorkerDeleteRequest.h"

// System headers

// Qserv headers

#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerDeleteRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


//////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequest ////////////////////
//////////////////////////////////////////////////////////////

WorkerDeleteRequest::pointer
WorkerDeleteRequest::create (ServiceProvider   &serviceProvider,
                             const std::string &worker,
                             const std::string &id,
                             int                priority,
                             const std::string &database,
                             unsigned int       chunk) {

    return WorkerDeleteRequest::pointer (
        new WorkerDeleteRequest (serviceProvider,
                                 worker,
                                 id,
                                 priority,
                                 database,
                                 chunk));
}

WorkerDeleteRequest::WorkerDeleteRequest (ServiceProvider   &serviceProvider,
                                          const std::string &worker,
                                          const std::string &id,
                                          int                priority,
                                          const std::string &database,
                                          unsigned int       chunk)
    :   WorkerRequest (serviceProvider,
                       worker,
                       "DELETE",
                       id,
                       priority),

        _database   (database),
        _chunk      (chunk),
        _deleteInfo () {
}

WorkerDeleteRequest::~WorkerDeleteRequest () {
}

bool
WorkerDeleteRequest::execute (bool incremental) {

   LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: " << database()
         << "  chunk: " << chunk());

    // TODO: provide the actual implementation instead of the dummy one.

    const bool complete = WorkerRequest::execute(incremental);
    if (complete) {
        _deleteInfo = ReplicaDeleteInfo(100.0);     // simulate 100% completed
    }
    return complete;
}

///////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequestX ////////////////////
///////////////////////////////////////////////////////////////

WorkerDeleteRequestX::pointer
WorkerDeleteRequestX::create (ServiceProvider   &serviceProvider,
                              const std::string &worker,
                              const std::string &id,
                              int                priority,
                              const std::string &database,
                              unsigned int       chunk) {

    return WorkerDeleteRequestX::pointer (
        new WorkerDeleteRequestX (serviceProvider,
                                  worker,
                                  id,
                                  priority,
                                  database,
                                  chunk));
}

WorkerDeleteRequestX::WorkerDeleteRequestX (ServiceProvider   &serviceProvider,
                                            const std::string &worker,
                                            const std::string &id,
                                            int                priority,
                                            const std::string &database,
                                            unsigned int       chunk)
    :   WorkerDeleteRequest (serviceProvider,
                             worker,
                             id,
                             priority,
                             database,
                             chunk) {
}

WorkerDeleteRequestX::~WorkerDeleteRequestX () {
}

bool
WorkerDeleteRequestX::execute (bool incremental) {

    // TODO: provide the actual implementation instead of the dummy one.

    return WorkerDeleteRequest::execute(incremental);
}


}}} // namespace lsst::qserv::replica_core

