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

#include "replica_core/WorkerRequestFactory.h"

// System headers

#include <algorithm>
#include <iterator>
#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerDeleteRequest.h"
#include "replica_core/WorkerFindAllRequest.h"
#include "replica_core/WorkerFindRequest.h"
#include "replica_core/WorkerReplicationRequest.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.WorkerRequestFactory");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {


///////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactory ////////////////////
///////////////////////////////////////////////////////////////

WorkerRequestFactory::WorkerRequestFactory (ServiceProvider &serviceProvider)
    :   _serviceProvider (serviceProvider) {
}

WorkerRequestFactory::~WorkerRequestFactory () {
}

WorkerRequestFactory::WorkerReplicationRequest_pointer
WorkerRequestFactory::createReplicationRequest (const std::string &worker,
                                                const std::string &id,
                                                int                priority,
                                                const std::string &database,
                                                unsigned int       chunk,
                                                const std::string &sourceWorker) {
    return WorkerReplicationRequest::create (
        _serviceProvider, worker, id, priority, database, chunk, sourceWorker);
}

WorkerRequestFactory::WorkerDeleteRequest_pointer
WorkerRequestFactory::createDeleteRequest (const std::string &worker,
                                           const std::string &id,
                                           int                priority,
                                           const std::string &database,
                                           unsigned int       chunk) {
    return WorkerDeleteRequest::create (
        _serviceProvider, worker, id, priority, database, chunk);
}

WorkerRequestFactory::WorkerFindRequest_pointer
WorkerRequestFactory::createFindRequest (const std::string &worker,
                                         const std::string &id,
                                         int                priority,
                                         const std::string &database,
                                         unsigned int       chunk) {
    return WorkerFindRequest::create (
        _serviceProvider, worker, id, priority, database, chunk);
}

WorkerRequestFactory::WorkerFindAllRequest_pointer
WorkerRequestFactory::createFindAllRequest (const std::string &worker,
                                            const std::string &id,
                                            int                priority,
                                            const std::string &database) {
    return WorkerFindAllRequest::create (
        _serviceProvider, worker, id, priority, database);
}


////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryX ////////////////////
////////////////////////////////////////////////////////////////

WorkerRequestFactoryX::WorkerRequestFactoryX (ServiceProvider &serviceProvider)
    :   WorkerRequestFactory (serviceProvider) {
}

WorkerRequestFactoryX::~WorkerRequestFactoryX () {
}

WorkerRequestFactory::WorkerReplicationRequest_pointer
WorkerRequestFactoryX::createReplicationRequest (const std::string &worker,
                                                 const std::string &id,
                                                 int                priority,
                                                 const std::string &database,
                                                 unsigned int       chunk,
                                                 const std::string &sourceWorker) {
    return WorkerReplicationRequestX::create (
        _serviceProvider, worker, id, priority, database, chunk, sourceWorker);
}

WorkerRequestFactory::WorkerDeleteRequest_pointer
WorkerRequestFactoryX::createDeleteRequest (const std::string &worker,
                                            const std::string &id,
                                            int                priority,
                                            const std::string &database,
                                            unsigned int       chunk) {
    return WorkerDeleteRequestX::create (
        _serviceProvider, worker, id, priority, database, chunk);
}

WorkerRequestFactory::WorkerFindRequest_pointer
WorkerRequestFactoryX::createFindRequest (const std::string &worker,
                                          const std::string &id,
                                          int                priority,
                                          const std::string &database,
                                          unsigned int       chunk) {
    return WorkerFindRequestX::create (
        _serviceProvider, worker, id, priority, database, chunk);
}

WorkerRequestFactory::WorkerFindAllRequest_pointer
WorkerRequestFactoryX::createFindAllRequest (const std::string &worker,
                                             const std::string &id,
                                             int                priority,
                                             const std::string &database) {
    return WorkerFindAllRequestX::create (
        _serviceProvider, worker, id, priority, database);
}

}}} // namespace lsst::qserv::replica_core

