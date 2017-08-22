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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERFINDALLREQUEST_H
#define LSST_QSERV_REPLICA_CORE_WORKERFINDALLREQUEST_H

/// WorkerFindAllRequest.h declares:
///
/// class WorkerFindAllRequest
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

#include "replica_core/ReplicaInfo.h"
#include "replica_core/WorkerRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/**
  * Class WorkerFindAllRequest represents a context and a state of replicas lookup
  * requsts within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindAllRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequest> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (ServiceProvider   &serviceProvider,
                           const std::string &worker,
                           const std::string &id,
                           int                priority,
                           const std::string &database);

    // Default construction and copy semantics are proxibited

    WorkerFindAllRequest () = delete;
    WorkerFindAllRequest (WorkerFindAllRequest const&) = delete;
    WorkerFindAllRequest & operator= (WorkerFindAllRequest const&) = delete;

    /// Destructor
    ~WorkerFindAllRequest () override;

    // Trivial accessors

    const std::string& database () const { return _database; }

   /**
     * Return a refernce to a result of the completed request.
     *
     * Note that this operation returns a meanigful result only when a request
     * is completed with status STATUS_SUCCEEDED.
     */
    const ReplicaInfoCollection& replicaInfoCollection () const;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute (bool incremental=true) override;

protected:

    /**
     * The normal constructor of the class.
     */
    WorkerFindAllRequest (ServiceProvider   &serviceProvider,
                          const std::string &worker,
                          const std::string &id,
                          int                priority,
                          const std::string &database);
private:

    // Parameters of the request

    std::string _database;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};


/**
  * Class WorkerFindAllRequestX provides an actual implementation for
  * the replicas lookup using XRootD.
  */
class WorkerFindAllRequestX
    :   public WorkerFindAllRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequestX> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (ServiceProvider   &serviceProvider,
                           const std::string &worker,
                           const std::string &id,
                           int                priority,
                           const std::string &database);

    // Default construction and copy semantics are proxibited

    WorkerFindAllRequestX () = delete;
    WorkerFindAllRequestX (WorkerFindAllRequestX const&) = delete;
    WorkerFindAllRequestX & operator= (WorkerFindAllRequestX const&) = delete;

    /// Destructor
    ~WorkerFindAllRequestX () override;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute (bool incremental=true) override;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerFindAllRequestX (ServiceProvider   &serviceProvider,
                           const std::string &worker,
                           const std::string &id,
                           int                priority,
                           const std::string &database);
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERFINDALLREQUEST_H