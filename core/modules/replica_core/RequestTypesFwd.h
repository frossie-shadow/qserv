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
#ifndef LSST_QSERV_REPLICA_CORE_REQUESTTYPESFWD_H
#define LSST_QSERV_REPLICA_CORE_REQUESTTYPESFWD_H

/// RequestTypesFwd.h declares:
///
/// Forward declarations for smart pointers and calback functions
/// corresponding to specific requests.

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations


////////////////////////////////////////////
// Replica creation and deletion requests //
////////////////////////////////////////////

class                                                           ReplicationRequest;
typedef std::shared_ptr   <ReplicationRequest>                  ReplicationRequest_pointer;
typedef std::function<void(ReplicationRequest_pointer)>         ReplicationRequest_callback_type;

class                                                           DeleteRequest;
typedef std::shared_ptr   <DeleteRequest>                       DeleteRequest_pointer;
typedef std::function<void(DeleteRequest_pointer)>              DeleteRequest_callback_type;


/////////////////////////////
// Replica lookup requests //
/////////////////////////////

class                                                           FindRequest;
typedef std::shared_ptr   <FindRequest>                         FindRequest_pointer;
typedef std::function<void(FindRequest_pointer)>                FindRequest_callback_type;

class                                                           FindAllRequest;
typedef std::shared_ptr   <FindAllRequest>                      FindAllRequest_pointer;
typedef std::function<void(FindAllRequest_pointer)>             FindAllRequest_callback_type;


////////////////////////////////////
// Replication request managememt //
////////////////////////////////////

template <typename POLICY>
class StopRequest;

class                      StopReplicationRequestPolicy;
using                      StopReplicationRequest = StopRequest<StopReplicationRequestPolicy>;
typedef std::shared_ptr   <StopReplicationRequest>              StopReplicationRequest_pointer;
typedef std::function<void(StopReplicationRequest_pointer)>     StopReplicationRequest_callback_type;

class                      StopDeleteRequestPolicy;
using                      StopDeleteRequest      = StopRequest<StopDeleteRequestPolicy>;
typedef std::shared_ptr   <StopDeleteRequest>                   StopDeleteRequest_pointer;
typedef std::function<void(StopDeleteRequest_pointer)>          StopDeleteRequest_callback_type;

class                      StopFindRequestPolicy;
using                      StopFindRequest        = StopRequest<StopFindRequestPolicy>;
typedef std::shared_ptr   <StopFindRequest>                     StopFindRequest_pointer;
typedef std::function<void(StopFindRequest_pointer)>            StopFindRequest_callback_type;

class                      StopFindAllRequestPolicy;
using                      StopFindAllRequest     = StopRequest<StopFindAllRequestPolicy>;
typedef std::shared_ptr   <StopFindAllRequest>                  StopFindAllRequest_pointer;
typedef std::function<void(StopFindAllRequest_pointer)>         StopFindAllRequest_callback_type;


template <typename POLICY>
class StatusRequest;

class                      StatusReplicationRequestPolicy;
using                      StatusReplicationRequest = StatusRequest<StatusReplicationRequestPolicy>;
typedef std::shared_ptr   <StatusReplicationRequest>                StatusReplicationRequest_pointer;
typedef std::function<void(StatusReplicationRequest_pointer)>       StatusReplicationRequest_callback_type;

class                      StatusDeleteRequestPolicy;
using                      StatusDeleteRequest      = StatusRequest<StatusDeleteRequestPolicy>;
typedef std::shared_ptr   <StatusDeleteRequest>                     StatusDeleteRequest_pointer;
typedef std::function<void(StatusDeleteRequest_pointer)>            StatusDeleteRequest_callback_type;

class                      StatusFindRequestPolicy;
using                      StatusFindRequest        = StatusRequest<StatusFindRequestPolicy>;
typedef std::shared_ptr   <StatusFindRequest>                       StatusFindRequest_pointer;
typedef std::function<void(StatusFindRequest_pointer)>              StatusFindRequest_callback_type;

class                      StatusFindAllRequestPolicy;
using                      StatusFindAllRequest     = StatusRequest<StatusFindAllRequestPolicy>;
typedef std::shared_ptr   <StatusFindAllRequest>                    StatusFindAllRequest_pointer;
typedef std::function<void(StatusFindAllRequest_pointer)>           StatusFindAllRequest_callback_type;


////////////////////////////////////////
// Worker service management requests //
////////////////////////////////////////

template <typename POLICY>
class ServiceManagementRequest;

class                      ServiceSuspendRequestPolicy;
using                      ServiceSuspendRequest = ServiceManagementRequest<ServiceSuspendRequestPolicy>;
typedef std::shared_ptr   <ServiceSuspendRequest>                           ServiceSuspendRequest_pointer;
typedef std::function<void(ServiceSuspendRequest_pointer)>                  ServiceSuspendRequest_callback_type;

class                      ServiceResumeRequestPolicy;
using                      ServiceResumeRequest  = ServiceManagementRequest<ServiceResumeRequestPolicy>;
typedef std::shared_ptr   <ServiceResumeRequest>                            ServiceResumeRequest_pointer;
typedef std::function<void(ServiceResumeRequest_pointer)>                   ServiceResumeRequest_callback_type;

class                      ServiceStatusRequestPolicy;
using                      ServiceStatusRequest  = ServiceManagementRequest<ServiceStatusRequestPolicy>;
typedef std::shared_ptr   <ServiceStatusRequest>                            ServiceStatusRequest_pointer;
typedef std::function<void(ServiceStatusRequest_pointer)>                   ServiceStatusRequest_callback_type;

class                      ServiceRequestsRequestPolicy;
using                      ServiceRequestsRequest = ServiceManagementRequest<ServiceRequestsRequestPolicy>;
typedef std::shared_ptr   <ServiceRequestsRequest>                           ServiceRequestsRequest_pointer;
typedef std::function<void(ServiceRequestsRequest_pointer)>                  ServiceRequestsRequest_callback_type;

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REQUESTTYPESFWD_H