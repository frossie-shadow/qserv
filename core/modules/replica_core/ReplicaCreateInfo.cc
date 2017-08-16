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

#include "replica_core/ReplicaCreateInfo.h"

// System headers

// Qserv headers

#include "proto/replication.pb.h"


namespace proto = lsst::qserv::proto;

namespace {

/// State translation

void setInfoImpl (const lsst::qserv::replica_core::ReplicaCreateInfo &rci,
                  proto::ReplicationReplicaCreateInfo                *info) {

    info->set_progress (rci.progress());
}
}  // namespace

namespace lsst {
namespace qserv {
namespace replica_core {


ReplicaCreateInfo::ReplicaCreateInfo (float progress)
    :   _progress (progress) {
}

ReplicaCreateInfo::ReplicaCreateInfo (const proto::ReplicationReplicaCreateInfo *info) {
    _progress = info->progress();
}


ReplicaCreateInfo::ReplicaCreateInfo (ReplicaCreateInfo const &rci) {
    _progress = rci._progress;
}


ReplicaCreateInfo&
ReplicaCreateInfo::operator= (ReplicaCreateInfo const &rci) {
    if (this != &rci) {
        _progress = rci._progress;
    }
    return *this;
}


ReplicaCreateInfo::~ReplicaCreateInfo () {
}


proto::ReplicationReplicaCreateInfo*
ReplicaCreateInfo::info () const {
    proto::ReplicationReplicaCreateInfo *ptr = new proto::ReplicationReplicaCreateInfo;
    ::setInfoImpl(*this, ptr);
    return ptr;
}

void
ReplicaCreateInfo::setInfo (lsst::qserv::proto::ReplicationReplicaCreateInfo *info) const {
    ::setInfoImpl(*this, info);
}


std::ostream&
operator<< (std::ostream& os, const ReplicaCreateInfo &rci) {
    os  << "ReplicaCreateInfo"
        << " progress: " << rci.progress();
    return os;
}

}}} // namespace lsst::qserv::replica_core
