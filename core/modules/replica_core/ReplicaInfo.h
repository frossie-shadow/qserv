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
#ifndef LSST_QSERV_REPLICA_CORE_REPLICAINFO_H
#define LSST_QSERV_REPLICA_CORE_REPLICAINFO_H

/// ReplicaInfo.h declares:
///
/// struct ReplicaInfo
/// (see individual class documentation for more information)

// System headers

#include <ostream>
#include <string>
#include <vector>

// Qserv headers


// Forward declarations

namespace lsst {
namespace qserv {
namespace proto {

class ReplicationReplicaInfo;

}}} // namespace lsst::qserv::proto


// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class ReplicaInfo represents a status of a replica received from
  * the correspondig worker service.
  *
  * Note that this class can only be constructed from an object of
  * the corresponding protobuf type. And there is a complementary operation
  * for translating the state of this class's object into an object of
  * the same protobuf type.
  */
class ReplicaInfo {

public:

    /// Possible statuses of a replica
    enum Status {
        NOT_FOUND,
        CORRUPT,
        INCOMPLETE,
        COMPLETE
    };
    
    /// Return the string representation of the status
    static std::string status2string (Status status);

    /**
     * Construct with the specified state. This is also the default constructor.
     *
     * @param status   - object status (see notes above)
     * @param worker   - the name of the worker wre the replica is located
     * @param database - the name of the database
     * @param chunk    - the chunk number
     */
    explicit ReplicaInfo (Status             status   = NOT_FOUND,
                          const std::string &worker   = "",
                          const std::string &database = "",
                          unsigned int       chunk    = 0);

    /// Construct from a protobuf object
    explicit ReplicaInfo (const lsst::qserv::proto::ReplicationReplicaInfo *info);

    /// Copy constructor
    ReplicaInfo (ReplicaInfo const &ri);

    /// Assignment operator
    ReplicaInfo& operator= (ReplicaInfo const &ri);

    /// Destructor
    ~ReplicaInfo ();

    // Trivial accessors

    Status status () const { return _status; }

    const std::string& worker   () const { return _worker; }
    const std::string& database () const { return _database; }

    unsigned int chunk () const { return _chunk; }

    /**
     * Return a protobuf object
     *
     * OWNERSHIP TRANSFER NOTE: this method allocates a new object and
     * returns a pointer along with its ownership.
     */
    lsst::qserv::proto::ReplicationReplicaInfo *info () const;

    /**
     * Initialize a protobuf object from the object's state
     */
    void setInfo (lsst::qserv::proto::ReplicationReplicaInfo *info) const;

private:

    // Data members
    
    Status _status;

    std::string _worker;
    std::string _database;

    unsigned int _chunk;
};


/// Overloaded streaming operator for type ReplicaInfo
std::ostream& operator<< (std::ostream &os, const ReplicaInfo &ri);


/// The collection type for transient representations
typedef std::vector<ReplicaInfo> ReplicaInfoCollection;


/// Overloaded streaming operator for type ReplicaInfoCollection
std::ostream& operator<< (std::ostream &os, const ReplicaInfoCollection &ric);


}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_REPLICAINFO_H