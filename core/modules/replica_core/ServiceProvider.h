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
#ifndef LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H
#define LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H

/// ServiceProvider.h declares:
///
/// class ServiceProvider
/// (see individual class documentation for more information)

// System headers

#include <memory>       // shared_ptr, enable_shared_from_this
#include <string>
#include <vector>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

class Configuration;

/**
  * Class ServiceProvider hosts various serviceses for the master server.
  */
class ServiceProvider {

public:

    // Default construction and copy semantics are proxibited

    ServiceProvider () = delete;
    ServiceProvider (ServiceProvider const&) = delete;
    ServiceProvider & operator= (ServiceProvider const&) = delete;

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit ServiceProvider (Configuration &configuration);

    /**
     * Return a reference to the configuration service
     */
    Configuration& config () const { return _configuration; }

    /**
     * Make sure this worker is known in the configuration. Throws exception
     * std::invalid_argument otherwise.
     */
    void assertWorkerIsValid (const std::string &workerName);

    /**
     * Make sure workers are now known in the configuration and they're different.
     * Throws exception std::invalid_argument otherwise.
     */
    void assertWorkersAreDifferent (const std::string &workerOneName,
                                    const std::string &workerTwoName);

private:

    Configuration& _configuration;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_SERVICEPROVIDER_H