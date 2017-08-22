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
#include "replica_core/Configuration.h"

// System headers

#include <boost/lexical_cast.hpp>
#include <iterator>
#include <sstream>
#include <stdexcept>

// Qserv headers

#include "util/ConfigStore.h"

namespace {

// Some reasonable defaults
  
const size_t       defaultRequestBufferSizeBytes     {1024};
const unsigned int defaultRetryTimeoutSec            {1};
const uint16_t     defaultControllerHttpPort         {80};
const size_t       defaultControllerHttpThreads      {1};
const unsigned int defaultControllerRequestTimeoutSec{3600};
const size_t       defaultWorkerNumConnectionsLimit  {1};
const size_t       defaultWorkerNumProcessingThreads {1};
const std::string  defaultWorkerSvcHost              {"localhost"};
const uint16_t     defaultWorkerSvcPort              {50000};
const std::string  defaultWorkerXrootdHost           {"localhost"};
const uint16_t     defaultWorkerXrootdPort           {1094};


/**
 * Fetch and parse a value of the specified key into. Return the specified
 * default value if the parameter was not found.
 *
 * The function may throw the following exceptions:
 *
 *   std::bad_lexical_cast
 */
template <typename T, typename D>
void parseKeyVal (lsst::qserv::util::ConfigStore &configStore,
                  const std::string &key,
                  T &val,
                  D &defaultVal) {

    const std::string str = configStore.get(key);
    val = str.empty() ? defaultVal : boost::lexical_cast<T>(str);        
}

}  // namespace

namespace lsst {
namespace qserv {
namespace replica_core {

Configuration::Configuration (const std::string &configFile)
    :   _configFile                 (configFile),
        _workers                    (),
        _requestBufferSizeBytes     (defaultRequestBufferSizeBytes),
        _retryTimeoutSec            (defaultRetryTimeoutSec),
        _controllerHttpPort         (defaultControllerHttpPort),
        _controllerHttpThreads      (defaultControllerHttpThreads),
        _controllerRequestTimeoutSec(defaultControllerRequestTimeoutSec),
        _workerNumConnectionsLimit  (defaultWorkerNumConnectionsLimit),
        _workerNumProcessingThreads (defaultWorkerNumProcessingThreads) {

    loadConfiguration();
}

Configuration::~Configuration () {
}

bool
Configuration::isKnownWorker (const std::string &name) const {
    return _workerInfo.count(name) > 0;
}

const WorkerInfo&
Configuration::workerInfo (const std::string &name) const {
    if (!isKnownWorker(name)) 
        throw std::out_of_range("Configuration::workerInfo() uknown worker name '"+name+"'");
    return _workerInfo.at(name);
}

void
Configuration::loadConfiguration () {

    lsst::qserv::util::ConfigStore configStore(_configFile);

    // Parse the list of worker names

    std::istringstream ss(configStore.getRequired("common.workers"));
    std::istream_iterator<std::string> begin(ss), end;
    _workers = std::vector<std::string>(begin, end);

    ::parseKeyVal(configStore, "common.request_buf_size_bytes",     _requestBufferSizeBytes,      defaultRequestBufferSizeBytes);
    ::parseKeyVal(configStore, "common.request_retry_interval_sec", _retryTimeoutSec,             defaultRetryTimeoutSec);

    ::parseKeyVal(configStore, "controller.http_server_port",       _controllerHttpPort,          defaultControllerHttpPort);
    ::parseKeyVal(configStore, "controller.http_server_threads",    _controllerHttpThreads,       defaultControllerHttpThreads);
    ::parseKeyVal(configStore, "controller.request_timeout_sec",    _controllerRequestTimeoutSec, defaultControllerRequestTimeoutSec);

    ::parseKeyVal(configStore, "worker.max_connections",            _workerNumConnectionsLimit,   defaultWorkerNumConnectionsLimit);
    ::parseKeyVal(configStore, "worker.num_processing_threads",     _workerNumProcessingThreads,  defaultWorkerNumProcessingThreads);

    // Optional common parameters for workers

    uint16_t commonWorkerSvcPort;
    uint16_t commonWorkerXrootdPort;

    ::parseKeyVal(configStore, "worker.svc_port",    commonWorkerSvcPort,    defaultWorkerSvcPort);
    ::parseKeyVal(configStore, "worker.xrootd_port", commonWorkerXrootdPort, defaultWorkerXrootdPort);

    // Parse optional worker-specific configuraton sections. Assume default
    // or (previously parsed) common values if a whole secton or individual
    // parameters are missing.

    for (const std::string &name : _workers) {

        const std::string section = "worker:"+name;
        if (_workerInfo.count(name))
            throw std::range_error("Configuration::loadConfiguration() duplicate worker entry: '" +
                                   name + "' in: [common] or ["+section+"], configuration file: " + _configFile);

        _workerInfo[name].name = name;

        ::parseKeyVal(configStore, section+".svc_host",    _workerInfo[name].svcHost,    defaultWorkerSvcHost);
        ::parseKeyVal(configStore, section+".svc_port",    _workerInfo[name].svcPort,    commonWorkerSvcPort);
        ::parseKeyVal(configStore, section+".xrootd_host", _workerInfo[name].xrootdHost, defaultWorkerXrootdHost);
        ::parseKeyVal(configStore, section+".xrootd_port", _workerInfo[name].xrootdPort, commonWorkerXrootdPort);
    }
}
    
}}} // namespace lsst::qserv::replica_core
