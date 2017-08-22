#include <iostream>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerRequestFactory.h"
#include "replica_core/WorkerServer.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_worker");

void runAllWorkers (rc::ServiceProvider      &provider,
                    rc::WorkerRequestFactory &requestFactory) {

    for (const std::string &workerName : provider.config().workers()) {
        
        // Create the server and run it within a dedicated thread

        rc::WorkerServer::pointer server =
            rc::WorkerServer::create (provider, requestFactory, workerName);

        std::thread workerSvcThread ([server]() {
            server->run();
        });
        workerSvcThread.detach();
        
        // Run the heartbeat monitor for the server within another thread
 
        std::thread heartbeatThread ([server]() {
            rc::BlockPost blockPost(1000, 5000);
            while (true) {
                blockPost.wait();
                LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
                    << "  worker: " << server->worker()
                    << "  processor: " << rc::WorkerProcessor::state2string(server->processor().state())
                    << "  new, in-progress, finished: "
                    << server->processor().numNewRequests() << ", "
                    << server->processor().numInProgressRequests() << ", "
                    << server->processor().numFinishedRequests());
            }
        });
        heartbeatThread.detach();
    }
}

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
void run (const std::string &configFileName) {
    
    try {
        rc::Configuration        config        {configFileName};
        rc::ServiceProvider      provider      {config};
        rc::WorkerRequestFactory requestFactory{provider};

        runAllWorkers (provider, requestFactory);

        // Block the thread forewer or until an exception happens
        rc::BlockPost blockPost(1000, 5000);
        while (true) {
            blockPost.wait();
        }

    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, e.what());
    }
}
}  /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;
 
    if (argc != 2) {
        std::cerr << "usage: <config>" << std::endl;
        return 1;
    }
    const std::string configFileName (argv[1]);

    ::run (configFileName);

    return 0;
}
