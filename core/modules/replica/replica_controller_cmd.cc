#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica_core/BlockPost.h"
#include "replica_core/Configuration.h"
#include "replica_core/DeleteRequest.h"
#include "replica_core/FindRequest.h"
#include "replica_core/FindAllRequest.h"
#include "replica_core/Controller.h"
#include "replica_core/ReplicationRequest.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/StatusRequest.h"
#include "replica_core/StopRequest.h"

namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_controller_cmd");

const char* usage =
    "Usage:\n"
    "  <config> <operation> [<parameters>]\n"
    "\n"
    "Supported operations:\n"
    "  REPLICA_CREATE        <worker> <source_worker> <db> <chunk>\n"
    "  REPLICA_CREATE,CANCEL <worker> <source_worker> <db> <chunk>\n"
    "  REPLICA_DELETE        <worker> <db> <chunk>\n"
    "  REPLICA_FIND          <worker> <db> <chunk>\n"
    "  REPLICA_FIND_ALL      <worker> <db>\n"
    "\n"
    "  REQUEST_STATUS:REPLICA_CREATE   <worker> <id> [-track]\n"
    "  REQUEST_STATUS:REPLICA_DELETE   <worker> <id> [-track]\n"
    "  REQUEST_STATUS:REPLICA_FIND     <worker> <id> [-track]\n"
    "  REQUEST_STATUS:REPLICA_FIND_ALL <worker> <id> [-track]\n"
    "\n"
    "  REQUEST_STOP:REPLICA_CREATE   <worker> <id> [-track]\n"
    "  REQUEST_STOP:REPLICA_DELETE   <worker> <id> [-track]\n"
    "  REQUEST_STOP:REPLICA_FIND     <worker> <id> [-track]\n"
    "  REQUEST_STOP:REPLICA_FIND_ALL <worker> <id> [-track]";


/// (Run-time) assert for the minimum number of arguments
void assertArguments (int argc, int minArgc) {
    if (argc < minArgc) {
        std::cerr << usage << std::endl;
        std::exit(1);
    }
}

/// Return 'true' if the specified value is found in the collection
bool found_in (const std::string &val,
               const std::vector<std::string> &col) {
    return col.end() != std::find(col.begin(), col.end(), val);
}

// Command line parameters

std::string configFileName;
std::string operation;
std::string worker;
std::string sourceWorker;
std::string db;
std::string id;
uint32_t chunk;
bool keepTracking = false;


/// Report result of the operation
template <class T>
void printRequest (typename T::pointer &request) {
    LOGS(_log, LOG_LVL_INFO, request->id() << "  " << request->responseData());
    LOGS(_log, LOG_LVL_INFO, request->id() << "  " << request->performance());
}


/// Run the test
bool test () {

    try {

        rc::Configuration   config  {configFileName};
        rc::ServiceProvider provider{config};

        rc::Controller::pointer controller = rc::Controller::create(provider);

        // Start the controller in its own thread before injecting any requests

        controller->run();

        rc::Request::pointer request;

        if ("REPLICA_CREATE" == operation) {
            request = controller->replicate (
                worker, sourceWorker, db, chunk,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                });

        } else if ("REPLICA_CREATE,CANCEL" == operation) {
            request = controller->replicate (
                worker, sourceWorker, db, chunk,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                });

            rc::BlockPost blockPost (0, 500);
            blockPost.wait();

            request->cancel();

        } else if ("REPLICA_DELETE" == operation) {
            request = controller->deleteReplica (
                worker, db, chunk,
                [] (rc::DeleteRequest::pointer request) {
                    printRequest<rc::DeleteRequest>(request);
                });

        } else if ("REPLICA_FIND" == operation) {
            request = controller->findReplica (
                worker, db, chunk,
                [] (rc::FindRequest::pointer request) {
                    printRequest<rc::FindRequest>(request);
                });

        } else if ("REPLICA_FIND_ALL" == operation) {
            request = controller->findAllReplicas (
                worker, db,
                [] (rc::FindAllRequest::pointer request) {
                    printRequest<rc::FindAllRequest>(request);
                });

        } else if ("REQUEST_STATUS:REPLICA_CREATE"  == operation) {
            request = controller->statusOfReplication (
                worker, id,
                [] (rc::StatusReplicationRequest::pointer request) {
                    printRequest<rc::StatusReplicationRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_DELETE"  == operation) {
            request = controller->statusOfDelete (
                worker, id,
                [] (rc::StatusDeleteRequest::pointer request) {
                    printRequest<rc::StatusDeleteRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND"  == operation) {
            request = controller->statusOfFind (
                worker, id,
                [] (rc::StatusFindRequest::pointer request) {
                    printRequest<rc::StatusFindRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND_ALL"  == operation) {
            request = controller->statusOfFindAll (
                worker, id,
                [] (rc::StatusFindAllRequest::pointer request) {
                    printRequest<rc::StatusFindAllRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_CREATE"  == operation) {
            request = controller->stopReplication (
                worker, id,
                [] (rc::StopReplicationRequest::pointer request) {
                    printRequest<rc::StopReplicationRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_DELETE"  == operation) {
            request = controller->stopReplicaDelete (
                worker, id,
                [] (rc::StopDeleteRequest::pointer request) {
                    printRequest<rc::StopDeleteRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND"  == operation) {
            request = controller->stopReplicaFind (
                worker, id,
                [] (rc::StopFindRequest::pointer request) {
                    printRequest<rc::StopFindRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND_ALL"  == operation) {
            request = controller->stopReplicaFindAll (
                worker, id,
                [] (rc::StopFindAllRequest::pointer request) {
                    printRequest<rc::StopFindAllRequest>(request);
                },
                keepTracking);

        } else {
            return false;
        }

        // Wait before the request is finished. Then stop the master controller

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between iterations

        while (request->state() != rc::Request::State::FINISHED) {
            blockPost.wait();
        }
        controller->stop();

        // Block the current thread indefinitively or untill the controller is cancelled.

        LOGS(_log, LOG_LVL_INFO, "waiting for: controller->join()");
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    ::assertArguments (argc, 4);

    configFileName = argv[1];
    operation      = argv[2];
    worker         = argv[3];
    
    if (found_in(operation, {"REPLICA_CREATE",
                             "REPLICA_CREATE,CANCEL"})) {
        ::assertArguments (argc, 7);
        sourceWorker =            argv[4];
        db           =            argv[5];
        chunk        = std::stoul(argv[6]);

    } else if (found_in(operation, {"REPLICA_DELETE",
                                    "REPLICA_FIND"})) {
        ::assertArguments (argc, 6);
        db    =            argv[4];
        chunk = std::stoul(argv[5]);

    } else if (found_in(operation, {"REPLICA_FIND_ALL"})) {

        ::assertArguments (argc, 5);
        db = argv[4];

    } else if (found_in(operation, {"REPLICA_FIND_ALL",
                                    "REQUEST_STATUS:REPLICA_CREATE",
                                    "REQUEST_STATUS:REPLICA_DELETE",
                                    "REQUEST_STATUS:REPLICA_FIND",
                                    "REQUEST_STATUS:REPLICA_FIND_ALL",
                                    "REQUEST_STOP:REPLICA_CREATE",
                                    "REQUEST_STOP:REPLICA_DELETE",
                                    "REQUEST_STOP:REPLICA_FIND",
                                    "REQUEST_STOP:REPLICA_FIND_ALL"})) {
        ::assertArguments (argc, 5);
        id = argv[4];
        if (argc > 5) {
            if (argv[5] == std::string("-keep")) {
                keepTracking = true;
            } else {
                std::cerr << ::usage << std::endl;
                return 1;
            }
        }

    } else {
        std::cerr << ::usage << std::endl;
        return 1;
    }
    ::test();
    return 0;
}