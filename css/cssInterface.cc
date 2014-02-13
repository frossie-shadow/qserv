// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file CssInterface.cc
  *
  * @brief Interface to the Common State System.
  *
  * @Author Jacek Becla, SLAC
  */

/*
 * Based on:
 * http://zookeeper.apache.org/doc/r3.3.4/zookeeperProgrammers.html#ZooKeeper+C+client+API
 *
 * To do:
 *  - logging
 *  - perhaps switch to async (seems to be recommended by zookeeper)
 */


// standard library imports
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h> // for memset

// local imports
#include "cssInterface.h"
#include "cssException.h"


using std::cout;
using std::endl;
using std::exception;
using std::ostringstream;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::css;

/**
 * Initialize the interface.
 *
 * @param connInfo connection information
 */
qCss::CssInterface::CssInterface(string const& connInfo, bool verbose) :
    _verbose(verbose) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    _zh = zookeeper_init(connInfo.c_str(), 0, 10000, 0, 0, 0);
    if ( !_zh ) {
        throw std::runtime_error("Failed to connect");
    }
}

qCss::CssInterface::~CssInterface() {
    zookeeper_close(_zh);
}

void
qCss::CssInterface::create(string const& key, string const& value) {
    if (_verbose) {
        cout << "*** CssInterface::create(), " << key << " --> " << value << endl;
    }
    char buffer[512];
    int rc = zoo_create(_zh, key.c_str(), value.c_str(), value.length(), 
                        &ZOO_OPEN_ACL_UNSAFE, 0, buffer, sizeof(buffer)-1);
    if (rc!=ZOK) {
        zooFailure(rc, "create");
    }
}

bool
qCss::CssInterface::exists(string const& key) {
    if (_verbose) {
        cout << "*** CssInterface::exist(), key: " << key << endl;
    }
    struct Stat stat;
    int rc = zoo_exists(_zh, key.c_str(), 0,  &stat);
    if (rc==ZOK) {
        return 1;
    }
    if (rc==ZNONODE) {
        return 0;
    }
    zooFailure(rc, "exists", key);
    return false;
}

string
qCss::CssInterface::get(string const& key) {
    if (_verbose) {
        cout << "*** CssInterface::get(), key: " << key << endl;
    }
    char buffer[512];
    memset(buffer, 0, 512);
    int buflen = sizeof(buffer);
    struct Stat stat;
    int rc = zoo_get(_zh, key.c_str(), 0, buffer, &buflen, &stat);
    if (rc=ZOK) {
        zooFailure(rc, "get", key);
    }
    if (_verbose) {
        cout << "*** got: '" << buffer << "'" << endl;
    }
    return string(buffer);
}

std::vector<string> 
qCss::CssInterface::getChildren(string const& key) {
    if (_verbose) {
        cout << "*** CssInterface::getChildren(), key: " << key << endl;
    }
    struct String_vector strings;
    int rc = zoo_get_children(_zh, key.c_str(), 0, &strings);
    if (rc!=ZOK) {
        zooFailure(rc, "getChildren", key);
    }
    cout << "got " << strings.count << " children" << endl;
    vector<string> v;
    int i;
    for (i=0 ; i < strings.count ; i++) {
        cout << "   " << i+1 << ": " << strings.data[i] << endl;
        v.push_back(strings.data[i]);
    }
    return v;
}

void
qCss::CssInterface::deleteNode(string const& key) {
    if (_verbose) {
        cout << "*** CssInterface::deleteNode, key: " << key << endl;
    }
    int rc = zoo_delete(_zh, key.c_str(), -1);
    if (rc!=ZOK) {
        zooFailure(rc, "deleteNode", key);
    }   
}

/**
  * @param rc       return code returned by zookeeper
  * @param fName    function name where the error happened
  * @param extraMsg optional extra message to include in the error message
  */
void
qCss::CssInterface::zooFailure(int rc, 
                               std::string const& fName, 
                               std::string const& extraMsg) {
    if (rc==ZNONODE) {
        if (_verbose) {
            cout << "*** CssInterface::" << fName << "(), key: " << extraMsg 
                 << " does not exist." << endl;
        }
        throw qCss::CssException(qCss::CssException::KEY_DOES_NOT_EXIST, extraMsg);
    }
    if (rc==ZCONNECTIONLOSS) {
        if (_verbose) {
            cout << "*** CssInterface::" << fName 
                 << "(). Can't connect to zookeeper" << endl;
        }
        throw qCss::CssException(qCss::CssException::CONN_FAILURE);
    }
    if (rc==ZNOAUTH) {
        if (_verbose) {
            cout << "*** CssInterface::" << fName << "(). "
                 << "Zookeeper authorization failure." << endl;
        }
        throw qCss::CssException(qCss::CssException::AUTH_FAILURE);
    }
    ostringstream s;
    s << "*** CssInterface::" << fName << "(), zookeeper error #" << rc;
    if (extraMsg != "") {
        s << " (" << extraMsg << ")";
    }
    if (_verbose) {
        cout << s.str() << endl;
    }
    throw qCss::CssException(CssException::INTERNAL_ERROR, s.str());
}
