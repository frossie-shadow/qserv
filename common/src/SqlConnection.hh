/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_SQL_H
#define LSST_QSERV_SQL_H
// sql.h - SQL interface module.  Convenience code/abstraction layer
// fro calling into MySQL.  Uncertain of how this usage conflicts with
// db usage via the python MySQLdb api. 


// Standard
#include <string>
#include <vector>

// Boost
#include <boost/thread.hpp>
// MySQL
#include <mysql/mysql.h> // MYSQL is typedef, so we can't forward declare it.

#include "SqlErrorObject.hh"

namespace lsst {
namespace qserv {

/// class SqlConfig : Value class for configuring the MySQL connection
class SqlConfig {
public:
    SqlConfig() : port(0) {}
    std::string hostname;
    std::string username;
    std::string password;
    std::string dbName;
    unsigned int port;
    std::string socket;
};

/// class SqlConnection : Class for interacting with a MySQL database.
class SqlConnection {
public:
    SqlConnection(SqlConfig const& sc, bool useThreadMgmt=false); 
    ~SqlConnection(); 
    bool connectToDb(SqlErrorObject&);
    bool selectDb(std::string const& dbName, SqlErrorObject&);
    bool apply(std::string const& sql, SqlErrorObject&);

    bool dbExists(std::string const& dbName, SqlErrorObject&);
    bool createDb(std::string const& dbName, SqlErrorObject&, 
                  bool failIfExists=true);
    bool dropDb(std::string const& dbName, SqlErrorObject&);
    bool tableExists(std::string const& tableName, 
                     SqlErrorObject&,
                     std::string const& dbName="");
    bool dropTable(std::string const& tableName,
                   SqlErrorObject&,
                   bool failIfDoesNotExist=true,
                   std::string const& dbName="");
    bool listTables(std::vector<std::string>&, 
                    SqlErrorObject&,
                    std::string const& prefixed="",
                    std::string const& dbName="");

    std::string getActiveDbName() const { return _config.dbName; }

    // FIXME: remove, not thread safe, use SqlErrorObject instead
    char const* getMySqlError() const { return _mysqlError; }
    int getMySqlErrno() const { return _mysqlErrno; }

private:
    bool _init(SqlErrorObject&);
    bool _connect(SqlErrorObject&);
    bool _discardResults(SqlErrorObject&);
    bool _setErrorObject(SqlErrorObject&);

    MYSQL* _conn;
    std::string _error;
    int _mysqlErrno;
    const char* _mysqlError;
    SqlConfig _config;
    bool _connected;
    bool _useThreadMgmt;
    static boost::mutex _sharedMutex;
    static bool _isReady;
}; // class SqlConnection


}} // namespace lsst::qserv
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_SQL_H
