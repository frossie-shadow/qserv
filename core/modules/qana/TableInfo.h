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

/// \file
/// \brief Table metadata classes

/// \page table_types Table Types
///
/// There are 4 different kinds of tables in the Qserv system. The first
/// and simplest is the replicated table. These are available in their
/// entirety to every worker. Arbitrary joins are allowed between them,
/// and there is no need to validate or rewrite such joins in any way.
///
/// The second kind is the "director" table. Director tables are spatially
/// partitioned into chunks (based on longitude and latitude) that are
/// distributed across the Qserv workers. Each chunk can be subdivided into
/// sub-chunks to make near-neighbor joins tractable (more on this later).
/// Additionally, the rows in close spatial proximity to each sub-chunk
/// are stored in an "overlap" table, itself broken into chunks. This allows
/// near-neighbor queries to look outside of the spatial boundaries of a
/// sub-chunk for matches to a position inside it without consulting other
/// workers and incurring the attendant network and implementation costs.
///
/// "Child" tables are partitioned into chunks according to a director table.
/// A child table contains (at least conceptually) a foreign key into a
/// director table, and each of its rows is assigned to the same chunk as the
/// corresponding director table row. Overlap is not stored for child tables,
/// nor is it possible to create sub-chunks for them on the fly.
///
/// Finally, "match" tables provide an N-to-M mapping between two director
/// tables that have been partitioned in the same way, i.e. that have chunks
/// and sub-chunks which line up exactly in superposition. A match table
/// contains a pair of foreign keys into two director tables `A` and `B`,
/// and matches between `a` ∈ `A` and `b` ∈ `B` are stored in the chunks
/// of both `a` and `b`. A match can relate director table rows `a` and `b`
/// from different chunks so long as `a` falls into the overlap of the
/// chunk containing `b` (and vice versa).

#ifndef LSST_QSERV_QANA_TABLEINFO_H
#define LSST_QSERV_QANA_TABLEINFO_H

// System headers
#include <stdint.h>
#include <string>
#include <vector>

// Local headers
#include "query/ColumnRef.h"


namespace lsst {
namespace qserv {
namespace qana {

/// `TableInfo` is a base class for table metadata. A subclass is provided
/// for each [kind of table](\ref table_types) supported by Qserv except
/// replicated tables, which are omitted because they are uninteresting for
/// query analysis.
struct TableInfo {
    enum Kind { DIRECTOR = 0, CHILD, MATCH, NUM_KINDS };

    std::string database;
    std::string table;
    Kind kind;

    TableInfo(std::string const& db, std::string const& t, Kind k) :
        database(db), table(t), kind(k) {}
    virtual ~TableInfo() {}

    /// `getColumnRefs` returns a vector of all possible references to columns
    /// from this table (optionally aliased) that can be involved in an
    /// admissible join predicate.
    virtual std::vector<query::ColumnRef> const getColumnRefs(
        std::string const& alias) const
    {
        return std::vector<query::ColumnRef>();
    }
};

/// `TableInfoLt` is a less-than comparison functor for non-null `TableInfo`
/// pointers.
struct TableInfoLt {
    bool operator()(TableInfo const* t1, TableInfo const* t2) const {
        int c = t1->table.compare(t2->table);
        return c < 0 || (c == 0 && t1->database < t2->database);
    }
};


/// `DirTableInfo` contains metadata for director tables.
struct DirTableInfo : TableInfo {
    std::string pk;  ///< `pk` is the name of the director's primary key column.
    std::string lon; ///< `lon` is the name of the director's longitude column.
    std::string lat; ///< `lat` is the name of the director's latitude column.
    int32_t pid; ///< `pid` is the director's partitioning ID.

    DirTableInfo(std::string const& db, std::string const& t) :
        TableInfo(db, t, DIRECTOR), pid(0) {}

    virtual std::vector<query::ColumnRef> const getColumnRefs(
        std::string const& alias) const;
};


/// `ChildTableInfo` contains metadata for child tables.
struct ChildTableInfo : TableInfo {
    /// `director` is an unowned pointer to the metadata
    /// for the director table referenced by `fk`.
    DirTableInfo const* director;
    /// `fk` is the name of the foreign key column referencing `director->pk`.
    std::string fk;

    ChildTableInfo(std::string const& db, std::string const& t) :
        TableInfo(db, t, CHILD), director(0) {}

    virtual std::vector<query::ColumnRef> const getColumnRefs(
        std::string const& alias) const;
};


/// `MatchTableInfo` contains metadata for child tables.
struct MatchTableInfo : TableInfo {
    /// `director` is a pair of unowned pointers to the metadata for the
    /// director tables referenced by `fk.first` and `fk.second`.
    std::pair<DirTableInfo const*, DirTableInfo const*> director;
    /// `fk` is the pair of names for the foreign key columns referencing
    /// `director.first->pk` and `director.second->pk`.
    std::pair<std::string, std::string> fk;

    MatchTableInfo(std::string const& db, std::string const& t) :
        TableInfo(db, t, MATCH), director(0, 0) {}

    virtual std::vector<query::ColumnRef> const getColumnRefs(
        std::string const& alias) const;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_TABLEINFO_H
