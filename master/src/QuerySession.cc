/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// QuerySession.cc houses the implementation of the class
// QuerySession, which is a container for input query state (and
// related state available prior to execution.
#include "lsst/qserv/master/QuerySession.h"

#include <algorithm>
#include "lsst/qserv/master/PlanWriter.h"
#include "lsst/qserv/master/SelectParser.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/QueryMapping.h"
#include "lsst/qserv/master/QueryPlugin.h"
#include "lsst/qserv/master/AggregatePlugin.h"
#include "lsst/qserv/master/SpatialSpecPlugin.h"
#include "lsst/qserv/master/TablePlugin.h"

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
struct printConstraintHelper {
    printConstraintHelper(std::ostream& os_) : os(os_) {}
    void operator()(qMaster::Constraint const& c) {
        os << "Constraint " << c.name << " ";
        std::copy(c.params.begin(), c.params.end(), 
                  std::ostream_iterator<std::string>(os, ","));
        os << "[" << c.params.size() << "]";
    }
    std::ostream& os;
};
void printConstraints(qMaster::ConstraintVector const& cv) {
    std::for_each(cv.begin(), cv.end(), printConstraintHelper(std::cout));
}
void build(qMaster::SelectParser::Ptr p) {
    // Perform parse

    // Extract characteristics

    // Need hints to be sent back up to python for scope calculation.
    // hints in sequence of pairs:
    // constraint name --> paramstr (comma separated values)
    // sequences will be passed back up and be re-split. 

    // Prepare templates (2-phase --> 2 templates)

}

} // anonymous namespace

namespace lsst { namespace qserv  { namespace master {

QuerySession::QuerySession() {} // do nothing.

void QuerySession::setQuery(std::string const& q) {
    _original = q;
    _initContext();
    assert(_context.get());
    SelectParser::Ptr p;
    p = SelectParser::newInstance(q);
    p->setup();
    _stmt = p->getSelectStmt();
    _preparePlugins();
    _applyLogicPlugins();
    _generateConcrete();
    _applyConcretePlugins();
    _showFinal(); // DEBUG    
}

bool QuerySession::getHasAggregate() const {
    // Aggregate: having an aggregate fct spec in the select list.
    // Stmt itself knows whether aggregation is present. More
    // generally, aggregation is a separate pass. In computing a
    // multi-pass execution, the statement makes use of a (proper,
    // probably) subset of its components to compose each pass. Right
    // now, the only goal is to support aggregation using two passes.
    
    // FIXME
    return false;
}

boost::shared_ptr<ConstraintVector> QuerySession::getConstraints() const {
    boost::shared_ptr<WhereClause const> wc = _stmt->getWhere();
    boost::shared_ptr<QsRestrictor::List const> p = wc->getRestrs();
    boost::shared_ptr<ConstraintVector> cv;

    if(p.get()) {
        cv.reset(new ConstraintVector(p->size()));
        int i=0;
        QsRestrictor::List::const_iterator li;
        for(li = p->begin(); li != p->end(); ++li) {
            Constraint c;
            QsRestrictor const& r = **li;
            c.name = r._name;
            StringList::const_iterator si;
            for(si = r._params.begin(); si != r._params.end(); ++si) {
                c.params.push_back(*si);
            }
            (*cv)[i] = c;
            ++i;
        }
        //printConstraints(cv);
        return cv;
    }
    // No constraint vector
    return cv;
}

void QuerySession::addChunk(qMaster::ChunkSpec const& cs) {
    _chunks.push_back(cs);
}


void QuerySession::setResultTable(std::string const& resultTable) {
    _resultTable = resultTable;
}

MergeFixup QuerySession::makeMergeFixup() const {
    MergeFixup mf;
    // TODO
    return mf;
}

QuerySession::Iter QuerySession::cQueryBegin() {
    return Iter(*this, _chunks.begin());
}
QuerySession::Iter QuerySession::cQueryEnd() {
    return Iter(*this, _chunks.end());
}


void QuerySession::_initContext() {
    _context.reset(new QueryContext()); 
    _context->defaultDb = "LSST";
    _context->username = "default";
}
void QuerySession::_preparePlugins() {
    _plugins.reset(new PluginList);

    _plugins->push_back(QueryPlugin::newInstance("Aggregate"));
    _plugins->push_back(QueryPlugin::newInstance("Table"));
    _plugins->push_back(QueryPlugin::newInstance("SpatialSpec"));
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).prepare();
    }
}
void QuerySession::_applyLogicPlugins() {
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyLogical(*_stmt, *_context);
    }
}
void QuerySession::_generateConcrete() {
    _hasMerge = false;
    // In making a statement concrete, the query's execution is split
    // into a parallel portion and a merging/aggregation portion. 
    // In many cases, not much needs to be done, since nearly all of
    // it can be parallelized.
    // If the query requires aggregation, the select list needs to get
    // converted into a parallel portion, and the merging includes the
    // post-parallel steps to merge sub-results.  When the statement
    // results in merely a collection of unordered concatenated rows,
    // the merge statement can be left empty, signifying that the sub
    // results can be concatenated directly into the output table.
    //
    // Important parts of the merge statement are 
    _stmtParallel = _stmt->copyDeep(); // Needs to copy SelectList, since the
                           // parallel statement's version will get
                           // updated by plugins. Plugins probably
                           // need access to the original as a
                           // reference.
    _stmtMerge = _stmt->copyMerge(); // Copies SelectList and Mods,
                                      // but not FROM, and perhaps not
                                      // WHERE(???)
    
    
    // Compute default merge predicate:
}


void QuerySession::_applyConcretePlugins() {
    QueryPlugin::Plan p(*_stmt, *_stmtParallel, *_stmtMerge, _hasMerge);
    PluginList::iterator i;
    for(i=_plugins->begin(); i != _plugins->end(); ++i) {
        (**i).applyPhysical(p, *_context);
    }
}


/// Some code useful for debugging. 
void QuerySession::_showFinal() {
    // Print out the end result.
    QueryTemplate par = _stmtParallel->getTemplate();
    QueryTemplate mer = _stmtMerge->getTemplate();
    
    std::cout << "parallel: " << par.dbgStr() << std::endl;
    std::cout << "merge: " << mer.dbgStr() << std::endl;
}

std::string QuerySession::_buildChunkQuery(ChunkSpec const& s) { 
    // TODO: subchunk support
    // This logic may be pushed over to the qserv worker in the future.
    assert(_stmtParallel.get());
    QueryTemplate cqTemp = _stmtParallel->getTemplate();
    assert(_context->queryMapping.get());
    return _context->queryMapping->apply(s, cqTemp);
}

////////////////////////////////////////////////////////////////////////
// QuerySession::Iter
////////////////////////////////////////////////////////////////////////
QuerySession::Iter::Iter(QuerySession& qs, ChunkSpecList::iterator i)
    : _qs(&qs), _pos(i), _dirty(true) {
    assert(qs._context.get());
    _hasChunks = qs._context->hasChunks();
    _hasSubChunks = qs._context->hasSubChunks();
}

ChunkQuerySpec& QuerySession::Iter::dereference() const {
    if(_dirty) { _updateCache(); }
    return _cache;
}

void QuerySession::Iter::_buildCache() const {
    assert(_qs != NULL);
    _cache.db = _qs->_context->defaultDb;
    _cache.query = _qs->_buildChunkQuery(*_pos);
    _cache.chunkId = _pos->chunkId;
    if(_hasSubChunks) {
        _cache.subChunks.assign(_pos->subChunks.begin(), _pos->subChunks.end());
    }
}


////////////////////////////////////////////////////////////////////////
// initQuerySession
////////////////////////////////////////////////////////////////////////
void initQuerySession() {
    // Plugins should probably be registered once, at startup.
    registerAggregatePlugin(); 
    registerTablePlugin();
    registerSpatialSpecPlugin();
}

}}} // namespace lsst::qserv::master
