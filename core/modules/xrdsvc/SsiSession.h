// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_XRDSVC_SSISESSION_H
#define LSST_QSERV_XRDSVC_SSISESSION_H

// System headers
#include <mutex>
#include <vector>

// Third-party headers
#include "XrdSsi/XrdSsiSession.hh"
#include "XrdSsi/XrdSsiResponder.hh"

// Local headers
#include "global/ResourceUnit.h"
#include "wbase/MsgProcessor.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace xrdsvc {
  class SsiResponder;
}}}

namespace lsst {
namespace qserv {
namespace xrdsvc {

/// An implementation of both XrdSsiSession and XrdSsiResponder that is used by
/// SsiService to provide qserv worker services. The XrdSsi interface encourages
/// such an approach, and object lifetimes are somewhat unclear when the
/// responsibilities are separated into separate XrdSsiSession and
/// XrdSsiResponder classes.
class SsiSession : public XrdSsiSession, public XrdSsiResponder {
public:
    typedef std::shared_ptr<ResourceUnit::Checker> ValidatorPtr;
    typedef util::VoidCallable<void> CancelFunc;
    typedef std::shared_ptr<CancelFunc> CancelFuncPtr;

    /// Construct a new session (called by SsiService)
    SsiSession(char const* sname,
               ValidatorPtr validator,
               std::shared_ptr<wbase::MsgProcessor> processor)
        : XrdSsiSession(strdup(sname), 0),
          XrdSsiResponder(this, nullptr),
          _validator(validator),
          _processor(processor),
          _cancelled(false)
        {}

    virtual ~SsiSession() {
        // XrdSsiSession::sessName is unmanaged, need to free()
        if(sessName) { ::free(sessName); sessName = 0; }
    }

    // XrdSsiSession and XrdSsiResponder interfaces
    virtual void ProcessRequest(XrdSsiRequest* req, unsigned short timeout);
    virtual void RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                                 bool cancel=false);

    virtual bool Unprovision(bool forced);

private:

    void _addCanceller(CancelFuncPtr p);

    class ReplyChannel;
    friend class ReplyChannel;

    ValidatorPtr _validator; ///< validates request against what's available
    std::shared_ptr<wbase::MsgProcessor> _processor; ///< actual msg processor

    /// Stash of cancellation functions to be called to cancel msgs in flight on
    /// _processor.
    std::vector<CancelFuncPtr> _cancellers;
    std::mutex _cancelMutex; ///< For _cancellers and _cancelled
    bool _cancelled; ///< true if the session has been cancelled.

    friend class SsiProcessor; // Allow access for cancellation
}; // class SsiSession
}}} // namespace lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_SSISESSION_H
