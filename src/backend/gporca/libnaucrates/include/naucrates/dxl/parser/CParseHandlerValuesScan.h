//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerValuesScan.h
//
//	@doc:
//		SAX parse handler class for parsing ValuesScan operator nodes.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerValuesScan_H
#define GPDXL_CParseHandlerValuesScan_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysicalValuesScan.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"


namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerValuesScan
//
//	@doc:
//		Parse handler for parsing a ValuesScan operator
//
//---------------------------------------------------------------------------
class CParseHandlerValuesScan : public CParseHandlerPhysicalOp
{
private:
	// the ValuesScan operator
	CDXLPhysicalValuesScan *m_dxl_op;

	// private copy ctor
	CParseHandlerValuesScan(const CParseHandlerValuesScan &);

	// set up initial handlers
	void SetupInitialHandlers();

	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
	);

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
	);

public:
	// ctor
	CParseHandlerValuesScan(CMemoryPool *mp,
							CParseHandlerManager *parse_handler_mgr,
							CParseHandlerBase *parse_handler_root);
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerValuesScan_H

// EOF
