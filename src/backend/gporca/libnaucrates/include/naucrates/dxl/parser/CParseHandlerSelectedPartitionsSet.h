//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2026 Greengage community.
//
//	@filename:
//		CParseHandlerSelectedPartitionsSet.cpp
//
//	@doc:
//		SAX parse handler class for parsing selected partitions
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSelectedPartitionsSet_H
#define GPDXL_CParseHandlerSelectedPartitionsSet_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;


class CParseHandlerSelectedPartitionsSet : public CParseHandlerBase
{
private:
	CBitSet* m_selected_parts;
	
	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
		) override;
	
	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
		) override;
	
public:
	CParseHandlerSelectedPartitionsSet(const CParseHandlerSelectedPartitionsSet &) = delete;
	
	// ctor/dtor
	CParseHandlerSelectedPartitionsSet(CMemoryPool *mp,
								CParseHandlerManager *parse_handler_mgr,
								CParseHandlerBase *parse_handler_base);

	~CParseHandlerSelectedPartitionsSet() override;
	
	CBitSet *
	GetSelectedParts() const
	{
		return m_selected_parts;
	}
};

}

#endif //GPDXL_CParseHandlerSelectedPartitionsSet_H
