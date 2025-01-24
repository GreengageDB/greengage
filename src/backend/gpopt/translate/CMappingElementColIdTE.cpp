//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingElementColIdTE.cpp
//
//	@doc:
//		Implementation of the functions that provide the mapping from CDXLNode to
//		Var during DXL->Query translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/translate/CMappingElementColIdTE.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMappingElementColIdTE::CMappingElementColIdTE
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingElementColIdTE::CMappingElementColIdTE(ULONG colid, ULONG query_level,
											   TargetEntry *target_entry)
	: m_colid(colid), m_query_level(query_level), m_target_entry(target_entry)
{
}

// EOF
