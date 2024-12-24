//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDProviderRelcache.cpp
//
//	@doc:
//		Implementation of a relcache-based metadata provider, which uses GPDB's
//		relcache to lookup objects given their ids.
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "gpopt/relcache/CMDProviderRelcache.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/utils/gpdbdefs.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

CWStringBase *
CMDProviderRelcache::GetMDObjDXLStr(CMemoryPool *mp GPOS_UNUSED,
									CMDAccessor *md_accessor GPOS_UNUSED,
									IMDId *md_id GPOS_UNUSED) const
{
	// not used
	return nullptr;
}

// return the requested metadata object
IMDCacheObject *
CMDProviderRelcache::GetMDObj(CMemoryPool *mp, CMDAccessor *md_accessor,
							  IMDId *mdid, IMDCacheObject::Emdtype mdtype) const
{
	IMDCacheObject *md_obj =
		CTranslatorRelcacheToDXL::RetrieveObject(mp, md_accessor, mdid, mdtype);
	GPOS_ASSERT(nullptr != md_obj);

	return md_obj;
}

// EOF
