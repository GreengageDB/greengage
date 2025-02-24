//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDProvider.cpp
//
//	@doc:
//		Abstract class for retrieving metadata from an external location
//---------------------------------------------------------------------------

#include "naucrates/md/IMDProvider.h"

#include "naucrates/md/CMDIdGPDB.h"

using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDProvider::GetGPDBTypeMdid
//
//	@doc:
//		Return the mdid for the requested type
//
//---------------------------------------------------------------------------
IMDId *
IMDProvider::GetGPDBTypeMdid(CMemoryPool *mp,
							 CSystemId
#ifdef GPOS_DEBUG
								 sysid
#endif	// GPOS_DEBUG
							 ,
							 IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDId::EmdidGeneral == sysid.MdidType());
	GPOS_ASSERT(IMDType::EtiGeneric > type_info);

	switch (type_info)
	{
		case IMDType::EtiInt2:
			return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_INT2);

		case IMDType::EtiInt4:
			return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_INT4);

		case IMDType::EtiInt8:
			return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_INT8);

		case IMDType::EtiBool:
			return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_BOOL);

		case IMDType::EtiOid:
			return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_OID);

		default:
			return NULL;
	}
}

// EOF
