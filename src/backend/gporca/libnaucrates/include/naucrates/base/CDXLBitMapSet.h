//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2026 Greengage community.
//
//	@filename:
//		CDXLBitMapSet.h
//
//	@doc:
//		providing methods for serialization CDXLBitSet to DXL
//---------------------------------------------------------------------------

#ifndef GPOPT_MAIN_CDXLBITMAPSET_H
#define GPOPT_MAIN_CDXLBITMAPSET_H

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "gpos/common/CBitSet.h"

namespace gpnaucrates
{
using namespace gpos;
using namespace gpdxl;


class CDXLBitMapSet final
{
public:
	CDXLBitMapSet() = delete;
	CDXLBitMapSet(CDXLBitMapSet&) = delete;
	CDXLBitMapSet(CDXLBitMapSet&&) = delete;
	~CDXLBitMapSet() = delete;
	
	static void SerializeToDXL(CMemoryPool *m_mp,
							   CXMLSerializer *xml_serializer,
							   const CWStringConst *strElementToken,
							   const CBitSet *set);
};

}

#endif	//GPOPT_MAIN_CDXLBITMAPSET_H
