//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2026 Greengage community.
//
//	@filename:
//		CDXLBitMapSet.cpp
//
//	@doc:
//		Implementation of CBitMap serialization to the DXL
//---------------------------------------------------------------------------

#include "naucrates/base/CDXLBitMapSet.h"

#include "gpos/common/CBitSetIter.h"

using namespace gpos;
using namespace gpdxl;

namespace gpnaucrates
{
void
CDXLBitMapSet::SerializeToDXL(CMemoryPool *m_mp, CXMLSerializer *xml_serializer,
							  const CWStringConst *strElementToken,
							  const CBitSet *set)
{
	GPOS_ASSERT(0 != set->Size());

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), strElementToken);

	CWStringDynamic *str = GPOS_NEW(m_mp) CWStringDynamic(m_mp);
	CBitSetIter bsiter(*set);
	bsiter.Advance();

	if (set->Size() == 1)
	{
		str->AppendFormat(GPOS_WSZ_LIT("%d"), bsiter.Bit());
	}
	else
	{
		ULONG rangeStart = bsiter.Bit();
		ULONG rangeEnd = bsiter.Bit();

		while (bsiter.Advance())
		{
			auto value = bsiter.Bit();
			if (value - rangeEnd == 1)
			{
				rangeEnd = value;
			}
			else
			{
				if (rangeEnd - rangeStart == 0)
				{
					str->AppendFormat(GPOS_WSZ_LIT("%d,"), rangeStart);
				}
				else
				{
					str->AppendFormat(GPOS_WSZ_LIT("%d:%d,"), rangeStart,
									  rangeEnd);
				}

				rangeStart = value;
				rangeEnd = value;
			}
		}

		if (rangeEnd - rangeStart == 0)
		{
			str->AppendFormat(GPOS_WSZ_LIT("%d"), rangeStart);
		}
		else
		{
			str->AppendFormat(GPOS_WSZ_LIT("%d:%d"), rangeStart, rangeEnd);
		}
	}

	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenValue),
								 str);

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix), strElementToken);
	GPOS_DELETE(str);
}

}  // namespace gpnaucrates
