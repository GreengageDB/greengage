//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2026 Greengage community.
//
//	@filename:
//		CParseHandlerSelectedPartitionsSet.cpp
//
//	@doc:
//		Implementation of the parse handler class for selected partitions
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerSelectedPartitionsSet.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"


CParseHandlerSelectedPartitionsSet::CParseHandlerSelectedPartitionsSet(
	CMemoryPool *mp, CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_base)
	: CParseHandlerBase(mp, parse_handler_mgr, parse_handler_base),
	  m_selected_parts(nullptr)
{
}

CParseHandlerSelectedPartitionsSet::~CParseHandlerSelectedPartitionsSet()
{
	m_selected_parts->Release();
}

void
gpdxl::CParseHandlerSelectedPartitionsSet::StartElement(
	const XMLCh *const,	 // element_uri,
	const XMLCh *const element_local_name,
	const XMLCh *const,	 // element_qname,
	const Attributes &attr)
{
	if (0 == XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenSelectedPartitionSet),
				 element_local_name))
	{
		GPOS_ASSERT(1 == attr.getLength());
		
		m_selected_parts = GPOS_NEW(m_mp) CBitSet(m_mp);
		
		const XMLCh *attribute_value =
			attr.getValue(CDXLTokens::XmlstrToken(EdxltokenValue));
		
		// Get range values
		XMLStringTokenizer bits_components(
			attribute_value, CDXLTokens::XmlstrToken(EdxltokenComma));
		const ULONG num_tokens = bits_components.countTokens();
		
		for (ULONG ul = 0; ul < num_tokens; ul++)
		{
			XMLCh *bitRange = bits_components.nextToken();
			// Single value range
			if(XMLString::indexOf(bitRange, ':') == -1)
			{
				m_selected_parts->ExchangeSet(
					CDXLOperatorFactory::ConvertAttrValueToInt(
						m_parse_handler_mgr->GetDXLMemoryManager(), bitRange, 
						EdxltokenSelectedPartitionSet, EdxltokenValue)
					);
			}
			else
			{
				XMLStringTokenizer range_components(
					bitRange, CDXLTokens::XmlstrToken(EdxltokenColon));
				if(range_components.countTokens() != 2)
				{
					GPOS_RAISE(gpdxl::ExmaDXL,
							   gpdxl::ExmiDXLInvalidAttributeValue,
							   CDXLTokens::GetDXLTokenStr(EdxltokenSelectedPartitionSet)->GetBuffer(),
							   CDXLTokens::GetDXLTokenStr(EdxltokenValue)->GetBuffer());
				}
				INT rangeStart = CDXLOperatorFactory::ConvertAttrValueToInt(
					m_parse_handler_mgr->GetDXLMemoryManager(),
					range_components.nextToken(),
					EdxltokenSelectedPartitionSet, EdxltokenValue);
				INT rangeEnd = CDXLOperatorFactory::ConvertAttrValueToInt(
					m_parse_handler_mgr->GetDXLMemoryManager(),
					range_components.nextToken(),
					EdxltokenSelectedPartitionSet, EdxltokenValue);
				for(INT i = rangeStart; i <= rangeEnd; i++)
				{
					m_selected_parts->ExchangeSet(i);
				}
			}
		}
	}
	else
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}
}

void
gpdxl::CParseHandlerSelectedPartitionsSet::EndElement(
	const XMLCh *const,	 // element_uri,
	const XMLCh *const element_local_name,
	const XMLCh *const	// element_qname
)
{
	if (0 != XMLString::compareString(
				 CDXLTokens::XmlstrToken(EdxltokenSelectedPartitionSet),
				 element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag,
				   str->GetBuffer());
	}
//	m_selected_parts->AddRef();
	m_parse_handler_mgr->DeactivateHandler();
}
