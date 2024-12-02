//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDelete.cpp
//
//	@doc:
//		Implementation of logical Delete operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalDelete.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::CLogicalDelete
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDelete::CLogicalDelete(CMemoryPool *mp)
	: CLogical(mp),
	  m_ptabdesc(NULL),
	  m_pdrgpcr(NULL),
	  m_pdrgpcrOutput(NULL),
	  m_pcrCtid(NULL),
	  m_pcrSegmentId(NULL),
	  m_pcrTableOid(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::CLogicalDelete
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDelete::CLogicalDelete(CMemoryPool *mp, CTableDescriptor *ptabdesc,
							   CColRefArray *colref_array, CColRef *pcrCtid,
							   CColRef *pcrSegmentId, CColRef *pcrTableOid)
	: CLogical(mp),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcr(colref_array),
	  m_pdrgpcrOutput(NULL),
	  m_pcrCtid(pcrCtid),
	  m_pcrSegmentId(pcrSegmentId),
	  m_pcrTableOid(pcrTableOid)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);

	m_pdrgpcrOutput =
		PdrgpcrCreateMapping(mp, ptabdesc->Pdrgpcoldesc(), UlOpId());

	m_pcrsLocalUsed->Include(m_pdrgpcr);
	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);
	if (NULL != m_pcrTableOid)
	{
		m_pcrsLocalUsed->Include(m_pcrTableOid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::CLogicalDelete
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDelete::CLogicalDelete(CMemoryPool *mp, CTableDescriptor *ptabdesc,
							   CColRefArray *colref_array,
							   CColRefArray *pdrgpcrOutput, CColRef *pcrCtid,
							   CColRef *pcrSegmentId, CColRef *pcrTableOid)
	: CLogical(mp),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcr(colref_array),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pcrCtid(pcrCtid),
	  m_pcrSegmentId(pcrSegmentId),
	  m_pcrTableOid(pcrTableOid)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);

	m_pcrsLocalUsed->Include(m_pdrgpcr);
	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);
	if (NULL != m_pcrTableOid)
	{
		m_pcrsLocalUsed->Include(m_pcrTableOid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::~CLogicalDelete
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalDelete::~CLogicalDelete()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalDelete::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalDelete *popDelete = CLogicalDelete::PopConvert(pop);

	return m_pcrCtid == popDelete->PcrCtid() &&
		   m_pcrSegmentId == popDelete->PcrSegmentId() &&
		   m_pcrTableOid == popDelete->PcrTableOid() &&
		   m_ptabdesc->MDId()->Equals(popDelete->Ptabdesc()->MDId()) &&
		   m_pdrgpcr->Equals(popDelete->Pdrgpcr()) &&
		   m_pdrgpcrOutput->Equals(popDelete->PdrgpcrOutput());
}
//---------------------------------------------------------------------------

//	@function:
//		CLogicalDelete::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDelete::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));
	ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrCtid));
	ulHash =
		gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrSegmentId));
	ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(m_pcrTableOid));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalDelete::PopCopyWithRemappedColumns(CMemoryPool *mp,
										   UlongToColRefMap *colref_mapping,
										   BOOL must_exist)
{
	CColRefArray *colref_array =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcr, colref_mapping, must_exist);

	CColRefArray *pdrgpcrOutput = NULL;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}

	CColRef *pcrCtid = CUtils::PcrRemap(m_pcrCtid, colref_mapping, must_exist);
	CColRef *pcrSegmentId =
		CUtils::PcrRemap(m_pcrSegmentId, colref_mapping, must_exist);

	CColRef *pcrTableOid = NULL;
	if (NULL != m_pcrTableOid)
	{
		pcrTableOid =
			CUtils::PcrRemap(m_pcrTableOid, colref_mapping, must_exist);
	}

	return GPOS_NEW(mp)
		CLogicalDelete(mp, m_ptabdesc, colref_array, pdrgpcrOutput, pcrCtid,
					   pcrSegmentId, pcrTableOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDelete::DeriveOutputColumns(CMemoryPool *mp,
									CExpressionHandle &	 //exprhdl
)
{
	CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp);
	pcrsOutput->Include(m_pdrgpcr);
	pcrsOutput->Include(m_pdrgpcrOutput);

	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalDelete::DeriveKeyCollection(CMemoryPool *mp,
									CExpressionHandle &	 // exprhdl
) const
{
	const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalDelete::DeriveMaxCard(CMemoryPool *,  // mp
							  CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalDelete::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfDelete2DML);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalDelete::PstatsDerive(CMemoryPool *,	 // mp,
							 CExpressionHandle &exprhdl,
							 IStatisticsArray *	 // not used
) const
{
	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDelete::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Deleted Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "], ";
	m_pcrCtid->OsPrint(os);
	os << ", ";
	m_pcrSegmentId->OsPrint(os);
	os << ", ";
	if (NULL != m_pcrTableOid)
	{
		m_pcrTableOid->OsPrint(os);
		os << ", ";
	}

	os << "Output Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] Key sets: {";

	const ULONG ulColumns = m_pdrgpcrOutput->Size();
	const CBitSetArray *pdrgpbsKeys = m_ptabdesc->PdrgpbsKeys();
	for (ULONG ul = 0; ul < pdrgpbsKeys->Size(); ul++)
	{
		CBitSet *pbs = (*pdrgpbsKeys)[ul];
		if (0 < ul)
		{
			os << ", ";
		}
		os << "[";
		ULONG ulPrintedKeys = 0;
		for (ULONG ulKey = 0; ulKey < ulColumns; ulKey++)
		{
			if (pbs->Get(ulKey))
			{
				if (0 < ulPrintedKeys)
				{
					os << ",";
				}
				os << ulKey;
				ulPrintedKeys++;
			}
		}
		os << "]";
	}
	os << "}";

	return os;
}

// EOF
