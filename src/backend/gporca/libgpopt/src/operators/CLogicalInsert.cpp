//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInsert.cpp
//
//	@doc:
//		Implementation of logical Insert operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalInsert.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::CLogicalInsert
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalInsert::CLogicalInsert(CMemoryPool *mp)
	: CLogical(mp),
	  m_ptabdesc(NULL),
	  m_pdrgpcrSource(NULL),
	  m_pdrgpcrOutput(NULL)

{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::CLogicalInsert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInsert::CLogicalInsert(CMemoryPool *mp, CTableDescriptor *ptabdesc,
							   CColRefArray *pdrgpcrSource)
	: CLogical(mp),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrSource(pdrgpcrSource),
	  m_pdrgpcrOutput(NULL)

{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);

	m_pdrgpcrOutput =
		PdrgpcrCreateMapping(mp, ptabdesc->Pdrgpcoldesc(), UlOpId());

	m_pcrsLocalUsed->Include(m_pdrgpcrSource);
	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::CLogicalInsert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInsert::CLogicalInsert(CMemoryPool *mp, CTableDescriptor *ptabdesc,
							   CColRefArray *pdrgpcrSource,
							   CColRefArray *pdrgpcrOutput)
	: CLogical(mp),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrSource(pdrgpcrSource),
	  m_pdrgpcrOutput(pdrgpcrOutput)

{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);

	m_pcrsLocalUsed->Include(m_pdrgpcrSource);
	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::~CLogicalInsert
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalInsert::~CLogicalInsert()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrSource);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalInsert::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalInsert *popInsert = CLogicalInsert::PopConvert(pop);

	return m_ptabdesc->MDId()->Equals(popInsert->Ptabdesc()->MDId()) &&
		   m_pdrgpcrSource->Equals(popInsert->PdrgpcrSource()) &&
		   m_pdrgpcrOutput->Equals(popInsert->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalInsert::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrSource));

	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInsert::PopCopyWithRemappedColumns(CMemoryPool *mp,
										   UlongToColRefMap *colref_mapping,
										   BOOL must_exist)
{
	CColRefArray *colref_array =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrSource, colref_mapping, must_exist);

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
	m_ptabdesc->AddRef();

	return GPOS_NEW(mp)
		CLogicalInsert(mp, m_ptabdesc, colref_array, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalInsert::DeriveOutputColumns(CMemoryPool *mp,
									CExpressionHandle &	 //exprhdl
)
{
	CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp);
	pcrsOutput->Include(m_pdrgpcrSource);
	pcrsOutput->Include(m_pdrgpcrOutput);

	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalInsert::DeriveKeyCollection(CMemoryPool *mp,
									CExpressionHandle &	 // exprhdl
) const
{
	const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInsert::DeriveMaxCard(CMemoryPool *,  // mp
							  CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInsert::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfInsert2DML);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalInsert::PstatsDerive(CMemoryPool *,	 // mp,
							 CExpressionHandle &exprhdl,
							 IStatisticsArray *	 // not used
) const
{
	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalInsert::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Source Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrSource);
	os << "]";
	os << ", Output Columns: [";
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
