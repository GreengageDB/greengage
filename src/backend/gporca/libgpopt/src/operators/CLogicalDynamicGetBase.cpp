//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalDynamicGetBase.cpp
//
//	@doc:
//		Implementation of dynamic table access base class
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalDynamicGetBase.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(CMemoryPool *mp)
	: CLogical(mp),
	  m_pnameAlias(nullptr),
	  m_ptabdesc(GPOS_NEW(mp) CTableDescriptorHashSet(mp)),
	  m_scan_id(0),
	  m_pdrgpcrOutput(nullptr),
	  m_pdrgpdrgpcrPart(nullptr),
	  m_pcrsDist(nullptr)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(
	CMemoryPool *mp, const CName *pnameAlias, CTableDescriptor *ptabdesc,
	ULONG scan_id, CColRefArray *pdrgpcrOutput, CColRef2dArray *pdrgpdrgpcrPart,
	IMdIdArray *partition_mdids)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(GPOS_NEW(mp) CTableDescriptorHashSet(mp)),
	  m_scan_id(scan_id),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pdrgpdrgpcrPart(pdrgpdrgpcrPart),
	  m_pcrsDist(nullptr),
	  m_partition_mdids(partition_mdids)

{
	GPOS_ASSERT(nullptr != ptabdesc);
	GPOS_ASSERT(nullptr != pnameAlias);
	GPOS_ASSERT(nullptr != pdrgpcrOutput);
	GPOS_ASSERT(nullptr != pdrgpdrgpcrPart);

	m_ptabdesc->Insert(ptabdesc);

	m_pcrsDist = CLogical::PcrsDist(mp, Ptabdesc(), m_pdrgpcrOutput);
	m_root_col_mapping_per_part =
		ConstructRootColMappingPerPart(mp, m_pdrgpcrOutput, m_partition_mdids);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::CLogicalDynamicGetBase
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::CLogicalDynamicGetBase(CMemoryPool *mp,
											   const CName *pnameAlias,
											   CTableDescriptor *ptabdesc,
											   ULONG scan_id,
											   IMdIdArray *partition_mdids)
	: CLogical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(GPOS_NEW(mp) CTableDescriptorHashSet(mp)),
	  m_scan_id(scan_id),
	  m_pdrgpcrOutput(nullptr),
	  m_pcrsDist(nullptr),
	  m_partition_mdids(partition_mdids)
{
	GPOS_ASSERT(nullptr != ptabdesc);
	GPOS_ASSERT(nullptr != pnameAlias);

	m_ptabdesc->Insert(ptabdesc);

	// generate a default column set for the table descriptor
	m_pdrgpcrOutput = PdrgpcrCreateMapping(mp, Ptabdesc()->Pdrgpcoldesc(),
										   UlOpId(), Ptabdesc()->MDId());
	m_pdrgpdrgpcrPart = PdrgpdrgpcrCreatePartCols(mp, m_pdrgpcrOutput,
												  Ptabdesc()->PdrgpulPart());
	m_pcrsDist = CLogical::PcrsDist(mp, Ptabdesc(), m_pdrgpcrOutput);

	m_root_col_mapping_per_part =
		ConstructRootColMappingPerPart(mp, m_pdrgpcrOutput, m_partition_mdids);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::~CLogicalDynamicGetBase
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicGetBase::~CLogicalDynamicGetBase()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrPart);
	CRefCount::SafeRelease(m_partition_mdids);
	CRefCount::SafeRelease(m_root_col_mapping_per_part);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDynamicGetBase::DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalDynamicGetBase::DeriveKeyCollection(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
) const
{
	const CBitSetArray *pdrgpbs = Ptabdesc()->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalDynamicGetBase::DerivePropertyConstraint(CMemoryPool *mp,
												 CExpressionHandle &  // exprhdl
) const
{
	return PpcDeriveConstraintFromTable(mp, Ptabdesc(), m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicGetBase::PpartinfoDerive
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalDynamicGetBase::DerivePartitionInfo(CMemoryPool *mp,
											CExpressionHandle &	 // exprhdl
) const
{
	IMDId *mdid = Ptabdesc()->MDId();
	mdid->AddRef();
	m_pdrgpdrgpcrPart->AddRef();

	CPartInfo *ppartinfo = GPOS_NEW(mp) CPartInfo(mp);
	ppartinfo->AddPartConsumer(mp, m_scan_id, mdid, m_pdrgpdrgpcrPart);

	return ppartinfo;
}

// Construct a mapping from each column in root table to an index in each child
// partition's table descr by matching column names For each partition, this
// iterates over each child partition and compares the column names and creates
// a mapping. In the common case, the root and child partition's columns have
// the same colref. However, if they've been dropped/swapped, the mapping will
// be different. This method is fairly expensive, as it's building multiple hashmaps
// and ends up getting called from a few different places in the codebase.
ColRefToUlongMapArray *
CLogicalDynamicGetBase::ConstructRootColMappingPerPart(
	CMemoryPool *mp, CColRefArray *root_cols, IMdIdArray *partition_mdids)
{
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	ColRefToUlongMapArray *part_maps = GPOS_NEW(mp) ColRefToUlongMapArray(mp);

	// Build hashmap of colname to the index
	ColNameToIndexMap *root_mapping = GPOS_NEW(mp) ColNameToIndexMap(mp);
	for (ULONG i = 0; i < root_cols->Size(); ++i)
	{
		CColRef *root_colref = (*root_cols)[i];
		root_mapping->Insert(root_colref->Name().Pstr(), GPOS_NEW(mp) ULONG(i));
	}

	for (ULONG ul = 0; ul < partition_mdids->Size(); ++ul)
	{
		IMDId *part_mdid = (*partition_mdids)[ul];
		const IMDRelation *partrel = mda->RetrieveRel(part_mdid);

		GPOS_ASSERT(nullptr != partrel);

		ColRefToUlongMap *mapping = GPOS_NEW(mp) ColRefToUlongMap(mp);
		// The root mapping cannot contain dropped columns, but may be
		// in a different order than the child cols.Iterate through each of the child
		// cols, and retrieve the corresponding index in the parent table
		for (ULONG j = 0; j < partrel->ColumnCount(); ++j)
		{
			const IMDColumn *coldesc = partrel->GetMdCol(j);
			const CWStringConst *colname = coldesc->Mdname().GetMDName();

			if (coldesc->IsDropped())
			{
				continue;
			}

			ULONG *root_idx = root_mapping->Find(colname);
			if (nullptr != root_idx)
			{
				mapping->Insert((*root_cols)[*root_idx],
								GPOS_NEW(mp) ULONG(*root_idx));
			}
			else
			{
				root_mapping->Release();
				GPOS_RAISE(
					CException::ExmaInvalid, CException::ExmiInvalid,
					GPOS_WSZ_LIT(
						"Cannot generate root to child partition column mapping"));
			}
		}
		part_maps->Append(mapping);
	}
	root_mapping->Release();
	return part_maps;
}
