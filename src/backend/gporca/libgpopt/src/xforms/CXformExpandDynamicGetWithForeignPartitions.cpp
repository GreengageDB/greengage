//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CXformExpandDynamicGetWithForeignPartitions.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExpandDynamicGetWithForeignPartitions.h"

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalDynamicForeignGet.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDRelationGPDB.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandDynamicGetWithForeignPartitions::CXformExpandDynamicGetWithForeignPartitions
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandDynamicGetWithForeignPartitions::
	CXformExpandDynamicGetWithForeignPartitions(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalDynamicGet(mp)))
{
}

CXform::EXformPromise
CXformExpandDynamicGetWithForeignPartitions::Exfp(
	CExpressionHandle &exprhdl) const
{
	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(exprhdl.Pop());
	if (popGet->ContainsForeignParts())
	{
		return CXform::ExfpHigh;
	}

	// No need to run this xform if the relation being scanned does not
	// contain foreign partitions
	return CXform::ExfpNone;
}


// Expands a dynamic get with foreign partitions into CLogicalDynamicForeignGet(s) and a CLogicalDynamicGet
// This xform separates the DynamicGet into non-foreign partitions and foreign partitions grouped by the
// foreign server. If there are only foreign scans of a single server OID, this xform will produce only 1
// CLogicalDynamicForeignGet. However, if there are multiple servers, it will produce a UNION of these
// CLogicalDynamicForeignGets. Additionally, if there are any non-foreign partitions, these will also be
// in a CLogicalDynamicGet that is in this UNION.

// The physical plan created after this xform and the corresponding logical->physical xforms will create the following from
// Dynamic get containing both foreign and non-foreign tables.
//    +--CPhysicalSerialUnionAll ]
//       |--CPhysicalDynamicTableScan "part" ("part"), Columns: ["a" (9), "b" (10), Scan Id: 1 Parts to scan: 5
//       +--CPhysicalDynamicForeignScan "part" ("part"), Columns: ["a" (18), "b" (19)] Scan Id: 1 Parts to scan: 3]

void
CXformExpandDynamicGetWithForeignPartitions::Transform(CXformContext *pxfctxt,
													   CXformResult *pxfres,
													   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));
	GPOS_ASSERT(nullptr != pxfres);

	CMemoryPool *mp = pxfctxt->Pmp();
	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(pexpr->Pop());
	// only run this xform if it contains foreign partitions
	GPOS_ASSERT(popGet->ContainsForeignParts());

	IMdIdArray *foreign_server_mdids = popGet->ForeignServerMdIds();
	IMdIdArray *all_part_mdids = popGet->GetPartitionMdids();
	BOOL hasSecurityQuals = popGet->HasSecurityQuals();

	// create map from server-> (bitset of selected parts)
	SForeignServerToBitSetMap *foreign_server_to_bitset_map =
		GPOS_NEW(mp) SForeignServerToBitSetMap(mp);
	CBitSet *non_foreign_parts_set = GPOS_NEW(mp) CBitSet(mp);

	// iterate over all partitions. If it is not foreign, place in non_foreign_parts array,
	// otherwise place in foreign server map
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	for (ULONG ul = 0; ul < all_part_mdids->Size(); ++ul)
	{
		IMDId *foreign_server_mdid = (*foreign_server_mdids)[ul];

		IMDId *partMdid = (*all_part_mdids)[ul];
		partMdid->AddRef();
		// if partition is not foreign, add to non foreign array
		if (!foreign_server_mdid->IsValid())
		{
			non_foreign_parts_set->ExchangeSet(ul);
		}
		else
		{
			// this is a foreign partition. However, we need to separate it by the foreign server mdid,
			// as each server can have a different distribution derivation
			// (some foreign tables can only be executed on segments, others only the coordinator)
			// place these in a map from server->bitset of foreign partitions
			const IMDRelation *pmdrel = md_accessor->RetrieveRel(partMdid);

			OID foreign_server_oid =
				CMDIdGPDB::CastMdid(foreign_server_mdid)->Oid();
			SForeignServer foreign_server_lookup = {
				foreign_server_oid, pmdrel->GetRelDistribution()};
			const CBitSet *foreign_server =
				foreign_server_to_bitset_map->Find(&foreign_server_lookup);
			if (nullptr == foreign_server)
			{
				// create array for foreign server and insert
				CBitSet *part_set = GPOS_NEW(mp) CBitSet(mp);
				part_set->ExchangeSet(ul);
				BOOL fres GPOS_ASSERTS_ONLY =
					foreign_server_to_bitset_map->Insert(
						GPOS_NEW(mp) SForeignServer{
							foreign_server_oid, pmdrel->GetRelDistribution()},
						part_set);
				GPOS_ASSERT(fres);
			}
			else
			{
				// array for foreign server already exists in map, just append
				(const_cast<CBitSet *>(foreign_server))->ExchangeSet(ul);
			}
		}
	}

	BOOL no_union_all = foreign_server_to_bitset_map->GetKeys()->Size() == 1 &&
						non_foreign_parts_set->Size() == 0;

	// By this point we have an array of non-foreign parts and a map from foreign_server->(arry of parts)
	// Now we can create the DynamicGet operators and union them if necessary.
	// We need a union if there is any non-foreign part, or multiple different servers
	CExpressionArray *expressionsForUnion = GPOS_NEW(mp) CExpressionArray(mp);
	CColRef2dArray *inputColArrayForUnion = GPOS_NEW(mp) CColRef2dArray(mp);

	// Create a regular dynamic from the non-foreign partitions and add to union all expression array
	if (non_foreign_parts_set->Size() > 0)
	{
		all_part_mdids->AddRef();
		popGet->Ptabdesc()->AddRef();
		popGet->PdrgpdrgpcrPart()->AddRef();
		CName *new_alias = GPOS_NEW(mp) CName(mp, popGet->Name());
		// This will be null if no static partitioning was performed
		if (popGet->GetPartitionConstraintsDisj())
		{
			popGet->GetPartitionConstraintsDisj()->AddRef();
		}
		CColRefArray *pdrgpcrNew =
			CUtils::PdrgpcrCopy(mp, popGet->PdrgpcrOutput());
		CLogicalDynamicGet *nonForeignDynamicGet =
			GPOS_NEW(mp) CLogicalDynamicGet(
				mp, new_alias, popGet->Ptabdesc(), popGet->ScanId(), pdrgpcrNew,
				popGet->PdrgpdrgpcrPart(), all_part_mdids,
				popGet->GetPartitionConstraintsDisj(), popGet->FStaticPruned(),
				GPOS_NEW(mp) IMdIdArray(mp) /* foreign_server_mdids */,
				non_foreign_parts_set, hasSecurityQuals);
		CExpression *pexprNonForeignDynamicGet =
			GPOS_NEW(mp) CExpression(mp, nonForeignDynamicGet);

		// addref for use in union all
		nonForeignDynamicGet->PdrgpcrOutput()->AddRef();
		// add to union all arrays
		inputColArrayForUnion->Append(nonForeignDynamicGet->PdrgpcrOutput());
		expressionsForUnion->Append(pexprNonForeignDynamicGet);
	}
	else
	{
		non_foreign_parts_set->Release();
	}

	// loop over each key in the map, create a DynamicForeignGet for each
	// foreign server using the selected parts bitset
	SForeignServerToCBitSetIter map_iter(foreign_server_to_bitset_map);

	while (map_iter.Advance())
	{
		SForeignServer foreign_server = *(map_iter.Key());
		CBitSet *selected_parts_set = const_cast<CBitSet *>(map_iter.Value());
		selected_parts_set->AddRef();
		popGet->Ptabdesc()->AddRef();
		popGet->PdrgpdrgpcrPart()->AddRef();
		CName *new_alias = GPOS_NEW(mp) CName(mp, popGet->Name());

		CColRefArray *pdrgpcrNew;
		// Only generate new colrefs if we're creating a union all, otherwise we can use existing output colrefs
		if (no_union_all)
		{
			popGet->PdrgpcrOutput()->AddRef();
			pdrgpcrNew = popGet->PdrgpcrOutput();
		}
		else
		{
			pdrgpcrNew = CUtils::PdrgpcrCopy(mp, popGet->PdrgpcrOutput());
		}
		all_part_mdids->AddRef();
		CLogicalDynamicForeignGet *dynamicForeignGet =
			GPOS_NEW(mp) CLogicalDynamicForeignGet(
				mp, new_alias, popGet->Ptabdesc(), popGet->ScanId(), pdrgpcrNew,
				popGet->PdrgpdrgpcrPart(), all_part_mdids, selected_parts_set,
				foreign_server.m_foreign_server_oid,
				foreign_server.m_exec_location);
		CExpression *pexprDynamicForeignGet =
			GPOS_NEW(mp) CExpression(mp, dynamicForeignGet);

		if (no_union_all)
		{
			// no union needed, just return the dynamicForeignGet
			expressionsForUnion->Release();
			inputColArrayForUnion->Release();
			pxfres->Add(pexprDynamicForeignGet);
			foreign_server_to_bitset_map->Release();
			return;
		}
		else
		{
			dynamicForeignGet->PdrgpcrOutput()->AddRef();
			inputColArrayForUnion->Append(dynamicForeignGet->PdrgpcrOutput());
			expressionsForUnion->Append(pexprDynamicForeignGet);
		}
	}

	foreign_server_to_bitset_map->Release();

	// Create a UNION ALL node above the gets
	popGet->PdrgpcrOutput()->AddRef();
	CExpression *pexprResult = GPOS_NEW(mp)
		CExpression(mp,
					GPOS_NEW(mp) CLogicalUnionAll(mp, popGet->PdrgpcrOutput(),
												  inputColArrayForUnion),
					expressionsForUnion);
	// add alternative to transformation result
	pxfres->Add(pexprResult);
}


// EOF
