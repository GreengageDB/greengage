//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CReqdPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------

#include "gpopt/base/CReqdPropPlan.h"

#include "gpos/base.h"
#include "gpos/common/CPrintablePointer.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CEnfdDistribution.h"
#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/CEnfdPartitionPropagation.h"
#include "gpopt/base/CEnfdRewindability.h"
#include "gpopt/base/CPartFilterMap.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CPartInfo.h"
#include "gpopt/base/CPartitionPropagationSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/search/CGroupExpression.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan(CColRefSet *pcrs, CEnfdOrder *peo,
							 CEnfdDistribution *ped, CEnfdRewindability *per,
							 CCTEReq *pcter)
	: m_pcrs(pcrs),
	  m_peo(peo),
	  m_ped(ped),
	  m_per(per),
	  m_pepp(NULL),
	  m_pcter(pcter)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(NULL != ped);
	GPOS_ASSERT(NULL != per);
	GPOS_ASSERT(NULL != pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan(CColRefSet *pcrs, CEnfdOrder *peo,
							 CEnfdDistribution *ped, CEnfdRewindability *per,
							 CEnfdPartitionPropagation *pepp, CCTEReq *pcter)
	: m_pcrs(pcrs),
	  m_peo(peo),
	  m_ped(ped),
	  m_per(per),
	  m_pepp(pepp),
	  m_pcter(pcter)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(NULL != ped);
	GPOS_ASSERT(NULL != per);
	GPOS_ASSERT(NULL != pepp);
	GPOS_ASSERT(NULL != pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::~CReqdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropPlan::~CReqdPropPlan()
{
	CRefCount::SafeRelease(m_pcrs);
	CRefCount::SafeRelease(m_peo);
	CRefCount::SafeRelease(m_ped);
	CRefCount::SafeRelease(m_per);
	CRefCount::SafeRelease(m_pepp);
	CRefCount::SafeRelease(m_pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::ComputeReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdProp *prpInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL == m_pcrs);

	CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcrs =
		popPhysical->PcrsRequired(mp, exprhdl, prppInput->PcrsRequired(),
								  child_index, pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCTEs
//
//	@doc:
//		Compute required CTEs
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::ComputeReqdCTEs(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdProp *prpInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt)
{
	GPOS_ASSERT(NULL == m_pcter);

	CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcter =
		popPhysical->PcteRequired(mp, exprhdl, prppInput->Pcter(), child_index,
								  pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::Compute(CMemoryPool *mp, CExpressionHandle &exprhdl,
					   CReqdProp *prpInput, ULONG child_index,
					   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_CHECK_ABORT;

	CReqdPropPlan *prppInput = CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	ComputeReqdCols(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);
	ComputeReqdCTEs(mp, exprhdl, prpInput, child_index, pdrgpdpCtxt);
	CPartFilterMap *ppfmDerived =
		PpfmCombineDerived(mp, exprhdl, prppInput, child_index, pdrgpdpCtxt);

	ULONG ulOrderReq = 0;
	ULONG ulDistrReq = 0;
	ULONG ulRewindReq = 0;
	ULONG ulPartPropagateReq = 0;
	popPhysical->LookupRequest(ulOptReq, &ulOrderReq, &ulDistrReq, &ulRewindReq,
							   &ulPartPropagateReq);

	m_peo = GPOS_NEW(mp) CEnfdOrder(
		popPhysical->PosRequired(mp, exprhdl, prppInput->Peo()->PosRequired(),
								 child_index, pdrgpdpCtxt, ulOrderReq),
		popPhysical->Eom(prppInput, child_index, pdrgpdpCtxt, ulOrderReq));

	m_ped = popPhysical->Ped(mp, exprhdl, prppInput, child_index, pdrgpdpCtxt,
							 ulDistrReq);

	GPOS_ASSERT(
		CDistributionSpec::EdtUniversal != m_ped->PdsRequired()->Edt() &&
		"CDistributionSpecUniversal is a derive-only, cannot be required");

	m_per = GPOS_NEW(mp) CEnfdRewindability(
		popPhysical->PrsRequired(mp, exprhdl, prppInput->Per()->PrsRequired(),
								 child_index, pdrgpdpCtxt, ulRewindReq),
		popPhysical->Erm(prppInput, child_index, pdrgpdpCtxt, ulRewindReq));

	m_pepp = GPOS_NEW(mp) CEnfdPartitionPropagation(
		popPhysical->PppsRequired(mp, exprhdl,
								  prppInput->Pepp()->PppsRequired(),
								  child_index, pdrgpdpCtxt, ulPartPropagateReq),
		CEnfdPartitionPropagation::EppmSatisfy, ppfmDerived);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PpfmCombineDerived
//
//	@doc:
//		Combine derived part filter map from input requirements and
//		derived plan properties in the passed context
//
//---------------------------------------------------------------------------
CPartFilterMap *
CReqdPropPlan::PpfmCombineDerived(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CReqdPropPlan *prppInput, ULONG child_index,
								  CDrvdPropArray *pdrgpdpCtxt)
{
	// get partitioning info below required child
	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(child_index);
	const ULONG ulConsumers = ppartinfo->UlConsumers();

	CPartFilterMap *ppfmDerived = GPOS_NEW(mp) CPartFilterMap(mp);

	// a bit set of found scan id's with part filters
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp);

	// copy part filters from input requirements
	for (ULONG ul = 0; ul < ulConsumers; ul++)
	{
		ULONG scan_id = ppartinfo->ScanId(ul);
		BOOL fCopied = ppfmDerived->FCopyPartFilter(
			mp, scan_id, prppInput->Pepp()->PpfmDerived(), NULL);
		if (fCopied)
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				pbs->ExchangeSet(scan_id);
			GPOS_ASSERT(!fSet);
		}
	}

	// copy part filters from previously optimized children
	const ULONG size = pdrgpdpCtxt->Size();
	for (ULONG ulDrvdProps = 0; ulDrvdProps < size; ulDrvdProps++)
	{
		CDrvdPropPlan *pdpplan =
			CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[ulDrvdProps]);
		for (ULONG ul = 0; ul < ulConsumers; ul++)
		{
			ULONG scan_id = ppartinfo->ScanId(ul);
			BOOL fFound = pbs->Get(scan_id);

			if (!fFound)
			{
				BOOL fCopied = ppfmDerived->FCopyPartFilter(
					mp, scan_id, pdpplan->Ppfm(), NULL);
				if (fCopied)
				{
#ifdef GPOS_DEBUG
					BOOL fSet =
#endif	// GPOS_DEBUG
						pbs->ExchangeSet(scan_id);
					GPOS_ASSERT(!fSet);
				}
			}
		}
	}

	pbs->Release();

	return ppfmDerived;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::InitReqdPartitionPropagation
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::InitReqdPartitionPropagation(CMemoryPool *mp,
											CPartInfo *ppartinfo)
{
	GPOS_ASSERT(NULL == m_pepp &&
				"Required Partition Propagation has been initialized already");

	CPartIndexMap *ppim = GPOS_NEW(mp) CPartIndexMap(mp);

	CEnfdPartitionPropagation::EPartitionPropagationMatching eppm =
		CEnfdPartitionPropagation::EppmSatisfy;
	for (ULONG ul = 0; ul < ppartinfo->UlConsumers(); ul++)
	{
		ULONG scan_id = ppartinfo->ScanId(ul);
		IMDId *mdid = ppartinfo->GetRelMdId(ul);
		CPartKeysArray *pdrgppartkeys = ppartinfo->Pdrgppartkeys(ul);
		CPartConstraint *ppartcnstr = ppartinfo->Ppartcnstr(ul);

		mdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstr->AddRef();

		ppim->Insert(scan_id, GPOS_NEW(mp) UlongToPartConstraintMap(mp),
					 CPartIndexMap::EpimConsumer,
					 0,	 //ulExpectedPropagators
					 mdid, pdrgppartkeys, ppartcnstr);
	}

	m_pepp = GPOS_NEW(mp) CEnfdPartitionPropagation(
		GPOS_NEW(mp)
			CPartitionPropagationSpec(ppim, GPOS_NEW(mp) CPartFilterMap(mp)),
		eppm,
		GPOS_NEW(mp) CPartFilterMap(mp)	 // derived part filter map
	);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Pps
//
//	@doc:
//		Given a property spec type, return the corresponding property spec
//		member
//
//---------------------------------------------------------------------------
CPropSpec *
CReqdPropPlan::Pps(ULONG ul) const
{
	CPropSpec::EPropSpecType epst = (CPropSpec::EPropSpecType) ul;
	switch (epst)
	{
		case CPropSpec::EpstOrder:
			return m_peo->PosRequired();

		case CPropSpec::EpstDistribution:
			return m_ped->PdsRequired();

		case CPropSpec::EpstRewindability:
			return m_per->PrsRequired();

		case CPropSpec::EpstPartPropagation:
			if (NULL != m_pepp)
			{
				return m_pepp->PppsRequired();
			}
			return NULL;

		default:
			GPOS_ASSERT(!"Invalid property spec index");
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FProvidesReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 ULONG ulOptReq) const
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());

	// check if operator provides required columns
	if (!popPhysical->FProvidesReqdCols(exprhdl, m_pcrs, ulOptReq))
	{
		return false;
	}

	CColRefSet *pcrsOutput = exprhdl.DeriveOutputColumns();

	// check if property spec members use columns from operator output
	BOOL fProvidesReqdCols = true;
	for (ULONG ul = 0; fProvidesReqdCols && ul < CPropSpec::EpstSentinel; ul++)
	{
		CPropSpec *pps = Pps(ul);
		if (NULL == pps)
		{
			continue;
		}

		CColRefSet *pcrsUsed = pps->PcrsUsed(mp);
		fProvidesReqdCols = pcrsOutput->ContainsAll(pcrsUsed);
		pcrsUsed->Release();
	}

	return fProvidesReqdCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::Equals(const CReqdPropPlan *prpp) const
{
	GPOS_ASSERT(NULL != prpp);

	BOOL result = PcrsRequired()->Equals(prpp->PcrsRequired()) &&
				  Pcter()->Equals(prpp->Pcter()) &&
				  Peo()->Matches(prpp->Peo()) && Ped()->Matches(prpp->Ped()) &&
				  Per()->Matches(prpp->Per());

	if (result)
	{
		if (NULL == Pepp() || NULL == prpp->Pepp())
		{
			result = (NULL == Pepp() && NULL == prpp->Pepp());
		}
		else
		{
			result = Pepp()->Matches(prpp->Pepp());
		}
	}

	return result;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::HashValue() const
{
	GPOS_ASSERT(NULL != m_pcrs);
	GPOS_ASSERT(NULL != m_peo);
	GPOS_ASSERT(NULL != m_ped);
	GPOS_ASSERT(NULL != m_per);
	GPOS_ASSERT(NULL != m_pcter);

	ULONG ulHash = m_pcrs->HashValue();
	ulHash = gpos::CombineHashes(ulHash, m_peo->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_ped->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_per->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_pcter->HashValue());

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FSatisfied(const CDrvdPropRelational *pdprel,
						  const CDrvdPropPlan *pdpplan) const
{
	GPOS_ASSERT(NULL != pdprel);
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(pdprel->IsComplete());

	// first, check satisfiability of relational properties
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}

	// second, check satisfiability of plan properties;
	// if max cardinality <= 1, then any order requirement is already satisfied;
	// we only need to check satisfiability of distribution and rewindability
	if (pdprel->GetMaxCard().Ull() <= 1)
	{
		GPOS_ASSERT(NULL != pdpplan->Ppim());

		return pdpplan->Pds()->FSatisfies(this->Ped()->PdsRequired()) &&
			   pdpplan->Prs()->FSatisfies(this->Per()->PrsRequired()) &&
			   pdpplan->Ppim()->FSatisfies(this->Pepp()->PppsRequired()) &&
			   pdpplan->GetCostModel()->FSatisfies(this->Pcter());
	}

	// otherwise, check satisfiability of all plan properties
	return pdpplan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FCompatible(CExpressionHandle &exprhdl, CPhysical *popPhysical,
						   const CDrvdPropRelational *pdprel,
						   const CDrvdPropPlan *pdpplan) const
{
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(NULL != pdprel);

	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}

	return m_peo->FCompatible(pdpplan->Pos()) &&
		   m_ped->FCompatible(pdpplan->Pds()) &&
		   m_per->FCompatible(pdpplan->Prs()) &&
		   pdpplan->Ppim()->FSatisfies(m_pepp->PppsRequired()) &&
		   popPhysical->FProvidesReqdCTEs(exprhdl, m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CReqdPropPlan::PrppEmpty(CMemoryPool *mp)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	CDistributionSpec *pds =
		GPOS_NEW(mp) CDistributionSpecAny(COperator::EopSentinel);
	CRewindabilitySpec *prs = GPOS_NEW(mp) CRewindabilitySpec(
		CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped =
		GPOS_NEW(mp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per =
		GPOS_NEW(mp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(mp) CCTEReq(mp);

	return GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, ped, per, pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CReqdPropPlan::OsPrint(IOstream &os) const
{
	if (GPOS_FTRACE(EopttracePrintRequiredColumns))
	{
		os << "req cols: [";
		if (NULL != m_pcrs)
		{
			os << (*m_pcrs);
		}
		os << "], ";
	}

	os << "req CTEs: [";
	if (NULL != m_pcter)
	{
		os << (*m_pcter);
	}

	os << "], req order: [";
	if (NULL != m_peo)
	{
		os << (*m_peo);
	}

	os << "], req dist: [";
	if (NULL != m_ped)
	{
		os << (*m_ped);
	}

	os << "], req rewind: [";
	if (NULL != m_per)
	{
		os << "], req rewind: [" << (*m_per);
	}

	os << "], req partition propagation: [";
	if (NULL != m_pepp)
	{
		os << GetPrintablePtr(m_pepp);
	}
	os << "]";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::UlHashForCostBounding(const CReqdPropPlan *prpp)
{
	GPOS_ASSERT(NULL != prpp);

	ULONG ulHash = prpp->PcrsRequired()->HashValue();

	if (NULL != prpp->Ped())
	{
		ulHash = CombineHashes(ulHash, prpp->Ped()->HashValue());
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FEqualForCostBounding(const CReqdPropPlan *prppFst,
									 const CReqdPropPlan *prppSnd)
{
	GPOS_ASSERT(NULL != prppFst);
	GPOS_ASSERT(NULL != prppSnd);

	if (NULL == prppFst->Ped() || NULL == prppSnd->Ped())
	{
		return NULL == prppFst->Ped() && NULL == prppSnd->Ped() &&
			   prppFst->PcrsRequired()->Equals(prppSnd->PcrsRequired());
	}

	return prppFst->PcrsRequired()->Equals(prppSnd->PcrsRequired()) &&
		   prppFst->Ped()->Matches(prppSnd->Ped());
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppRemap
//
//	@doc:
//		Map input required and derived plan properties into new required
//		plan properties for the CTE producer
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CReqdPropPlan::PrppRemapForCTE(CMemoryPool *mp, CReqdPropPlan *prppProducer,
							   CDrvdPropPlan *pdpplanProducer,
							   CDrvdPropPlan *pdpplanConsumer,
							   UlongToColRefMap *colref_mapping)
{
	GPOS_ASSERT(NULL != colref_mapping);
	GPOS_ASSERT(NULL != prppProducer);
	GPOS_ASSERT(NULL != pdpplanProducer);
	GPOS_ASSERT(NULL != pdpplanConsumer);

	// Remap derived sort order to a required sort order.
	COrderSpec *pos = pdpplanConsumer->Pos()->PosCopyWithRemappedColumns(
		mp, colref_mapping, false /*must_exist*/);
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, prppProducer->Peo()->Eom());

	// Remap derived distribution only if:
	// (1) it can be used as required distribution, and
	// (2) either the producer's derived distribution spec isn't duplicate
	// sensitive, or, the consumer's derived distribution spec is duplicate
	// sensitive. This is to ensure we don't accidentally raise a duplicate
	// insensitive request when it's supposed to be duplicate sensitive,
	// such as when the input is replicated.
	//
	// Also, fix distribution specs with equivalent columns, since those may come
	// from different consumers and NOT be equivalent in the producer.
	// For example:
	//     with cte as (select a,b from foo where b<10)
	//     select * from cte x1 join cte x2 on x1.a=x2.b
	// On the query side, columns x1.a and x2.b are equivalent, but we should NOT
	// treat columns a and b of the producer as equivalent.

	CDistributionSpec *pdsDerivedProducer = pdpplanProducer->Pds();
	CDistributionSpec *pdsDerivedConsumer = pdpplanConsumer->Pds();

	BOOL producer_duplicate_sensitive =
		CDistributionSpec::EdtRandom == pdsDerivedProducer->Edt()
			? CDistributionSpecRandom::PdsConvert(pdsDerivedProducer)
				  ->IsDuplicateSensitive()
			: false;
	BOOL consumer_duplicate_sensitive =
		CDistributionSpec::EdtRandom == pdsDerivedConsumer->Edt()
			? CDistributionSpecRandom::PdsConvert(pdsDerivedConsumer)
				  ->IsDuplicateSensitive()
			: false;
	CEnfdDistribution *ped = NULL;
	if (pdsDerivedConsumer->FRequirable() &&
		(!producer_duplicate_sensitive || consumer_duplicate_sensitive))
	{
		CDistributionSpec *pdsNoEquiv =
			pdsDerivedConsumer->StripEquivColumns(mp);
		CDistributionSpec *pds = pdsNoEquiv->PdsCopyWithRemappedColumns(
			mp, colref_mapping, false /*must_exist*/);
		ped = GPOS_NEW(mp) CEnfdDistribution(pds, prppProducer->Ped()->Edm());
		pdsNoEquiv->Release();
	}
	else
	{
		prppProducer->Ped()->AddRef();
		ped = prppProducer->Ped();
	}

	// other properties are copied from input

	prppProducer->PcrsRequired()->AddRef();
	CColRefSet *pcrsRequired = prppProducer->PcrsRequired();

	prppProducer->Per()->AddRef();
	CEnfdRewindability *per = prppProducer->Per();

	prppProducer->Pepp()->AddRef();
	CEnfdPartitionPropagation *pepp = prppProducer->Pepp();

	prppProducer->Pcter()->AddRef();
	CCTEReq *pcter = prppProducer->Pcter();

	return GPOS_NEW(mp) CReqdPropPlan(pcrsRequired, peo, ped, per, pepp, pcter);
}


// EOF
