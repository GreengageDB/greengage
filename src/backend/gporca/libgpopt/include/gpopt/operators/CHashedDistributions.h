//	Greengage Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CHashedDistributions_H
#define GPOPT_CHashedDistributions_H

#include "gpos/memory/CMemoryPool.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"

namespace gpopt
{
// Build hashed distributions used in physical union all during
// distribution derivation. The class is an array of hashed
// distribution on input column of each child, and an output hashed
// distribution on UnionAll output columns

class CHashedDistributions : public CDistributionSpecArray
{
public:
	CHashedDistributions(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
						 CColRef2dArray *pdrgpdrgpcrInput);
};
}  // namespace gpopt

#endif	//GPOPT_CHashedDistributions_H
