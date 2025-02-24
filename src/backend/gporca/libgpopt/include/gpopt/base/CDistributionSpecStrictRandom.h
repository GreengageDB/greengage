//	Greengage Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CDistributionSpecStrictRandom_H
#define GPOPT_CDistributionSpecStrictRandom_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecRandom.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecStrictRandom
//
//	@doc:
//		Class for representing forced random distribution.
//
//---------------------------------------------------------------------------
class CDistributionSpecStrictRandom : public CDistributionSpecRandom
{
public:
	//ctor
	CDistributionSpecStrictRandom();

	// accessor
	virtual EDistributionType
	Edt() const
	{
		return CDistributionSpec::EdtStrictRandom;
	}

	virtual const CHAR *
	SzId() const
	{
		return "STRICT RANDOM";
	}

	// does this distribution match the given one
	virtual BOOL Matches(const CDistributionSpec *pds) const;

	// does this distribution satisfy the given one
	virtual BOOL FSatisfies(const CDistributionSpec *pds) const;
};	// class CDistributionSpecStrictRandom
}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecStrictRandom_H

// EOF
