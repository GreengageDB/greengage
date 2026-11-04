//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2016 Pivotal, Inc.
//
//	@filename:
//		CEscapeMechanismTest.cpp
//
//	@doc:
//		Test for optimizing queries for exploring fewer alternatives and
//		run optimization process faster
//---------------------------------------------------------------------------

#include "unittest/gpopt/minidump/CEscapeMechanismTest.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/exception.h"
#include "gpopt/minidump/CMinidumperUtils.h"

#include "unittest/gpopt/CTestUtils.h"


using namespace gpopt;

ULONG CEscapeMechanismTest::m_ulEscapeMechanismTestCounter =
	0;	// start from first test

// minidump files
//
// An xform disabled during optimization of a minidump above its join arity
// limit stays disabled for the following minidumps, so the minidumps that
// expect JoinAssociativity to be explored go first.
const CHAR *rgszEscapeMechanismFileNames[] = {
	"../data/dxl/minidump/JoinArityAssociativityAtLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityBelowLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityCommutativityAtLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityCommutativityAboveLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityCommutativityBelowLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityAboveLimit.mdp",
	"../data/dxl/minidump/JoinArityAssociativityTotalAboveLimit.mdp",
};


//---------------------------------------------------------------------------
//	@function:
//		CEscapeMechanismTest::EresUnittest
//
//	@doc:
//		Unittest for expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEscapeMechanismTest::EresUnittest()
{
	CUnittest rgut[] = {
		GPOS_UNITTEST_FUNC(EresUnittest_RunTests),
	};

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	// reset metadata cache
	CMDCache::Reset();

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CEscapeMechanismTest::EresUnittest_RunTests
//
//	@doc:
//		Run all Minidump-based tests with plan matching
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEscapeMechanismTest::EresUnittest_RunTests()
{
	return CTestUtils::EresUnittest_RunTestsWithoutAdditionalTraceFlags(
		rgszEscapeMechanismFileNames, &m_ulEscapeMechanismTestCounter,
		GPOS_ARRAY_SIZE(rgszEscapeMechanismFileNames), true, /* fMatchPlans */
		true /* fTestSpacePruning */
	);
}

// EOF
