//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformFactoryTest.cpp
//
//	@doc:
//		Unittests for management of the global xform set
//---------------------------------------------------------------------------

#include "unittest/gpopt/xforms/CXformFactoryTest.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/xforms/xforms.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformFactoryTest::EresUnittest
//
//	@doc:
//		Unittest for xform factory
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformFactoryTest::EresUnittest()
{
	CUnittest rgut[] = {
		GPOS_UNITTEST_FUNC(CXformFactoryTest::EresUnittest_Basic)};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformFactoryTest::EresUnittest_Basic
//
//	@doc:
//		create factory and instantiate
//
//---------------------------------------------------------------------------
GPOS_RESULT
CXformFactoryTest::EresUnittest_Basic()
{
#ifdef GPOS_DEBUG

	CXform *pxf = CXformFactory::Pxff()->Pxf(CXform::ExfGet2TableScan);
	GPOS_UNITTEST_ASSERT(CXform::ExfGet2TableScan == pxf->Exfid());

	pxf = CXformFactory::Pxff()->Pxf(CXform::ExfImplementInnerJoin);
	GPOS_UNITTEST_ASSERT(CXform::ExfImplementInnerJoin == pxf->Exfid());

	pxf = CXformFactory::Pxff()->Pxf(CXform::ExfGbAgg2HashAgg);
	GPOS_UNITTEST_ASSERT(CXform::ExfGbAgg2HashAgg == pxf->Exfid());

	pxf = CXformFactory::Pxff()->Pxf(CXform::ExfInnerJoinCommutativity);
	GPOS_UNITTEST_ASSERT(CXform::ExfInnerJoinCommutativity == pxf->Exfid());

	pxf = CXformFactory::Pxff()->Pxf(CXform::ExfJoinAssociativity);
	GPOS_UNITTEST_ASSERT(CXform::ExfJoinAssociativity == pxf->Exfid());

#endif	// GPOS_DEBUG

	return GPOS_OK;
}



// EOF
