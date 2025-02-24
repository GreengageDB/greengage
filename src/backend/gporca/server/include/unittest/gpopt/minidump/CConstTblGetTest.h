//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2018 Pivotal, Inc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstTblGetTest_H
#define GPOPT_CConstTblGetTest_H

#include "gpos/base.h"

namespace gpopt
{
class CConstTblGetTest
{
private:
	// counter used to mark last successful test
	static gpos::ULONG m_ulTestCounter;

public:
	// unittests
	static gpos::GPOS_RESULT EresUnittest();

	static gpos::GPOS_RESULT EresUnittest_RunTests();

};	// class CConstTblGetTest
}  // namespace gpopt

#endif	// !GPOPT_CConstTblGetTest_H

// EOF
