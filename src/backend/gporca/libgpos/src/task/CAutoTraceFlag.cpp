//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CAutoTraceFlag.cpp
//
//	@doc:
//		Auto object to toggle TF in scope
//---------------------------------------------------------------------------

#include "gpos/task/CAutoTraceFlag.h"

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoTraceFlag::CAutoTraceFlag
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTraceFlag::CAutoTraceFlag(ULONG trace, BOOL orig)
	: m_trace(trace), m_orig(false)
{
	// GPDB: tolerate running without a task; this happens while an
	// optimizer-fallback exception unwinds, after the worker has been
	// removed from the pool.  Dereferencing the NULL task here crashed
	// the coordinator (custom partition opclass INSERT path).
	if (nullptr != ITask::Self())
		m_orig = ITask::Self()->SetTrace(m_trace, orig);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTraceFlag::~CAutoTraceFlag
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoTraceFlag::~CAutoTraceFlag()
{
	// reset original value; see ctor for the no-task case
	if (nullptr != ITask::Self())
		ITask::Self()->SetTrace(m_trace, m_orig);
}


// EOF
