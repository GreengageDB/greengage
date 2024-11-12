//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 VMware, Inc. or its affiliates.
//
//	@filename:
//		CMemoryPoolPallocManager.cpp
//
//	@doc:
//		MemoryPoolManager implementation that creates
//		CMemoryPoolPalloc memory pools
//
//---------------------------------------------------------------------------

#include "gpopt/utils/CMemoryPoolPallocManager.h"

#include "gpopt/utils/CMemoryPoolPalloc.h"
#include "gpopt/utils/gpdbdefs.h"

using namespace gpos;

// ctor
CMemoryPoolPallocManager::CMemoryPoolPallocManager(CMemoryPool *internal,
												   EMemoryPoolType)
	: CMemoryPoolManager(internal, EMemoryPoolExternal)
{
}

// create new memory pool
CMemoryPool *
CMemoryPoolPallocManager::NewMemoryPool()
{
	return GPOS_NEW(GetInternalMemoryPool()) CMemoryPoolPalloc();
}

void
CMemoryPoolPallocManager::DeleteImpl(void *ptr,
									 CMemoryPool::EAllocationType eat)
{
	CMemoryPoolPalloc::DeleteImpl(ptr, eat);
}

// get user requested size of allocation
ULONG
CMemoryPoolPallocManager::UserSizeOfAlloc(const void *ptr)
{
	return CMemoryPoolPalloc::UserSizeOfAlloc(ptr);
}

void
CMemoryPoolPallocManager::Init()
{
	CMemoryPoolManager::SetupGlobalMemoryPoolManager<CMemoryPoolPallocManager,
													 CMemoryPoolPalloc>();
}

// EOF
