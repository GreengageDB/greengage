//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHeapObject.h
//
//	@doc:
//		Class of all objects that must reside on the heap;
//---------------------------------------------------------------------------
#ifndef GPOS_CHeapObject_H
#define GPOS_CHeapObject_H

#include "gpos/types.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CHeapObject
//
//	@doc:
//		Constructor tests stack layout to ensure object is not allocated on stack;
//		constructor is protected to prevent direct instantiation of class;
//
//---------------------------------------------------------------------------
class CHeapObject
{
protected:
	CHeapObject();
};
}  // namespace gpos

#endif	// !GPOS_CHeapObject_H

// EOF
