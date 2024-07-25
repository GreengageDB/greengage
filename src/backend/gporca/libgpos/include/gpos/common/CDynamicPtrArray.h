//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CDynamicPtrArray.h
//
//	@doc:
//		Dynamic array of pointers frequently used in optimizer data structures
//---------------------------------------------------------------------------
#ifndef GPOS_CDynamicPtrArray_H
#define GPOS_CDynamicPtrArray_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/clibwrapper.h"

namespace gpos
{
// forward declaration
template <class T, void (*CleanupFn)(T *)>
class CDynamicPtrArray;

// comparison function signature
using CompareFn = INT (*)(const void *, const void *);

// frequently used destroy functions

// NOOP
template <class T>
inline void
CleanupNULL(T *)
{
}

// plain delete
template <class T>
inline void
CleanupDelete(T *elem)
{
	GPOS_DELETE(elem);
}

// delete of array
template <class T>
inline void
CleanupDeleteArray(T *elem)
{
	GPOS_DELETE_ARRAY(elem);
}

// release ref-count'd object
template <class T>
inline void
CleanupRelease(T *elem)
{
	(dynamic_cast<CRefCount *>(elem))->Release();
}

// Compare function used by CDynamicPtrArray::Sort
inline INT
CompareUlongPtr(const void *right, const void *left)
{
	return *((ULONG *) right) - *((ULONG *) left);
}


// commonly used array types

// arrays of unsigned integers
using ULongPtrArray = CDynamicPtrArray<ULONG, CleanupDelete>;
// array of unsigned integer arrays
using ULongPtr2dArray = CDynamicPtrArray<ULongPtrArray, CleanupRelease>;

// arrays of integers
using IntPtrArray = CDynamicPtrArray<INT, CleanupDelete>;

// array of strings
using StringPtrArray = CDynamicPtrArray<CWStringBase, CleanupDelete>;

// array of string arrays
using StringPtr2dArray = CDynamicPtrArray<StringPtrArray, CleanupRelease>;

// arrays of chars
using CharPtrArray = CDynamicPtrArray<CHAR, CleanupDelete>;

//---------------------------------------------------------------------------
//	@class:
//		CDynamicPtrArray
//
//	@doc:
//		Simply dynamic array for pointer types
//
//---------------------------------------------------------------------------
template <class T, void (*CleanupFn)(T *)>
class CDynamicPtrArray : public CRefCount
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// currently allocated size
	ULONG m_capacity;

	// min size
	ULONG m_min_size;

	// current size
	ULONG m_size;

	// expansion factor
	ULONG m_expansion_factor;

	// actual array
	T **m_elems;

	// resize function
	void
	Resize(ULONG new_size)
	{
		GPOS_ASSERT(new_size > m_capacity &&
					"Invalid call to Resize, cannot shrink array");

		// get new target array
		T **new_elems = GPOS_NEW_ARRAY(m_mp, T *, new_size);

		if (m_size > 0)
		{
			GPOS_ASSERT(nullptr != m_elems);
			clib::Memcpy(new_elems, m_elems, sizeof(T *) * m_size);
			GPOS_DELETE_ARRAY(m_elems);
		}

		m_elems = new_elems;
		m_capacity = new_size;
	}

public:
	CDynamicPtrArray<T, CleanupFn>(const CDynamicPtrArray<T, CleanupFn> &) =
		delete;

	// ctor
	explicit CDynamicPtrArray<T, CleanupFn>(CMemoryPool *mp, ULONG min_size = 4,
											ULONG expansion_factor = 10)
		: m_mp(mp),
		  m_capacity(0),
		  m_min_size(std::max((ULONG) 4, min_size)),
		  m_size(0),
		  m_expansion_factor(std::max((ULONG) 2, expansion_factor)),
		  m_elems(nullptr)
	{
		// do not allocate in constructor; defer allocation to first insertion
		GPOS_CPL_ASSERT(nullptr != CleanupFn,
						"No valid destroy function specified");
	}

	// dtor
	~CDynamicPtrArray<T, CleanupFn>() override
	{
		Clear();

		GPOS_DELETE_ARRAY(m_elems);
	}

	// clear elements
	void
	Clear()
	{
		for (ULONG i = 0; i < m_size; i++)
		{
			CleanupFn(m_elems[i]);
		}
		m_size = 0;
	}

	// append element to end of array
	void
	Append(T *elem)
	{
		if (m_size == m_capacity)
		{
			// resize at least by 4 elements or percentage as given by ulExp
			ULONG new_size =
				(ULONG)(m_capacity * (1 + (m_expansion_factor / 100.0)));
			ULONG min_expand_size = m_capacity + 4;

			Resize(std::max(std::max(min_expand_size, new_size), m_min_size));
		}

		GPOS_ASSERT(m_size < m_capacity);

		m_elems[m_size] = elem;
		++m_size;
	}

	// append array -- flatten it
	void
	AppendArray(const CDynamicPtrArray<T, CleanupFn> *arr)
	{
		GPOS_ASSERT(nullptr != arr);
		GPOS_ASSERT(this != arr && "Cannot append array to itself");

		ULONG total_size = m_size + arr->m_size;
		if (total_size > m_capacity)
		{
			Resize(total_size);
		}

		GPOS_ASSERT(m_size <= m_capacity);
		GPOS_ASSERT_IMP(m_size == m_capacity, 0 == arr->m_size);

		GPOS_ASSERT(total_size <= m_capacity);

		// at this point old memory is no longer accessible, hence, no self-copy
		if (arr->m_size > 0)
		{
			GPOS_ASSERT(nullptr != arr->m_elems);
			clib::Memcpy(m_elems + m_size, arr->m_elems,
						 arr->m_size * sizeof(T *));
		}

		m_size = total_size;
	}


	// number of elements currently held
	ULONG
	Size() const
	{
		return m_size;
	}

	// sort array
	void
	Sort(CompareFn compare_func)
	{
		clib::Qsort(m_elems, m_size, sizeof(T *), compare_func);
	}

	// equality check
	BOOL
	Equals(const CDynamicPtrArray<T, CleanupFn> *arr) const
	{
		BOOL is_equal = (Size() == arr->Size());

		for (ULONG i = 0; i < m_size && is_equal; i++)
		{
			is_equal = (*m_elems[i] == *arr->m_elems[i]);
		}

		return is_equal;
	}

	BOOL
	operator==(const CDynamicPtrArray<T, CleanupFn> &other)
	{
		if (this == &other)
		{
			// same object reference
			return true;
		}

		return Equals(&other);
	}

	// lookup object
	T *
	Find(const T *elem) const
	{
		GPOS_ASSERT(nullptr != elem);

		for (ULONG i = 0; i < m_size; i++)
		{
			if (*m_elems[i] == *elem)
			{
				return m_elems[i];
			}
		}

		return nullptr;
	}

	// lookup object position
	ULONG
	IndexOf(const T *elem) const
	{
		GPOS_ASSERT(nullptr != elem);

		for (ULONG ul = 0; ul < m_size; ul++)
		{
			if (*m_elems[ul] == *elem)
			{
				return ul;
			}
		}

		return gpos::ulong_max;
	}

	// check if array is sorted
	BOOL
	IsSorted(CompareFn compare_func) const
	{
		for (ULONG i = 1; i < m_size; i++)
		{
			if (compare_func(&m_elems[i - 1], &m_elems[i]) > 0)
			{
				return false;
			}
		}

		return true;
	}

	// accessor for n-th element
	T *
	operator[](ULONG pos) const
	{
		GPOS_ASSERT(pos < m_size && "Out of bounds access");
		return (T *) m_elems[pos];
	}

	// replace an element in the array
	void
	Replace(ULONG pos, T *new_elem)
	{
		GPOS_ASSERT(pos < m_size && "Out of bounds access");
		CleanupFn(m_elems[pos]);
		m_elems[pos] = new_elem;
	}

	// swap two array entries
	void
	Swap(ULONG pos1, ULONG pos2)
	{
		GPOS_ASSERT(pos1 < m_size && pos2 < m_size &&
					"Swap positions out of bounds");
		T *temp = m_elems[pos1];

		m_elems[pos1] = m_elems[pos2];
		m_elems[pos2] = temp;
	}

	// return the last element of the array and at the same time remove it from the array
	T *
	RemoveLast()
	{
		if (0 == m_size)
		{
			return nullptr;
		}

		return m_elems[--m_size];
	}

	// return the indexes of first appearances of elements of the first array
	// in the second array if the first array is not included in the second,
	// return null
	// equality comparison between elements is via the "==" operator
	ULongPtrArray *
	IndexesOfSubsequence(CDynamicPtrArray<T, CleanupFn> *subsequence)
	{
		GPOS_ASSERT(nullptr != subsequence);

		ULONG subsequence_length = subsequence->Size();
		ULongPtrArray *indexes = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

		for (ULONG ul1 = 0; ul1 < subsequence_length; ul1++)
		{
			T *elem = (*subsequence)[ul1];
			ULONG index = IndexOf(elem);
			if (gpos::ulong_max == index)
			{
				// not found
				indexes->Release();
				return nullptr;
			}

			indexes->Append(GPOS_NEW(m_mp) ULONG(index));
		}
		return indexes;
	}

	// Eliminate members from an array that are not contained in a given list of indexes
	CDynamicPtrArray<T, CleanupFn> *
	CreateReducedArray(ULongPtrArray *indexes_to_choose)
	{
		CDynamicPtrArray<T, CleanupFn> *result =
			GPOS_NEW(m_mp) CDynamicPtrArray<T, CleanupFn>(m_mp, m_min_size,
														  m_expansion_factor);
		ULONG list_size = indexes_to_choose->Size();

		for (ULONG i = 0; i < list_size; i++)
		{
			result->Append((*this)[*((*indexes_to_choose)[i])]);
		}
		return result;
	}

};	// class CDynamicPtrArray
}  // namespace gpos

#endif	// !GPOS_CDynamicPtrArray_H

// EOF
