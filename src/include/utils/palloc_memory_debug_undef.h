#ifdef MemoryContextAlloc
#undef MemoryContextAlloc
#endif

#ifdef MemoryContextAllocZero
#undef MemoryContextAllocZero
#endif
#ifdef MemoryContextAllocZeroAligned
#undef MemoryContextAllocZeroAligned
#endif

#ifdef palloc
#undef palloc
#endif
#ifdef palloc0
#undef palloc0
#endif
#ifdef repalloc
#undef repalloc
#endif

#ifdef MemoryContextAllocHuge
#undef MemoryContextAllocHuge
#endif
#ifdef repalloc_huge
#undef repalloc_huge
#endif

#ifdef MemoryContextStrdup
#undef MemoryContextStrdup
#endif
#ifdef pstrdup
#undef pstrdup
#endif
#ifdef pnstrdup
#undef pnstrdup
#endif

#ifdef psprintf
#undef psprintf
#endif
