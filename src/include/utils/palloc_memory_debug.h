#ifndef PALLOC_OVERRIDE_H
#define PALLOC_OVERRIDE_H

#ifndef DYN_MEM_HTABLE_SIZE
#define DYN_MEM_HTABLE_SIZE 32
#endif

#define EXTRA_DYNAMIC_MEMORY_DEBUG_INIT_MAGIC 0x12345678

typedef struct
{
	Size count;
	Size bytes;
} MemoryContextChunkStat;

typedef struct
{
	const char *parent_func;
	Size line;
} MemoryContextChunkStatKey;

typedef struct
{
	MemoryContextChunkStatKey key;

	int32_t init;
	const char *filename;
	const char *func;
} MemoryContextChunkInfo;

#define MEMORYCONTEXTCHUNKINFO_RAWSIZE 40

typedef struct
{
	MemoryContextChunkInfo info;
	MemoryContextChunkStat stat;
} MemoryContextChunkTableEntry;

#define MEMORY_CONTEXT_INFO_PARAMS \
	const char *parent_func, const char *filename, int line

void *_MemoryContextAlloc(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
						  Size size);
void *_MemoryContextAllocZero(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
							  Size size);
void *_MemoryContextAllocZeroAligned(MEMORY_CONTEXT_INFO_PARAMS,
									 MemoryContext context, Size size);
void *_MemoryContextAllocExtended(MEMORY_CONTEXT_INFO_PARAMS,
								  MemoryContext context, Size size, int flags);
void *_MemoryContextAllocHuge(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
							  Size size);

void *_palloc(MEMORY_CONTEXT_INFO_PARAMS, Size size);
void *_palloc0(MEMORY_CONTEXT_INFO_PARAMS, Size size);
void *_palloc_extended(MEMORY_CONTEXT_INFO_PARAMS, Size size, int flags);

void *_repalloc(MEMORY_CONTEXT_INFO_PARAMS, void *pointer, Size size);
void *_repalloc_huge(MEMORY_CONTEXT_INFO_PARAMS, void *pointer, Size size);

char *_MemoryContextStrdup(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
						   const char *string);
char *_pstrdup(MEMORY_CONTEXT_INFO_PARAMS, const char *in);
char *_pnstrdup(MEMORY_CONTEXT_INFO_PARAMS, const char *in, Size len);

char *_psprintf(MEMORY_CONTEXT_INFO_PARAMS, const char *fmt, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 4, 5)));

#define MEMORY_CONTEXT_INFO_MACROS __func__, __FILE__, __LINE__

#define MemoryContextAlloc(...) \
	_MemoryContextAlloc(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define MemoryContextAllocZero(...) \
	_MemoryContextAllocZero(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define MemoryContextAllocZeroAligned(...) \
	_MemoryContextAllocZeroAligned(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define MemoryContextAllocExtended(...) \
	_MemoryContextAllocExtended(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define MemoryContextAllocHuge(...) \
	_MemoryContextAllocHuge(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)

#define palloc(...) _palloc(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define palloc0(...) _palloc0(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define palloc_extended(...) \
	_palloc_extended(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)

#define repalloc(...) _repalloc(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define repalloc_huge(...) \
	_repalloc_huge(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)

#define MemoryContextStrdup(...) \
	_MemoryContextStrdup(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define pstrdup(...) _pstrdup(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)
#define pnstrdup(...) _pnstrdup(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)

#define psprintf(...) _psprintf(MEMORY_CONTEXT_INFO_MACROS, __VA_ARGS__)

#endif
