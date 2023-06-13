#ifndef PALLOC_OVERRIDE_H
#define PALLOC_OVERRIDE_H

#ifndef DYN_MEM_TOP_COUNT
#define DYN_MEM_TOP_COUNT 10
#endif
#ifndef DYN_MEM_HTABLE_SIZE
#define DYN_MEM_HTABLE_SIZE 1024
#endif

typedef struct
{
	uint64_t count;
	uint64_t bytes;
} MemoryContextChunkStat;

typedef struct
{
	const char *parent_func;
	int line;
} MemoryContextChunkStatKey;

typedef struct
{
	MemoryContextChunkStatKey key;
	#define DYNAMIC_MEMORY_DEBUG_INIT_MAGIC 0x12345678
	int32_t init;
	const char *file;
	const char *exec_func;
} MemoryContextChunkInfo;

typedef struct
{
	MemoryContextChunkInfo chunk_info;
	MemoryContextChunkStat stat;
} MemoryContextChunkStat_htabEntry;

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
void *_MemoryContextAlloc(MemoryContext context, Size size, const char *func, const char *file, int LINE);
void *_MemoryContextAllocZero(MemoryContext context, Size size, const char *func, const char *file, int LINE);
void *_MemoryContextAllocZeroAligned(MemoryContext context, Size size, const char *func, const char *file, int LINE);

void *_palloc(Size size, const char *func, const char *file, int LINE);
void *_palloc0(Size size, const char *func, const char *file, int LINE);
void *_repalloc(void *pointer, Size size, const char *func, const char *file, int LINE);

/* Higher-limit allocators. */
void *_MemoryContextAllocHuge(MemoryContext context, Size size, const char *func, const char *file, int LINE);
void *_repalloc_huge(void *pointer, Size size, const char *func, const char *file, int LINE);

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
char *_MemoryContextStrdup(MemoryContext context, const char *string, const char *func, const char *file, int LINE);
char *_pstrdup(const char *in, const char *func, const char *file, int LINE);
char *_pnstrdup(const char *in, Size len, const char *func, const char *file, int LINE);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
char *_psprintf(const char *func, const char *file, int LINE, const char *fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 4, 5)));

#define MemoryContextAlloc(__context__, __size__) _MemoryContextAlloc(__context__, __size__, __func__, __FILE__, __LINE__)
#define MemoryContextAllocZero(__context__, __size__) _MemoryContextAllocZero(__context__, __size__, __func__, __FILE__, __LINE__)
#define MemoryContextAllocZeroAligned(__context__, __size__) _MemoryContextAllocZeroAligned(__context__, __size__, __func__, __FILE__, __LINE__)

#define palloc(__size__) _palloc(__size__, __func__, __FILE__, __LINE__)
#define palloc0(__size__) _palloc0(__size__, __func__, __FILE__, __LINE__)
#define repalloc(__pointer__, __size__) _repalloc(__pointer__, __size__, __func__, __FILE__, __LINE__)

#define MemoryContextAllocHuge(__context__,__size__) _MemoryContextAllocHuge(__context__,__size__, __func__, __FILE__, __LINE__)
#define repalloc_huge(__pointer__, __size__) _repalloc_huge(__pointer__, __size__, __func__, __FILE__, __LINE__)

#define MemoryContextStrdup(__context__, __pointer__) _MemoryContextStrdup(__context__, __pointer__, __func__, __FILE__, __LINE__)
#define pstrdup(__pointer__) _pstrdup(__pointer__, __func__, __FILE__, __LINE__)
#define pnstrdup(__pointer__, __size__) _pnstrdup(__pointer__,__size__, __func__, __FILE__, __LINE__)

#define psprintf(...) _psprintf(__func__, __FILE__, __LINE__, __VA_ARGS__)

#endif
