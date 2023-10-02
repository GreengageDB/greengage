#include "utils/palloc_memory_debug_undef.h"

static void
MemoryContextWriteFuncAndLineToAllocedMemory(void *ptr, const char *parent_func,
		const char *exec_func, const char *file, int line)
{
	Assert(parent_func);
	Assert(exec_func);
	Assert(file);
	Assert(line);

	StandardChunkHeader *header = (StandardChunkHeader *)
		((char *) ptr - STANDARDCHUNKHEADERSIZE);

	header->info.key.parent_func = parent_func;
	header->info.key.line = line;
	header->info.exec_func = exec_func;
	header->info.file = file;
	header->info.init = DYNAMIC_MEMORY_DEBUG_INIT_MAGIC;
}

#define MEMORY_CONTEXT_ALLOC_FUNC(__func_name__)                                                 \
void *                                                                                           \
_##__func_name__(MemoryContext context, Size size, const char *func, const char *file, int LINE) \
{                                                                                                \
	void * ret = __func_name__(context, size);                                                   \
                                                                                                 \
	if (ret)                                                                                     \
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, #__func_name__, file, LINE);     \
                                                                                                 \
	return ret;                                                                                  \
}

#define MEMORY_CONTEXT_PALLOC_FUNC(__func_name__)                                            \
void *                                                                                       \
_##__func_name__(Size size, const char *func, const char *file, int LINE)                    \
{                                                                                            \
	void * ret = __func_name__(size);                                                        \
                                                                                             \
	if (ret)                                                                                 \
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, #__func_name__, file, LINE); \
                                                                                             \
	return ret;                                                                              \
}

#define MEMORY_CONTEXT_REPALLOC_FUNC(__func_name__)                                          \
void *                                                                                       \
_##__func_name__(void *pointer, Size size, const char *func, const char *file, int LINE)     \
{                                                                                            \
	void * ret = __func_name__(pointer, size);                                               \
                                                                                             \
	if (ret)                                                                                 \
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, #__func_name__, file, LINE); \
                                                                                             \
	return ret;                                                                              \
}

/*
 * We can not use one simple function because
 * each name of allocation callback requires to
 * execute different callbacks.
 * But we can use macro for the same callbacks (
 * which use the same atributes).
 */
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAlloc)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocZero)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocZeroAligned)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocHuge)

MEMORY_CONTEXT_PALLOC_FUNC(palloc)
MEMORY_CONTEXT_PALLOC_FUNC(palloc0)

MEMORY_CONTEXT_REPALLOC_FUNC(repalloc)
MEMORY_CONTEXT_REPALLOC_FUNC(repalloc_huge)

char *
_MemoryContextStrdup(MemoryContext context, const char *string, const char *func, const char *file, int LINE)
{
	void * ret = MemoryContextStrdup(context, string);

	if (ret)
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, __func__, file, LINE);

	return ret;
}

char *
_pstrdup(const char *in, const char *func, const char *file, int LINE)
{
	void * ret = pstrdup(in);

	if (ret)
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, __func__, file, LINE);

	return ret;
}

char *
_pnstrdup(const char *in, Size len, const char *func, const char *file, int LINE)
{
	void * ret = pnstrdup(in, len);

	if (ret)
		MemoryContextWriteFuncAndLineToAllocedMemory(ret, func, __func__, file, LINE);

	return ret;
}

static int
MemoryContextChunkStats_comparator(const void *l, const void *r)
{
	const MemoryContextChunkStat_htabEntry *l_entry =
		*(MemoryContextChunkStat_htabEntry **)l;
	const MemoryContextChunkStat_htabEntry *r_entry =
		*(MemoryContextChunkStat_htabEntry **)r;

	return r_entry->stat.bytes - l_entry->stat.bytes;
}

static void
MemoryContext_printTopListOfChunks()
{
	if (!chunks_htable)
		return;

	long ChunksCount = hash_get_num_entries(chunks_htable);
	if (!ChunksCount)
	{
		hash_destroy(chunks_htable);
		chunks_htable = NULL;
		return;
	}

	MemoryContext ChunksStatContext =
		AllocSetContextCreate(NULL,
							  "ChunksStat_tempContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldcontext = MemoryContextSwitchTo(ChunksStatContext);

	HASH_SEQ_STATUS hash_seq;
	int idx = 0;
	MemoryContextChunkStat_htabEntry *entry = NULL;

	MemoryContextChunkStat_htabEntry **chunks =
		palloc(ChunksCount * sizeof(MemoryContextChunkStat_htabEntry *));

	int show_count = ChunksCount < DYN_MEM_TOP_COUNT ?
						ChunksCount : DYN_MEM_TOP_COUNT;

	uint64 sumBytes = 0;

	hash_seq_init(&hash_seq, chunks_htable);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		chunks[idx++] = entry;
		sumBytes += entry->stat.bytes;
	}

	qsort(chunks, ChunksCount, sizeof(MemoryContextChunkStat_htabEntry *),
		  MemoryContextChunkStats_comparator);
	write_stderr("\tList of top %d (all %ld) the biggest allocations (summary %lu bytes)\n",
				 show_count, ChunksCount, sumBytes);
	write_stderr("\tfunction, file:line, bytes, count, function_of_allocation\n");

	for (int idx = 0; idx < show_count; idx++)
	{
		write_stderr("\t%s, %s:%d, " UINT64_FORMAT " bytes, " UINT64_FORMAT ", %s\n",
			chunks[idx]->chunk_info.key.parent_func, chunks[idx]->chunk_info.file,
			chunks[idx]->chunk_info.key.line, chunks[idx]->stat.bytes,
			chunks[idx]->stat.count, chunks[idx]->chunk_info.exec_func);
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(ChunksStatContext);
	hash_destroy(chunks_htable);
	chunks_htable = NULL;
}
