/* This file should be included into mcxt.c. */

#include "postgres.h"

#include "utils/memutils.h"

#include "utils/palloc_memory_debug_undef.h"

/*
 * `func` is the function which performed an allocation, `parent_func` is the
 *  callee of `func`.
 */
#define MEMORY_CONTEXT_INFO_PARAMS \
	const char *parent_func, const char *filename, int line

#define MEMORY_CONTEXT_INFO_ARGS parent_func, filename, line

/* Write info about an allocation to the AllocChunk's header. */
static void
MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_PARAMS, void *ptr,
							  const char *func)
{
	MemoryContextChunkInfo *info = AllocPointerGetChunkInfo(ptr);

	info->key.parent_func = parent_func;
	info->key.line = line;

	info->func = func;
	info->filename = filename;
	info->init = EXTRA_DYNAMIC_MEMORY_DEBUG_INIT_MAGIC;
}

/* Overriden allocation functions. */

#define MEMORY_CONTEXT_ALLOC_FUNC(f)                                         \
	void *_##f(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context, Size size) \
	{                                                                        \
		void *chunk = f(context, size);                                      \
		if (chunk != NULL)                                                   \
			MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, chunk,   \
										  #f);                               \
		return chunk;                                                        \
	}

#define MEMORY_CONTEXT_PALLOC_FUNC(f)                                      \
	void *_##f(MEMORY_CONTEXT_INFO_PARAMS, Size size)                      \
	{                                                                      \
		void *chunk = f(size);                                             \
		if (chunk != NULL)                                                 \
			MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, chunk, \
										  #f);                             \
		return chunk;                                                      \
	}

#define MEMORY_CONTEXT_REPALLOC_FUNC(f)                                    \
	void *_##f(MEMORY_CONTEXT_INFO_PARAMS, void *pointer, Size size)       \
	{                                                                      \
		void *chunk = f(pointer, size);                                    \
		if (chunk != NULL)                                                 \
			MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, chunk, \
										  #f);                             \
		return chunk;                                                      \
	}

MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAlloc)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocZero)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocZeroAligned)
MEMORY_CONTEXT_ALLOC_FUNC(MemoryContextAllocHuge)

MEMORY_CONTEXT_PALLOC_FUNC(palloc)
MEMORY_CONTEXT_PALLOC_FUNC(palloc0)

MEMORY_CONTEXT_REPALLOC_FUNC(repalloc)
MEMORY_CONTEXT_REPALLOC_FUNC(repalloc_huge)

void *
_palloc_extended(MEMORY_CONTEXT_INFO_PARAMS, Size size, int flags)
{
	void *chunk = palloc_extended(size, flags);
	if (chunk != NULL)
		MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, chunk,
									  "palloc_extended");
	return chunk;
}

void *
_MemoryContextAllocExtended(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
							Size size, int flags)
{
	void *chunk = MemoryContextAllocExtended(context, size, flags);
	if (chunk != NULL)
		MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, chunk,
									  "MemoryContextAllocExtended");
	return chunk;
}

char *
_MemoryContextStrdup(MEMORY_CONTEXT_INFO_PARAMS, MemoryContext context,
					 const char *string)
{
	void *nstr = MemoryContextStrdup(context, string);
	if (nstr != NULL)
		MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, nstr,
									  "MemoryContextStrdup");
	return nstr;
}

char *
_pstrdup(MEMORY_CONTEXT_INFO_PARAMS, const char *in)
{
	char *nstr = pstrdup(in);
	if (nstr != NULL)
		MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, nstr,
									  "pstrdup");
	return nstr;
}

char *
_pnstrdup(MEMORY_CONTEXT_INFO_PARAMS, const char *in, Size len)
{
	void *nstr = pnstrdup(in, len);
	if (nstr != NULL)
		MemoryContextChunkCollectInfo(MEMORY_CONTEXT_INFO_ARGS, nstr,
									  "pnstrdup");
	return nstr;
}

/* This should mirror psprintf(), except for the _palloc() call. */
char *
_psprintf(MEMORY_CONTEXT_INFO_PARAMS, const char *fmt, ...)
{
	int			save_errno = errno;
	size_t		len = 128;		/* initial assumption about buffer size */

	for (;;)
	{
		char	   *result;
		va_list		args;
		size_t		newlen;

		/*
		 * Allocate result buffer.  Note that in frontend this maps to malloc
		 * with exit-on-error.
		 */
		result = (char *) _palloc(MEMORY_CONTEXT_INFO_ARGS, len);

		/* Try to format the data. */
		errno = save_errno;
		va_start(args, fmt);
		newlen = pvsnprintf(result, len, fmt, args);
		va_end(args);

		if (newlen < len)
			return result;		/* success */

		/* Release buffer and loop around to try again with larger len. */
		pfree(result);
		len = newlen;
	}
}

/* Helper functions. */

static int
MemoryContextChunkTableCompare(const void *a, const void *b)
{
	const MemoryContextChunkTableEntry *lhs =
		*(MemoryContextChunkTableEntry **) a;
	const MemoryContextChunkTableEntry *rhs =
		*(MemoryContextChunkTableEntry **) b;

	return rhs->stat.bytes - lhs->stat.bytes;
}

/* 
 * Dump info about collected allocation stats. This will be called recursively
 * by MemoryContextStatsDetail().
 */
static void
MemoryContextDumpChunkStats(MemoryContext context, int level, int max_children,
							bool print_to_stderr)
{
	HTAB *chunk_table = AllocSetTakeChunkTable(context);

	if (chunk_table == NULL)
		return;

	Size chunk_count = hash_get_num_entries(chunk_table);

	if (chunk_count == 0)
	{
		hash_destroy(chunk_table);
		return;
	}

	MemoryContext chunk_stat_ctx = AllocSetContextCreate(
		TopMemoryContext, "MemoryContextDumpTopChunkStatsCtx",
		ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldcontext = MemoryContextSwitchTo(chunk_stat_ctx);

	MemoryContextChunkTableEntry **chunks =
		palloc(chunk_count * sizeof(MemoryContextChunkTableEntry *));

	Size idx = 0;
	Size summary_bytes = 0;
	MemoryContextChunkTableEntry *entry = NULL;

	HASH_SEQ_STATUS hash_seq;
	hash_seq_init(&hash_seq, chunk_table);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		chunks[idx++] = entry;
		summary_bytes += entry->stat.bytes;
	}

	Size show_count =
		(chunk_count > max_children) ? max_children : chunk_count;

	qsort(chunks, chunk_count, sizeof(MemoryContextChunkTableEntry *),
		  MemoryContextChunkTableCompare);

	if (print_to_stderr)
	{
		for (int l = 0; l < level; l++)
			fprintf(stderr, "  ");

		fprintf(
			stderr,
			"  "
			"Extra: Top %zu (all %zu) biggest allocations "
			"(%zu bytes in total):\n",
			show_count, chunk_count, summary_bytes);
	}
	else
	{
		ereport(
			LOG_SERVER_ONLY,
			(errhidestmt(true), errhidecontext(true),
			 errmsg_internal(
				 "  "
				 "Extra: Top %zu (all %zu) biggest allocations "
				 "(%zu bytes in total):\n",
				 show_count, chunk_count, summary_bytes)));
	}

	for (Size i = 0; i < show_count; i++)
	{
		if (print_to_stderr)
		{
			for (int l = 0; l < level; l++)
				fprintf(stderr, "  ");

			fprintf(stderr,
					"    "
					"%s:%zu: %s() was called %zu times from %s(), "
					"for %zu bytes total\n",
					chunks[i]->info.filename, chunks[i]->info.key.line,
					chunks[i]->info.func, chunks[i]->stat.count,
					chunks[i]->info.key.parent_func, chunks[i]->stat.bytes);
		}
		else
		{
			ereport(
				LOG_SERVER_ONLY,
				(errhidestmt(true), errhidecontext(true),
				 errmsg_internal(
					 "    "
					 "%s:%zu: %s() was called %zu times from %s(), "
					 "for %zu bytes total\n",
					 chunks[i]->info.filename, chunks[i]->info.key.line,
					 chunks[i]->info.func, chunks[i]->stat.count,
					 chunks[i]->info.key.parent_func, chunks[i]->stat.bytes)));
		}
	}

	MemoryContextSwitchTo(oldcontext);

	MemoryContextDelete(chunk_stat_ctx);
	hash_destroy(chunk_table);
}
