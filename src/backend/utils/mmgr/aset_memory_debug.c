/* This file should be included into aset.c. */

#include "postgres.h"

#include "utils/palloc_memory_debug.h"

#include "utils/memutils.h"
#include "access/hash.h"
#include "utils/hsearch.h"

/* Public functions, required by mcxt_memory_debug.c. */
HTAB *
AllocSetTakeChunkTable(MemoryContext context)
{
	Assert(AllocSetIsValid(context));
	Assert(IsA(context, AllocSetContext));

	HTAB *table = ((AllocSet) context)->chunkTable;
	((AllocSet) context)->chunkTable = NULL;

	return table;
}

MemoryContextChunkInfo *
AllocPointerGetChunkInfo(void *ptr)
{
	return &AllocPointerGetChunk(ptr)->info;
}

/* Update individual chunk stats. */
static void
AllocChunkUpdateStats(HTAB *chunk_table, AllocChunk chunk)
{
	bool found = false;
	MemoryContextChunkTableEntry *r = NULL;

	r = hash_search(chunk_table, &chunk->info.key, HASH_ENTER, &found);
	Assert(r != NULL);

	if (found)
	{
		r->stat.count++;
		r->stat.bytes += chunk->size;
	}
	else
	{
		r->info = chunk->info;
		r->stat.bytes = chunk->size;
		r->stat.count = 1;
	}
}

static uint32
MemoryContextChunkStatKeyHash(const void *key, Size keysize)
{
	Assert(keysize == sizeof(MemoryContextChunkStatKey));
	return DatumGetUInt32(hash_any((const unsigned char *) key,
								   keysize));
}

static int
MemoryContextChunkStatKeyCompare(const void *a, const void *b,
								 Size keysize)
{
	Assert(keysize == sizeof(MemoryContextChunkStatKey));

	MemoryContextChunkStatKey *lhs = (MemoryContextChunkStatKey *) a;
	MemoryContextChunkStatKey *rhs = (MemoryContextChunkStatKey *) b;

	return !(strcmp(lhs->parent_func, rhs->parent_func) == 0 &&
			 lhs->line == rhs->line);
}

static bool
AllocSetChunkIsFree(AllocChunk chunk, AllocSet set)
{
	if (chunk->size > set->allocChunkLimit)
		return false;

	AllocChunk free_chunk = set->freelist[AllocSetFreeIndex(chunk->size)];

	while (free_chunk != NULL)
	{
		if (free_chunk == chunk)
			return true;

		free_chunk = (AllocChunk) free_chunk->aset;
	}

	return false;
}

/* Update every chunk's stats table inside an AllocSet. */
static void
AllocSetUpdateAllocatedChunkStats(AllocSet set)
{
	if (set->chunkTable == NULL)
	{
		HASHCTL hash_ctl =
		{
			.keysize = sizeof(MemoryContextChunkStatKey),
			.entrysize = sizeof(MemoryContextChunkTableEntry),
			.hash = MemoryContextChunkStatKeyHash,
			.match = MemoryContextChunkStatKeyCompare,
		};

		set->chunkTable = hash_create("AllocSetUpdateAllocatedChunkStats",
									  DYN_MEM_HTABLE_SIZE, &hash_ctl,
									  HASH_FUNCTION | HASH_ELEM | HASH_COMPARE);
	}

	for (AllocBlock block = set->blocks; block != NULL; block = block->next)
	{
		AllocChunk chunk;

		for (chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
			 (char *) chunk < (char *) block->freeptr;
			 chunk = (AllocChunk) ((char *) chunk + chunk->size + ALLOC_CHUNKHDRSZ))
		{
			if (AllocSetChunkIsFree(chunk, set))
				continue;

			/*
			 * The chunk is currently in usage. If we didn't fill the info, the
			 * chunk wasn't allocated by one of our macros. Make sure we still
			 * account it's memory.
			 */
			if (chunk->info.init != EXTRA_DYNAMIC_MEMORY_DEBUG_INIT_MAGIC ||
				chunk->info.func == NULL ||
				chunk->info.key.parent_func == NULL ||
				chunk->info.filename == NULL)
			{
				chunk->info.func = "<no information>";
				chunk->info.filename = "<unknown>";
				chunk->info.key.parent_func = "<no information>";
			}

			AllocChunkUpdateStats(set->chunkTable, chunk);
		}
	}
}
