#include "utils/hsearch.h"
#include "access/hash.h"

extern HTAB *chunks_htable;

static void
update_AllocChunkStats(AllocChunk chunk)
{
	MemoryContextChunkStat_htabEntry *r = NULL;
	bool found = false;

	r = hash_search(chunks_htable, &chunk->info.key, HASH_ENTER, &found);
	Assert(r);

	if (found)
	{
		r->stat.count++;
		r->stat.bytes += chunk->size;
		return;
	}

	r->chunk_info = chunk->info;
	r->stat.bytes = chunk->size;
	r->stat.count = 1;
}

static uint32
MemoryContextChunkStatKeyHash(const void *key, Size keysize)
{
	Assert(keysize == sizeof(MemoryContextChunkStatKey));
	return DatumGetUInt32(hash_any((const unsigned char *) key,
								   keysize));
}

static int
MemoryContextChunkStatKeyCompare(const void *key1, const void *key2,
								 Size keysize)
{
	Assert(keysize == sizeof(MemoryContextChunkStatKey));
	MemoryContextChunkStatKey *k1 = (MemoryContextChunkStatKey *)key1;
	MemoryContextChunkStatKey *k2 = (MemoryContextChunkStatKey *)key2;

	if (0 == strcmp(k1->parent_func, k2->parent_func) && (k1->line == k2->line))
		return 0;

	return 1;
}

static bool
AllocSet_AllocChunk_is_free(AllocChunk chunk, AllocSet set)
{
	if (chunk->size > set->allocChunkLimit)
		return false;

	int a_fidx = AllocSetFreeIndex(chunk->size);
	AllocChunk free_chunk = set->freelist[a_fidx];

	while (free_chunk)
	{
		if (free_chunk == chunk)
			return true;

		free_chunk = (AllocChunk) free_chunk->sharedHeader;
	}

	return false;
}

static void
AllocSetGetAllocatedChunkStats(AllocSet set)
{
	HASHCTL		hash_ctl;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(MemoryContextChunkStatKey);
	hash_ctl.entrysize = sizeof(MemoryContextChunkStat_htabEntry);
	hash_ctl.hash = MemoryContextChunkStatKeyHash;
	hash_ctl.match = MemoryContextChunkStatKeyCompare;

	if (!chunks_htable)
		chunks_htable = hash_create("HTAB chunks_stats",
								 DYN_MEM_HTABLE_SIZE, &hash_ctl, HASH_FUNCTION |
								 HASH_ELEM | HASH_COMPARE | HASH_CONTEXT);

	for (AllocBlock block = set->blocks; block != NULL; block = block->next)
	{
		AllocChunk chunk = (AllocChunk) (((char *)block) + ALLOC_BLOCKHDRSZ);
		for (; (char *)chunk < (char *)block->freeptr;
		     chunk = (AllocChunk) ((char *)chunk +
		                           chunk->size + ALLOC_CHUNKHDRSZ))
		{
			if (AllocSet_AllocChunk_is_free(chunk, set))
				continue;
			if (chunk->info.init != DYNAMIC_MEMORY_DEBUG_INIT_MAGIC)
				continue;
			if (!chunk->info.key.parent_func || !chunk->info.key.line ||
			    !chunk->info.file || !chunk->info.exec_func)
				continue;

			update_AllocChunkStats(chunk);
		}
	}
}
