/*
 * Set of blooms. Main entry point to find a bloom and work with it.
 * Used to track create, extend, truncate events.
 */
#include "gg_tables_tracking_guc.h"
#include "bloom_set.h"
#include "tf_shmem.h"

LWLock	   *bloom_set_lock;
tf_entry_lock_t bloom_locks[MAX_DB_TRACK_COUNT];

static inline Size
bloom_entry_size(uint32 size)
{
	return (offsetof(bloom_entry_t, bloom) + full_bloom_size(2 * size));
}

static inline void *
bloom_entry_get(bloom_set_t *set, int idx)
{
	return (void *) ((uint8 *) set->bloom_entries + idx * bloom_entry_size(set->bloom_size));
}

/*
 * bloom_set api assumes that we are working with the single bloom set.
 * This object is considered as singleton.
 */
bloom_set_t *bloom_set = NULL;

static inline void
bloom_set_check_state(void)
{
	if (tf_shared_state == NULL || bloom_set == NULL)
		ereport(ERROR,
				(errmsg("Failed to access shared memory due to wrong extension initialization"),
				 errhint("Load extension's code through shared_preload_library configuration")));
}

static void
bloom_entry_init(const uint32_t size, bloom_entry_t *bloom_entry)
{
	bloom_entry->dbid = InvalidOid;
	bloom_entry->master_version = InvalidVersion;
	bloom_entry->work_version = InvalidVersion;
	bloom_init(size, &bloom_entry->bloom);
}


void
bloom_set_init(const uint32_t bloom_count, const uint32_t bloom_size)
{
	bloom_set = &tf_shared_state->bloom_set;

	bloom_set->bloom_count = bloom_count;
	bloom_set->bloom_size = bloom_size;

	LWLockPadded *locks = GetNamedLWLockTranche("gg_tables_tracking");

	/*
	 * Start assigning locks from second lock. The first one will be assigned
	 * to a drops_track_lock.
	 */
	locks++;
	bloom_set_lock = &(locks[0].lock);
	locks++;

	for (uint32_t i = 0; i < bloom_count; i++)
	{
		bloom_entry_t *bloom_entry = bloom_entry_get(bloom_set, i);

		bloom_entry_init(bloom_size, bloom_entry);
		bloom_locks[i].lock = &locks[i].lock;
		bloom_locks[i].entry = (void *) bloom_entry;
	}

	init_bloom_invariants();
}

Size
bloom_set_required_size(uint32 size, int count)
{
	return (offsetof(bloom_set_t, bloom_entries) + count * bloom_entry_size(size));
}

/*
 * Finds the entry in bloom_set by given dbid.
 * That's a simple linear search, should be reworked (depends on target dbs count).
 */
static bloom_entry_t *
find_bloom_entry(Oid dbid)
{
	bloom_entry_t *bloom_entry;
	int			i = 0;

	for (i = 0; i < bloom_set->bloom_count; i++)
	{
		bloom_entry = bloom_entry_get(bloom_set, i);
		if (bloom_entry->dbid == dbid)
			break;
	}

	if (i == bloom_set->bloom_count)
		return NULL;

	return bloom_entry;
}

/* Bind available filter to given dbid */
bool
bloom_set_bind(Oid dbid)
{
	bloom_entry_t *bloom_entry;

	bloom_set_check_state();

	LWLockAcquire(bloom_set_lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(dbid);
	if (bloom_entry)
	{
		LWLockRelease(bloom_set_lock);
		return true;
	}
	bloom_entry = find_bloom_entry(InvalidOid);
	if (bloom_entry == NULL)
	{
		LWLockRelease(bloom_set_lock);
		return false;
	}
	bloom_entry->dbid = dbid;
	bloom_entry->master_version = StartVersion;
	bloom_entry->work_version = StartVersion;
	pg_atomic_init_flag(&bloom_entry->capture_in_progress);
	LWLockRelease(bloom_set_lock);

	return true;
}

/*
 * Fill the Bloom filter with 0 or 1. Used for setting
 * full snapshots.
 */
bool
bloom_set_trigger_bits(Oid dbid, bool on)
{
	bloom_op_ctx_t ctx = bloom_set_get_entry(dbid, LW_SHARED, LW_EXCLUSIVE);

	if (ctx.entry)
	{
		if (on)
			bloom_set_all(&ctx.entry->bloom);
		else
			bloom_clear(&ctx.entry->bloom);

		bloom_set_release(&ctx);
		return true;
	}

	bloom_set_release(&ctx);

	return false;
}

/* Unbind used filter by given dbid */
void
bloom_set_unbind(Oid dbid)
{
	bloom_entry_t *bloom_entry;

	bloom_set_check_state();

	LWLockAcquire(bloom_set_lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(dbid);
	if (bloom_entry == NULL)
	{
		LWLockRelease(bloom_set_lock);
		return;
	}
	bloom_entry->dbid = InvalidOid;
	bloom_clear(&bloom_entry->bloom);
	LWLockRelease(bloom_set_lock);
}

/* Find bloom by dbid, set bit based on relNode hash */
void
bloom_set_set(Oid dbid, Oid relNode)
{
	bloom_op_ctx_t ctx = bloom_set_get_entry(dbid, LW_SHARED, LW_EXCLUSIVE);

	if (ctx.entry)
	{
		bloom_set_bits(&ctx.entry->bloom, relNode);
	}
	bloom_set_release(&ctx);

}

/* Find bloom by dbid, merge bytes from another bloom to it */
bool
bloom_set_merge(Oid dbid, bloom_t *from)
{
	if (!from)
		return false;

	bloom_op_ctx_t ctx = bloom_set_get_entry(dbid, LW_SHARED, LW_EXCLUSIVE);

	if (ctx.entry)
	{
		bloom_merge(&ctx.entry->bloom, from);
		bloom_set_release(&ctx);
		return true;
	}
	bloom_set_release(&ctx);

	return false;
}

bool
bloom_set_is_all_bits_triggered(Oid dbid)
{
	bool		is_triggered = false;
	bloom_op_ctx_t ctx = bloom_set_get_entry(dbid, LW_SHARED, LW_SHARED);

	if (ctx.entry)
	{
		is_triggered = ctx.entry->bloom.is_set_all;
	}

	bloom_set_release(&ctx);

	return is_triggered;
}

bloom_op_ctx_t
bloom_set_get_entry(Oid dbid, LWLockMode s_mode, LWLockMode e_mode)
{
	bloom_op_ctx_t ctx = {0};

	bloom_set_check_state();

	LWLockAcquire(bloom_set_lock, s_mode);
	ctx.entry_lock = LWLockAcquireEntry(dbid, e_mode);
	ctx.entry = find_bloom_entry(dbid);
	ctx.set_lock = bloom_set_lock;

	return ctx;
}
void
bloom_set_release(bloom_op_ctx_t *ctx)
{
	if (ctx->entry_lock)
		LWLockRelease(ctx->entry_lock);
	LWLockRelease(ctx->set_lock);
}

/*
 * Acquire lock corresponding to dbid in bloom_set.
 */
LWLock *
LWLockAcquireEntry(Oid dbid, LWLockMode mode)
{
	for (int i = 0; i < db_track_count; ++i)
	{
		bloom_entry_t *bloom_entry = (bloom_entry_t *) (bloom_locks[i].entry);

		if (bloom_entry->dbid == dbid)
		{
			LWLockAcquire(bloom_locks[i].lock, mode);
			return bloom_locks[i].lock;
		}
	}

	return NULL;
}
