#include "bloom_set.h"
#include "tf_shmem.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

#include "gg_tables_tracking_guc.h"

static shmem_startup_hook_type next_shmem_startup_hook = NULL;
tf_shared_state_t *tf_shared_state = NULL;

static Size
tf_shmem_calc_size(void)
{
	Size		size;

	size = offsetof(tf_shared_state_t, bloom_set);
	size = add_size(size, bloom_set_required_size(bloom_size, db_track_count));

	return size;
}

static void
tf_shmem_hook(void)
{
	bool		found;
	Size		size;

	size = tf_shmem_calc_size();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	tf_shared_state = ShmemInitStruct("toolkit_track_files", size, &found);

	if (!found)
	{
		pg_atomic_init_flag(&tf_shared_state->tracking_is_initialized);

		bloom_set_init(db_track_count, bloom_size);
	}

	LWLockRelease(AddinShmemInitLock);

	if (next_shmem_startup_hook)
		next_shmem_startup_hook();
}

void
tf_shmem_init()
{
	RequestAddinShmemSpace(tf_shmem_calc_size());
	/*
	 * drops_track_lock and bloom_set_lock locks plus one lock for each db entry.
	 */
	RequestNamedLWLockTranche("gg_tables_tracking", 2 + db_track_count);

	next_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = tf_shmem_hook;
}

void
tf_shmem_deinit(void)
{
	shmem_startup_hook = next_shmem_startup_hook;
}
