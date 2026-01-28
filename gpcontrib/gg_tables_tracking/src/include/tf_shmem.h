#ifndef TF_SHMEM_H
#define TF_SHMEM_H

#include "storage/lwlock.h"
#include "port/atomics.h"

#include "bloom_set.h"

/*
 * Stores the Bloom filter in shared memory.
 * tracking_is_initialized - a flag indicating
 * bgworker bound dbids at startup/recovery.
 *
 * bloom_set - set of db_track_count Bloom filters.
 */
typedef struct
{
	pg_atomic_flag tracking_is_initialized;
	bloom_set_t bloom_set;
}	tf_shared_state_t;

extern tf_shared_state_t *tf_shared_state;

void		tf_shmem_init(void);
void		tf_shmem_deinit(void);

#endif   /* TF_SHMEM_H */
