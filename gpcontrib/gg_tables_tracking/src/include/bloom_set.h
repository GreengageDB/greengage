#ifndef BLOOM_SET_H
#define BLOOM_SET_H

#include "postgres.h"
#include "storage/lwlock.h"
#include "port/atomics.h"

#include "track_bloom.h"

#define InvalidVersion		((uint32) 0)
#define ControlVersion		((uint32) 1)
#define StartVersion		((uint32) 2)

/* Bloom set entry. */
typedef struct
{
	Oid			dbid;			/* dbid of tracked database or InvalidOid */
	uint32		master_version; /* Auxiliary counter, which is sent from
								 * master to support transaction semantics */
	uint32		work_version;	/* Auxiliary counter which represents current
								 * state of bloom filter */
	pg_atomic_flag capture_in_progress; /* indicates whether tracking
										 * acquisition is in progress */
	bloom_t		bloom;			/* bloom filter itself */
}	bloom_entry_t;

/* Set of all allocated bloom filters*/
typedef struct
{
	uint8		bloom_count;	/* count of bloom_entry_t in bloom_entries */
	uint32		bloom_size;		/* size of bloom filter */
	bloom_entry_t bloom_entries[FLEXIBLE_ARRAY_MEMBER]; /* array of
														 * bloom_entry_t */
}	bloom_set_t;

typedef struct
{
	LWLock	   *set_lock;
	LWLock	   *entry_lock;
	bloom_entry_t *entry;
}	bloom_op_ctx_t;

/*
 * Locks on each bloom_entry_t in bloom_set.
 */
typedef struct
{
	void	   *entry;			/* It's a key that binds lock to bloom_entry */
	LWLock	   *lock;
}	tf_entry_lock_t;

extern LWLock *bloom_set_lock;
extern tf_entry_lock_t bloom_locks[];

Size		bloom_set_required_size(uint32 size, int count);
void		bloom_set_init(const uint32 bloom_count, const uint32 bloom_size);
bool		bloom_set_bind(Oid dbid);
void		bloom_set_unbind(Oid dbid);
void		bloom_set_set(Oid dbid, Oid relNode);
bool		bloom_set_merge(Oid dbid, bloom_t *from);
bool		bloom_set_trigger_bits(Oid dbid, bool on);
bool		bloom_set_is_all_bits_triggered(Oid dbid);
bloom_op_ctx_t bloom_set_get_entry(Oid dbid, LWLockMode s_mode, LWLockMode e_mode);
void		bloom_set_release(bloom_op_ctx_t *ctx);
LWLock	   *LWLockAcquireEntry(Oid dbid, LWLockMode mode);

#endif   /* BLOOM_SET_H */
