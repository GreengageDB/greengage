/*-------------------------------------------------------------------------
 *
 * tempcat_registry.c
 *	  Shared-memory registry of dependency edges held by temporary objects.
 *
 * The pg_depend and pg_shdepend rows of a temporary object are virtual
 * (they live in the owning backend's temp memory catalog, like every other
 * row of the object), so no other session can discover them in a catalog
 * scan.  This registry is the cluster-wide replacement: at transaction
 * commit, a backend that created such rows publishes their edges here.
 * The pg_inherits rows of temporary children are published as synthetic
 * auto edges (plain inheritance records no pg_depend row).  The consult
 * sites in performDeletion(), shdepDropOwned(), shdepReassignOwned(),
 * DropRole() and the domain/type/alter walkers block drops and alters
 * of the referenced shared objects while the owning session is alive.
 *
 * The owner identity in each edge is (pgprocno, pid).  ProcKill() zeroes
 * the PGPROC's pid on clean exit; after an unclean kill the pid is stale
 * until the slot is reused, so liveness also checks the OS process.  An
 * edge whose owner is dead is ignored, and the next publisher removes it:
 * unlike on-disk rows, nothing outlives the owning session for long.
 *
 * Entries are keyed by the whole edge, with no payload.  Mutations hold
 * TempcatDependencyLock exclusively, consults hold it shared.
 *
 * The registry also reserves the OIDs of virtual catalog rows.  The OID
 * collision check in GetNewOidWithIndex() scans catalog indexes, which
 * cannot see virtual rows: without a reservation, the shared counter could
 * wrap onto a live temporary object's OID and hand it to another session.
 * Reservations are made at allocation time (the uncommitted window must be
 * covered too) and regenerated from the committed snapshot at commit or
 * abort.  A watermark - the smallest reserved OID - lets allocations below
 * it skip the probe entirely.
 *
 * src/backend/access/common/tempcat_registry.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/tempcat.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/hsearch.h"

/* Upper bound on edges/OIDs published by all sessions together. */
#define TEMPCAT_REGISTRY_MAX_EDGES 16384
#define TEMPCAT_REGISTRY_MAX_OIDS 16384

/* Entry of the OID reservation hash; the oid is the key. */
typedef struct TempcatOidReservation
{
	Oid			oid;
	int			pgprocno;
	int			pid;
} TempcatOidReservation;

static HTAB *TempcatRegistryHash;
static HTAB *TempcatOidHash;
static pg_atomic_uint32 *TempcatOidWatermark;

static bool tempcat_owner_is_me(int pgprocno, int pid);
static bool tempcat_owner_is_alive(int pgprocno, int pid);
static void tempcat_oid_watermark_recompute(void);

static bool
tempcat_edge_owner_is_me(const TempcatDepEdge *edge)
{
	return tempcat_owner_is_me(edge->pgprocno, edge->pid);
}

static bool
tempcat_edge_owner_is_alive(const TempcatDepEdge *edge)
{
	return tempcat_owner_is_alive(edge->pgprocno, edge->pid);
}

Size
tempcat_registry_shmem_size(void)
{
	return add_size(hash_estimate_size(TEMPCAT_REGISTRY_MAX_EDGES,
									  sizeof(TempcatDepEdge)),
					add_size(hash_estimate_size(TEMPCAT_REGISTRY_MAX_OIDS,
												sizeof(TempcatOidReservation)),
							 sizeof(pg_atomic_uint32)));
}

void
tempcat_registry_init(void)
{
	HASHCTL		ctl;
	bool		found;

	/* The edge struct is its own hash key. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TempcatDepEdge);
	ctl.entrysize = sizeof(TempcatDepEdge);

	TempcatRegistryHash = ShmemInitHash("Tempcat dependency registry",
										TEMPCAT_REGISTRY_MAX_EDGES,
										TEMPCAT_REGISTRY_MAX_EDGES,
										&ctl,
										HASH_ELEM | HASH_BLOBS);

	/* OID reservations: key is the oid, payload the owner identity. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TempcatOidReservation);

	TempcatOidHash = ShmemInitHash("Tempcat OID reservations",
								   TEMPCAT_REGISTRY_MAX_OIDS,
								   TEMPCAT_REGISTRY_MAX_OIDS,
								   &ctl,
								   HASH_ELEM | HASH_BLOBS);

	TempcatOidWatermark = ShmemInitStruct("Tempcat OID watermark",
										  sizeof(pg_atomic_uint32),
										  &found);
	if (!found)
		pg_atomic_init_u32(TempcatOidWatermark, PG_UINT32_MAX);
}

/*
 * Replace this backend's published edges with the given set.  Edges left
 * behind by dead backends are removed as well: their owner is known to be
 * gone exactly when the PGPROC slot's pid no longer matches.
 *
 * Runs during commit, so it must not throw: a full shared dynahash would
 * ereport(ERROR); HASH_ENTER_NULL skips the extra edges with a WARNING.
 */
void
tempcat_registry_publish(int nedges, const TempcatDepEdge *edges,
						 int noids, const Oid *oids)
{
	HASH_SEQ_STATUS status;
	TempcatDepEdge *entry;
	int			i;
	bool		overflow = false;

	LWLockAcquire(TempcatDependencyLock, LW_EXCLUSIVE);

	hash_seq_init(&status, TempcatRegistryHash);
	while ((entry = (TempcatDepEdge *) hash_seq_search(&status)) != NULL)
	{
		if (tempcat_edge_owner_is_me(entry) ||
			!tempcat_edge_owner_is_alive(entry))
			(void) hash_search(TempcatRegistryHash, entry,
							   HASH_REMOVE, NULL);
	}

	for (i = 0; i < nedges; i++)
	{
		TempcatDepEdge edge = edges[i];

		edge.pgprocno = MyProc->pgprocno;
		edge.pid = MyProc->pid;

		entry = (TempcatDepEdge *) hash_search(TempcatRegistryHash, &edge,
											   HASH_ENTER_NULL, NULL);
		if (entry == NULL)
			overflow = true;
	}

	/*
	 * Same regeneration for the OID reservations: allocation-time
	 * reservations of rolled-back transactions and OIDs of dropped objects
	 * disappear with this rewrite.
	 */
	{
		TempcatOidReservation *oidentry;

		hash_seq_init(&status, TempcatOidHash);
		while ((oidentry = (TempcatOidReservation *) hash_seq_search(&status)) != NULL)
		{
			if (tempcat_owner_is_me(oidentry->pgprocno, oidentry->pid) ||
				!tempcat_owner_is_alive(oidentry->pgprocno, oidentry->pid))
				(void) hash_search(TempcatOidHash, oidentry,
								   HASH_REMOVE, NULL);
		}
	}

	for (i = 0; i < noids; i++)
	{
		TempcatOidReservation *oidentry;

		oidentry = (TempcatOidReservation *) hash_search(TempcatOidHash, &oids[i],
														 HASH_ENTER_NULL, NULL);
		if (oidentry == NULL)
			overflow = true;
		else if (!tempcat_owner_is_me(oidentry->pgprocno, oidentry->pid))
		{
			oidentry->pgprocno = MyProc->pgprocno;
			oidentry->pid = MyProc->pid;
		}
	}

	tempcat_oid_watermark_recompute();

	LWLockRelease(TempcatDependencyLock);

	if (overflow)
		ereport(WARNING,
				(errmsg("out of temporary dependency registry slots"),
				 errdetail("Some dependencies of temporary objects are not published; drops of the objects they reference are not blocked."),
				 errhint("Drop unused temporary objects in other sessions, or raise the registry size.")));
}

void
tempcat_registry_unpublish(void)
{
	HASH_SEQ_STATUS status;
	TempcatDepEdge *entry;
	TempcatOidReservation *oidentry;

	if (TempcatRegistryHash == NULL)
		return;

	LWLockAcquire(TempcatDependencyLock, LW_EXCLUSIVE);

	hash_seq_init(&status, TempcatRegistryHash);
	while ((entry = (TempcatDepEdge *) hash_seq_search(&status)) != NULL)
	{
		if (tempcat_edge_owner_is_me(entry))
			(void) hash_search(TempcatRegistryHash, entry,
							   HASH_REMOVE, NULL);
	}

	hash_seq_init(&status, TempcatOidHash);
	while ((oidentry = (TempcatOidReservation *) hash_seq_search(&status)) != NULL)
	{
		if (tempcat_owner_is_me(oidentry->pgprocno, oidentry->pid))
			(void) hash_search(TempcatOidHash, oidentry,
							   HASH_REMOVE, NULL);
	}

	tempcat_oid_watermark_recompute();

	LWLockRelease(TempcatDependencyLock);
}

/*
 * before_shmem_exit, not on_proc_exit: the unpublish takes a LWLock and
 * must run before ProcKill() (on_shmem_exit) detaches the PGPROC.
 */
void
tempcat_registry_unpublish_on_exit(int code, Datum arg)
{
	tempcat_registry_unpublish();
}

bool
tempcat_registry_has_dependents(const ObjectAddress *referenced,
								char deptype,
								ObjectAddress *depender)
{
	HASH_SEQ_STATUS status;
	TempcatDepEdge *entry;
	bool		result = false;

	if (TempcatRegistryHash == NULL)
		return false;

	LWLockAcquire(TempcatDependencyLock, LW_SHARED);

	hash_seq_init(&status, TempcatRegistryHash);
	while ((entry = (TempcatDepEdge *) hash_seq_search(&status)) != NULL)
	{
		if (entry->refclassid != referenced->classId ||
			entry->refobjid != referenced->objectId)
			continue;
		/* Whole-object references match any sub-object and vice versa. */
		if (entry->refobjsubid != 0 && referenced->objectSubId != 0 &&
			entry->refobjsubid != referenced->objectSubId)
			continue;
		if (deptype != 0 && entry->deptype != deptype)
			continue;
		/*
		 * pg_shdepend edges (dbid == InvalidOid) are cluster-wide; the
		 * rest must be from this database.
		 */
		if (entry->dbid != InvalidOid && entry->dbid != MyDatabaseId)
			continue;
		/* The owning session's own rows are visible in its catalog scans. */
		if (tempcat_edge_owner_is_me(entry))
			continue;
		if (!tempcat_edge_owner_is_alive(entry))
			continue;

		if (depender != NULL)
		{
			depender->classId = entry->classid;
			depender->objectId = entry->objid;
			depender->objectSubId = entry->objsubid;
		}
		result = true;
		hash_seq_term(&status);
		break;
	}

	LWLockRelease(TempcatDependencyLock);

	return result;
}

static bool
tempcat_owner_is_me(int pgprocno, int pid)
{
	return (MyProc != NULL &&
			pgprocno == MyProc->pgprocno &&
			pid == MyProc->pid);
}

/*
 * ProcKill() zeroes the PGPROC's pid on clean exits; the OS check covers
 * an unclean kill whose PGPROC slot has not been reused yet (a recycled
 * pid is caught by the pid-to-slot comparison).
 */
static bool
tempcat_owner_is_alive(int pgprocno, int pid)
{
	PGPROC	   *proc;

	if (pgprocno < 0 || pgprocno >= MaxBackends)
		return false;
	proc = &ProcGlobal->allProcs[pgprocno];

	if (proc->pid == 0 || proc->pid != pid)
		return false;

#ifdef HAVE_KILL
	if (kill(pid, 0) != 0 && errno == ESRCH)
		return false;
#endif

	return true;
}

/*
 * Is this OID owned by a live session's temporary object?  Used by the
 * OID allocator's collision loop: below the watermark nothing is
 * reserved and the probe can be skipped.
 */
bool
tempcat_registry_oid_taken(Oid oid)
{
	TempcatOidReservation *entry;
	bool		taken = false;

	if (TempcatOidHash == NULL ||
		oid < pg_atomic_read_u32(TempcatOidWatermark))
		return false;

	LWLockAcquire(TempcatDependencyLock, LW_SHARED);

	entry = (TempcatOidReservation *) hash_search(TempcatOidHash, &oid,
												  HASH_FIND, NULL);
	if (entry != NULL &&
		tempcat_owner_is_alive(entry->pgprocno, entry->pid))
		taken = true;

	LWLockRelease(TempcatDependencyLock);

	return taken;
}

/*
 * Reserve an OID for a virtual catalog row, at the moment the allocator
 * hands it out: the row is not in the committed snapshot yet, and the
 * transaction might still abort.  Runs in normal statement context.
 */
void
tempcat_registry_reserve_oid(Oid oid)
{
	TempcatOidReservation *entry;

	Assert(TempcatOidHash != NULL);

	LWLockAcquire(TempcatDependencyLock, LW_EXCLUSIVE);

	entry = (TempcatOidReservation *) hash_search(TempcatOidHash, &oid,
												  HASH_ENTER_NULL, NULL);
	if (entry == NULL)
	{
		LWLockRelease(TempcatDependencyLock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of temporary OID reservation slots"),
				 errdetail("Too many OIDs are reserved for temporary objects across all sessions."),
				 errhint("Drop unused temporary objects in other sessions, or raise the registry size.")));
	}

	entry->pgprocno = MyProc->pgprocno;
	entry->pid = MyProc->pid;

	/* Lower the watermark if needed; removals recompute it in full. */
	if (oid < pg_atomic_read_u32(TempcatOidWatermark))
		pg_atomic_write_u32(TempcatOidWatermark, oid);

	LWLockRelease(TempcatDependencyLock);
}

/* Caller must hold TempcatDependencyLock exclusively. */
static void
tempcat_oid_watermark_recompute(void)
{
	HASH_SEQ_STATUS status;
	TempcatOidReservation *oidentry;
	Oid			watermark = PG_UINT32_MAX;

	hash_seq_init(&status, TempcatOidHash);
	while ((oidentry = (TempcatOidReservation *) hash_seq_search(&status)) != NULL)
	{
		if (oidentry->oid < watermark)
			watermark = oidentry->oid;
	}

	/* Never raise concurrently-lowered values: write the true minimum. */
	pg_atomic_write_u32(TempcatOidWatermark, watermark);
}
