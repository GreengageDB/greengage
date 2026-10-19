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
 * src/backend/access/common/tempcat_registry.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/tempcat.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/hsearch.h"

/* Upper bound on edges published by all sessions together. */
#define TEMPCAT_REGISTRY_MAX_EDGES 16384

static HTAB *TempcatRegistryHash;

static bool tempcat_edge_owner_is_me(const TempcatDepEdge *edge);
static bool tempcat_edge_owner_is_alive(const TempcatDepEdge *edge);

Size
tempcat_registry_shmem_size(void)
{
	return hash_estimate_size(TEMPCAT_REGISTRY_MAX_EDGES,
							  sizeof(TempcatDepEdge));
}

void
tempcat_registry_init(void)
{
	HASHCTL		ctl;

	/* The edge struct is its own hash key. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TempcatDepEdge);
	ctl.entrysize = sizeof(TempcatDepEdge);

	TempcatRegistryHash = ShmemInitHash("Tempcat dependency registry",
										TEMPCAT_REGISTRY_MAX_EDGES,
										TEMPCAT_REGISTRY_MAX_EDGES,
										&ctl,
										HASH_ELEM | HASH_BLOBS);
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
tempcat_registry_publish(int nedges, const TempcatDepEdge *edges)
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
		break;
	}

	LWLockRelease(TempcatDependencyLock);

	return result;
}

static bool
tempcat_edge_owner_is_me(const TempcatDepEdge *edge)
{
	return (MyProc != NULL &&
			edge->pgprocno == MyProc->pgprocno &&
			edge->pid == MyProc->pid);
}

/*
 * ProcKill() zeroes the PGPROC's pid on clean exits; the OS check covers
 * an unclean kill whose PGPROC slot has not been reused yet (a recycled
 * pid is caught by the pid-to-slot comparison).
 */
static bool
tempcat_edge_owner_is_alive(const TempcatDepEdge *edge)
{
	PGPROC	   *proc;

	if (edge->pgprocno < 0 || edge->pgprocno >= MaxBackends)
		return false;
	proc = &ProcGlobal->allProcs[edge->pgprocno];

	if (proc->pid == 0 || proc->pid != edge->pid)
		return false;

#ifdef HAVE_KILL
	if (kill(edge->pid, 0) != 0 && errno == ESRCH)
		return false;
#endif

	return true;
}
