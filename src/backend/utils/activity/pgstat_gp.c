/*-------------------------------------------------------------------------
 *
 * pgstat_gp.c
 *	  Greengage-specific cumulative statistics.
 *
 * This file collects the GPDB extensions to the cumulative statistics system
 * that used to live in the (now removed) UDP-collector pgstat.c:
 *	  - per-backend resource group / session id reporting (PgBackendStatus),
 *	  - the per-backend local resource-queue/portal statistics hash.
 *
 * NOTE (PG15 merge): PostgreSQL 15 replaced the UDP statistics collector with
 * the shared-memory cumulative stats system.  The resource-queue level stats
 * used to be forwarded to that collector; they are now reported into the
 * shared-memory stats system as the PGSTAT_KIND_RESQUEUE kind (keyed by queue
 * OID), so pg_stat_resqueues again sees cross-backend per-queue totals.  The
 * backend-local per-portal hash below still tracks elapsed exec/wait time and
 * is flushed into the shared per-queue entry by pgstat_report_queuestat().
 *
 * Portions Copyright (c) 2006-2023, Greenplum inc
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_gp.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "utils/backend_status.h"
#include "utils/hsearch.h"
#include "utils/pgstat_internal.h"

/* GUC: collect resource-queue level statistics */
bool		pgstat_collect_queuelevel = false;

/* Local hash of per-portal resource-queue statistics for this backend. */
static HTAB *localStatPortalHash = NULL;


/*
 * Report the resource group a backend currently belongs to.
 */
void
pgstat_report_resgroup(Oid groupid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry)
		return;

	/*
	 * Update my status entry, following the protocol of bumping
	 * st_changecount before and after.  We use a volatile pointer here to
	 * ensure the compiler doesn't try to get cute.
	 */
	beentry->st_changecount++;
	beentry->st_rsgid = groupid;
	beentry->st_changecount++;
	Assert((beentry->st_changecount & 1) == 0);
}

/*
 * Report (from cdbgang) that the session id has been reset.
 */
void
pgstat_report_sessionid(int new_sessionid)
{
	volatile PgBackendStatus *beentry = MyBEEntry;

	if (!beentry)
		return;

	beentry->st_changecount++;
	beentry->st_session_id = new_sessionid;
	beentry->st_changecount++;
	Assert((beentry->st_changecount & 1) == 0);
}

/*
 * pgstat_init_localportalhash() -
 *
 *	Create the backend-local cache of per-portal resource-queue statistics.
 */
void
pgstat_init_localportalhash(void)
{
	HASHCTL		info;

	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(PgStat_StatPortalEntry);

	localStatPortalHash = hash_create("Local Stat Portal Hash",
									  1,
									  &info,
									  HASH_ELEM | HASH_BLOBS);
}

/*
 * pgstat_getportalentry() -
 *
 *	Return the (PgStat_StatPortalEntry *) for a given portal (and queue).
 */
PgStat_StatPortalEntry *
pgstat_getportalentry(uint32 portalid, Oid queueid)
{
	PgStat_StatPortalEntry *portalentry;
	bool		found;

	portalentry = hash_search(localStatPortalHash,
							  (void *) &portalid,
							  HASH_ENTER, &found);

	Assert(portalentry != NULL);

	/* Initialize if we have not seen this portal before! */
	if (!found || portalentry->queueentry.queueid == InvalidOid)
	{
		portalentry->portalid = portalid;
		portalentry->queueentry.queueid = queueid;
		portalentry->queueentry.n_queries_exec = 0;
		portalentry->queueentry.n_queries_wait = 0;
		portalentry->queueentry.elapsed_exec = 0;
		portalentry->queueentry.elapsed_wait = 0;
	}

	return portalentry;
}

/*
 * pgstat_report_queuestat() -
 *
 *	Called from tcop/postgres.c at end of statement.
 *
 *	PG15 removed the UDP statistics collector that used to aggregate the
 *	per-backend per-portal accounting into cross-backend per-queue totals.  We
 *	now forward that accounting directly into the shared-memory cumulative
 *	stats system: for each portal we add its counters into the shared
 *	PGSTAT_KIND_RESQUEUE entry for its queue (keyed by queue OID, global), then
 *	reset the backend-local accounting.  The shared entry is updated
 *	synchronously (like replication-slot stats), so the totals are immediately
 *	visible to pgstat_fetch_stat_queueentry() / pg_stat_resqueues.
 */
void
pgstat_report_queuestat(void)
{
	HASH_SEQ_STATUS hstat;
	PgStat_StatPortalEntry *pentry;

	if (!pgstat_collect_queuelevel || localStatPortalHash == NULL)
		return;

	hash_seq_init(&hstat, localStatPortalHash);
	while ((pentry = (PgStat_StatPortalEntry *) hash_seq_search(&hstat)) != NULL)
	{
		Oid			queueid = pentry->queueentry.queueid;
		PgStat_EntryRef *entry_ref;
		PgStat_StatQueueEntry *statent;

		/* Skip already-consumed / never-initialized portal entries. */
		if (queueid == InvalidOid)
			continue;

		/* Forward this portal's accounting into the shared per-queue entry. */
		entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RESQUEUE, InvalidOid,
												queueid, false);
		statent = &((PgStatShared_Resqueue *) entry_ref->shared_stats)->stats;
		statent->queueid = queueid;
		statent->n_queries_exec += pentry->queueentry.n_queries_exec;
		statent->n_queries_wait += pentry->queueentry.n_queries_wait;
		statent->elapsed_exec += pentry->queueentry.elapsed_exec;
		statent->elapsed_wait += pentry->queueentry.elapsed_wait;
		pgstat_unlock_entry(entry_ref);

		/* Reset the backend-local counters for this portal. */
		pentry->queueentry.queueid = InvalidOid;
		pentry->queueentry.n_queries_exec = 0;
		pentry->queueentry.n_queries_wait = 0;
		pentry->queueentry.elapsed_exec = 0;
		pentry->queueentry.elapsed_wait = 0;
	}
}

/*
 * pgstat_fetch_stat_queueentry() -
 *
 *	Support function for the SQL-callable resource-queue stats functions.
 *
 *	Reads the cross-backend per-queue totals from the shared-memory cumulative
 *	stats system (PGSTAT_KIND_RESQUEUE).  Returns NULL if the queue has no
 *	recorded statistics yet, in which case the callers report zeroes.
 */
PgStat_StatQueueEntry *
pgstat_fetch_stat_queueentry(Oid queueid)
{
	return (PgStat_StatQueueEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid);
}
