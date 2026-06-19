/*-------------------------------------------------------------------------
 *
 * pgstat_gp.c
 *	  Greenplum-specific cumulative statistics.
 *
 * This file collects the GPDB extensions to the cumulative statistics system
 * that used to live in the (now removed) UDP-collector pgstat.c:
 *	  - per-backend resource group / session id reporting (PgBackendStatus),
 *	  - the per-backend local resource-queue/portal statistics hash.
 *
 * NOTE (PG15 merge): PostgreSQL 15 replaced the UDP statistics collector with
 * the shared-memory cumulative stats system.  The resource-queue level stats
 * used to be forwarded to that collector; there is no collector to send them
 * to any more, so pgstat_report_queuestat()/pgstat_fetch_stat_queueentry()
 * are degraded (local-only / no cross-backend aggregation) until they are
 * re-implemented on top of the shared-memory stats infrastructure.
 * See GPDB_15_MERGE_FIXME below.
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
 *	GPDB_15_MERGE_FIXME: the UDP statistics collector was removed in PG15, so
 *	the accumulated per-queue statistics can no longer be forwarded to a
 *	collector.  For now we just consume (reset) the backend-local accounting
 *	so it does not accumulate indefinitely.  Re-implement on top of the
 *	shared-memory cumulative stats system to restore cross-backend
 *	aggregation of resource-queue statistics.
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
		/* Reset the counters for this entry. */
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
 *	GPDB_15_MERGE_FIXME: with the UDP collector gone there is no cross-backend
 *	resource-queue stats hash to read; return NULL (callers report zeroes)
 *	until this is re-implemented on the shared-memory stats system.
 */
PgStat_StatQueueEntry *
pgstat_fetch_stat_queueentry(Oid queueid)
{
	return NULL;
}
