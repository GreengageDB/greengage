#include "postgres.h"

#include "access/xact.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_extension.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "libpq-fe.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "pgstat.h"

#include "gg_tables_tracking_worker.h"
#include "gg_tables_tracking_guc.h"
#include "bloom_set.h"
#include "tf_shmem.h"

#define TOOLKIT_BINARY_NAME "gg_tables_tracking"
#define SQL(...) #__VA_ARGS__

typedef struct
{
	Oid			dbid;
	bool		get_full_snapshot_on_recovery;
}	tracked_db_t;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

void		gg_tables_tracking_main(Datum);

/*
 * Signal handler for SIGTERM
 * Set a flag to let the main loop to terminate, and set our latch to wake
 * it up.
 */
static void
tracking_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 * Set a flag to tell the main loop to reread the config file, and set
 * our latch to wake it up.
 */
static void
tracking_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static bool
is_extension_installed(void)
{
	Oid			schema_oid;

	/* Check if the schema exists */
	schema_oid = get_namespace_oid("gg_tables_tracking", true);

	return OidIsValid(schema_oid);
}

static List *
get_tracked_dbs()
{
	StringInfoData query;
	List	   *tracked_dbs = NIL;
	tracked_db_t *trackedDb;
	MemoryContext topcontext = CurrentMemoryContext;

	initStringInfo(&query);
	appendStringInfo(&query, SQL(
		WITH _ AS (
			WITH _ AS (
				SELECT "setdatabase", regexp_split_to_array(UNNEST("setconfig"), '=') AS "setconfig"
				FROM "pg_db_role_setting" WHERE "setrole"=0)
			SELECT "setdatabase", json_object(array_agg("setconfig"[1]), array_agg("setconfig"[2])) AS "setconfig"
			FROM _ GROUP BY 1)
		SELECT "setdatabase",
				("setconfig"->>'gg_tables_tracking.tracking_snapshot_on_recovery')::bool as "snapshot" FROM _ WHERE
				("setconfig"->>'gg_tables_tracking.tracking_is_db_tracked')::bool IS TRUE));

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errmsg("SPI_connect failed")));

	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute(query.data, true, 0) != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("SPI_execute failed")));

	for (uint64 row = 0; row < SPI_processed; row++)
	{
		HeapTuple	val = SPI_tuptable->vals[row];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		bool		isnull = false;
		Oid			dbid = DatumGetObjectId(SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "setdatabase"), &isnull));
		bool		get_snapshot_on_recovery = DatumGetBool(SPI_getbinval(val, tupdesc, SPI_fnumber(tupdesc, "snapshot"), &isnull));

		if (isnull)
			get_snapshot_on_recovery = get_full_snapshot_on_recovery;

		MemoryContext oldcontext = MemoryContextSwitchTo(topcontext);

		trackedDb = (tracked_db_t *) palloc0(sizeof(tracked_db_t));
		trackedDb->dbid = dbid;
		trackedDb->get_full_snapshot_on_recovery = get_snapshot_on_recovery;
		tracked_dbs = lappend(tracked_dbs, trackedDb);

		MemoryContextSwitchTo(oldcontext);
	}
	SPI_finish();
	PopActiveSnapshot();

	pfree(query.data);

	return tracked_dbs;
}

static void
track_dbs_local(List *tracked_dbs)
{
	ListCell   *cell;
	tracked_db_t *trackedDb;

	foreach(cell, tracked_dbs)
	{
		trackedDb = (tracked_db_t *) lfirst(cell);

		bloom_set_bind(trackedDb->dbid);
		bloom_set_trigger_bits(trackedDb->dbid,
							   trackedDb->get_full_snapshot_on_recovery);
	}
}

/*
 * Dispatch tracking setup to all segments
 * Uses the internal_bind_db function defined in track_files.c
 */
static bool
dispatch_tracking_setup(List *tracked_dbs, List *segment_ids)
{
	StringInfoData sql;
	ListCell   *cell;
	tracked_db_t *trackedDb;
	CdbPgResults cdb_pgresults = {NULL, 0};

	if (list_length(tracked_dbs) == 0 || list_length(segment_ids) == 0)
		return true;

	initStringInfo(&sql);

	/* Build a single SQL statement that calls internal_bind_db for each tracked db */
	appendStringInfo(&sql, "SELECT ");

	foreach(cell, tracked_dbs)
	{
		trackedDb = (tracked_db_t *) lfirst(cell);

		if (cell != list_head(tracked_dbs))
			appendStringInfo(&sql, ", ");

		appendStringInfo(&sql,
			"gg_tables_tracking.internal_bind_db(%u, %s)",
			trackedDb->dbid,
			trackedDb->get_full_snapshot_on_recovery ? "true" : "false");
	}

	ereport(DEBUG1,
		(errmsg("[gg_tables_tracking] Dispatching to segments: %s", sql.data)));

	/* Dispatch to all segments */
	CdbDispatchCommandToSegments(sql.data,
								 0,
								 segment_ids,
								 &cdb_pgresults);

	if (cdb_pgresults.numResults > 0)
		cdbdisp_clearCdbPgResults(&cdb_pgresults);

	pfree(sql.data);

	ereport(LOG,
			(errmsg("[gg_tables_tracking] Successfully dispatched tracking setup to %d segment(s)",
				list_length(segment_ids))));

	return true;
}

/*
 * Mark segments as initialized even when there are no databases to track
 */
static bool
dispatch_empty_initialization(List *segment_ids)
{
	CdbPgResults cdb_pgresults = {NULL, 0};

	if (list_length(segment_ids) == 0)
		return true;

	ereport(DEBUG1,
		(errmsg("[gg_tables_tracking] Marking %d segment(s) as initialized (no databases to track)",
			list_length(segment_ids))));

	/* Just mark the segments as initialized */
	CdbDispatchCommandToSegments(
		"SELECT gg_tables_tracking.internal_initialize_segments()",
		0,
		segment_ids,
		&cdb_pgresults);

	if (cdb_pgresults.numResults > 0)
		cdbdisp_clearCdbPgResults(&cdb_pgresults);

	ereport(LOG,
			(errmsg("[gg_tables_tracking] Successfully marked %d segment(s) as initialized",
				list_length(segment_ids))));

	return true;
}

/*
 * Check which segments need initialization and initialize them
 * Returns true if all segments are initialized, false otherwise.
 * This must be called on every worker cycle to handle segment
 * failures and mirror promotions.
 */
static bool
ensure_all_segments_initialized(List *tracked_dbs)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	List	   *uninitialized_segments = NIL;
	bool		all_initialized = true;
	int			i;

	ereport(DEBUG1,
		(errmsg("[gg_tables_tracking] Checking segment initialization status")));

	CdbDispatchCommand("SELECT * FROM gg_tables_tracking.tracking_is_segment_initialized()",
						   0,
						   &cdb_pgresults);

	/* Collect segments that are not initialized */
	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			ereport(WARNING,
				(errmsg("[gg_tables_tracking] Failed to get initialization status from segment %d", i)));
			all_initialized = false;
			continue;
		}

		if (PQntuples(pgresult) > 0)
		{
			int32		segindex;
			bool		is_initialized;

			segindex = atoi(PQgetvalue(pgresult, 0, 0));
			is_initialized = (strcmp(PQgetvalue(pgresult, 0, 1), "t") == 0);

			if (!is_initialized)
			{
				uninitialized_segments = lappend_int(uninitialized_segments, segindex);
				all_initialized = false;

				ereport(LOG,
					(errmsg("[gg_tables_tracking] Segment %d requires initialization", segindex)));
			}
			else
			{
				ereport(DEBUG2,
					(errmsg("[gg_tables_tracking] Segment %d is initialized", segindex)));
			}
		}
	}

	if (cdb_pgresults.numResults > 0)
		cdbdisp_clearCdbPgResults(&cdb_pgresults);

	/* If we found uninitialized segments, initialize them */
	if (list_length(uninitialized_segments) > 0)
	{
		ereport(LOG,
			(errmsg("[gg_tables_tracking] Found %d uninitialized segment(s), initializing now",
				list_length(uninitialized_segments))));

		if (list_length(tracked_dbs) > 0)
		{
			/* Initialize segments with tracked databases */
			dispatch_tracking_setup(tracked_dbs, uninitialized_segments);
		}
		else
		{
			/* No databases to track, but still mark segments as initialized */
			dispatch_empty_initialization(uninitialized_segments);
		}
		all_initialized = true;
		list_free(uninitialized_segments);
	}
	else
		ereport(DEBUG1,
			(errmsg("[gg_tables_tracking] All segments are initialized")));

	return all_initialized;
}

/*
 * Main worker tracking status check
 *
 * Each iteration:
 * 1. Check if extension is installed (early return if not)
 * 2. Check coordinator initialization
 * 3. Check all segments initialization status
 * 4. Initialize only uninitialized segments
 * 5. Set global flag only when all are initialized
 */
static void
worker_tracking_status_check()
{
	List	   *tracked_dbs = NIL;
	bool		coordinator_initialized;
	bool		all_segments_initialized;

	StartTransactionCommand();

	/*
	* EARLY CHECK: If extension is not installed, skip this cycle.
	 */
	if (!is_extension_installed())
	{
		ereport(LOG,
			(errmsg("[gg_tables_tracking] Extension not yet installed, skipping initialization")));
		CommitTransactionCommand();
		return;
	}


	tracked_dbs = get_tracked_dbs();

	/*
	 * Step 1: Ensure coordinator is initialized
	 */
	coordinator_initialized = !pg_atomic_unlocked_test_flag(
						&tf_shared_state->tracking_is_initialized);

	if (!coordinator_initialized)
	{
		ereport(LOG,
			(errmsg("[gg_tables_tracking] Initializing tracking on coordinator for %d database(s)",
				list_length(tracked_dbs))));

		/* Initialize coordinator */
		if (list_length(tracked_dbs) > 0)
			track_dbs_local(tracked_dbs);
	}
	/*
	 * Step 2: Ensure all segments are initialized
	 * This will:
	 * - Check each segment's initialization status
	 * - Dispatch setup only to uninitialized segments
	 * - Automatically handle segment restarts/failures
	 */
	all_segments_initialized = ensure_all_segments_initialized(tracked_dbs);

	if (all_segments_initialized)
	{
		if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized))
		{
			pg_atomic_test_set_flag(&tf_shared_state->tracking_is_initialized);
			ereport(LOG,
					(errmsg("[gg_tables_tracking] Cluster fully initialized")));
		}
	}
	else
	{
		if (!pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized))
		{
			/* We were initialized but lost some segments */
			pg_atomic_clear_flag(&tf_shared_state->tracking_is_initialized);
		}
	}

	if (tracked_dbs)
		list_free_deep(tracked_dbs);

	CommitTransactionCommand();
}

/* Main worker cycle. Scans pg_db_role_setting and binds tracked dbids to
 * corresponding Bloom filter. Dispatches to segments for binding. */
void
gg_tables_tracking_main(Datum main_arg)
{
	instr_time	current_time_timeout;
	instr_time	start_time_timeout;
	long		current_timeout = -1;

	ereport(LOG, (errmsg("[gg_tables_tracking] Starting background worker")));

	/*
	 * The worker shouldn't exist when the master boots in utility mode.
	 */
	if (IS_QUERY_DISPATCHER() && Gp_role != GP_ROLE_DISPATCH)
	{
		proc_exit(0);
	}

	pqsignal(SIGHUP, tracking_sighup);
	pqsignal(SIGTERM, tracking_sigterm);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL, 0);

	while (!got_sigterm)
	{
		int			rc;
		long		timeout = tracking_worker_naptime_sec * 1000;

		if (current_timeout <= 0)
		{
			worker_tracking_status_check();

			INSTR_TIME_SET_CURRENT(start_time_timeout);
			current_timeout = timeout;
		}

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   current_timeout, PG_WAIT_EXTENSION);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(&MyProc->procLatch);
			CHECK_FOR_INTERRUPTS();
		}

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(LOG, (errmsg("[gg_tables_tracking] bgworker is being terminated by postmaster death.")));
			proc_exit(1);
		}

		if (got_sighup)
		{
			ereport(DEBUG1, (errmsg("[gg_tables_tracking] got sighup")));
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * We can wake up during WaitLatch very often, thus, timeout is
		 * calculated manually.
		 */
		INSTR_TIME_SET_CURRENT(current_time_timeout);
		INSTR_TIME_SUBTRACT(current_time_timeout, start_time_timeout);
		current_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(current_time_timeout);
	}

	ereport(LOG, (errmsg("[gg_tables_tracking] stop worker process")));

	proc_exit(0);
}

void
gg_tables_tracking_worker_register()
{
	BackgroundWorker worker = {0};

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = (tracking_worker_naptime_sec / 2);
	if (worker.bgw_restart_time < 1)
		worker.bgw_restart_time = 1;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, TOOLKIT_BINARY_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "gg_tables_tracking_main");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "gg_tables_tracking");

	RegisterBackgroundWorker(&worker);
}
