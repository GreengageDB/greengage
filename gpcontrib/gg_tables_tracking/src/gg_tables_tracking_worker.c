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
 * Main worker tracking status check
 *
 * 1. Test the global flag whether reinitialization is required.
 * 2. Get the db oids, for which the tracking_is_db_tracked is set.
 * 3. Assign unassigned bloom filters to the dbs
 * 4. Set global flag to initialize the segment
 */
static void
worker_tracking_status_check()
{
	List	   *tracked_dbs = NIL;

	if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized))
	{
		StartTransactionCommand();

		tracked_dbs = get_tracked_dbs();

		if (list_length(tracked_dbs) > 0)
			track_dbs_local(tracked_dbs);

		if (tracked_dbs)
			list_free_deep(tracked_dbs);

		CommitTransactionCommand();

		pg_atomic_test_set_flag(&tf_shared_state->tracking_is_initialized);
	}
}

/*
 * Main worker cycle. Scans pg_db_role_setting and binds tracked dbids to
 * corresponding Bloom filter. Lives on segments for binding.
 */
void
gg_tables_tracking_main(Datum main_arg)
{
	instr_time	current_time_timeout;
	instr_time	start_time_timeout;
	long		current_timeout = -1;

	ereport(LOG, (errmsg("[gg_tables_tracking] Starting background worker")));

	/*
	 * The worker shouldn't exist when the coordinator boots in utility mode.
	 */
	if (IS_QUERY_DISPATCHER() && Gp_role != GP_ROLE_DISPATCH)
	{
		proc_exit(0);
	}

	/* Run as utility to get the pg_db_role_setting info */
	if (!IS_QUERY_DISPATCHER())
		Gp_role = GP_ROLE_UTILITY;

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
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, TOOLKIT_BINARY_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "gg_tables_tracking_main");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "gg_tables_tracking");

	RegisterBackgroundWorker(&worker);
}
