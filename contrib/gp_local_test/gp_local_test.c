/* gp_local_test.c */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

void _PG_init(void);
PGDLLEXPORT void gp_local_test_main(Datum main_arg) pg_attribute_noreturn();

PG_FUNCTION_INFO_V1(gp_local_test_current_counter);

static volatile sig_atomic_t got_sigterm = false;

static void
gp_local_test_sigterm(SIGNAL_ARGS)
{
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
}

static bool
gp_local_test_table_exists(void)
{
    bool exists = false;

    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    {
        int ret = SPI_execute(
            "SELECT 1 FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = 'state' AND n.nspname = 'ext_local_test'",
            true, 1);

        exists = (ret == SPI_OK_SELECT && SPI_processed > 0);
    }
    PopActiveSnapshot();
    SPI_finish();

    return exists;
}
/*
 * Background worker main loop: every 5s, bump the 'counter' row
 * via ordinary SPI, exactly the way a client backend would.
 * Nothing here is GPDB-aware -- this is the point.
 */
void
gp_local_test_main(Datum main_arg)
{
    pqsignal(SIGTERM, gp_local_test_sigterm);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("postgres", NULL);

    while (!got_sigterm)
    {
        int rc = WaitLatch(&MyProc->procLatch,
                            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                            5000L);
        ResetLatch(&MyProc->procLatch);

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        if (got_sigterm)
            break;

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();

        PG_TRY();
        {
            if (gp_local_test_table_exists())
            {
                SPI_connect();
                PushActiveSnapshot(GetTransactionSnapshot());
                SPI_execute("SELECT ext_local_test.bump_counter('counter')",
                            false, 0);
                PopActiveSnapshot();
                SPI_finish();
            }
            /* else: extension not installed yet in this database -- skip this cycle */
        }
        PG_CATCH();
        {
            /* don't let a transient error kill the worker permanently */
            EmitErrorReport();
            FlushErrorState();
            AbortCurrentTransaction();
        }
        PG_END_TRY();

        if (IsTransactionState())
            CommitTransactionCommand();
    }

    proc_exit(0);
}

void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        return;

    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "gp_local_test worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    worker.bgw_main = gp_local_test_main;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

/* current_counter(text) -- plain SPI read, called from SQL */
Datum
gp_local_test_current_counter(PG_FUNCTION_ARGS)
{
    text *code = PG_GETARG_TEXT_PP(0);
    int32 result = 0;

    SPI_connect();
    {
        Oid argtypes[1] = { TEXTOID };
        Datum values[1] = { PointerGetDatum(code) };

        SPI_execute_with_args(
            "SELECT value FROM ext_local_test.state WHERE code = $1",
            1, argtypes, values, NULL, true, 1);

        if (SPI_processed > 0)
        {
            bool isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                     SPI_tuptable->tupdesc, 1, &isnull);
            if (!isnull)
                result = DatumGetInt32(d);
        }
    }
    SPI_finish();

    PG_RETURN_INT32(result);
}

