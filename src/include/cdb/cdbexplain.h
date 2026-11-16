/*-------------------------------------------------------------------------
 *
 * cdbexplain.h
 *    Functions supporting the Greengage EXPLAIN ANALYZE command
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbexplain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBEXPLAIN_H
#define CDBEXPLAIN_H

#include "executor/instrument.h"        /* instr_time */

struct CdbDispatchResults;              /* #include "cdb/cdbdispatchresult.h" */
struct PlanState;                       /* #include "nodes/execnodes.h" */
struct QueryDesc;                       /* #include "executor/execdesc.h" */
struct StringInfoData;                  /* #include "lib/stringinfo.h" */

struct CdbExplain_ShowStatCtx;          /* private, in "cdb/cdbexplain.c" */

typedef struct
{
	double		vmax;			/* maximum value of statistic */
	double		vsum;			/* sum of values */
	int			vcnt;			/* count of values > 0 */
	int			imax;			/* id of 1st observation having maximum value */
} CdbExplain_Agg;

static inline void
cdbexplain_agg_init0(CdbExplain_Agg *agg)
{
    agg->vmax = 0;
    agg->vsum = 0;
    agg->vcnt = 0;
    agg->imax = 0;
}

static inline bool
cdbexplain_agg_upd(CdbExplain_Agg *agg, double v, int id)
{
    if (v > 0)
    {
        agg->vsum += v;
        agg->vcnt++;

        if (v > agg->vmax ||
            agg->vcnt == 0)
        {
            agg->vmax = v;
            agg->imax = id;
            return true;
        }
    }
    return false;
}

static inline double
cdbexplain_agg_avg(CdbExplain_Agg *agg)
{
    return (agg->vcnt > 0) ? agg->vsum / agg->vcnt
                           : 0;
}

/*
 * cdbexplain_localExecStats
 *    Called by qDisp to build NodeSummary and SliceSummary blocks
 *    containing EXPLAIN ANALYZE statistics for a root slice that
 *    has been executed locally in the qDisp process.  Attaches these
 *    structures to the PlanState nodes' Instrumentation objects for
 *    later use by cdbexplain_showExecStats().
 *
 * 'planstate' is the top PlanState node of the slice.
 * 'showstatctx' is a CdbExplain_ShowStatCtx object which was created by
 *      calling cdbexplain_showExecStatsBegin().
 */
void
cdbexplain_localExecStats(struct PlanState                 *planstate,
                          struct CdbExplain_ShowStatCtx    *showstatctx);

/*
 * Hooks for extensions to send statistics of their own from each qExec to
 * the qDisp, with the statistics of the qExec's slice that EXPLAIN ANALYZE
 * collects, and auto_explain with log_analyze too: the hooks cannot tell
 * which one asked, and a qExec does not know the options of an EXPLAIN,
 * those of extensions included.  A slice that the qDisp runs itself sends
 * no message.
 *
 * On a qExec, cdbexplain_send_hook runs while the statistics of its slice
 * are written, in ExecutorEnd of each statement whose slice table asks for
 * INSTRUMENT_CDB, so an extension can tell at ExecutorStart that it will
 * run; queryDesc is that of the statement.  It adds its data to buf with
 * cdbexplain_appendExtensionData, under a name that tells it from the data
 * of other extensions.
 *
 * On the qDisp, cdbexplain_recv_hook runs where the qDisp takes in the
 * statistics of a slice: for each qExec's message once with name NULL,
 * data NULL and len 0, so that an extension can tell a qExec that sent
 * nothing of its own, then once for each piece of data the qExec sent.
 * The qDisp takes in the statistics of an initplan dispatched on its own
 * when the initplan ends, and those of the main plan when EXPLAIN prints
 * the plan, before explain_per_plan_hook runs for it, or when auto_explain
 * logs it; each at most once, so each message reaches the hook at most
 * once, and those of the main plan of a statement that auto_explain does
 * not log never do.  showstatctx is that of the statement, queryDesc->showstatctx,
 * which EXPLAIN sets before ExecutorStart and auto_explain in its
 * ExecutorStart hook, and which lives as long as the QueryDesc; sliceIndex,
 * segindex and qe_identifier are those of the qExec, segindex -1 for one
 * on the coordinator.  The data is not aligned and lives only during the
 * call.
 *
 * Each hook calls the one it replaced, if any.
 */
typedef void (*cdbexplain_send_hook_type) (struct QueryDesc *queryDesc,
										   struct StringInfoData *buf);
extern PGDLLIMPORT cdbexplain_send_hook_type cdbexplain_send_hook;

typedef void (*cdbexplain_recv_hook_type) (struct CdbExplain_ShowStatCtx *showstatctx,
										   int sliceIndex, int segindex,
										   int qe_identifier,
										   const char *name,
										   const char *data, int len);
extern PGDLLIMPORT cdbexplain_recv_hook_type cdbexplain_recv_hook;

extern void cdbexplain_appendExtensionData(struct StringInfoData *buf,
										   const char *name,
										   const void *data, int len);

/*
 * cdbexplain_sendExecStats
 *    Called by qExec process to send EXPLAIN ANALYZE statistics to qDisp.
 *    On the qDisp, libpq will attach this data to the PGresult object.
 */
void
cdbexplain_sendExecStats(struct QueryDesc *queryDesc);

/*
 * cdbexplain_recvExecStats
 *    Called by qDisp to transfer a slice's EXPLAIN ANALYZE statistics
 *    from the CdbDispatchResults structures to the PlanState tree.
 *    Recursively does the same for slices that are descendants of the
 *    one specified.
 *
 * 'showstatctx' is a CdbExplain_ShowStatCtx object which was created by
 *      calling cdbexplain_showExecStatsBegin().
 */
void
cdbexplain_recvExecStats(struct PlanState              *planstate,
                         struct CdbDispatchResults     *dispatchResults,
                         int                            sliceIndex,
                         struct CdbExplain_ShowStatCtx *showstatctx);

/*
 * cdbexplain_showExecStatsBegin
 *    Called by qDisp process to create a CdbExplain_ShowStatCtx structure
 *    in which to accumulate overall statistics for a query.
 *
 * 'explaincxt' is a MemoryContext from which to allocate the ShowStatCtx as
 *      well as any needed buffers and the like.  The explaincxt ptr is saved
 *      in the ShowStatCtx.  The caller is expected to reset or destroy the
 *      explaincxt not too long after calling cdbexplain_showExecStatsEnd(); so
 *      we don't bother to pfree() memory that we allocate from this context.
 * 'querystarttime' is the timestamp of the start of the query, in a
 *      platform-dependent format.
 */
struct CdbExplain_ShowStatCtx *
cdbexplain_showExecStatsBegin(struct QueryDesc *queryDesc,
                              instr_time        querystarttime);

/*
 * cdbexplain_showStatCtxFree
 *	  Release memory allocated for CdbExplain_ShowStatCtx structure and its
 *	  internals. Memory for insides of the slices array elements is allocated
 *	  in ExplainPrintPlan(). If ExplainPrintPlan() is called from the
 *	  auto_explain extension, then this memory is released in
 *	  standard_ExecutorEnd() -> FreeExecutorState() to avoid memory leak in the
 *	  case of queries with multiple call of SQL functions.
 *	  If ExplainPrintPlan() is called from ExplainOnePlan(), then this memory
 *	  is released in PortalDrop().
 */
void
cdbexplain_showStatCtxFree(struct CdbExplain_ShowStatCtx *ctx);



#endif   /* CDBEXPLAIN_H */
