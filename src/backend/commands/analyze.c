/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  the Postgres statistics generator
 *
 *
 * There are a few things in Greengage that make this more complicated
 * than in upstream:
 *
 * Dispatching
 * -----------
 *
 * Greengage is an MPP system, so we need to collect the statistics from
 * all the segments. The segment servers don't keep statistics (unless you
 * connect to a segment in utility node and run ANALYZE directly), and
 * the orchestration of ANALYZE happens in the dispatcher. The high
 * level logic is the same as in upstream, but a few functions have been
 * modified to gather data from the segments, instead of reading directly
 * from local disk:
 *
 * acquire_sample_rows(), when called in the dispatcher, calls into the
 * segments to acquire the sample across all segments.
 * RelationGetNumberOfBlocks() calls have been replaced with a wrapper
 * function, AcquireNumberOfBlocks(), which likewise calls into the
 * segments, to get total relation size across all segments.
 *
 * AcquireNumberOfBlocks() calls pg_relation_size(), which already
 * contains the logic to gather the size from all segments.
 *
 * Acquiring the sample rows is more tricky. When called in dispatcher,
 * acquire_sample_rows() calls a helper function called gp_acquire_sample_rows()
 * in the segments, to collect a sample on each segment. It then merges
 * the sample rows from each segment to produce a sample of the whole
 * cluster. gp_acquire_sample_rows() in turn calls acquire_sample_rows(), to
 * collect the sample on the segment.
 *
 * One complication with collecting the sample is the way that very
 * large datums are handled. We don't want to transfer multi-gigabyte
 * tuples from each segment. That would slow things down, and risk
 * running out of memory, if the sample contains a lot of them. They
 * are not very useful for statistics, anyway; hardly anyone builds an
 * index or does lookups where the histogram or MCV is meaningful for
 * very large keys. PostgreSQL also ignores any datums larger than
 * WIDTH_THRESHOLD (1kB) in the statistics computation, and we use the
 * same limit to restrict what gets transferred from the segments.
 * We substitute the very large datums with NULLs in the sample, but
 * keep track separately, which datums came out as NULLs because they
 * were too large, as opposed to "real" NULLs.
 *
 *
 * Merging leaf statistics with hyperloglog
 * ----------------------------------------
 *
 * TODO: explain how this works.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/multixact.h"
#include "access/relation.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "access/tuptoaster.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/dbcommands.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/sampling.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "catalog/heap.h"
#include "catalog/pg_am.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/analyzeutils.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/hyperloglog/gp_hyperloglog.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "executor/tstoreReceiver.h"
#include "tcop/utility.h"

/*
 * For Hyperloglog, we define an error margin of 0.3%. If the number of
 * distinct values estimated by hyperloglog is within an error of 0.3%,
 * we consider everything as distinct.
 */
#define GP_HLL_ERROR_MARGIN  0.003

/* Fix attr number of return record of function gp_acquire_sample_rows */
#define FIX_ATTR_NUM  3

/* Per-index data for ANALYZE */
typedef struct AnlIndexData
{
	IndexInfo  *indexInfo;		/* BuildIndexInfo result */
	double		tupleFract;		/* fraction of rows for partial index */
	VacAttrStats **vacattrstats;	/* index attrs to analyze */
	int			attr_cnt;
} AnlIndexData;


/* Default statistics target (GUC parameter) */
int			default_statistics_target = 100;

/* A few variables that don't seem worth passing around as parameters */
static MemoryContext anl_context = NULL;
static BufferAccessStrategy vac_strategy;

Bitmapset	**acquire_func_colLargeRowIndexes;
double		 *acquire_func_colLargeRowLength;


static void do_analyze_rel(Relation onerel,
						   VacuumParams *params, List *va_cols,
						   AcquireSampleRowsFunc acquirefunc, BlockNumber relpages,
						   bool inh, bool in_outer_xact, int elevel,
						   gp_acquire_sample_rows_context *ctx);
static void compute_index_stats(Relation onerel, double totalrows,
								AnlIndexData *indexdata, int nindexes,
								HeapTuple *rows, int numrows,
								MemoryContext col_context);
static VacAttrStats *examine_attribute(Relation onerel, int attnum,
									   Node *index_expr, int elevel);
static int acquire_sample_rows(Relation onerel, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows);
static int acquire_sample_rows_dispatcher(Relation onerel, bool inh, int elevel,
										  HeapTuple *rows, int targrows,
										  double *totalrows, double *totaldeadrows);
static BlockNumber acquire_index_number_of_blocks(Relation indexrel, Relation tablerel);

static void gp_acquire_correlations_dispatcher(Oid relOid, bool inh, float4 *correlations, bool *correlationsIsNull);
static int	compare_rows(const void *a, const void *b);
static int	acquire_inherited_sample_rows(Relation onerel, int elevel,
										  HeapTuple *rows, int targrows,
										  double *totalrows, double *totaldeadrows);
static void update_attstats(Oid relid, bool inh,
							int natts, VacAttrStats **vacattrstats);
static Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);
static Datum ind_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);

static void analyze_rel_internal(Oid relid, RangeVar *relation,
								 VacuumParams *params, List *va_cols,
								 bool in_outer_xact, BufferAccessStrategy bstrategy,
								 gp_acquire_sample_rows_context *ctx);
static void acquire_hll_by_query(Relation onerel, int nattrs, VacAttrStats **attrstats, int elevel);

/*
 *	analyze_rel() -- analyze one relation
 *
 * relid identifies the relation to analyze.  If relation is supplied, use
 * the name therein for reporting any failure to open/lock the rel; do not
 * use it once we've successfully opened the rel, since it might be stale.
 */
void
analyze_rel(Oid relid, RangeVar *relation,
			VacuumParams *params, List *va_cols, bool in_outer_xact,
			BufferAccessStrategy bstrategy, gp_acquire_sample_rows_context *ctx)
{
	bool		optimizerBackup;

	/*
	 * Temporarily disable ORCA because it's slow to start up, and it
	 * wouldn't come up with any better plan for the simple queries that
	 * we run.
	 */
	optimizerBackup = optimizer;
	optimizer = false;

	PG_TRY();
	{
		analyze_rel_internal(relid, relation, params, va_cols,
							 in_outer_xact, bstrategy, ctx);
	}
	/* Clean up in case of error. */
	PG_CATCH();
	{
		optimizer = optimizerBackup;

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();

	optimizer = optimizerBackup;
}

static void
analyze_rel_internal(Oid relid, RangeVar *relation,
			VacuumParams *params, List *va_cols, bool in_outer_xact,
			BufferAccessStrategy bstrategy, gp_acquire_sample_rows_context *ctx)
{
	Relation	onerel;
	int			elevel;
	AcquireSampleRowsFunc acquirefunc = NULL;
	BlockNumber relpages = 0;

	/* Select logging level */
	if (params->options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	/* Set up static variables */
	vac_strategy = bstrategy;

	/*
	 * Check for user-requested abort.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Open the relation, getting ShareUpdateExclusiveLock to ensure that two
	 * ANALYZEs don't run on it concurrently.  (This also locks out a
	 * concurrent VACUUM, which doesn't matter much at the moment but might
	 * matter if we ever try to accumulate stats on dead tuples.) If the rel
	 * has been dropped since we last saw it, we don't need to process it.
	 *
	 * Make sure to generate only logs for ANALYZE in this case.
	 */
	onerel = vacuum_open_relation(relid, relation, params->options & ~(VACOPT_VACUUM),
								  params->log_min_duration >= 0,
								  ShareUpdateExclusiveLock);

	/* leave if relation could not be opened or locked */
	if (!onerel)
		return;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
		"analyze_after_hold_lock", DDLNotSpecified,
		"", RelationGetRelationName(onerel));
#endif

	/*
	 * analyze_rel can be called in 3 different contexts:  explicitly by the user
	 * (eg. ANALYZE, VACUUM ANALYZE), implicitly by autovacuum, or implicitly by
	 * autostats.
	 *
	 * In the first case, we always want to make sure the user is the owner of the
	 * table.  In the autovacuum case, it will be called as superuser so we don't
	 * really care, but the ownership check should always succeed.  For autostats,
	 * we only do the check if gp_autostats_allow_nonowner=false, otherwise we can
	 * proceed with the analyze.
	 *
	 * This check happens also when building the relation list to analyze for a
	 * manual operation, and needs to be done additionally here as ANALYZE could
	 * happen across multiple transactions where relation ownership could have
	 * changed in-between.  Make sure to generate only logs for ANALYZE in
	 * this case.
	 */

	if (!(params->auto_stats && gp_autostats_allow_nonowner))
	{
		if (!vacuum_is_relation_owner(RelationGetRelid(onerel),
			onerel->rd_rel,
		  params->options & VACOPT_ANALYZE))
		{
			{
				relation_close(onerel, ShareUpdateExclusiveLock);
				return;
			}
		}
	}

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to analyze these is rather pointless, since their contents are
	 * probably not up-to-date on disk.  (We don't throw a warning here; it
	 * would just lead to chatter during a database-wide ANALYZE.)
	 */
	if (RELATION_IS_OTHER_TEMP(onerel))
	{
		relation_close(onerel, ShareUpdateExclusiveLock);
		return;
	}

	/*
	 * We can ANALYZE any table except pg_statistic. See update_attstats
	 */
	if (RelationGetRelid(onerel) == StatisticRelationId)
	{
		relation_close(onerel, ShareUpdateExclusiveLock);
		return;
	}

	/*
	 * Check that it's of an analyzable relkind, and set up appropriately.
	 */
	if (onerel->rd_rel->relkind == RELKIND_RELATION ||
		onerel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		/* Regular table, so we'll use the regular row acquisition function */
		acquirefunc = gp_acquire_sample_rows_func;

		/* Also get regular table's size */
		relpages = AcquireNumberOfBlocks(onerel);
	}
	else if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * For a foreign table, call the FDW's hook function to see whether it
		 * supports analysis.
		 */
		FdwRoutine *fdwroutine;
		bool		ok = false;

		fdwroutine = GetFdwRoutineForRelation(onerel, false);

		if (fdwroutine->AnalyzeForeignTable != NULL)
			ok = fdwroutine->AnalyzeForeignTable(onerel,
												 &acquirefunc,
												 &relpages);

		if (!ok)
		{
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot analyze this foreign table",
							RelationGetRelationName(onerel))));
			relation_close(onerel, ShareUpdateExclusiveLock);
			return;
		}
	}
	else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * For partitioned tables, we want to do the recursive ANALYZE below.
		 */
	}
	else
	{
		/* No need for a WARNING if we already complained during VACUUM */
		if (!(params->options & VACOPT_VACUUM))
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot analyze non-tables or special system tables",
							RelationGetRelationName(onerel))));
		relation_close(onerel, ShareUpdateExclusiveLock);
		return;
	}

	/*
	 * OK, let's do it.  First let other backends know I'm in ANALYZE.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags |= PROC_IN_ANALYZE;
	LWLockRelease(ProcArrayLock);
	pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE,
								  RelationGetRelid(onerel));

	/*
	 * Do the normal non-recursive ANALYZE.  We can skip this for partitioned
	 * tables, which don't contain any rows.
	 *
	 * On QE, when receiving ANALYZE request through gp_acquire_sample_rows.
	 * We should only perform do_analyze_rel for the parent table only
	 * or all it's children tables. Because, QD will send two acquire sample
	 * rows requests to QE.
	 * To distinguish the two requests, we check the ctx->inherited value here.
	 */
	if (onerel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE && (!ctx || !ctx->inherited))
		do_analyze_rel(onerel, params, va_cols, acquirefunc,
					   relpages, false, in_outer_xact, elevel, ctx);

	/*
	 * If there are child tables, do recursive ANALYZE.
	 */
	if (onerel->rd_rel->relhassubclass && (!ctx || ctx->inherited))
		do_analyze_rel(onerel, params, va_cols, acquirefunc, relpages,
					   true, in_outer_xact, elevel, ctx);

	/* MPP-6929: metadata tracking */
	if (!vacuumStatement_IsTemporary(onerel) && (Gp_role == GP_ROLE_DISPATCH))
	{
		char *asubtype = "";

		if (IsAutoVacuumWorkerProcess())
			asubtype = "AUTO";

		MetaTrackUpdObject(RelationRelationId,
						   RelationGetRelid(onerel),
						   GetUserId(),
						   "ANALYZE",
						   asubtype
			);
	}
	
	/*
	 * Close source relation now, but keep lock so that no one deletes it
	 * before we commit.  (If someone did, they'd fail to clean up the entries
	 * we made in pg_statistic.  Also, releasing the lock before commit would
	 * expose us to concurrent-update failures in update_attstats.)
	 */
	relation_close(onerel, NoLock);

	pgstat_progress_end_command();

	/*
	 * Reset my PGXACT flag.  Note: we need this here, and not in vacuum_rel,
	 * because the vacuum flag is cleared by the end-of-xact code.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags &= ~PROC_IN_ANALYZE;
	LWLockRelease(ProcArrayLock);
}

/*
 *	do_analyze_rel() -- analyze one relation, recursively or not
 *
 * Note that "acquirefunc" is only relevant for the non-inherited case.
 * For the inherited case, acquire_inherited_sample_rows() determines the
 * appropriate acquirefunc for each child table.
 */
static void
do_analyze_rel(Relation onerel, VacuumParams *params,
			   List *va_cols, AcquireSampleRowsFunc acquirefunc,
			   BlockNumber relpages, bool inh, bool in_outer_xact,
			   int elevel, gp_acquire_sample_rows_context *ctx)
{
	int			attr_cnt,
				tcnt,
				i,
				ind;
	Relation   *Irel;
	int			nindexes;
	bool		hasindex;
	VacAttrStats **vacattrstats;
	AnlIndexData *indexdata;
	int			targrows,
				numrows;
	double		totalrows,
				totaldeadrows;
	HeapTuple  *rows;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	MemoryContext caller_context;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	Bitmapset **colLargeRowIndexes;
	double     *colLargeRowLength;
	bool		sample_needed;

	if (inh)
		ereport(elevel,
				(errmsg("analyzing \"%s.%s\" inheritance tree",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
	else
		ereport(elevel,
				(errmsg("analyzing \"%s.%s\"",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));

	/*
	 * Set up a working context so that we can easily free whatever junk gets
	 * created.
	 */
	anl_context = AllocSetContextCreate(CurrentMemoryContext,
										"Analyze",
										ALLOCSET_DEFAULT_SIZES);
	caller_context = MemoryContextSwitchTo(anl_context);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(onerel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/* measure elapsed time iff autovacuum logging requires it */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		pg_rusage_init(&ru0);
		if (params->log_min_duration > 0)
			starttime = GetCurrentTimestamp();
	}

	/*
	 * Determine which columns to analyze
	 *
	 * Note that system attributes are never analyzed, so we just reject them
	 * at the lookup stage.  We also reject duplicate column mentions.  (We
	 * could alternatively ignore duplicates, but analyzing a column twice
	 * won't work; we'd end up making a conflicting update in pg_statistic.)
	 */
	if (va_cols != NIL)
	{
		Bitmapset  *unique_cols = NULL;
		ListCell   *le;

		vacattrstats = (VacAttrStats **) palloc(list_length(va_cols) *
												sizeof(VacAttrStats *));
		tcnt = 0;
		foreach(le, va_cols)
		{
			char	   *col = strVal(lfirst(le));

			i = attnameAttNum(onerel, col, false);
			if (i == InvalidAttrNumber)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" does not exist",
								col, RelationGetRelationName(onerel))));
			if (bms_is_member(i, unique_cols))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" appears more than once",
								col, RelationGetRelationName(onerel))));
			unique_cols = bms_add_member(unique_cols, i);

			vacattrstats[tcnt] = examine_attribute(onerel, i, NULL, elevel);
			if (vacattrstats[tcnt] != NULL)
				tcnt++;
		}
		attr_cnt = tcnt;
	}
	else
	{
		attr_cnt = onerel->rd_att->natts;
		vacattrstats = (VacAttrStats **)
			palloc(attr_cnt * sizeof(VacAttrStats *));
		tcnt = 0;
		for (i = 1; i <= attr_cnt; i++)
		{
			vacattrstats[tcnt] = examine_attribute(onerel, i, NULL, elevel);
			if (vacattrstats[tcnt] != NULL)
				tcnt++;
		}
		attr_cnt = tcnt;
	}

	/*
	 * Open all indexes of the relation, and see if there are any analyzable
	 * columns in the indexes.  We do not analyze index columns if there was
	 * an explicit column list in the ANALYZE command, however.  If we are
	 * doing a recursive scan, we don't want to touch the parent's indexes at
	 * all.
	 */
	if (!inh)
		vac_open_indexes(onerel, AccessShareLock, &nindexes, &Irel);
	else
	{
		Irel = NULL;
		nindexes = 0;
	}
	hasindex = (nindexes > 0);
	indexdata = NULL;
	if (hasindex)
	{
		indexdata = (AnlIndexData *) palloc0(nindexes * sizeof(AnlIndexData));
		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];
			IndexInfo  *indexInfo;

			thisdata->indexInfo = indexInfo = BuildIndexInfo(Irel[ind]);
			thisdata->tupleFract = 1.0; /* fix later if partial */
			if (indexInfo->ii_Expressions != NIL && va_cols == NIL)
			{
				ListCell   *indexpr_item = list_head(indexInfo->ii_Expressions);

				thisdata->vacattrstats = (VacAttrStats **)
					palloc(indexInfo->ii_NumIndexAttrs * sizeof(VacAttrStats *));
				tcnt = 0;
				for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
				{
					int			keycol = indexInfo->ii_IndexAttrNumbers[i];

					if (keycol == 0)
					{
						/* Found an index expression */
						Node	   *indexkey;

						if (indexpr_item == NULL)	/* shouldn't happen */
							elog(ERROR, "too few entries in indexprs list");
						indexkey = (Node *) lfirst(indexpr_item);
						indexpr_item = lnext(indexpr_item);
						thisdata->vacattrstats[tcnt] =
							examine_attribute(Irel[ind], i + 1, indexkey, elevel);
						if (thisdata->vacattrstats[tcnt] != NULL)
							tcnt++;
					}
				}
				thisdata->attr_cnt = tcnt;
			}
		}
	}

	/*
	 * Determine how many rows we need to sample, using the worst case from
	 * all analyzable columns.  We use a lower bound of 100 rows to avoid
	 * possible overflow in Vitter's algorithm.  (Note: that will also be the
	 * target in the corner case where there are no analyzable columns.)
	 *
	 * GPDB: If the caller specified the 'targrows', just use that.
	 */
	if (ctx)
	{
		targrows = ctx->targrows;
	}
	else /* funny indentation to avoid re-indenting upstream code */
  {
	targrows = 100;
	for (i = 0; i < attr_cnt; i++)
	{
		if (targrows < vacattrstats[i]->minrows)
			targrows = vacattrstats[i]->minrows;
	}
	for (ind = 0; ind < nindexes; ind++)
	{
		AnlIndexData *thisdata = &indexdata[ind];

		for (i = 0; i < thisdata->attr_cnt; i++)
		{
			if (targrows < thisdata->vacattrstats[i]->minrows)
				targrows = thisdata->vacattrstats[i]->minrows;
		}
	}
  }
	/* end of funny indentation */

	/*
	 * Maintain information if the row of a column exceeds WIDTH_THRESHOLD
	 */
	colLargeRowIndexes = (Bitmapset **) palloc0(sizeof(Bitmapset *) * onerel->rd_att->natts);
	colLargeRowLength = (double *)palloc0(sizeof(double) * onerel->rd_att->natts);

	if ((params->options & VACOPT_FULLSCAN) != 0)
	{
		if (onerel->rd_rel->relispartition)
		{
			acquire_hll_by_query(onerel, attr_cnt, vacattrstats, elevel);

			ereport(elevel, (errmsg("HLL FULL SCAN")));
		}
	}

	sample_needed = needs_sample(onerel, vacattrstats, attr_cnt);
	if (ctx || sample_needed)
	{
		if (ctx)
			MemoryContextSwitchTo(caller_context);
		rows = (HeapTuple *) palloc(targrows * sizeof(HeapTuple));

		pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
								 inh ? PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH :
								 PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS);

		/*
		 * Acquire the sample rows
		 *
		 * colLargeRowIndexes is passed out-of-band, in a global variable,
		 * to avoid changing the function signature from upstream's.
		 *
		 * The same as colLargeRowIndexes. colLargeRowLength stores total
		 * length of too wide rows in the sample for every attribute of
		 * the target relation. ANALYZE ignores too wide columns during
		 * analysis(See comments of WIDTH_THRESHOLD), the stawidth can be
		 * far smaller than the real average width for varlena datums which
		 * are larger than WIDTH_THRESHOLD but stored uncompressed.
		 */
		acquire_func_colLargeRowIndexes = colLargeRowIndexes;
		acquire_func_colLargeRowLength = colLargeRowLength;
		if (inh)
			numrows = acquire_inherited_sample_rows(onerel, elevel,
													rows, targrows,
													&totalrows, &totaldeadrows);
		else
			numrows = (*acquirefunc) (onerel, elevel,
									  rows, targrows,
									  &totalrows, &totaldeadrows);
		acquire_func_colLargeRowIndexes = NULL;
		acquire_func_colLargeRowLength = NULL;
		if (ctx)
			MemoryContextSwitchTo(anl_context);
	}
	else
	{
		/* If we're just merging stats from leafs, these are not needed either */
		totalrows = 0;
		totaldeadrows = 0;
		numrows = 0;
		rows = NULL;
	}

	if (ctx)
	{
		ctx->sample_rows = rows;
		ctx->num_sample_rows = numrows;
		ctx->totalrows = totalrows;
		ctx->totaldeadrows = totaldeadrows;
	}

	/*
	 * Compute the statistics.  Temporary results during the calculations for
	 * each column are stored in a child context.  The calc routines are
	 * responsible to make sure that whatever they store into the VacAttrStats
	 * structure is allocated in anl_context.
	 *
	 * When we have a root partition, we use the leaf partition statistics to
	 * derive root table statistics. In that case, we do not need to collect a
	 * sample. Therefore, the statistics calculation depends on root level have
	 * any tuples. In addition, we continue for statistics calculation if
	 * optimizer_analyze_root_partition or ROOTPARTITION is specified in the
	 * ANALYZE statement.
	 */
	if (numrows > 0 || !sample_needed)
	{
		HeapTuple *validRows = (HeapTuple *) palloc(numrows * sizeof(HeapTuple));
		MemoryContext col_context,
					old_context;
		bool		build_ext_stats;

		pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
									 PROGRESS_ANALYZE_PHASE_COMPUTE_STATS);

		col_context = AllocSetContextCreate(anl_context,
											"Analyze Column",
											ALLOCSET_DEFAULT_SIZES);
		old_context = MemoryContextSwitchTo(col_context);

		/*
		 * Get correlations from segments.
		 */
		if (Gp_role == GP_ROLE_DISPATCH && GpPolicyIsPartitioned(onerel->rd_cdbpolicy))
		{
			/*
			 * in gp_acquire_correlations_dispatcher we get correlations and correlationsIsNull
			 * for all columns even if stats is requested for subset of columns.
			 * It's more simpler to implement this way.
			 */
			float4 *correlations = (float4 *)palloc0(sizeof(float4) * onerel->rd_att->natts);
			bool *correlationsIsNull = (bool *)palloc0(sizeof(bool) * onerel->rd_att->natts);
			gp_acquire_correlations_dispatcher(onerel->rd_rel->oid, inh, correlations, correlationsIsNull);
			for (i = 0; i < attr_cnt; i++)
			{
				VacAttrStats *stats = vacattrstats[i];
				stats->partitiontbl_qd = true;
				stats->corrnull = correlationsIsNull[stats->attr->attnum - 1];
				stats->corrval = correlations[stats->attr->attnum - 1];
			}
			pfree(correlations);
			pfree(correlationsIsNull);
		}

		for (i = 0; i < attr_cnt; i++)
		{
			VacAttrStats *stats = vacattrstats[i];
			stats->tupDesc = onerel->rd_att;
			/*
			 * utilize hyperloglog and merge utilities to derive
			 * root table statistics by directly calling merge_leaf_stats()
			 * if all leaf partition attributes are analyzed
			 */
			if(stats->merge_stats)
			{
				(*stats->compute_stats) (stats, std_fetch_func, 0, 0);
				MemoryContextResetAndDeleteChildren(col_context);
				continue;
			}
			/*
			 * if merge_stats is not set, it is still possible that we don't want to sample
			 * (eg: in the case of autoanalyze). In this case, don't populate statistics
			 * for this attribute
			 */
			if (!sample_needed)
				continue;
			Assert(sample_needed);

			Bitmapset  *rowIndexes = colLargeRowIndexes[stats->attr->attnum - 1];
			int			validRowsLength;

			/* If there are too wide rows in the sample, remove them
			 * from the sample being sent for stats collection
			 */
			if (rowIndexes)
			{
				validRowsLength = 0;
				for (int rownum = 0; rownum < numrows; rownum++)
				{
					/* if row is too wide, leave it out of the sample */
					if (bms_is_member(rownum, rowIndexes))
						continue;

					validRows[validRowsLength] = rows[rownum];
					validRowsLength++;
				}
				stats->rows = validRows;
			}
			else
			{
				stats->rows = rows;
				validRowsLength = numrows;
			}
			AttributeOpts *aopt =
			get_attribute_options(onerel->rd_id, stats->attr->attnum);

			/*
			 * get total length and number of too wide rows in the sample,
			 * in case get wrong stawidth.
			 */
			stats->totalwidelength = colLargeRowLength[stats->attr->attnum - 1];
			stats->widerow_num = numrows - validRowsLength;

			if (validRowsLength > 0)
			{
				stats->compute_stats(stats,
									 std_fetch_func,
									 validRowsLength, // numbers of rows in sample excluding toowide if any.
									 totalrows);
				/*
				 * Store HLL/HLL fullscan information for leaf partitions in
				 * the stats object. If table was created with "analyze_hll_non_part_table" option, also collect
				 * HLL stats for non-leaf tables
				 */
				bool analyze_hll_non_part_table = false;
				if (onerel->rd_options != NULL &&
							((StdRdOptions *) onerel->rd_options)->analyze_hll_non_part_table)
				{
					analyze_hll_non_part_table = true;
				}
				if (onerel->rd_rel->relkind == RELKIND_RELATION && (onerel->rd_rel->relispartition || analyze_hll_non_part_table))
				{
					MemoryContext old_context;
					Datum *hll_values;

					old_context = MemoryContextSwitchTo(stats->anl_context);
					hll_values = (Datum *) palloc(sizeof(Datum));
					int16 hll_length = 0;
					int16 stakind = 0;
					if(stats->stahll_full != NULL)
					{
						hll_length = datumGetSize(PointerGetDatum(stats->stahll_full), false, -1);
						hll_values[0] = datumCopy(PointerGetDatum(stats->stahll_full), false, hll_length);
						stakind = STATISTIC_KIND_FULLHLL;
					}
					else if(stats->stahll != NULL)
					{
						((GpHLLCounter) (stats->stahll))->relPages = relpages;
						((GpHLLCounter) (stats->stahll))->relTuples = totalrows;

						hll_length = gp_hyperloglog_len((GpHLLCounter)stats->stahll);
						hll_values[0] = datumCopy(PointerGetDatum(stats->stahll), false, hll_length);
						stakind = STATISTIC_KIND_HLL;
					}
					MemoryContextSwitchTo(old_context);
					if (stakind > 0)
					{
						stats->stakind[STATISTIC_NUM_SLOTS-1] = stakind;
						stats->stavalues[STATISTIC_NUM_SLOTS-1] = hll_values;
						stats->numvalues[STATISTIC_NUM_SLOTS-1] =  1;
						stats->statyplen[STATISTIC_NUM_SLOTS-1] = hll_length;
					}
				}
			}
			else
			{
				// All the rows were too wide to be included in the sample. We cannot
				// do much in that case, but at least we know there were no NULLs, and
				// that every item was >= WIDTH_THRESHOLD in width.
				stats->stats_valid = true;
				stats->stanullfrac = 0.0;
				stats->stawidth = stats->totalwidelength/numrows;
				stats->stadistinct = 0.0;		/* "unknown" */
			}
			stats->rows = rows; // Reset to original rows

			/*
			 * If the appropriate flavor of the n_distinct option is
			 * specified, override with the corresponding value.
			 */
			aopt = get_attribute_options(onerel->rd_id, stats->attr->attnum);
			if (aopt != NULL)
			{
				float8		n_distinct;

				n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
				if (n_distinct != 0.0)
					stats->stadistinct = n_distinct;
			}

			MemoryContextResetAndDeleteChildren(col_context);
		}

		/*
		 * Datums exceeding WIDTH_THRESHOLD are masked as NULL in the sample, and
		 * are used as is to evaluate index statistics. It is less likely to have
		 * indexes on very wide columns, so the effect will be minimal.
		 */
		if (hasindex)
			compute_index_stats(onerel, totalrows,
								indexdata, nindexes,
								rows, numrows,
								col_context);

		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(col_context);

		/*
		 * Emit the completed stats rows into pg_statistic, replacing any
		 * previous statistics for the target columns.  (If there are stats in
		 * pg_statistic for columns we didn't process, we leave them alone.)
		 */
		update_attstats(RelationGetRelid(onerel), inh,
						attr_cnt, vacattrstats);

		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];

			update_attstats(RelationGetRelid(Irel[ind]), false,
							thisdata->attr_cnt, thisdata->vacattrstats);
		}

		/*
		 * Should we build extended statistics for this relation?
		 *
		 * The extended statistics catalog does not include an inheritance
		 * flag, so we can't store statistics built both with and without
		 * data from child relations. We can store just one set of statistics
		 * per relation. For plain relations that's fine, but for inheritance
		 * trees we have to pick whether to store statistics for just the
		 * one relation or the whole tree. For plain inheritance we store
		 * the (!inh) version, mostly for backwards compatibility reasons.
		 * For partitioned tables that's pointless (the non-leaf tables are
		 * always empty), so we store stats representing the whole tree.
		 */

		/*
		 * Don't build external stats for partitioned tables during autovacuum.
		 * External stats cannot be merged and therefore would require sampling,
		 * which is much more expensive. Users can instead explicitly run analyze
		 * on the root partition to trigger sampling.
		 */
		if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
			build_ext_stats = IsAutoVacuumWorkerProcess() ? false : inh;
		else
			build_ext_stats = !inh;
		/*
		 * Build extended statistics (if there are any).
		 *
		 * For now we only build extended statistics on individual relations,
		 * not for relations representing inheritance trees.
		 */
		if (build_ext_stats)
			BuildRelationExtStatistics(onerel, totalrows, numrows, rows,
									   attr_cnt, vacattrstats);
	}

	pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
								 PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE);

	/*
	 * Update pages/tuples stats in pg_class ... but not if we're doing
	 * inherited stats.
	 *
	 * GPDB: Coordinator node does not store relation data or metadata. That
	 * includes visibility map information. Instead, relevant info is gathered
	 * through dispatch requests. In this case, after vacuum is dispatched then
	 * relallvisible is aggregated and stored in pg_class. Coordinator node
	 * should look there for relallvisible.
	 */
	if (!inh)
	{
		BlockNumber relallvisible;

		if (RelationStorageIsAO(onerel))
			relallvisible = 0;
		else
		{
			if (Gp_role != GP_ROLE_DISPATCH)
				visibilitymap_count(onerel, &relallvisible, NULL);
			else
			{
				/*
				 * On the QD, retrieve the value of relallvisible from
				 * pg_class, which was aggregated from the QEs and updated
				 * earlier in vacuum_rel().
				 */
				HeapTuple	ctup;
				Form_pg_class pgcform;

				ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(onerel->rd_id));
				if (!HeapTupleIsValid(ctup))
					elog(ERROR, "pg_class entry for relid %u vanished during analyzing",
						 onerel->rd_id);
				pgcform = (Form_pg_class) GETSTRUCT(ctup);
				relallvisible = pgcform->relallvisible;
				heap_freetuple(ctup);
			}
		}

		vac_update_relstats(onerel,
							relpages,
							totalrows,
							relallvisible,
							hasindex,
							InvalidTransactionId,
							InvalidMultiXactId,
							in_outer_xact,
							false /* isVacuum */);
	}

	/*
	 * Same for indexes. Vacuum always scans all indexes, so if we're part of
	 * VACUUM ANALYZE, don't overwrite the accurate count already inserted by
	 * VACUUM.
	 */
	if (!inh && !(params->options & VACOPT_VACUUM))
	{
		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];
			double		totalindexrows;
			BlockNumber	estimatedIndexPages;

			if (totalrows < 1.0)
			{
				/**
				 * If there are no rows in the relation, no point trying to estimate
				 * number of pages in the index.
				 */
				elog(elevel, "ANALYZE skipping index %s since relation %s has no rows.",
					 RelationGetRelationName(Irel[ind]), RelationGetRelationName(onerel));
				estimatedIndexPages = 1;
			}
			else
			{
				/**
				 * NOTE: we don't attempt to estimate the number of tuples in an index.
				 * We will assume it to be equal to the estimated number of tuples in the relation.
				 * This does not hold for partial indexes. The number of tuples matching will be
				 * derived in selfuncs.c using the base table statistics.
				 */
				estimatedIndexPages = acquire_index_number_of_blocks(Irel[ind], onerel);
				elog(elevel, "ANALYZE estimated relpages=%u for index %s",
					 estimatedIndexPages, RelationGetRelationName(Irel[ind]));
			}

			totalindexrows = ceil(thisdata->tupleFract * totalrows);
			vac_update_relstats(Irel[ind],
								estimatedIndexPages,
								totalindexrows,
								0,
								false,
								InvalidTransactionId,
								InvalidMultiXactId,
								in_outer_xact,
								false /* isVacuum */);
		}
	}

	/*
	 * Report ANALYZE to the stats collector, too.  However, if doing
	 * inherited stats we shouldn't report, because the stats collector only
	 * tracks per-table stats.  Reset the changes_since_analyze counter only
	 * if we analyzed all columns; otherwise, there is still work for
	 * auto-analyze to do.
	 */
	if (!inh)
		pgstat_report_analyze(onerel, totalrows, totaldeadrows,
							  (va_cols == NIL));

	/* If this isn't part of VACUUM ANALYZE, let index AMs do cleanup */
	if (!(params->options & VACOPT_VACUUM))
	{
		for (ind = 0; ind < nindexes; ind++)
		{
			IndexBulkDeleteResult *stats;
			IndexVacuumInfo ivinfo;

			ivinfo.index = Irel[ind];
			ivinfo.analyze_only = true;
			ivinfo.estimated_count = true;
			ivinfo.message_level = elevel;
			ivinfo.num_heap_tuples = onerel->rd_rel->reltuples;
			ivinfo.strategy = vac_strategy;

			stats = index_vacuum_cleanup(&ivinfo, NULL);

			if (stats)
				pfree(stats);
		}
	}

	/* Done with indexes */
	vac_close_indexes(nindexes, Irel, NoLock);

	/* Log the action if appropriate */
	if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
	{
		if (params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, GetCurrentTimestamp(),
									   params->log_min_duration))
			ereport(LOG,
					(errmsg("automatic analyze of table \"%s.%s.%s\" system usage: %s",
							get_database_name(MyDatabaseId),
							get_namespace_name(RelationGetNamespace(onerel)),
							RelationGetRelationName(onerel),
							pg_rusage_show(&ru0))));
	}

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Restore current context and release memory */
	MemoryContextSwitchTo(caller_context);
	MemoryContextDelete(anl_context);
	anl_context = NULL;
}

/*
 * Compute statistics about indexes of a relation
 */
static void
compute_index_stats(Relation onerel, double totalrows,
					AnlIndexData *indexdata, int nindexes,
					HeapTuple *rows, int numrows,
					MemoryContext col_context)
{
	MemoryContext ind_context,
				old_context;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	int			ind,
				i;

	ind_context = AllocSetContextCreate(anl_context,
										"Analyze Index",
										ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(ind_context);

	for (ind = 0; ind < nindexes; ind++)
	{
		AnlIndexData *thisdata = &indexdata[ind];
		IndexInfo  *indexInfo = thisdata->indexInfo;
		int			attr_cnt = thisdata->attr_cnt;
		TupleTableSlot *slot;
		EState	   *estate;
		ExprContext *econtext;
		ExprState  *predicate;
		Datum	   *exprvals;
		bool	   *exprnulls;
		int			numindexrows,
					tcnt,
					rowno;
		double		totalindexrows;

		/* Ignore index if no columns to analyze and not partial */
		if (attr_cnt == 0 && indexInfo->ii_Predicate == NIL)
			continue;

		/*
		 * Need an EState for evaluation of index expressions and
		 * partial-index predicates.  Create it in the per-index context to be
		 * sure it gets cleaned up at the bottom of the loop.
		 */
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		/* Need a slot to hold the current heap tuple, too */
		slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel),
										&TTSOpsHeapTuple);

		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = slot;

		/* Set up execution state for predicate. */
		predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

		/* Compute and save index expression values */
		exprvals = (Datum *) palloc(numrows * attr_cnt * sizeof(Datum));
		exprnulls = (bool *) palloc(numrows * attr_cnt * sizeof(bool));
		numindexrows = 0;
		tcnt = 0;
		for (rowno = 0; rowno < numrows; rowno++)
		{
			HeapTuple	heapTuple = rows[rowno];

			vacuum_delay_point();

			/*
			 * Reset the per-tuple context each time, to reclaim any cruft
			 * left behind by evaluating the predicate or index expressions.
			 */
			ResetExprContext(econtext);

			/* Set up for predicate or expression evaluation */
			ExecStoreHeapTuple(heapTuple, slot, false);

			/* If index is partial, check predicate */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}
			numindexrows++;

			if (attr_cnt > 0)
			{
				/*
				 * Evaluate the index row to compute expression values. We
				 * could do this by hand, but FormIndexDatum is convenient.
				 */
				FormIndexDatum(indexInfo,
							   slot,
							   estate,
							   values,
							   isnull);

				/*
				 * Save just the columns we care about.  We copy the values
				 * into ind_context from the estate's per-tuple context.
				 */
				for (i = 0; i < attr_cnt; i++)
				{
					VacAttrStats *stats = thisdata->vacattrstats[i];
					int			attnum = stats->attr->attnum;

					if (isnull[attnum - 1])
					{
						exprvals[tcnt] = (Datum) 0;
						exprnulls[tcnt] = true;
					}
					else
					{
						exprvals[tcnt] = datumCopy(values[attnum - 1],
												   stats->attrtype->typbyval,
												   stats->attrtype->typlen);
						exprnulls[tcnt] = false;
					}
					tcnt++;
				}
			}
		}

		/*
		 * Having counted the number of rows that pass the predicate in the
		 * sample, we can estimate the total number of rows in the index.
		 */
		thisdata->tupleFract = (double) numindexrows / (double) numrows;
		totalindexrows = ceil(thisdata->tupleFract * totalrows);

		/*
		 * Now we can compute the statistics for the expression columns.
		 */
		if (numindexrows > 0)
		{
			MemoryContextSwitchTo(col_context);
			for (i = 0; i < attr_cnt; i++)
			{
				VacAttrStats *stats = thisdata->vacattrstats[i];
				AttributeOpts *aopt =
				get_attribute_options(stats->attr->attrelid,
									  stats->attr->attnum);

				stats->exprvals = exprvals + i;
				stats->exprnulls = exprnulls + i;
				stats->rowstride = attr_cnt;
				stats->compute_stats(stats,
									 ind_fetch_func,
									 numindexrows,
									 totalindexrows);

				/*
				 * If the n_distinct option is specified, it overrides the
				 * above computation.  For indices, we always use just
				 * n_distinct, not n_distinct_inherited.
				 */
				if (aopt != NULL && aopt->n_distinct != 0.0)
					stats->stadistinct = aopt->n_distinct;

				MemoryContextResetAndDeleteChildren(col_context);
			}
		}

		/* And clean up */
		MemoryContextSwitchTo(ind_context);

		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		MemoryContextResetAndDeleteChildren(ind_context);
	}

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(ind_context);
}

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
static VacAttrStats *
examine_attribute(Relation onerel, int attnum, Node *index_expr, int elevel)
{
	Form_pg_attribute attr = TupleDescAttr(onerel->rd_att, attnum - 1);
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	/* Never analyze dropped columns */
	if (attr->attisdropped)
		return NULL;

	/* Don't analyze column if user has specified not to */
	if (attr->attstattarget == 0)
		return NULL;

	/*
	 * Create the VacAttrStats struct.  Note that we only have a copy of the
	 * fixed fields of the pg_attribute tuple.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
	stats->elevel = elevel;
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);

	/*
	 * When analyzing an expression index, believe the expression tree's type
	 * not the column datatype --- the latter might be the opckeytype storage
	 * type of the opclass, which is not interesting for our purposes.  (Note:
	 * if we did anything with non-expression index columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	if (index_expr)
	{
		stats->attrtypid = exprType(index_expr);
		stats->attrtypmod = exprTypmod(index_expr);

		/*
		 * If a collation has been specified for the index column, use that in
		 * preference to anything else; but if not, fall back to whatever we
		 * can get from the expression.
		 */
		if (OidIsValid(onerel->rd_indcollation[attnum - 1]))
			stats->attrcollid = onerel->rd_indcollation[attnum - 1];
		else
			stats->attrcollid = exprCollation(index_expr);
	}
	else
	{
		stats->attrtypid = attr->atttypid;
		stats->attrtypmod = attr->atttypmod;
		stats->attrcollid = attr->attcollation;
	}

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));
	if (!HeapTupleIsValid(typtuple))
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS-1; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * The last slots of statistics is reserved for hyperloglog counter which
	 * is saved as a bytea. Therefore the type information is hardcoded for the
	 * bytea.
	 */
	stats->statypid[i] = BYTEAOID; // oid for bytea
	stats->statyplen[i] = -1; // variable length type
	stats->statypbyval[i] = false; // bytea is pass by reference
	stats->statypalign[i] = 'i'; // INT alignment (4-byte)

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
		ok = std_typanalyze(stats);

	stats->corrnull = true;
	stats->corrval = 0;
	stats->partitiontbl_qd = false;

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats->attr);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * GPDB: If we are the dispatcher, then issue analyze on the segments and
 * collect the statistics from them.
 */
int
gp_acquire_sample_rows_func(Relation onerel, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	if (Gp_role == GP_ROLE_DISPATCH &&
		onerel->rd_cdbpolicy && !GpPolicyIsEntry(onerel->rd_cdbpolicy))
	{
		/* Fetch sample from the segments. */
		return acquire_sample_rows_dispatcher(onerel, false, elevel,
											  rows, targrows,
											  totalrows, totaldeadrows);
	}

    if (RelationIsAppendOptimized(onerel))
        return table_relation_acquire_sample_rows(onerel, elevel, rows, 
									  	 		  targrows, totalrows, totaldeadrows);
    
    return acquire_sample_rows(onerel, elevel, rows,
							   targrows, totalrows, totaldeadrows);
}

/*
 * acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[], which
 * must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also estimate the total numbers of live and dead rows in the table,
 * and return them into *totalrows and *totaldeadrows, respectively.
 *
 * The returned list of tuples is in order by physical position in the table.
 * (We will rely on this later to derive correlation estimates.)
 *
 * As of May 2004 we use a new two-stage method:  Stage one selects up
 * to targrows random blocks (or all blocks, if there aren't so many).
 * Stage two scans these blocks and uses the Vitter algorithm to create
 * a random sample of targrows rows (or less, if there are less in the
 * sample of blocks).  The two stages are executed simultaneously: each
 * block is processed as soon as stage one returns its number and while
 * the rows are read stage two controls which ones are to be inserted
 * into the sample.
 *
 * Although every row has an equal chance of ending up in the final
 * sample, this sampling method is not perfect: not every possible
 * sample has an equal chance of being selected.  For large relations
 * the number of different blocks represented by the sample tends to be
 * too small.  We can live with that for now.  Improvements are welcome.
 *
 * An important property of this sampling method is that because we do
 * look at a statistically unbiased set of blocks, we should get
 * unbiased estimates of the average numbers of live and dead rows per
 * block.  The previous sampling method put too much credence in the row
 * density near the start of the table.
 */
static int
acquire_sample_rows(Relation onerel, int elevel,
					HeapTuple *rows, int targrows,
					double *totalrows, double *totaldeadrows)
{
	int			numrows = 0;	/* # rows now in reservoir */
	double		samplerows = 0; /* total # rows collected */
	double		liverows = 0;	/* # live rows seen */
	double		deadrows = 0;	/* # dead rows seen */
	double		rowstoskip = -1;	/* -1 means not set yet */
	BlockNumber totalblocks;
	TransactionId OldestXmin;
	BlockSamplerData bs;
	ReservoirStateData rstate;
	TupleTableSlot *slot;
	TableScanDesc scan;
	BlockNumber nblocks;
	BlockNumber blksdone = 0;

	Assert(targrows > 0);

	/*
	 * GPDB: Legacy analyze does make a lot of assumptions regarding the file
	 * layout of a relation. These assumptions are heap specific and do not hold
	 * for AO/AOCO relations. In the case of AO/AOCO, what is actually needed and
	 * used instead of number of blocks, is number of tuples. Moreover, BlockNumber
	 * is uint32 and Number of tuples is uint64. That means that after row number
	 * UINT_MAX we will never analyze the table.
	 * 
	 * We introduced a tuple based sampling approach for AO/CO tables to address
	 * above problems, all corresponding logics were moved out of here and enclosed
	 * in table_relation_acquire_sample_rows(). So leave here an assertion to ensure
	 * the relation should not be an AO/CO table.
	 */
	Assert(!RelationStorageIsAO(onerel));

	totalblocks = RelationGetNumberOfBlocks(onerel);

	/* Need a cutoff xmin for HeapTupleSatisfiesVacuum */
	OldestXmin = GetOldestXmin(onerel, PROCARRAY_FLAGS_VACUUM);

	/* Prepare for sampling block numbers */
	nblocks = BlockSampler_Init(&bs, totalblocks, targrows, random());

	/* Report sampling block numbers */
	pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
								 nblocks);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	scan = table_beginscan_analyze(onerel);
	slot = table_slot_create(onerel, NULL);

	/* Outer loop over blocks to sample */
	while (BlockSampler_HasMore(&bs))
	{
		BlockNumber targblock = BlockSampler_Next(&bs);

		vacuum_delay_point();

		if (!table_scan_analyze_next_block(scan, targblock, vac_strategy))
			continue;

		while (table_scan_analyze_next_tuple(scan, OldestXmin, &liverows, &deadrows, slot))
		{
			/*
			 * The first targrows sample rows are simply copied into the
			 * reservoir. Then we start replacing tuples in the sample until
			 * we reach the end of the relation.  This algorithm is from Jeff
			 * Vitter's paper (see full citation in utils/misc/sampling.c). It
			 * works by repeatedly computing the number of tuples to skip
			 * before selecting a tuple, which replaces a randomly chosen
			 * element of the reservoir (current set of tuples).  At all times
			 * the reservoir is a true random sample of the tuples we've
			 * passed over so far, so when we fall off the end of the relation
			 * we're done.
			 */
			if (numrows < targrows)
				rows[numrows++] = ExecCopySlotHeapTuple(slot);
			else
			{
				/*
				 * t in Vitter's paper is the number of records already
				 * processed.  If we need to compute a new S value, we must
				 * use the not-yet-incremented value of samplerows as t.
				 */
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					/*
					 * Found a suitable tuple, so save it, replacing one old
					 * tuple at random
					 */
					int			k = (int) (targrows * sampler_random_fract(rstate.randstate));

					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}

				rowstoskip -= 1;
			}

			samplerows += 1;
		}

		pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE,
									 ++blksdone);
		SIMPLE_FAULT_INJECTOR("analyze_block");
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);

	/*
	 * If we didn't find as many tuples as we wanted then we're done. No sort
	 * is needed, since they're already in order.
	 *
	 * Otherwise we need to sort the collected tuples by position
	 * (itempointer). It's not worth worrying about corner cases where the
	 * tuples are already sorted.
	 */
	if (numrows == targrows)
		qsort((void *) rows, numrows, sizeof(HeapTuple), compare_rows);

	/*
	 * Estimate total numbers of live and dead rows in relation, extrapolating
	 * on the assumption that the average tuple density in pages we didn't
	 * scan is the same as in the pages we did scan.  Since what we scanned is
	 * a random sample of the pages in the relation, this should be a good
	 * assumption.
	 */
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
	else
	{
		*totalrows = 0.0;
		*totaldeadrows = 0.0;
	}

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": scanned %d of %u pages, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f estimated total rows",
					RelationGetRelationName(onerel),
					bs.m, totalblocks,
					liverows, deadrows,
					numrows, *totalrows)));

	return numrows;
}

/*
 * qsort comparator for sorting rows[] array
 */
static int
compare_rows(const void *a, const void *b)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
	OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
	BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
	OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}


/*
 * acquire_inherited_sample_rows -- acquire sample rows from inheritance tree
 *
 * This has the same API as acquire_sample_rows, except that rows are
 * collected from all inheritance children as well as the specified table.
 * We fail and return zero if there are no inheritance children, or if all
 * children are foreign tables that don't support ANALYZE.
 */
int
acquire_inherited_sample_rows(Relation onerel, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows, double *totaldeadrows)
{
	List	   *tableOIDs;
	Relation   *rels;
	AcquireSampleRowsFunc *acquirefuncs;
	double	   *relblocks;
	double		totalblocks;
	int			numrows,
				nrels,
				i;
	ListCell   *lc;
	bool		has_child;

	/*
	 * Find all members of inheritance set.  We only need AccessShareLock on
	 * the children.
	 */
	tableOIDs =
		find_all_inheritors(RelationGetRelid(onerel), AccessShareLock, NULL);

	/*
	 * Check that there's at least one descendant, else fail.  This could
	 * happen despite analyze_rel's relhassubclass check, if table once had a
	 * child but no longer does.  In that case, we can clear the
	 * relhassubclass field so as not to make the same mistake again later.
	 * (This is safe because we hold ShareUpdateExclusiveLock.)
	 * Please refer to https://github.com/GreengageDB/greengage/issues/14644
	 */
	if (list_length(tableOIDs) < 2)
	{
		/* CCI because we already updated the pg_class row in this command */
		CommandCounterIncrement();
		SetRelationHasSubclass(RelationGetRelid(onerel), false);
		*totalrows = 0;
		*totaldeadrows = 0;
		ereport(elevel,
				(errmsg("skipping analyze of \"%s.%s\" inheritance tree --- this inheritance tree contains no child tables",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
		if (Gp_role == GP_ROLE_EXECUTE)
			return 0;
	}

	/*
	 * Like in acquire_sample_rows(), if we're in the QD, fetch the sample
	 * from segments.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		return acquire_sample_rows_dispatcher(onerel,
											  true, /* inherited stats */
											  elevel, rows, targrows,
											  totalrows, totaldeadrows);
	}

	/*
	 * Identify acquirefuncs to use, and count blocks in all the relations.
	 * The result could overflow BlockNumber, so we use double arithmetic.
	 */
	rels = (Relation *) palloc(list_length(tableOIDs) * sizeof(Relation));
	acquirefuncs = (AcquireSampleRowsFunc *)
		palloc(list_length(tableOIDs) * sizeof(AcquireSampleRowsFunc));
	relblocks = (double *) palloc(list_length(tableOIDs) * sizeof(double));
	totalblocks = 0;
	nrels = 0;
	has_child = false;
	foreach(lc, tableOIDs)
	{
		Oid			childOID = lfirst_oid(lc);
		Relation	childrel;
		AcquireSampleRowsFunc acquirefunc = NULL;
		BlockNumber relpages = 0;

		/* We already got the needed lock */
		childrel = table_open(childOID, NoLock);

		/* Ignore if temp table of another backend */
		if (RELATION_IS_OTHER_TEMP(childrel))
		{
			/* ... but release the lock on it */
			Assert(childrel != onerel);
			table_close(childrel, AccessShareLock);
			continue;
		}

		/* Check table type (MATVIEW can't happen, but might as well allow) */
		if (childrel->rd_rel->relkind == RELKIND_RELATION ||
			childrel->rd_rel->relkind == RELKIND_MATVIEW)
		{
			/* Regular table, so use the regular row acquisition function */
			acquirefunc = gp_acquire_sample_rows_func;
			relpages = AcquireNumberOfBlocks(childrel);
		}
		else if (childrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		{
			/*
			 * For a foreign table, call the FDW's hook function to see
			 * whether it supports analysis.
			 */
			FdwRoutine *fdwroutine;
			bool		ok = false;

			fdwroutine = GetFdwRoutineForRelation(childrel, false);

			if (fdwroutine->AnalyzeForeignTable != NULL)
				ok = fdwroutine->AnalyzeForeignTable(childrel,
													 &acquirefunc,
													 &relpages);

			if (!ok)
			{
				/* ignore, but release the lock on it */
				Assert(childrel != onerel);
				table_close(childrel, AccessShareLock);
				continue;
			}
		}
		else
		{
			/*
			 * ignore, but release the lock on it.  don't try to unlock the
			 * passed-in relation
			 */
			Assert(childrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
			if (childrel != onerel)
				table_close(childrel, AccessShareLock);
			else
				table_close(childrel, NoLock);
			continue;
		}

		/* OK, we'll process this child */
		has_child = true;
		rels[nrels] = childrel;
		acquirefuncs[nrels] = acquirefunc;
		relblocks[nrels] = (double) relpages;
		totalblocks += (double) relpages;
		nrels++;
	}

	/*
	 * If we don't have at least one child table to consider, fail.  If the
	 * relation is a partitioned table, it's not counted as a child table.
	 */
	if (!has_child)
	{
		ereport(elevel,
				(errmsg("skipping analyze of \"%s.%s\" inheritance tree --- this inheritance tree contains no analyzable child tables",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
		return 0;
	}

	/*
	 * Now sample rows from each relation, proportionally to its fraction of
	 * the total block count.  (This might be less than desirable if the child
	 * rels have radically different free-space percentages, but it's not
	 * clear that it's worth working harder.)
	 */
	pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_TOTAL,
								 nrels);
	numrows = 0;
	*totalrows = 0;
	*totaldeadrows = 0;
	for (i = 0; i < nrels; i++)
	{
		Relation	childrel = rels[i];
		AcquireSampleRowsFunc acquirefunc = acquirefuncs[i];
		double		childblocks = relblocks[i];

		pgstat_progress_update_param(PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID,
									 RelationGetRelid(childrel));

		if (childblocks > 0)
		{
			int			childtargrows;

			childtargrows = (int) rint(targrows * childblocks / totalblocks);
			/* Make sure we don't overrun due to roundoff error */
			childtargrows = Min(childtargrows, targrows - numrows);
			if (childtargrows > 0)
			{
				int			childrows;
				double		trows,
							tdrows;

				/* Fetch a random sample of the child's rows */
				childrows = (*acquirefunc) (childrel, elevel,
											rows + numrows, childtargrows,
											&trows, &tdrows);

				/* We may need to convert from child's rowtype to parent's */
				if (childrows > 0 &&
					!equalTupleDescs(RelationGetDescr(childrel),
									 RelationGetDescr(onerel),
									 false))
				{
					TupleConversionMap *map;

					map = convert_tuples_by_name(RelationGetDescr(childrel),
												 RelationGetDescr(onerel),
												 gettext_noop("could not convert row type"));
					if (map != NULL)
					{
						int			j;

						for (j = 0; j < childrows; j++)
						{
							HeapTuple	newtup;

							newtup = execute_attr_map_tuple(rows[numrows + j], map);
							heap_freetuple(rows[numrows + j]);
							rows[numrows + j] = newtup;
						}
						free_conversion_map(map);
					}
				}

				/* And add to counts */
				numrows += childrows;
				*totalrows += trows;
				*totaldeadrows += tdrows;
			}
		}

		/*
		 * Note: we cannot release the child-table locks, since we may have
		 * pointers to their TOAST tables in the sampled rows.
		 */
		table_close(childrel, NoLock);
		pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_DONE,
									 i + 1);
	}

	return numrows;
}

/*
 * This function acquires the HLL counter for the entire table by
 * using the hyperloglog extension gp_hyperloglog_accum().
 *
 * Unlike acquire_sample_rows(), this returns the HLL counter for
 * the entire table, and not jsut a sample, and it stores the HLL
 * counter into a separate attribute in the stats stahll_full to
 * distinguish it from the HLL for sampled data. This functions scans
 * the full table only once.
 */
static void
acquire_hll_by_query(Relation onerel, int nattrs, VacAttrStats **attrstats, int elevel)
{
	StringInfoData str, columnStr;
	int			i;
	int			ret;
	Datum	   *vals;
	MemoryContext oldcxt;
	const char *schemaName = get_namespace_name(RelationGetNamespace(onerel));

	initStringInfo(&str);
	initStringInfo(&columnStr);
	for (i = 0; i < nattrs; i++)
	{
		const char *attname = quote_identifier(NameStr(attrstats[i]->attr->attname));
		appendStringInfo(&columnStr, "pg_catalog.gp_hyperloglog_accum(%s)", attname);
		if(i != nattrs-1)
			appendStringInfo(&columnStr, ", ");
	}

	appendStringInfo(&str, "select %s from %s.%s as Ta ",
					 columnStr.data,
					 quote_identifier(schemaName),
					 quote_identifier(RelationGetRelationName(onerel)));

	oldcxt = CurrentMemoryContext;

	if (SPI_OK_CONNECT != SPI_connect())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to connect to execute internal query")));

	elog(elevel, "Executing SQL: %s", str.data);

	/*
	 * Do the query. We pass readonly==false, to force SPI to take a new
	 * snapshot. That ensures that we see all changes by our own transaction.
	 */
	ret = SPI_execute(str.data, false, 0);
	Assert(ret > 0);

	/*
	 * targrows in analyze_rel_internal() is an int,
	 * it's unlikely that this query will return more rows
	 */
	Assert(SPI_processed < 2);
	int sampleTuples = (int) SPI_processed;

	/* Ok, read in the tuples to *rows */
	MemoryContextSwitchTo(oldcxt);
	vals = (Datum *) palloc0(nattrs * sizeof(Datum));
	bool isNull = false;

	for (i = 0; i < sampleTuples; i++)
	{
		HeapTuple	sampletup = SPI_tuptable->vals[i];
		int			j;

		for (j = 0; j < nattrs; j++)
		{
			int	tupattnum = attrstats[j]->tupattnum;
			Assert(tupattnum >= 1 && tupattnum <= nattrs);

			vals[tupattnum - 1] = heap_getattr(sampletup, j + 1,
											   SPI_tuptable->tupdesc,
											   &isNull);
			if (isNull)
			{
				attrstats[j]->stahll_full = (bytea *)gp_hyperloglog_init_def();
				continue;
			}

			int16 typlen;
			bool typbyval;
			get_typlenbyval(SPI_tuptable->tupdesc->tdtypeid, &typlen, &typbyval);
			int hll_length = datumGetSize(vals[tupattnum-1], typbyval, typlen);
			attrstats[j]->stahll_full = (bytea *)datumCopy(PointerGetDatum(vals[tupattnum - 1]), false, hll_length);
		}
	}

	SPI_finish();
}

/*
 * Compute relation size.
 *
 * In upstream, this is a simple RelationGetNumberOfBlocks() call.
 * In GPDB if we're in the dispatcher, we need to get the size from the
 * segments.
 */
BlockNumber
AcquireNumberOfBlocks(Relation onerel)
{
	int64		totalbytes;

	if (Gp_role == GP_ROLE_DISPATCH &&
		onerel->rd_cdbpolicy && !GpPolicyIsEntry(onerel->rd_cdbpolicy))
	{
		/* Query the segments using pg_relation_size(<rel>). */
		char		*relsize_sql;

		if (RelationStorageIsAO(onerel))
			/* 
			 * For AO tables, we want to consider only the core relation, no auxiliary tables.
			 * We also want to pull the logical size (based on the seg eof values),
			 * not physical, to most accurately inform optimizer and
			 * other consumers of these statistics.
			 */
			relsize_sql = psprintf("select pg_catalog.pg_relation_size(%u, /* include_ao_aux */ false, "
								   "/* physical_ao_size */ false)",
								   RelationGetRelid(onerel));
		else
			relsize_sql = psprintf("select pg_catalog.pg_relation_size(%u, 'main')", RelationGetRelid(onerel));

		totalbytes = get_size_from_segDBs(relsize_sql);
		pfree(relsize_sql);
		if (GpPolicyIsReplicated(onerel->rd_cdbpolicy))
		{
			/*
			 * pg_relation_size sums up the table size on each segment. That's
			 * correct for hash and randomly distributed tables. But for a
			 * replicated table, we want pg_class.relpages to count the data
			 * only once.
			 */
			totalbytes = totalbytes / onerel->rd_cdbpolicy->numsegments;
		}

		return RelationGuessNumberOfBlocksFromSize(totalbytes);
	}
	/* Check size on this server. */
	else
	{
		return RelationGetNumberOfBlocks(onerel);
	}
}

/*
 * Compute index relation's size.
 *
 * Like AcquireNumberOfBlocks(), but for indexes. Indexes don't
 * have a distribution policy, so we use the parent table's policy
 * to determine whether we need to get the size on segments or
 * locally.
 */
static BlockNumber
acquire_index_number_of_blocks(Relation indexrel, Relation tablerel)
{
	int64		totalbytes;

	if (Gp_role == GP_ROLE_DISPATCH &&
		tablerel->rd_cdbpolicy && !GpPolicyIsEntry(tablerel->rd_cdbpolicy))
	{
		/* Query the segments using pg_relation_size(<rel>). */
		char		relsize_sql[100];

		snprintf(relsize_sql, sizeof(relsize_sql),
				 "select pg_catalog.pg_relation_size(%u, 'main')", RelationGetRelid(indexrel));
		totalbytes = get_size_from_segDBs(relsize_sql);
		if (GpPolicyIsReplicated(tablerel->rd_cdbpolicy))
		{
			/*
			 * pg_relation_size sums up the table size on each segment. That's
			 * correct for hash and randomly distributed tables. But for a
			 * replicated table, we want pg_class.relpages to count the data
			 * only once.
			 */
			totalbytes = totalbytes / tablerel->rd_cdbpolicy->numsegments;
		}

		return RelationGuessNumberOfBlocksFromSize(totalbytes);
	}
	/* Check size on this server. */
	else
	{
		return RelationGetNumberOfBlocks(indexrel);
	}
}

/*
 * parse_record_to_string
 *
 * CDB: a copy of record_in, but only parse the record string
 * into separate strs for each column.
 */
static void
parse_record_to_string(char *string, TupleDesc tupdesc, char** values, bool *nulls)
{
	char	*ptr;
	int	ncolumns;
	int	i;
	bool	needComma;
	StringInfoData	buf;

	Assert(string != NULL);
	Assert(values != NULL);
	Assert(nulls != NULL);

	ncolumns = tupdesc->natts;
	needComma = false;

	/*
	 * Scan the string.  We use "buf" to accumulate the de-quoted data for
	 * each column, which is then fed to the appropriate input converter.
	 */
	ptr = string;

	/* Allow leading whitespace */
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr++ != '(')
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Missing left parenthesis.")));
	}

	initStringInfo(&buf);

	for (i = 0; i < ncolumns; i++)
	{
		/* Ignore dropped columns in datatype, but fill with nulls */
		if (TupleDescAttr(tupdesc, i)->attisdropped)
		{
			values[i] = NULL;
			nulls[i] = true;
			continue;
		}

		if (needComma)
		{
			/* Skip comma that separates prior field from this one */
			if (*ptr == ',')
				ptr++;
			else
			{
				/* *ptr must be ')' */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("malformed record literal: \"%s\"", string),
						 errdetail("Too few columns.")));
			}
		}

		/* Check for null: completely empty input means null */
		if (*ptr == ',' || *ptr == ')')
		{
			values[i] = NULL;
			nulls[i] = true;
		}
		else
		{
			/* Extract string for this column */
			bool		inquote = false;

			resetStringInfo(&buf);
			while (inquote || !(*ptr == ',' || *ptr == ')'))
			{
				char		ch = *ptr++;

				if (ch == '\0')
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							 errmsg("malformed record literal: \"%s\"",
									string),
							 errdetail("Unexpected end of input.")));
				}
				if (ch == '\\')
				{
					if (*ptr == '\0')
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								 errmsg("malformed record literal: \"%s\"",
										string),
								 errdetail("Unexpected end of input.")));
					}
					appendStringInfoChar(&buf, *ptr++);
				}
				else if (ch == '"')
				{
					if (!inquote)
						inquote = true;
					else if (*ptr == '"')
					{
						/* doubled quote within quote sequence */
						appendStringInfoChar(&buf, *ptr++);
					}
					else
						inquote = false;
				}
				else
					appendStringInfoChar(&buf, ch);
			}

			values[i] = palloc(strlen(buf.data) + 1);
			memcpy(values[i], buf.data, strlen(buf.data) + 1);
			nulls[i] = false;
		}

		/*
		 * Prep for next column
		 */
		needComma = true;
	}

	if (*ptr++ != ')')
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Too many columns.")));
	}
	/* Allow trailing whitespace */
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;
	if (*ptr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed record literal: \"%s\"", string),
				 errdetail("Junk after right parenthesis.")));
	}
}

/*
 * Build a querydesc for a sql, set "dest" to portal->holdStore
 */
static QueryDesc *build_querydesc(Portal portal, char *sql)
{
	List	   *raw_parsetree_list;
	RawStmt	   *parsetree;
	List	   *querytree_list;
	List	   *plantree_list;
	PlannedStmt *plan_stmt;
	DestReceiver *destReceiver;
	QueryDesc  *queryDesc = NULL;
	destReceiver = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(destReceiver,
									portal->holdStore,
									portal->holdContext,
									false);

	/*
	 * Parse the SQL string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(sql);

	/*
	 * Do parse analysis, rule rewrite, planning, and execution for each raw
	 * parsetree.
	 */

	/* There is only one element in list due to simple select. */
	Assert(list_length(raw_parsetree_list) == 1);
	parsetree = (RawStmt *) linitial(raw_parsetree_list);

	querytree_list = pg_analyze_and_rewrite(parsetree,
											sql,
											NULL,
											0,
											NULL);
	plantree_list = pg_plan_queries(querytree_list, 0, NULL);

	/* There is only one statement in list due to simple select. */
	Assert(list_length(plantree_list) == 1);
	plan_stmt = (PlannedStmt *) linitial(plantree_list);

	queryDesc = CreateQueryDesc(plan_stmt,
								sql,
								GetActiveSnapshot(),
								InvalidSnapshot,
								destReceiver,
								NULL,
								NULL,
								INSTRUMENT_NONE);



	list_free_deep(querytree_list);
	list_free_deep(raw_parsetree_list);

	return queryDesc;
}

static int
process_sample_rows(Portal portal,
					QueryDesc  *queryDesc,
					Relation onerel,
					HeapTuple *rows,
					int targrows,
					double *totalrows,
					double *totaldeadrows)
{
	/*
	 * 'colLargeRowIndexes' is essentially an argument, but it's passed via a
	 * global variable to avoid changing the AcquireSampleRowsFunc prototype.
	 */
	Bitmapset **colLargeRowIndexes = acquire_func_colLargeRowIndexes;
	double     *colLargeRowLength = acquire_func_colLargeRowLength;
	TupleDesc	relDesc = RelationGetDescr(onerel);
	TupleDesc	funcTupleDesc;
	TupleDesc	sampleTupleDesc;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	Datum	   *funcRetValues;
	bool	   *funcRetNulls;
	int			ncolumns;
	AttInMetadata *attinmeta;
	int			numLiveColumns;
	int			i;
	int			index = 0;
	TupleTableSlot *slot;
	Datum	   *dvalues;
	bool	   *dnulls;

	/*
	 * Count the number of columns, excluding dropped columns. We'll need that
	 * later.
	 */
	numLiveColumns = 0;
	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		if (attr->attisdropped)
			continue;

		numLiveColumns++;
	}

	/*
	 * Build a modified tuple descriptor for the table.
	 *
	 * Some datatypes need special treatment, so we cannot use the relation's
	 * original tupledesc.
	 *
	 * Also create tupledesc of return record of function gp_acquire_sample_rows.
	 */
	sampleTupleDesc = CreateTupleDescCopy(relDesc);
	ncolumns = numLiveColumns + FIX_ATTR_NUM;
	
	funcTupleDesc = CreateTemplateTupleDesc(ncolumns);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 1, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 2, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 3, "", FLOAT8ARRAYOID, -1, 0);

	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		Oid			typid = gp_acquire_sample_rows_col_type(attr->atttypid);

		TupleDescAttr(sampleTupleDesc, i)->atttypid = typid;

		if (!attr->attisdropped)
		{
			TupleDescInitEntry(funcTupleDesc, (AttrNumber) 4 + index, "",
							   typid, attr->atttypmod, attr->attndims);

			index++;
		}
	}

	/* For RECORD results, make sure a typmod has been assigned */
	Assert(funcTupleDesc->tdtypeid == RECORDOID && funcTupleDesc->tdtypmod < 0);
	assign_record_type_typmod(funcTupleDesc);

	attinmeta = TupleDescGetAttInMetadata(sampleTupleDesc);

	/*
	 * Read the result set from each segment. Gather the sample rows *rows,
	 * and sum up the summary rows for grand 'totalrows' and 'totaldeadrows'.
	 */
	funcRetValues = (Datum *) palloc0(funcTupleDesc->natts * sizeof(Datum));
	funcRetNulls = (bool *) palloc0(funcTupleDesc->natts * sizeof(bool));
	dvalues = (Datum *) palloc0(relDesc->natts * sizeof(Datum));
	dnulls = (bool *) palloc0(relDesc->natts * sizeof(bool));
	sampleTuples = 0;
	*totalrows = 0;
	*totaldeadrows = 0;

	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc, &TTSOpsMinimalTuple);

	for (;;)
	{
		bool		ok;
		TupleDesc	typeinfo;
		int			natts;
		Datum		attr;
		bool		isnull;
		double		this_totalrows = 0;
		double		this_totaldeadrows = 0;

		CHECK_FOR_INTERRUPTS();

		ok = tuplestore_gettupleslot(portal->holdStore, true, false, slot);

		if (!ok)
			break;

		typeinfo = slot->tts_tupleDescriptor;
		natts = typeinfo->natts;

		/* There should be only one attribute with OID RECORDOID. */
		if (1 != natts)
		{
			elog(ERROR,
				"wrong number of attributes %d when 1 expected",
				natts);
		}

		if (RECORDOID != typeinfo->attrs[0].atttypid)
		{
			elog(ERROR,
				"wrong attribute OID %d, RECORDOID %d is expected",
				typeinfo->attrs[0].atttypid, RECORDOID);

		}

		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);

		/* There should be only one attribute with OID RECORDOID */
		attr = slot_getattr(slot, 1, &isnull);
		if (isnull)
		{
			elog(ERROR,
				"null value for attribute in tuple");
		}

		/* Get record from attribute and parse it */
		{
			HeapTupleHeader rec = (HeapTupleHeader) PG_DETOAST_DATUM(attr);
			Oid			tupType;
			int32		tupTypmod;
			TupleDesc	tupdesc;
			HeapTupleData tuple;

			/* Extract type info from the tuple itself */
			tupType = HeapTupleHeaderGetTypeId(rec);
			tupTypmod = HeapTupleHeaderGetTypMod(rec);
			tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

			/* Build a temporary HeapTuple control structure */
			tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
			ItemPointerSetInvalid(&(tuple.t_self));
			tuple.t_data = rec;

			/* Break down the tuple into fields */
			heap_deform_tuple(&tuple, tupdesc, funcRetValues, funcRetNulls);

			if (!funcRetNulls[0])
			{
				/* This is a summary row. */
				this_totalrows = DatumGetFloat8(funcRetValues[0]);
				this_totaldeadrows = DatumGetFloat8(funcRetValues[1]);
				(*totalrows) += this_totalrows;
				(*totaldeadrows) += this_totaldeadrows;
			}
			else
			{
				/* This is a sample row. */
				if (sampleTuples >= targrows)
					elog(ERROR, "too many sample rows received from gp_acquire_sample_rows");

				/* Read the 'toolarge' bitmap, if any */
				if (colLargeRowIndexes && !funcRetNulls[2])
				{
					ArrayType  *arrayVal;
					Datum	   *largelength;
					bool	   *nulls;
					int	    numelems;
					arrayVal = DatumGetArrayTypeP(funcRetValues[2]);
					deconstruct_array(arrayVal, FLOAT8OID, 8, true, 'd',
								&largelength, &nulls, &numelems);

					for (i = 0; i < relDesc->natts; i++)
					{
						Form_pg_attribute attr = TupleDescAttr(relDesc, i);

						if (attr->attisdropped)
							continue;

						if (largelength[i] != (Datum) 0)
						{
							colLargeRowIndexes[i] = bms_add_member(colLargeRowIndexes[i], sampleTuples);
							colLargeRowLength[i] += DatumGetFloat8(largelength[i]);
						}
					}
				}

				/* Process the columns */
				index = 0;
				for (i = 0; i < relDesc->natts; i++)
				{
					Form_pg_attribute attr = TupleDescAttr(relDesc, i);

					if (attr->attisdropped)
					{
						dnulls[i] = true;
						continue;
					}

					dnulls[i] = funcRetNulls[FIX_ATTR_NUM + index];
					dvalues[i] = funcRetValues[FIX_ATTR_NUM + index];
					index++;	/* Move index to the next result set attribute */
				}

				/*
				* Form a tuple
				*/
				rows[sampleTuples] = heap_form_tuple(attinmeta->tupdesc,
													dvalues,
													dnulls);
				sampleTuples++;

				/*
				 * note: we don't set the OIDs in the sample. ANALYZE doesn't
				 * collect stats for them
				 */
			}
			ReleaseTupleDesc(tupdesc);
		}

		ExecClearTuple(slot);
	}
	ExecDropSingleTupleTableSlot(slot);
	pfree(funcRetValues);
	pfree(funcRetNulls);
	pfree(dvalues);
	pfree(dnulls);

	return sampleTuples;
}

/*
 * Collect a sample from segments.
 *
 * Calls the gp_acquire_sample_rows() helper function on each segment,
 * and merges the results.
 */
static int
acquire_sample_rows_dispatcher(Relation onerel, bool inh, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	StringInfoData str;
	int			perseg_targrows;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	char	   *sql;
	Portal		portal;
	QueryDesc  *queryDesc = NULL;

	Assert(targrows > 0.0);

	/*
	 * Step1: Construct SQL command to dispatch to segments.
	 *
	 * Acquire an evenly-sized sample from each segment.
	 *
	 * XXX: If there's a significant bias between the segments, i.e. some
	 * segments have a lot more rows than others, the sample will biased, too.
	 * Would be nice to improve that, but it's not clear how. We could issue
	 * another query to get the table size from each segment first, and use
	 * those to weigh the sample size to get from each segment. But that'd
	 * require an extra round-trip, which is also not good. The caller
	 * actually already did that, to get the total relation size, but it
	 * doesn't pass that down to us, let alone the per-segment sizes.
	 */
	if (GpPolicyIsReplicated(onerel->rd_cdbpolicy))
		perseg_targrows = targrows;
	else if (GpPolicyIsPartitioned(onerel->rd_cdbpolicy))
		perseg_targrows = targrows / onerel->rd_cdbpolicy->numsegments;
	else
		elog(ERROR, "acquire_sample_rows_dispatcher() cannot be used on a non-distributed table");

	/*
	 * Did not use 'select * from pg_catalog.gp_acquire_sample_rows(...) as (..);'
	 * here. Because it requires to specify columns explicitly which leads to
	 * permission check on each columns. This is not consistent with GPDB5 and
	 * may result in different behaviour under different acl configuration.
	 */
	initStringInfo(&str);
	appendStringInfo(&str, "select pg_catalog.gp_acquire_sample_rows(%u, %d, '%s');",
					 RelationGetRelid(onerel),
					 perseg_targrows,
					 inh ? "t" : "f");

	/*
	 * Step2: Execute the constructed SQL.
	 *
	 * Do not use SPI here, because there might be a large number of wide rows
	 * returned and stored in memory, SPI cannot spill data to disk which may
	 * lead to OOM easily.
	 *
	 * Do not use SPI cusror either, because we should use SPI_cursor_fetch to fetch
	 * results in batches, which may have bad performance.
	 *
	 * Use ExecutorStart|ExecutorRun|ExecutorEnd to execute a plan and store results
	 * into tuplestore could handle this situation well.
	 *
	 * Execute the given query and store the results into portal->holdStore to
	 * avoid memory error.
	 */
	elog(elevel, "Executing SQL: %s", str.data);
	sql = str.data;
	/* Create a new portal to run the query in */
	portal = CreateNewPortal();
	/* Don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;
	/* use a tuplestore to store received tuples to avoid out of memory error */
	PortalCreateHoldStore(portal);
	queryDesc = build_querydesc(portal, sql);

	/* Call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* Run the plan  */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	/* Wait for completion of all qExec processes. */
	if (queryDesc->estate->dispatcherState
		&& queryDesc->estate->dispatcherState->primaryResults)
	{
		cdbdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);
	}

	ExecutorFinish(queryDesc);
	/*
	 * Step3: process results.
	 */
	sampleTuples = process_sample_rows(portal, queryDesc, onerel, rows,
									targrows, totalrows, totaldeadrows);

	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);
	PortalDrop(portal, false);

	return sampleTuples;
}

/*
 *	update_attstats() -- update attribute statistics for one relation
 *
 *		Statistics are stored in several places: the pg_class row for the
 *		relation has stats about the whole relation, and there is a
 *		pg_statistic row for each (non-system) attribute that has ever
 *		been analyzed.  The pg_class values are updated by VACUUM, not here.
 *
 *		pg_statistic rows are just added or updated normally.  This means
 *		that pg_statistic will probably contain some deleted rows at the
 *		completion of a vacuum cycle, unless it happens to get vacuumed last.
 *
 *		To keep things simple, we punt for pg_statistic, and don't try
 *		to compute or store rows for pg_statistic itself in pg_statistic.
 *		This could possibly be made to work, but it's not worth the trouble.
 *		Note analyze_rel() has seen to it that we won't come here when
 *		vacuuming pg_statistic itself.
 *
 *		Note: there would be a race condition here if two backends could
 *		ANALYZE the same table concurrently.  Presently, we lock that out
 *		by taking a self-exclusive lock on the relation in analyze_rel().
 */
static void
update_attstats(Oid relid, bool inh, int natts, VacAttrStats **vacattrstats)
{
	Relation	sd;
	int			attno;

	if (natts <= 0)
		return;					/* nothing to do */

	sd = table_open(StatisticRelationId, RowExclusiveLock);

	for (attno = 0; attno < natts; attno++)
	{
		VacAttrStats *stats = vacattrstats[attno];
		HeapTuple	stup,
					oldtup;
		int			i,
					k,
					n;
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		bool		replaces[Natts_pg_statistic];

		/* Ignore attr if we weren't able to collect stats */
		if (!stats->stats_valid)
			continue;

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
			replaces[i] = true;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stats->attr->attnum);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}
		i = Anum_pg_statistic_staop1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}
		i = Anum_pg_statistic_stacoll1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->stacoll[k]);	/* stacollN */
		}
		i = Anum_pg_statistic_stanumbers1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
									   FLOAT4OID,
									   sizeof(float4), FLOAT4PASSBYVAL, 'i');
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}
		i = Anum_pg_statistic_stavalues1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;

				arry = construct_array(stats->stavalues[k],
									   stats->numvalues[k],
									   stats->statypid[k],
									   stats->statyplen[k],
									   stats->statypbyval[k],
									   stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		/* Is there already a pg_statistic tuple for this attribute? */
		oldtup = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(relid),
								 Int16GetDatum(stats->attr->attnum),
								 BoolGetDatum(inh));

		if (HeapTupleIsValid(oldtup))
		{
			/* Yes, replace it */
			stup = heap_modify_tuple(oldtup,
									 RelationGetDescr(sd),
									 values,
									 nulls,
									 replaces);
			ReleaseSysCache(oldtup);
			CatalogTupleUpdate(sd, &stup->t_self, stup);
		}
		else
		{
			/* No, insert new tuple */
			stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
			CatalogTupleInsert(sd, stup);
		}

		heap_freetuple(stup);
	}

	table_close(sd, RowExclusiveLock);
}

/*
 * Standard fetch function for use by compute_stats subroutines.
 *
 * This exists to provide some insulation between compute_stats routines
 * and the actual storage of the sample data.
 */
static Datum
std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			attnum = stats->tupattnum;
	HeapTuple	tuple = stats->rows[rownum];
	TupleDesc	tupDesc = stats->tupDesc;

	return heap_getattr(tuple, attnum, tupDesc, isNull);
}

/*
 * Fetch function for analyzing index expressions.
 *
 * We have not bothered to construct index tuples, instead the data is
 * just in Datum arrays.
 */
static Datum
ind_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			i;

	/* exprvals and exprnulls are already offset for proper column */
	i = rownum * stats->rowstride;
	*isNull = stats->exprnulls[i];
	return stats->exprvals[i];
}


/*==========================================================================
 *
 * Code below this point represents the "standard" type-specific statistics
 * analysis algorithms.  This code can be replaced on a per-data-type basis
 * by setting a nonzero value in pg_type.typanalyze.
 *
 *==========================================================================
 */


/*
 * In PostgreSQL, WIDTH_THRESHOLD is here, but we've moved it to vacuum.h in
 * GPDB.
 */

#define swapInt(a,b)	do {int _tmp; _tmp=a; a=b; b=_tmp;} while(0)
#define swapDatum(a,b)	do {Datum _tmp; _tmp=a; a=b; b=_tmp;} while(0)

/*
 * Extra information used by the default analysis routines
 */
typedef struct
{
	int			count;			/* # of duplicates */
	int			first;			/* values[] index of first occurrence */
} ScalarMCVItem;

typedef struct
{
	SortSupport ssup;
	int		   *tupnoLink;
} CompareScalarsContext;


static void compute_trivial_stats(VacAttrStatsP stats,
								  AnalyzeAttrFetchFunc fetchfunc,
								  int samplerows,
								  double totalrows);
static void compute_distinct_stats(VacAttrStatsP stats,
								   AnalyzeAttrFetchFunc fetchfunc,
								   int samplerows,
								   double totalrows);
static void compute_scalar_stats(VacAttrStatsP stats,
								 AnalyzeAttrFetchFunc fetchfunc,
								 int samplerows,
								 double totalrows);
static void merge_leaf_stats(VacAttrStatsP stats,
								 AnalyzeAttrFetchFunc fetchfunc,
								 int samplerows,
								 double totalrows);
static int	compare_scalars(const void *a, const void *b, void *arg);
static int	compare_mcvs(const void *a, const void *b);
static int	analyze_mcv_list(int *mcv_counts,
							 int num_mcv,
							 double stadistinct,
							 double stanullfrac,
							 int samplerows,
							 double totalrows);


/*
 * std_typanalyze -- the default type-specific typanalyze function
 */
bool
std_typanalyze(VacAttrStats *stats)
{
	Form_pg_attribute attr = stats->attr;
	Oid			ltopr;
	Oid			eqopr;
	StdAnalyzeData *mystats;

	/* If the attstattarget column is negative, use the default value */
	/* NB: it is okay to scribble on stats->attr since it's a copy */
	if (attr->attstattarget < 0)
		attr->attstattarget = default_statistics_target;

	/* Look for default "<" and "=" operators for column's type */
	get_sort_group_operators(stats->attrtypid,
							 false, false, false,
							 &ltopr, &eqopr, NULL,
							 NULL);

	/* Save the operator info for compute_stats routines */
	mystats = (StdAnalyzeData *) palloc(sizeof(StdAnalyzeData));
	mystats->eqopr = eqopr;
	mystats->eqfunc = OidIsValid(eqopr) ? get_opcode(eqopr) : InvalidOid;
	mystats->ltopr = ltopr;
	stats->extra_data = mystats;
	stats->merge_stats = false;

	/*
	 * Determine which standard statistics algorithm to use
	 */
	List *va_cols = list_make1(makeString(NameStr(stats->attr->attname)));
	if (get_rel_relkind(attr->attrelid) == RELKIND_PARTITIONED_TABLE &&
		!get_rel_relispartition(attr->attrelid) &&
		leaf_parts_analyzed(stats->attr->attrelid, InvalidOid, va_cols, stats->elevel) &&
		((!OidIsValid(eqopr)) || op_hashjoinable(eqopr, stats->attrtypid)))
	{
		stats->merge_stats = true;
		stats->compute_stats = merge_leaf_stats;
		stats->minrows = 300 * attr->attstattarget;
	}
	else
		if (OidIsValid(eqopr) && OidIsValid(ltopr))
	{
		/* Seems to be a scalar datatype */
		stats->compute_stats = compute_scalar_stats;
		/*--------------------
		 * The following choice of minrows is based on the paper
		 * "Random sampling for histogram construction: how much is enough?"
		 * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
		 * Proceedings of ACM SIGMOD International Conference on Management
		 * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
		 * says that for table size n, histogram size k, maximum relative
		 * error in bin size f, and error probability gamma, the minimum
		 * random sample size is
		 *		r = 4 * k * ln(2*n/gamma) / f^2
		 * Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain
		 *		r = 305.82 * k
		 * Note that because of the log function, the dependence on n is
		 * quite weak; even at n = 10^12, a 300*k sample gives <= 0.66
		 * bin size error with probability 0.99.  So there's no real need to
		 * scale for n, which is a good thing because we don't necessarily
		 * know it at this point.
		 *--------------------
		 */
		stats->minrows = 300 * attr->attstattarget;
	}
	else if (OidIsValid(eqopr))
	{
		/* We can still recognize distinct values */
		stats->compute_stats = compute_distinct_stats;
		/* Might as well use the same minrows as above */
		stats->minrows = 300 * attr->attstattarget;
	}
	else
	{
		/* Can't do much but the trivial stuff */
		stats->compute_stats = compute_trivial_stats;
		/* Might as well use the same minrows as above */
		stats->minrows = 300 * attr->attstattarget;
	}
	list_free(va_cols);
	return true;
}


/*
 *	compute_trivial_stats() -- compute very basic column statistics
 *
 *	We use this when we cannot find a hash "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows and the average datum width.
 */
static void
compute_trivial_stats(VacAttrStatsP stats,
					  AnalyzeAttrFetchFunc fetchfunc,
					  int samplerows,
					  double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);

	for (i = 0; i < samplerows; i++)
	{
		Datum		value;
		bool		isnull;

		vacuum_delay_point();

		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}
		nonnull_cnt++;

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}
	}

	/* We can only compute average width if we found some non-null values. */
	if (nonnull_cnt > 0)
	{
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		if (is_varwidth)
			stats->stawidth = total_width / (double) nonnull_cnt;
		else
			stats->stawidth = stats->attrtype->typlen;
		stats->stadistinct = 0.0;	/* "unknown" */
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		if (is_varwidth)
			stats->stawidth = 0;	/* "unknown" */
		else
			stats->stawidth = stats->attrtype->typlen;
		stats->stadistinct = 0.0;	/* "unknown" */
	}
}


/*
 *	compute_distinct_stats() -- compute column statistics including ndistinct
 *
 *	We use this when we can find only an "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, and the (estimated) number of distinct values.
 *
 *	The most common values are determined by brute force: we keep a list
 *	of previously seen values, ordered by number of times seen, as we scan
 *	the samples.  A newly seen value is inserted just after the last
 *	multiply-seen value, causing the bottommost (oldest) singly-seen value
 *	to drop off the list.  The accuracy of this method, and also its cost,
 *	depend mainly on the length of the list we are willing to keep.
 */
static void
compute_distinct_stats(VacAttrStatsP stats,
					   AnalyzeAttrFetchFunc fetchfunc,
					   int samplerows,
					   double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	int			toowide_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);
	FmgrInfo	f_cmpeq;
	typedef struct
	{
		Datum		value;
		int			count;
	} TrackItem;
	TrackItem  *track;
	int			track_cnt,
				track_max;
	int			num_mcv = stats->attr->attstattarget;
	StdAnalyzeData *mystats = (StdAnalyzeData *) stats->extra_data;

	/*
	 * We track up to 2*n values for an n-element MCV list; but at least 10
	 */
	track_max = 2 * num_mcv;
	if (track_max < 10)
		track_max = 10;
	track = (TrackItem *) palloc(track_max * sizeof(TrackItem));
	track_cnt = 0;

	fmgr_info(mystats->eqfunc, &f_cmpeq);

	stats->stahll = (bytea *)gp_hyperloglog_init_def();

	ereport(DEBUG2,
			(errmsg("Computing Minimal Stats for column %s",
					get_attname(stats->attr->attrelid, stats->attr->attnum, false))));

	for (i = 0; i < samplerows; i++)
	{
		Datum		value;
		bool		isnull;
		bool		match;
		int			firstcount1,
					j;

		vacuum_delay_point();

		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}
		nonnull_cnt++;

		stats->stahll = (bytea *)gp_hyperloglog_add_item((GpHLLCounter) stats->stahll, value, stats->attr->attlen, stats->attr->attbyval, stats->attr->attalign);

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));

			/*
			 * If the value is toasted, we want to detoast it just once to
			 * avoid repeated detoastings and resultant excess memory usage
			 * during the comparisons.  Also, check to see if the value is
			 * excessively wide, and if so don't detoast at all --- just
			 * ignore the value.
			 */
			if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
			{
				toowide_cnt++;
				continue;
			}
			value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}

		/*
		 * See if the value matches anything we're already tracking.
		 */
		match = false;
		firstcount1 = track_cnt;
		for (j = 0; j < track_cnt; j++)
		{
			if (DatumGetBool(FunctionCall2Coll(&f_cmpeq,
											   stats->attrcollid,
											   value, track[j].value)))
			{
				match = true;
				break;
			}
			if (j < firstcount1 && track[j].count == 1)
				firstcount1 = j;
		}

		if (match)
		{
			/* Found a match */
			track[j].count++;
			/* This value may now need to "bubble up" in the track list */
			while (j > 0 && track[j].count > track[j - 1].count)
			{
				swapDatum(track[j].value, track[j - 1].value);
				swapInt(track[j].count, track[j - 1].count);
				j--;
			}
		}
		else
		{
			/* No match.  Insert at head of count-1 list */
			if (track_cnt < track_max)
				track_cnt++;
			for (j = track_cnt - 1; j > firstcount1; j--)
			{
				track[j].value = track[j - 1].value;
				track[j].count = track[j - 1].count;
			}
			if (firstcount1 < track_cnt)
			{
				track[firstcount1].value = value;
				track[firstcount1].count = 1;
			}
		}
	}

	/* We can only compute real stats if we found some non-null values. */
	if (nonnull_cnt > 0)
	{
		int			nmultiple,
					summultiple;

		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		if (is_varwidth)
			stats->stawidth = (total_width + stats->totalwidelength) / (double) (nonnull_cnt + stats->widerow_num);
		else
			stats->stawidth = stats->attrtype->typlen;

		/* Count the number of values we found multiple times */
		summultiple = 0;
		for (nmultiple = 0; nmultiple < track_cnt; nmultiple++)
		{
			if (track[nmultiple].count == 1)
				break;
			summultiple += track[nmultiple].count;
		}

		((GpHLLCounter) (stats->stahll))->nmultiples = nmultiple;
		((GpHLLCounter) (stats->stahll))->ndistinct = track_cnt;
		((GpHLLCounter) (stats->stahll))->samplerows = samplerows;

		if (nmultiple == 0)
		{
			/*
			 * If we found no repeated non-null values, assume it's a unique
			 * column; but be sure to discount for any nulls we found.
			 */
			stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
		}
		else if (track_cnt < track_max && toowide_cnt == 0 &&
				 nmultiple == track_cnt)
		{
			/*
			 * Our track list includes every value in the sample, and every
			 * value appeared more than once.  Assume the column has just
			 * these values.  (This case is meant to address columns with
			 * small, fixed sets of possible values, such as boolean or enum
			 * columns.  If there are any values that appear just once in the
			 * sample, including too-wide values, we should assume that that's
			 * not what we're dealing with.)
			 */
			stats->stadistinct = track_cnt;
		}
		else
		{
			/*----------
			 * Estimate the number of distinct values using the estimator
			 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
			 *		n*d / (n - f1 + f1*n/N)
			 * where f1 is the number of distinct values that occurred
			 * exactly once in our sample of n rows (from a total of N),
			 * and d is the total number of distinct values in the sample.
			 * This is their Duj1 estimator; the other estimators they
			 * recommend are considerably more complex, and are numerically
			 * very unstable when n is much smaller than N.
			 *
			 * In this calculation, we consider only non-nulls.  We used to
			 * include rows with null values in the n and N counts, but that
			 * leads to inaccurate answers in columns with many nulls, and
			 * it's intuitively bogus anyway considering the desired result is
			 * the number of distinct non-null values.
			 *
			 * We assume (not very reliably!) that all the multiply-occurring
			 * values are reflected in the final track[] list, and the other
			 * nonnull values all appeared but once.  (XXX this usually
			 * results in a drastic overestimate of ndistinct.  Can we do
			 * any better?)
			 *----------
			 */
			int			f1 = nonnull_cnt - summultiple;
			int			d = f1 + nmultiple;
			double		n = samplerows - null_cnt;
			double		N = totalrows * (1.0 - stats->stanullfrac);
			double		stadistinct;

			/* N == 0 shouldn't happen, but just in case ... */
			if (N > 0)
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
			else
				stadistinct = 0;

			/* Clamp to sane range in case of roundoff error */
			if (stadistinct < d)
				stadistinct = d;
			if (stadistinct > N)
				stadistinct = N;
			/* And round to integer */
			stats->stadistinct = floor(stadistinct + 0.5);
		}

		/*
		 * If we estimated the number of distinct values at more than 10% of
		 * the total row count (a very arbitrary limit), then assume that
		 * stadistinct should scale with the row count rather than be a fixed
		 * value.
		 */
		if (stats->stadistinct > 0.1 * totalrows)
			stats->stadistinct = -(stats->stadistinct / totalrows);

		/*
		 * Decide how many values are worth storing as most-common values. If
		 * we are able to generate a complete MCV list (all the values in the
		 * sample will fit, and we think these are all the ones in the table),
		 * then do so.  Otherwise, store only those values that are
		 * significantly more common than the values not in the list.
		 *
		 * Note: the first of these cases is meant to address columns with
		 * small, fixed sets of possible values, such as boolean or enum
		 * columns.  If we can *completely* represent the column population by
		 * an MCV list that will fit into the stats target, then we should do
		 * so and thus provide the planner with complete information.  But if
		 * the MCV list is not complete, it's generally worth being more
		 * selective, and not just filling it all the way up to the stats
		 * target.
		 */
		if (track_cnt < track_max && toowide_cnt == 0 &&
			stats->stadistinct > 0 &&
			track_cnt <= num_mcv)
		{
			/* Track list includes all values seen, and all will fit */
			num_mcv = track_cnt;
		}
		else
		{
			int		   *mcv_counts;

			/* Incomplete list; decide how many values are worth keeping */
			if (num_mcv > track_cnt)
				num_mcv = track_cnt;

			if (num_mcv > 0)
			{
				mcv_counts = (int *) palloc(num_mcv * sizeof(int));
				for (i = 0; i < num_mcv; i++)
					mcv_counts[i] = track[i].count;

				num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
										   stats->stadistinct,
										   stats->stanullfrac,
										   samplerows, totalrows);
			}
		}

		/* Generate MCV slot entry */
		if (num_mcv > 0)
		{
			MemoryContext old_context;
			Datum	   *mcv_values;
			float4	   *mcv_freqs;

			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			mcv_values = (Datum *) palloc(num_mcv * sizeof(Datum));
			mcv_freqs = (float4 *) palloc(num_mcv * sizeof(float4));
			for (i = 0; i < num_mcv; i++)
			{
				mcv_values[i] = datumCopy(track[i].value,
										  stats->attrtype->typbyval,
										  stats->attrtype->typlen);
				mcv_freqs[i] = (double) track[i].count / (double) samplerows;
			}
			MemoryContextSwitchTo(old_context);

			stats->stakind[0] = STATISTIC_KIND_MCV;
			stats->staop[0] = mystats->eqopr;
			stats->stacoll[0] = stats->attrcollid;
			stats->stanumbers[0] = mcv_freqs;
			stats->numnumbers[0] = num_mcv;
			stats->stavalues[0] = mcv_values;
			stats->numvalues[0] = num_mcv;

			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
		}
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		if (is_varwidth)
			stats->stawidth = 0;	/* "unknown" */
		else
			stats->stawidth = stats->attrtype->typlen;
		stats->stadistinct = 0.0;		/* "unknown" */
	}

	/* We don't need to bother cleaning up any of our temporary palloc's */
}


/*
 *	compute_scalar_stats() -- compute column statistics
 *
 *	We use this when we can find "=" and "<" operators for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, the (estimated) number of distinct values, the
 *	distribution histogram, and the correlation of physical to logical order.
 *
 *	The desired stats can be determined fairly easily after sorting the
 *	data values into order.
 */
static void
compute_scalar_stats(VacAttrStatsP stats,
					 AnalyzeAttrFetchFunc fetchfunc,
					 int samplerows,
					 double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	int			toowide_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);
	double		corr_xysum;
	SortSupportData ssup;
	ScalarItem *values;
	int			values_cnt = 0;
	int		   *tupnoLink;
	ScalarMCVItem *track;
	int			track_cnt = 0;
	int			num_mcv = stats->attr->attstattarget;
	int			num_bins = stats->attr->attstattarget;
	StdAnalyzeData *mystats = (StdAnalyzeData *) stats->extra_data;

	values = (ScalarItem *) palloc(samplerows * sizeof(ScalarItem));
	tupnoLink = (int *) palloc(samplerows * sizeof(int));
	track = (ScalarMCVItem *) palloc(num_mcv * sizeof(ScalarMCVItem));

	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;
	ssup.ssup_collation = stats->attrcollid;
	ssup.ssup_nulls_first = false;

	/*
	 * For now, don't perform abbreviated key conversion, because full values
	 * are required for MCV slot generation.  Supporting that optimization
	 * would necessitate teaching compare_scalars() to call a tie-breaker.
	 */
	ssup.abbreviate = false;

	PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

	/* Initialize HLL counter to be stored in stats */
	stats->stahll = (bytea *)gp_hyperloglog_init_def();

	ereport(DEBUG2,
			(errmsg("Computing Scalar Stats for column %s",
					get_attname(stats->attr->attrelid, stats->attr->attnum, false))));

	/* Initial scan to find sortable values */
	for (i = 0; i < samplerows; i++)
	{
		Datum		value;
		bool		isnull;

		vacuum_delay_point();

		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}
		nonnull_cnt++;

		stats->stahll = (bytea *)gp_hyperloglog_add_item((GpHLLCounter) stats->stahll, value, stats->attr->attlen, stats->attr->attbyval, stats->attr->attalign);

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));

			/*
			 * If the value is toasted, we want to detoast it just once to
			 * avoid repeated detoastings and resultant excess memory usage
			 * during the comparisons.  Also, check to see if the value is
			 * excessively wide, and if so don't detoast at all --- just
			 * ignore the value.
			 */
			if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
			{
				toowide_cnt++;
				continue;
			}
			value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}

		/* Add it to the list to be sorted */
		values[values_cnt].value = value;
		values[values_cnt].tupno = values_cnt;
		tupnoLink[values_cnt] = values_cnt;
		values_cnt++;
	}

	/* We can only compute real stats if we found some sortable values. */
	if (values_cnt > 0)
	{
		int			ndistinct,	/* # distinct values in sample */
					nmultiple,	/* # that appear multiple times */
					num_hist,
					dups_cnt;
		int			slot_idx = 0;
		CompareScalarsContext cxt;

		/* Sort the collected values */
		cxt.ssup = &ssup;
		cxt.tupnoLink = tupnoLink;
		qsort_arg((void *) values, values_cnt, sizeof(ScalarItem),
				  compare_scalars, (void *) &cxt);

		/*
		 * Now scan the values in order, find the most common ones, and also
		 * accumulate ordering-correlation statistics.
		 *
		 * To determine which are most common, we first have to count the
		 * number of duplicates of each value.  The duplicates are adjacent in
		 * the sorted list, so a brute-force approach is to compare successive
		 * datum values until we find two that are not equal. However, that
		 * requires N-1 invocations of the datum comparison routine, which are
		 * completely redundant with work that was done during the sort.  (The
		 * sort algorithm must at some point have compared each pair of items
		 * that are adjacent in the sorted order; otherwise it could not know
		 * that it's ordered the pair correctly.) We exploit this by having
		 * compare_scalars remember the highest tupno index that each
		 * ScalarItem has been found equal to.  At the end of the sort, a
		 * ScalarItem's tupnoLink will still point to itself if and only if it
		 * is the last item of its group of duplicates (since the group will
		 * be ordered by tupno).
		 */
		corr_xysum = 0;
		ndistinct = 0;
		nmultiple = 0;
		dups_cnt = 0;
		for (i = 0; i < values_cnt; i++)
		{
			int			tupno = values[i].tupno;

			corr_xysum += ((double) i) * ((double) tupno);
			dups_cnt++;
			if (tupnoLink[tupno] == tupno)
			{
				/* Reached end of duplicates of this value */
				ndistinct++;
				if (dups_cnt > 1)
				{
					nmultiple++;
					if (track_cnt < num_mcv ||
						dups_cnt > track[track_cnt - 1].count)
					{
						/*
						 * Found a new item for the mcv list; find its
						 * position, bubbling down old items if needed. Loop
						 * invariant is that j points at an empty/ replaceable
						 * slot.
						 */
						int			j;

						if (track_cnt < num_mcv)
							track_cnt++;
						for (j = track_cnt - 1; j > 0; j--)
						{
							if (dups_cnt <= track[j - 1].count)
								break;
							track[j].count = track[j - 1].count;
							track[j].first = track[j - 1].first;
						}
						track[j].count = dups_cnt;
						track[j].first = i + 1 - dups_cnt;
					}
				}
				dups_cnt = 0;
			}
		}

		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		if (is_varwidth)
			stats->stawidth = (total_width + stats->totalwidelength) / (double) (nonnull_cnt + stats->widerow_num);
		else
			stats->stawidth = stats->attrtype->typlen;

		// interpolate NDV calculation based on the hll distinct count
		// for each column in leaf partitions which will be used later
		// to merge root stats
		((GpHLLCounter) (stats->stahll))->nmultiples = nmultiple;
		((GpHLLCounter) (stats->stahll))->ndistinct = ndistinct;
		((GpHLLCounter) (stats->stahll))->samplerows = samplerows;

		if (nmultiple == 0)
		{
			/*
			 * If we found no repeated non-null values, assume it's a unique
			 * column; but be sure to discount for any nulls we found.
			 */
			stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
		}
		else if (toowide_cnt == 0 && nmultiple == ndistinct)
		{
			/*
			 * Every value in the sample appeared more than once.  Assume the
			 * column has just these values.  (This case is meant to address
			 * columns with small, fixed sets of possible values, such as
			 * boolean or enum columns.  If there are any values that appear
			 * just once in the sample, including too-wide values, we should
			 * assume that that's not what we're dealing with.)
			 */
			stats->stadistinct = ndistinct;
		}
		else
		{
			/*----------
			 * Estimate the number of distinct values using the estimator
			 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
			 *		n*d / (n - f1 + f1*n/N)
			 * where f1 is the number of distinct values that occurred
			 * exactly once in our sample of n rows (from a total of N),
			 * and d is the total number of distinct values in the sample.
			 * This is their Duj1 estimator; the other estimators they
			 * recommend are considerably more complex, and are numerically
			 * very unstable when n is much smaller than N.
			 *
			 * In this calculation, we consider only non-nulls.  We used to
			 * include rows with null values in the n and N counts, but that
			 * leads to inaccurate answers in columns with many nulls, and
			 * it's intuitively bogus anyway considering the desired result is
			 * the number of distinct non-null values.
			 *
			 * Overwidth values are assumed to have been distinct.
			 *----------
			 */
			int			f1 = ndistinct - nmultiple + toowide_cnt;
			int			d = f1 + nmultiple;
			double		n = samplerows - null_cnt;
			double		N = totalrows * (1.0 - stats->stanullfrac);
			double		stadistinct;

			/* N == 0 shouldn't happen, but just in case ... */
			if (N > 0)
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
			else
				stadistinct = 0;

			/* Clamp to sane range in case of roundoff error */
			if (stadistinct < d)
				stadistinct = d;
			if (stadistinct > N)
				stadistinct = N;
			/* And round to integer */
			stats->stadistinct = floor(stadistinct + 0.5);
		}

		/*
		 * For FULLSCAN HLL, get ndistinct from the GpHLLCounter
		 * instead of computing it
		 */
		if (stats->stahll_full != NULL)
		{
			GpHLLCounter hLLFull = (GpHLLCounter) DatumGetByteaP(stats->stahll_full);
			GpHLLCounter hllFull_copy = gp_hll_copy(hLLFull);
			stats->stadistinct = round(gp_hyperloglog_estimate(hllFull_copy));
			pfree(hllFull_copy);
			if ((fabs(totalrows - stats->stadistinct) / (float) totalrows) < 0.05)
			{
				stats->stadistinct = -1;
			}

		}
		/*
		 * If we estimated the number of distinct values at more than 10% of
		 * the total row count (a very arbitrary limit), then assume that
		 * stadistinct should scale with the row count rather than be a fixed
		 * value.
		 */
		if (stats->stadistinct > 0.1 * totalrows)
			stats->stadistinct = -(stats->stadistinct / totalrows);

		/*
		 * Decide how many values are worth storing as most-common values. If
		 * we are able to generate a complete MCV list (all the values in the
		 * sample will fit, and we think these are all the ones in the table),
		 * then do so.  Otherwise, store only those values that are
		 * significantly more common than the values not in the list.
		 *
		 * Note: the first of these cases is meant to address columns with
		 * small, fixed sets of possible values, such as boolean or enum
		 * columns.  If we can *completely* represent the column population by
		 * an MCV list that will fit into the stats target, then we should do
		 * so and thus provide the planner with complete information.  But if
		 * the MCV list is not complete, it's generally worth being more
		 * selective, and not just filling it all the way up to the stats
		 * target.
		 */
		if (track_cnt == ndistinct && toowide_cnt == 0 &&
			stats->stadistinct > 0 &&
			track_cnt <= num_mcv)
		{
			/* Track list includes all values seen, and all will fit */
			num_mcv = track_cnt;
		}
		else
		{
			int		   *mcv_counts;

			/* Incomplete list; decide how many values are worth keeping */
			if (num_mcv > track_cnt)
				num_mcv = track_cnt;

			if (num_mcv > 0)
			{
				mcv_counts = (int *) palloc(num_mcv * sizeof(int));
				for (i = 0; i < num_mcv; i++)
					mcv_counts[i] = track[i].count;

				num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
										   stats->stadistinct,
										   stats->stanullfrac,
										   samplerows, totalrows);
			}
		}

		/* Generate MCV slot entry */
		if (num_mcv > 0)
		{
			MemoryContext old_context;
			Datum	   *mcv_values;
			float4	   *mcv_freqs;

			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			mcv_values = (Datum *) palloc(num_mcv * sizeof(Datum));
			mcv_freqs = (float4 *) palloc(num_mcv * sizeof(float4));
			for (i = 0; i < num_mcv; i++)
			{
				mcv_values[i] = datumCopy(values[track[i].first].value,
										  stats->attrtype->typbyval,
										  stats->attrtype->typlen);
				mcv_freqs[i] = (double) track[i].count / (double) samplerows;
			}
			MemoryContextSwitchTo(old_context);

			stats->stakind[slot_idx] = STATISTIC_KIND_MCV;
			stats->staop[slot_idx] = mystats->eqopr;
			stats->stacoll[slot_idx] = stats->attrcollid;
			stats->stanumbers[slot_idx] = mcv_freqs;
			stats->numnumbers[slot_idx] = num_mcv;
			stats->stavalues[slot_idx] = mcv_values;
			stats->numvalues[slot_idx] = num_mcv;

			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
			slot_idx++;
		}

		/*
		 * Generate a histogram slot entry if there are at least two distinct
		 * values not accounted for in the MCV list.  (This ensures the
		 * histogram won't collapse to empty or a singleton.)
		 */
		num_hist = ndistinct - num_mcv;
		if (num_hist > num_bins)
			num_hist = num_bins + 1;
		if (num_hist >= 2)
		{
			MemoryContext old_context;
			Datum	   *hist_values;
			int			nvals;
			int			pos,
						posfrac,
						delta,
						deltafrac;

			/* Sort the MCV items into position order to speed next loop */
			qsort((void *) track, num_mcv,
				  sizeof(ScalarMCVItem), compare_mcvs);

			/*
			 * Collapse out the MCV items from the values[] array.
			 *
			 * Note we destroy the values[] array here... but we don't need it
			 * for anything more.  We do, however, still need values_cnt.
			 * nvals will be the number of remaining entries in values[].
			 */
			if (num_mcv > 0)
			{
				int			src,
							dest;
				int			j;

				src = dest = 0;
				j = 0;			/* index of next interesting MCV item */
				while (src < values_cnt)
				{
					int			ncopy;

					if (j < num_mcv)
					{
						int			first = track[j].first;

						if (src >= first)
						{
							/* advance past this MCV item */
							src = first + track[j].count;
							j++;
							continue;
						}
						ncopy = first - src;
					}
					else
						ncopy = values_cnt - src;
					memmove(&values[dest], &values[src],
							ncopy * sizeof(ScalarItem));
					src += ncopy;
					dest += ncopy;
				}
				nvals = dest;
			}
			else
				nvals = values_cnt;
			Assert(nvals >= num_hist);

			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			hist_values = (Datum *) palloc(num_hist * sizeof(Datum));

			/*
			 * The object of this loop is to copy the first and last values[]
			 * entries along with evenly-spaced values in between.  So the
			 * i'th value is values[(i * (nvals - 1)) / (num_hist - 1)].  But
			 * computing that subscript directly risks integer overflow when
			 * the stats target is more than a couple thousand.  Instead we
			 * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
			 * the integral and fractional parts of the sum separately.
			 */
			delta = (nvals - 1) / (num_hist - 1);
			deltafrac = (nvals - 1) % (num_hist - 1);
			pos = posfrac = 0;

			for (i = 0; i < num_hist; i++)
			{
				hist_values[i] = datumCopy(values[pos].value,
										   stats->attrtype->typbyval,
										   stats->attrtype->typlen);
				pos += delta;
				posfrac += deltafrac;
				if (posfrac >= (num_hist - 1))
				{
					/* fractional part exceeds 1, carry to integer part */
					pos++;
					posfrac -= (num_hist - 1);
				}
			}

			MemoryContextSwitchTo(old_context);

			stats->stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
			stats->staop[slot_idx] = mystats->ltopr;
			stats->stacoll[slot_idx] = stats->attrcollid;
			stats->stavalues[slot_idx] = hist_values;
			stats->numvalues[slot_idx] = num_hist;

			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
			slot_idx++;
		}

		/* Generate a correlation entry if there are multiple values */
		if (values_cnt > 1)
		{
			MemoryContext old_context;
			float4	   *corrs;
			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			corrs = (float4 *) palloc(sizeof(float4));
			MemoryContextSwitchTo(old_context);
			if (stats->partitiontbl_qd )
			{
				if (!stats->corrnull)
				{
					corrs[0] = stats->corrval;
					stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
					stats->staop[slot_idx] = mystats->ltopr;
					stats->stacoll[slot_idx] = stats->attrcollid;
					stats->stanumbers[slot_idx] = corrs;
					stats->numnumbers[slot_idx] = 1;
					slot_idx++;
				}
			}
			else /* this is on QE */
			{
				double		corr_xsum,
							corr_x2sum;

				/*----------
				 * Since we know the x and y value sets are both
				 *		0, 1, ..., values_cnt-1
				 * we have sum(x) = sum(y) =
				 *		(values_cnt-1)*values_cnt / 2
				 * and sum(x^2) = sum(y^2) =
				 *		(values_cnt-1)*values_cnt*(2*values_cnt-1) / 6.
				 *----------
				 */
				corr_xsum = ((double) (values_cnt - 1)) *
					((double) values_cnt) / 2.0;
				corr_x2sum = ((double) (values_cnt - 1)) *
					((double) values_cnt) * (double) (2 * values_cnt - 1) / 6.0;
				/* And the correlation coefficient reduces to */
				corrs[0] = (values_cnt * corr_xysum - corr_xsum * corr_xsum) /
					(values_cnt * corr_x2sum - corr_xsum * corr_xsum);

				stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
				stats->staop[slot_idx] = mystats->ltopr;
				stats->stacoll[slot_idx] = stats->attrcollid;
				stats->stanumbers[slot_idx] = corrs;
				stats->numnumbers[slot_idx] = 1;
				slot_idx++;
			}
		}
	}
	else if (nonnull_cnt > 0)
	{
		/* We found some non-null values, but they were all too wide */
		Assert(nonnull_cnt == toowide_cnt);
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;
		if (is_varwidth)
			stats->stawidth = (total_width + stats->totalwidelength) / (double) (nonnull_cnt + stats->widerow_num);
		else
			stats->stawidth = stats->attrtype->typlen;
		/* Assume all too-wide values are distinct, so it's a unique column */
		stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;
		if (is_varwidth)
			stats->stawidth = 0;	/* "unknown" */
		else
			stats->stawidth = stats->attrtype->typlen;
		stats->stadistinct = 0.0;	/* "unknown" */
	}
	else
	{
		/*
		 * ORCA complains if a column has no statistics whatsoever, so store
		 * either the best we can figure out given what we have, or zero in
		 * case we don't have enough.
		 */
		stats->stats_valid = true;
		if (samplerows)
			stats->stanullfrac = (double) null_cnt / (double) samplerows;
		else
			stats->stanullfrac = 0.0;
		if (is_varwidth)
			stats->stawidth = 0;	/* "unknown" */
		else
			stats->stawidth = stats->attrtype->typlen;
		stats->stadistinct = 0.0;		/* "unknown" */
	}

	/* We don't need to bother cleaning up any of our temporary palloc's */
}

/*
 *	merge_leaf_stats() -- merge leaf stats for the root
 *
 *	This is only used when the relation is the root partition and merges
 *	the statistics available in pg_statistic for the leaf partitions.
 *
 *  We use this for two scenarios:
 *
 *	1. When we can find "=" and "<" operators for the datatype, and the
 *	"=" operator is hashjoinable. In this case, we determine the fraction
 *	of non-null rows, the average width, the most common values, the
 *	(estimated) number of distinct values, the distribution histogram.
 *
 *	2. When we can find neither "=" nor "<" operator for the data type. In
 *	this case, we only determine the fraction of non-null rows and the
 *	average width.
 */
static void
merge_leaf_stats(VacAttrStatsP stats,
				 AnalyzeAttrFetchFunc fetchfunc,
				 int samplerows,
				 double totalrows)
{
	List *all_children_list;
	List *oid_list;
	StdAnalyzeData *mystats = (StdAnalyzeData *) stats->extra_data;
	int numPartitions;

	ListCell *lc;
	float *relTuples;
	float *nDistincts;
	float *nMultiples;
	int relNum;
	float totalTuples = 0;
	float nmultiple = 0; // number of values that appeared more than once
	bool allDistinct = false;
	int slot_idx = 0;
	int sampleCount = 0;
	Oid ltopr = mystats->ltopr;
	Oid eqopr = mystats->eqopr;

	ereport(DEBUG2,
			(errmsg("Merging leaf partition stats to calculate root partition stats : column %s",
					get_attname(stats->attr->attrelid, stats->attr->attnum, false))));

	/* 
	 * Since we have acquired ShareUpdateExclusiveLock on the parent table when
	 * ANALYZE'ing it, we don't need extra lock to guard against concurrent DROP
	 * of either the parent or the child (which requries AccessExclusiveLock on
	 * the parent).
	 * Concurrent UPDATE is possible but because we are not updating the table
	 * ourselves, NoLock is sufficient here.
	 */
	all_children_list = find_all_inheritors(stats->attr->attrelid, NoLock, NULL);
	SIMPLE_FAULT_INJECTOR("merge_leaf_stats_after_find_children");

	oid_list = NIL;
	foreach (lc, all_children_list)
	{
		Oid			pkrelid = lfirst_oid(lc);

		/* skip intermediate partitions, we're only interested in leaves */
		if (get_rel_relkind(pkrelid) != RELKIND_RELATION)
			continue;

		oid_list = lappend_oid(oid_list, pkrelid);
	}
	numPartitions = list_length(oid_list);

	relTuples = (float *) palloc0(sizeof(float) * numPartitions);
	nDistincts = (float *) palloc0(sizeof(float) * numPartitions);
	nMultiples = (float *) palloc0(sizeof(float) * numPartitions);

	relNum = 0;
	foreach (lc, oid_list)
	{
		Oid			pkrelid = lfirst_oid(lc);

		relTuples[relNum] = get_rel_reltuples(pkrelid);
		totalTuples = totalTuples + relTuples[relNum];
		relNum++;
	}

	if (totalTuples == 0.0)
		return;

	MemoryContext old_context;

	HeapTuple *heaptupleStats =
		(HeapTuple *) palloc(numPartitions * sizeof(HeapTuple));

	// NDV calculations
	float4 colAvgWidth = 0;
	float4 nullCount = 0;
	GpHLLCounter *hllcounters = (GpHLLCounter *) palloc0(numPartitions * sizeof(GpHLLCounter));
	GpHLLCounter *hllcounters_fullscan = (GpHLLCounter *) palloc0(numPartitions * sizeof(GpHLLCounter));
	GpHLLCounter *hllcounters_copy = (GpHLLCounter *) palloc0(numPartitions * sizeof(GpHLLCounter));

	GpHLLCounter finalHLL = NULL;
	GpHLLCounter finalHLLFull = NULL;
	int i = 0;
	double ndistinct = 0.0;
	int fullhll_count = 0;
	int samplehll_count = 0;
	int totalhll_count = 0;
	double max_part_distinct = 0.0;
	foreach (lc, oid_list)
	{
		Oid		leaf_relid = lfirst_oid(lc);
		int32	stawidth = 0;
		float4	stanullfrac = 0.0;
		float4	stadistinct = 0.0;

		const char *attname = get_attname(stats->attr->attrelid, stats->attr->attnum, false);

		/*
		 * fetch_leaf_attnum and fetch_leaf_att_stats retrieve leaf partition
		 * table's pg_attribute tuple and pg_statistic tuple through index scan
		 * instead of system catalog cache. Since if using system catalog cache,
		 * the total tuple entries insert into the cache will up to:
		 * (number_of_leaf_tables * number_of_column_in_this_table) pg_attribute tuples
		 * +
		 * (number_of_leaf_tables * number_of_column_in_this_table) pg_statistic tuples
		 * which could use extremely large memroy in CacheMemoryContext.
		 * This happens when all of the leaf tables are analyzed. And the current function
		 * will execute for all columns.
		 *
		 * fetch_leaf_att_stats copy the original tuple, so remember to free it.
		 *
		 * As a side-effect, ANALYZE same root table serveral times in same session is much
		 * more slower than before since we don't rely on system catalog cache.
		 *
		 * But we still using the tuple descriptor in system catalog cache to retrieve
		 * attribute in fetched tuples. See get_attstatsslot.
		 */
		AttrNumber child_attno = fetch_leaf_attnum(leaf_relid, attname);
		heaptupleStats[i] = fetch_leaf_att_stats(leaf_relid, child_attno);

		// if there is no colstats, we can skip this partition's stats
		if (!HeapTupleIsValid(heaptupleStats[i]))
		{
			i++;
			continue;
		}

		stawidth = ((Form_pg_statistic) GETSTRUCT(heaptupleStats[i]))->stawidth;
		stanullfrac = ((Form_pg_statistic) GETSTRUCT(heaptupleStats[i]))->stanullfrac;
		colAvgWidth = colAvgWidth + (stawidth > 0 ? stawidth : 0) * relTuples[i];
		nullCount = nullCount + (stanullfrac > 0.0 ? stanullfrac : 0.0) * relTuples[i];

		AttStatsSlot hllSlot;

		(void) get_attstatsslot(&hllSlot, heaptupleStats[i], STATISTIC_KIND_FULLHLL,
								InvalidOid, ATTSTATSSLOT_VALUES);

		if (hllSlot.nvalues > 0)
		{
			hllcounters_fullscan[i] = (GpHLLCounter) DatumGetByteaP(hllSlot.values[0]);
			GpHLLCounter finalHLLFull_intermediate = finalHLLFull;
			finalHLLFull = gp_hyperloglog_merge_counters(finalHLLFull_intermediate, hllcounters_fullscan[i]);
			if (NULL != finalHLLFull_intermediate)
			{
				pfree(finalHLLFull_intermediate);
			}
			free_attstatsslot(&hllSlot);
			fullhll_count++;
			totalhll_count++;
		}

		(void) get_attstatsslot(&hllSlot, heaptupleStats[i], STATISTIC_KIND_HLL,
								InvalidOid, ATTSTATSSLOT_VALUES);

		if (hllSlot.nvalues > 0)
		{
			hllcounters[i] = (GpHLLCounter) DatumGetByteaP(hllSlot.values[0]);
			nDistincts[i] = (float) hllcounters[i]->ndistinct;
			nMultiples[i] = (float) hllcounters[i]->nmultiples;
			sampleCount += hllcounters[i]->samplerows;
			hllcounters_copy[i] = gp_hll_copy(hllcounters[i]);
			GpHLLCounter finalHLL_intermediate = finalHLL;
			finalHLL = gp_hyperloglog_merge_counters(finalHLL_intermediate, hllcounters[i]);
			if (NULL != finalHLL_intermediate)
			{
				pfree(finalHLL_intermediate);
			}
			free_attstatsslot(&hllSlot);
			samplehll_count++;
			totalhll_count++;

			// get the max ndistinct value from a single partition. If stadistinct < 0, it represents
			// the fraction of tuples that are distinct. We multiply this by the number of tuples
			// to get the number of distinct values to compare later on
			stadistinct = ((Form_pg_statistic) GETSTRUCT(heaptupleStats[i]))->stadistinct;
			if (stadistinct < 0)
				stadistinct = -1 * stadistinct * relTuples[i];
			if (stadistinct > max_part_distinct)
				max_part_distinct = stadistinct;
		}
		i++;
	}

	if (totalhll_count == 0)
	{
		/*
		 * If neither HLL nor HLL Full scan stats are available,
		 * continue merging stats based on the defaults, instead
		 * of reading them from HLL counter.
		 */
	}
	else
	{
		/*
		 * If all partitions have HLL full scan counters,
		 * merge root NDV's based on leaf partition HLL full scan
		 * counter
		 */
		if (fullhll_count == totalhll_count)
		{
			ndistinct = gp_hyperloglog_estimate(finalHLLFull);
			pfree(finalHLLFull);
			/*
			 * For fullscan the ndistinct is calculated based on the entire table scan
			 * so if it's within the marginal error, we consider everything as distinct,
			 * else the ndistinct value will provide the actual value and we do not ,
			 * need to do any additional calculation for the nmultiple
			 */
			if ((fabs(totalTuples - ndistinct) / (float) totalTuples) < GP_HLL_ERROR_MARGIN)
			{
				allDistinct = true;
			}
			nmultiple = ndistinct;
		}
		/*
		 * Else if all partitions have HLL counter based on sampled data,
		 * merge root NDV's based on leaf partition HLL counter on
		 * sampled data
		 */
		else if (finalHLL != NULL && samplehll_count == totalhll_count)
		{
			ndistinct = gp_hyperloglog_estimate(finalHLL);
			pfree(finalHLL);
			/*
			 * For sampled HLL counter, the ndistinct calculated is based on the
			 * sampled data. We consider everything distinct if the ndistinct
			 * calculated is within marginal error, else we need to calculate
			 * the number of distinct values for the table based on the estimator
			 * proposed by Haas and Stokes, used later in the code.
			 */
			if ((fabs(sampleCount - ndistinct) / (float) sampleCount) < GP_HLL_ERROR_MARGIN)
			{
				allDistinct = true;
			}
			else
			{
				/*
				 * The gp_hyperloglog_estimate() utility merges the number of
				 * distnct values accurately, but for the NDV estimator used later
				 * in the code, we also need additional information for nmultiples,
				 * i.e., the number of values that appeared more than once.
				 * At this point we have the information for nmultiples for each
				 * partition, but the nmultiples in one partition can be accounted as
				 * a distinct value in some other partition. In order to merge the
				 * approximate nmultiples better, we extract unique values in each
				 * partition as follows,
				 * P1 -> ndistinct1 , nmultiple1
				 * P2 -> ndistinct2 , nmultiple2
				 * P3 -> ndistinct3 , nmultiple3
				 * Root -> ndistinct(Root) (using gp_hyperloglog_estimate)
				 * nunique1 = ndistinct(Root) - gp_hyperloglog_estimate(P2 & P3)
				 * nunique2 = ndistinct(Root) - gp_hyperloglog_estimate(P1 & P3)
				 * nunique3 = ndistinct(Root) - gp_hyperloglog_estimate(P2 & P1)
				 * And finally once we have unique values in individual partitions,
				 * we can get the nmultiples on the ROOT as seen below,
				 * nmultiple(Root) = ndistinct(Root) - (sum of uniques in each partition)
				 */
				/*
				 * hllcounters_left array stores the merged hll result of all the
				 * hll counters towards the left of index i and excluding the hll
				 * counter at index i
				 */
				GpHLLCounter *hllcounters_left = (GpHLLCounter *) palloc0(numPartitions * sizeof(GpHLLCounter));

				/*
				 * hllcounters_right array stores the merged hll result of all the
				 * hll counters towards the right of index i and excluding the hll
				 * counter at index i
				 */
				GpHLLCounter *hllcounters_right = (GpHLLCounter *) palloc0(numPartitions * sizeof(GpHLLCounter));

				hllcounters_left[0] = gp_hyperloglog_init_def();
				hllcounters_right[numPartitions - 1] = gp_hyperloglog_init_def();

				/*
				 * The following loop populates the left and right array by accumulating the merged
				 * result of all the hll counters towards the left/right of the given index i excluding
				 * the counter at index i.
				 * Note that there might be empty values for some partitions, in which case the
				 * corresponding element in the left/right arrays will simply be the value
				 * of its neighbor.
				 * For E.g If the hllcounters_copy array is 1, null, 2, 3, null, 4
				 * the left and right arrays will be as follows:
				 * hllcounters_left:  default, 1, 1, (1,2), (1,2,3), (1,2,3)
				 * hllcounters_right: (2,3,4), (2,3,4), (3,4), 4, 4, default
				 */
				/*
				 * The first and the last element in the left and right arrays
				 * are default values since there is no element towards
				 * the left or right of them
				 */
				for (i = 1; i < numPartitions; i++)
				{
					/* populate left array */
					if (nDistincts[i - 1] == 0)
					{
						hllcounters_left[i] = gp_hll_copy(hllcounters_left[i - 1]);
					}
					else
					{
						GpHLLCounter hllcounter_temp1 = gp_hll_copy(hllcounters_copy[i - 1]);
						GpHLLCounter hllcounter_temp2 = gp_hll_copy(hllcounters_left[i - 1]);
						hllcounters_left[i] = gp_hyperloglog_merge_counters(hllcounter_temp1, hllcounter_temp2);
						pfree(hllcounter_temp1);
						pfree(hllcounter_temp2);
					}

					/* populate right array */
					if (nDistincts[numPartitions - i] == 0)
					{
						hllcounters_right[numPartitions - i - 1] = gp_hll_copy(hllcounters_right[numPartitions - i]);
					}
					else
					{
						GpHLLCounter hllcounter_temp1 = gp_hll_copy(hllcounters_copy[numPartitions - i]);
						GpHLLCounter hllcounter_temp2 = gp_hll_copy(hllcounters_right[numPartitions - i]);
						hllcounters_right[numPartitions - i - 1] = gp_hyperloglog_merge_counters(hllcounter_temp1, hllcounter_temp2);
						pfree(hllcounter_temp1);
						pfree(hllcounter_temp2);
					}
				}

				int nUnique = 0;
				for (i = 0; i < numPartitions; i++)
				{
					/* Skip if statistics are missing for the partition */
					if (nDistincts[i] == 0)
						continue;

					GpHLLCounter hllcounter_temp1 = gp_hll_copy(hllcounters_left[i]);
					GpHLLCounter hllcounter_temp2 = gp_hll_copy(hllcounters_right[i]);
					GpHLLCounter final = NULL;
					final = gp_hyperloglog_merge_counters(hllcounter_temp1, hllcounter_temp2);

					pfree(hllcounter_temp1);
					pfree(hllcounter_temp2);

					if (final != NULL)
					{
						float nUniques = ndistinct - gp_hyperloglog_estimate(final);
						nUnique += nUniques;
						nmultiple += nMultiples[i] * (nUniques / nDistincts[i]);
						pfree(final);
					}
					else
					{
						nUnique = ndistinct;
						break;
					}
				}

				// nmultiples for the ROOT
				nmultiple += ndistinct - nUnique;

				if (nmultiple < 0)
					nmultiple = 0;

				pfree(hllcounters_left);
				pfree(hllcounters_right);
			}
		}
		else
		{
			/* Else error out due to incompatible leaf HLL counter merge */
			pfree(hllcounters);
			pfree(hllcounters_fullscan);
			pfree(hllcounters_copy);
			pfree(nDistincts);
			pfree(nMultiples);

			ereport(ERROR,
					(errmsg("ANALYZE cannot merge since not all non-empty leaf partitions have consistent hyperloglog statistics for merge"),
					 errhint("Re-run ANALYZE or ANALYZE FULLSCAN")));
		}
	}
	pfree(hllcounters);
	pfree(hllcounters_fullscan);
	pfree(hllcounters_copy);
	pfree(nDistincts);
	pfree(nMultiples);

	if (allDistinct)
	{
		/* If we found no repeated values, assume it's a unique column */
		ndistinct = -1.0;
	}
	else if (!OidIsValid(eqopr) && !OidIsValid(ltopr))
	{
		/* If operators are not available, NDV is unknown. */
		ndistinct = 0;
	}
	else if ((int) nmultiple >= (int) ndistinct)
	{
		/*
		 * Every value in the sample appeared more than once.  Assume the
		 * column has just these values.
		 */
	}
	else
	{
		/*----------
		 * Estimate the number of distinct values using the estimator
		 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
		 *		n*d / (n - f1 + f1*n/N)
		 * where f1 is the number of distinct values that occurred
		 * exactly once in our sample of n rows (from a total of N),
		 * and d is the total number of distinct values in the sample.
		 * This is their Duj1 estimator; the other estimators they
		 * recommend are considerably more complex, and are numerically
		 * very unstable when n is much smaller than N.
		 *
		 * Overwidth values are assumed to have been distinct.
		 *----------
		 */
		int f1 = ndistinct - nmultiple;
		int d = f1 + nmultiple;
		double numer, denom, stadistinct;

		numer = (double) sampleCount * (double) d;

		denom = (double) (sampleCount - f1) +
				(double) f1 * (double) sampleCount / totalTuples;

		stadistinct = numer / denom;
		/* Clamp to sane range in case of roundoff error */
		if (stadistinct < (double) d)
			stadistinct = (double) d;
		if (stadistinct > totalTuples)
			stadistinct = totalTuples;
		ndistinct = floor(stadistinct + 0.5);

		// if there's data skew, the estimated ndistinct value may be heavily underestimated
		// thus ensure partitioned table's ndistinct is at least the maximum ndistinct value among its partitions
		if (max_part_distinct > ndistinct)
			ndistinct = max_part_distinct;
	}

	ndistinct = round(ndistinct);
	if (ndistinct > 0.1 * totalTuples)
		ndistinct = -(ndistinct / totalTuples);

	// finalize NDV calculation
	stats->stadistinct = ndistinct;
	stats->stats_valid = true;
	stats->stawidth = colAvgWidth / totalTuples;
	stats->stanullfrac = (float4) nullCount / (float4) totalTuples;

	// MCV calculations
	MCVFreqPair **mcvpairArray = NULL;
	int rem_mcv = 0;
	int num_mcv = 0;
	if (ndistinct > -1 && OidIsValid(eqopr))
	{
		if (ndistinct < 0)
		{
			ndistinct = -ndistinct * totalTuples;
		}

		old_context = MemoryContextSwitchTo(stats->anl_context);

		void *resultMCV[2];

		mcvpairArray = aggregate_leaf_partition_MCVs(
			stats->attr->attrelid, stats->attr->attnum,
			numPartitions, heaptupleStats, relTuples,
			stats->attr->attstattarget, ndistinct, &num_mcv, &rem_mcv,
			resultMCV);
		MemoryContextSwitchTo(old_context);

		if (num_mcv > 0)
		{
			stats->stakind[slot_idx] = STATISTIC_KIND_MCV;
			stats->staop[slot_idx] = mystats->eqopr;
			stats->stavalues[slot_idx] = (Datum *) resultMCV[0];
			stats->numvalues[slot_idx] = num_mcv;
			stats->stanumbers[slot_idx] = (float4 *) resultMCV[1];
			stats->numnumbers[slot_idx] = num_mcv;
			slot_idx++;
		}
	}

	// Histogram calculation
	if (OidIsValid(eqopr) && OidIsValid(ltopr))
	{
		old_context = MemoryContextSwitchTo(stats->anl_context);

		void *resultHistogram[1];
		int num_hist = aggregate_leaf_partition_histograms(
			stats->attr->attrelid, stats->attr->attnum,
			numPartitions, heaptupleStats, relTuples,
			stats->attr->attstattarget, mcvpairArray + num_mcv,
			rem_mcv, resultHistogram);
		MemoryContextSwitchTo(old_context);
		if (num_hist > 0)
		{
			stats->stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
			stats->staop[slot_idx] = mystats->ltopr;
			stats->stavalues[slot_idx] = (Datum *) resultHistogram[0];
			stats->numvalues[slot_idx] = num_hist;
			slot_idx++;
		}
	}
	for (i = 0; i < numPartitions; i++)
	{
		if (HeapTupleIsValid(heaptupleStats[i]))
			heap_freetuple(heaptupleStats[i]);
	}
	if (num_mcv > 0)
		pfree(mcvpairArray);
	pfree(heaptupleStats);
	pfree(relTuples);
}

/*
 * qsort_arg comparator for sorting ScalarItems
 *
 * Aside from sorting the items, we update the tupnoLink[] array
 * whenever two ScalarItems are found to contain equal datums.  The array
 * is indexed by tupno; for each ScalarItem, it contains the highest
 * tupno that that item's datum has been found to be equal to.  This allows
 * us to avoid additional comparisons in compute_scalar_stats().
 */
static int
compare_scalars(const void *a, const void *b, void *arg)
{
	Datum		da = ((const ScalarItem *) a)->value;
	int			ta = ((const ScalarItem *) a)->tupno;
	Datum		db = ((const ScalarItem *) b)->value;
	int			tb = ((const ScalarItem *) b)->tupno;
	CompareScalarsContext *cxt = (CompareScalarsContext *) arg;
	int			compare;

	compare = ApplySortComparator(da, false, db, false, cxt->ssup);
	if (compare != 0)
		return compare;

	/*
	 * The two datums are equal, so update cxt->tupnoLink[].
	 */
	if (cxt->tupnoLink[ta] < tb)
		cxt->tupnoLink[ta] = tb;
	if (cxt->tupnoLink[tb] < ta)
		cxt->tupnoLink[tb] = ta;

	/*
	 * For equal datums, sort by tupno
	 */
	return ta - tb;
}

/*
 * qsort comparator for sorting ScalarMCVItems by position
 */
static int
compare_mcvs(const void *a, const void *b)
{
	int			da = ((const ScalarMCVItem *) a)->first;
	int			db = ((const ScalarMCVItem *) b)->first;

	return da - db;
}

/*
 * Analyze the list of common values in the sample and decide how many are
 * worth storing in the table's MCV list.
 *
 * mcv_counts is assumed to be a list of the counts of the most common values
 * seen in the sample, starting with the most common.  The return value is the
 * number that are significantly more common than the values not in the list,
 * and which are therefore deemed worth storing in the table's MCV list.
 */
static int
analyze_mcv_list(int *mcv_counts,
				 int num_mcv,
				 double stadistinct,
				 double stanullfrac,
				 int samplerows,
				 double totalrows)
{
	double		ndistinct_table;
	double		sumcount;
	int			i;

	/*
	 * If the entire table was sampled, keep the whole list.  This also
	 * protects us against division by zero in the code below.
	 */
	if (samplerows == totalrows || totalrows <= 1.0)
		return num_mcv;

	/* Re-extract the estimated number of distinct nonnull values in table */
	ndistinct_table = stadistinct;
	if (ndistinct_table < 0)
		ndistinct_table = -ndistinct_table * totalrows;

	/*
	 * Exclude the least common values from the MCV list, if they are not
	 * significantly more common than the estimated selectivity they would
	 * have if they weren't in the list.  All non-MCV values are assumed to be
	 * equally common, after taking into account the frequencies of all the
	 * values in the MCV list and the number of nulls (c.f. eqsel()).
	 *
	 * Here sumcount tracks the total count of all but the last (least common)
	 * value in the MCV list, allowing us to determine the effect of excluding
	 * that value from the list.
	 *
	 * Note that we deliberately do this by removing values from the full
	 * list, rather than starting with an empty list and adding values,
	 * because the latter approach can fail to add any values if all the most
	 * common values have around the same frequency and make up the majority
	 * of the table, so that the overall average frequency of all values is
	 * roughly the same as that of the common values.  This would lead to any
	 * uncommon values being significantly overestimated.
	 */
	sumcount = 0.0;
	for (i = 0; i < num_mcv - 1; i++)
		sumcount += mcv_counts[i];

	while (num_mcv > 0)
	{
		double		selec,
					otherdistinct,
					N,
					n,
					K,
					variance,
					stddev;

		/*
		 * Estimated selectivity the least common value would have if it
		 * wasn't in the MCV list (c.f. eqsel()).
		 */
		selec = 1.0 - sumcount / samplerows - stanullfrac;
		if (selec < 0.0)
			selec = 0.0;
		if (selec > 1.0)
			selec = 1.0;
		otherdistinct = ndistinct_table - (num_mcv - 1);
		if (otherdistinct > 1)
			selec /= otherdistinct;

		/*
		 * If the value is kept in the MCV list, its population frequency is
		 * assumed to equal its sample frequency.  We use the lower end of a
		 * textbook continuity-corrected Wald-type confidence interval to
		 * determine if that is significantly more common than the non-MCV
		 * frequency --- specifically we assume the population frequency is
		 * highly likely to be within around 2 standard errors of the sample
		 * frequency, which equates to an interval of 2 standard deviations
		 * either side of the sample count, plus an additional 0.5 for the
		 * continuity correction.  Since we are sampling without replacement,
		 * this is a hypergeometric distribution.
		 *
		 * XXX: Empirically, this approach seems to work quite well, but it
		 * may be worth considering more advanced techniques for estimating
		 * the confidence interval of the hypergeometric distribution.
		 */
		N = totalrows;
		n = samplerows;
		K = N * mcv_counts[num_mcv - 1] / n;
		variance = n * K * (N - K) * (N - n) / (N * N * (N - 1));
		stddev = sqrt(variance);

		if (mcv_counts[num_mcv - 1] > selec * samplerows + 2 * stddev + 0.5)
		{
			/*
			 * The value is significantly more common than the non-MCV
			 * selectivity would suggest.  Keep it, and all the other more
			 * common values in the list.
			 */
			break;
		}
		else
		{
			/* Discard this value and consider the next least common value */
			num_mcv--;
			if (num_mcv == 0)
				break;
			sumcount -= mcv_counts[num_mcv - 1];
		}
	}
	return num_mcv;
}

/*
 * calculate correlations use the weighted mean algorithm.
 *
 * the formula for calculating the weighted mean is:
 * sum(correlationOnSeg[i] * (totalRowsOnSeg[i] / totalRows))
 * i is from 0 to N. N is the number of segments.
 */
static void
calculate_correlation_use_weighted_mean(CdbPgResults *cdb_pgresults,
												Relation onerel,
												TupleDesc tupleDesc,
												float4 *correlations,
												bool *correlationsIsNull,
												int live_natts)
{
	TupleDesc relDesc = RelationGetDescr(onerel);
	int		attNum = relDesc->natts;
	int		segmentNum = cdb_pgresults->numResults;

	/*
	 * totalRowsOnSeg, correlationOnSeg and correlationIsNullOnSeg are One-Dimensional arrays
	 * to store infos of the following results.
	 *
	 *     |    seg0  |  seg1  |  seg2
	 * --------------------------------
	 * att0｜   0*3+0 |  0*3+1 |  0*3+2
	 * att1｜   1*3+0 |  1*3+1 |  1*3+2
	 * att2｜   2*3+0 |  2*3+1 |  2*3+2
	 *
	 * We can use attno * segmentNum + segno as index to access the
	 * info of column attno on segment segno.
	 * index from attno * segmentNum to attno * segmentNum + segmentNum -1 store infos
	 * of column attno.
	 */
	int 	*totalRowsOnSeg = (int *)palloc0(sizeof(int) * segmentNum * attNum);
	float4  *correlationOnSeg = (float4 *)palloc0(sizeof(float4) * segmentNum * attNum);
	bool	*correlationIsNullOnSeg = (bool *)palloc0(sizeof(bool) * segmentNum * attNum);
	int		*totalRows = (int *)palloc0(sizeof(int) * attNum);
	char **funcRetValues = (char **) palloc0(tupleDesc->natts * sizeof(char *));
	bool *funcRetNulls = (bool *) palloc(tupleDesc->natts * sizeof(bool));

	for (int segno = 0; segno < segmentNum; segno++)
	{
		int			rows;
		float4		correlationValue;
		int			attno;
		struct pg_result *pgresult = cdb_pgresults->pg_results[segno];
		int			index;

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}
		/*
		 * gp_acquire_correlations returns a result for each alive columns.
		 */
		rows = PQntuples(pgresult);
		if (rows != live_natts || PQnfields(pgresult) != 1)
		{
			cdbdisp_clearCdbPgResults(cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							rows, PQnfields(pgresult))));
		}
		for (int j = 0; j < rows; j++)
		{
			char *rowStr = PQgetvalue(pgresult, j, 0);
			if (rowStr == NULL)
				elog(ERROR, "got NULL pointer from return value of gp_acquire_correlations");

			parse_record_to_string(rowStr, tupleDesc, funcRetValues, funcRetNulls);

			/* get the first column: attno, and it is not NULL */
			attno = DatumGetInt32(DirectFunctionCall1(int4in,
										CStringGetDatum(funcRetValues[0])));
			index = attno * segmentNum + segno;
			correlationIsNullOnSeg[index] = true;
			/* get the second column: correlation, and it maybe NULl */
			if (!funcRetNulls[1])
			{
				correlationValue = DatumGetFloat4(DirectFunctionCall1(float4in,
										CStringGetDatum(funcRetValues[1])));
				correlationsIsNull[attno] = false;

				correlationOnSeg[index] = correlationValue;
				correlationIsNullOnSeg[index] = false;
			}
			/* get the third column: totalrow on segment i, this is not NULL.
			 * if correlation for this column is NULL, the totalrow is 0.
			 */
			totalRowsOnSeg[index] = DatumGetInt32(DirectFunctionCall1(int4in,
								CStringGetDatum(funcRetValues[2])));
			totalRows[attno] += totalRowsOnSeg[index];
		}
	}

	/*
	 * Calculate overall correlation from correlation on each segments
	 * we use weighted average algorithm to calculate correlation to
	 * better handle skewed data between segments.
	 */
	for (int attno = 0; attno < attNum; attno++)
	{
		Form_pg_attribute relatt = TupleDescAttr(relDesc, attno);

		if (relatt->attisdropped)
			continue;
		float4 weight;
		for (int segno = 0; segno < segmentNum; segno++)
		{
			int index = attno * segmentNum + segno;
			if (!correlationIsNullOnSeg[index])
			{
				weight = (float4)totalRowsOnSeg[index] / totalRows[attno];
				correlations[attno] += correlationOnSeg[index] * weight;
			}
		}
	}

	pfree(totalRowsOnSeg);
	pfree(correlationOnSeg);
	pfree(correlationIsNullOnSeg);
	pfree(totalRows);
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		if (funcRetValues[i])
			pfree(funcRetValues[i]);
	}
	pfree(funcRetValues);
	pfree(funcRetNulls);
}

/*
 * calculate correlations use the mean algorithm.
 *
 * In some situations, we may not be able to obtain reltuples of a table,
 * such as none-leaf part of partitioned table or the parent table of the
 * inherited table. So we can not use the weighted mean algorithm.
 *
 * the formula for calculating the mean is:
 * sum(correlationOnSeg) / count(corrNotNullSeg)
 */
static void
calculate_correlation_use_mean(CdbPgResults *cdb_pgresults,
										Relation onerel,
										TupleDesc tupleDesc,
										float4 *correlations,
										bool *correlationsIsNull,
										int live_natts)
{
	TupleDesc relDesc = RelationGetDescr(onerel);
	int		attNum = relDesc->natts;
	int		segmentNum = cdb_pgresults->numResults;
	/*
	 * corrNotNullSegNum[attno] stores the number of segments for which
	 * the correlation of column attno is not null.
	 */
	int 	*corrNotNullSegNum = (int *)palloc0(sizeof(int) * attNum);
	char **funcRetValues = (char **) palloc0(tupleDesc->natts * sizeof(char *));
	bool *funcRetNulls = (bool *) palloc(tupleDesc->natts * sizeof(bool));

	for (int segno = 0; segno < segmentNum; segno++)
	{
		int			ntuples;
		float4		correlationValue;
		int			attno;
		struct pg_result *pgresult = cdb_pgresults->pg_results[segno];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							PQresultStatus(pgresult))));
		}
		/*
		 * gp_acquire_correlations returns a result for each alive columns.
		 */
		ntuples = PQntuples(pgresult);
		if (ntuples != live_natts || PQnfields(pgresult) != 1)
		{
			cdbdisp_clearCdbPgResults(cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							ntuples, PQnfields(pgresult))));
		}
		for (int j = 0; j < ntuples; j++)
		{
			char *rowStr = PQgetvalue(pgresult, j, 0);
			if (rowStr == NULL)
				elog(ERROR, "got NULL pointer from return value of gp_acquire_correlations");

			parse_record_to_string(rowStr, tupleDesc, funcRetValues, funcRetNulls);
			/* get the first column: attno, and it is not NULL */
			attno = DatumGetInt32(DirectFunctionCall1(int4in,
										CStringGetDatum(funcRetValues[0])));
			/* get the second column: correlation, and it maybe NULl */
			if (!funcRetNulls[1])
			{
				correlationValue = DatumGetFloat4(DirectFunctionCall1(float4in,
										CStringGetDatum(funcRetValues[1])));
				correlationsIsNull[attno] = false;
				correlations[attno] += correlationValue;
				corrNotNullSegNum[attno]++;
			}
		}
	}

	/*
	 * Calculate overall correlation from correlation on each segments
	 * we use the mean algorithm to calculate correlation.
	 */
	for (int attno = 0; attno < attNum; attno++)
	{
		Form_pg_attribute relatt = TupleDescAttr(relDesc, attno);

		if (relatt->attisdropped)
			continue;
		if (!correlationsIsNull[attno])
		{
			correlations[attno] = correlations[attno] / corrNotNullSegNum[attno];
		}
	}

	pfree(corrNotNullSegNum);
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		if (funcRetValues[i])
			pfree(funcRetValues[i]);
	}
	pfree(funcRetValues);
	pfree(funcRetNulls);
}

/*
 * We cannot use the same method as PostgreSQL does to calculate the
 * correlation in QD. When we collect data from segments to QD, this
 * will change the physical order of the data. such as in segment 1,
 * the data is 1,3,5,7,9. And in segment 2, the data is 2,4,6,8,10.
 * In each segment the data is ordered, and correlation is 1 in each segment.
 * But after we collect the data to QD, it may be 1,3,5,2,4,7,9,6,8,10.
 * And the correlation is 0.3 or something else and it is not stable.
 * And this will increase the cost of index scan which is shouldn't be done.
 *
 * So get correlations from segments and summarize them.
 */
static void
gp_acquire_correlations_dispatcher(Oid relOid, bool inh, float4 *correlations, bool *correlationsIsNull)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	char       *sql;
	Relation onerel;
	int attNum;
	int live_natts;
	TupleDesc relDesc;
	/*
	 * For child table of inherited tables and leaf table of partitioned table,
	 * We use the weighted mean average to calculate the correlation collected
	 * from the segments.
	 * the formula for calculating the weighted mean is:
	 * sum(correlationOnSeg[i] * (totalRowsOnSeg[i] / totalRows))
	 * i is from 0 to N. N is the number of segments.
	 *
	 * However, since reltuples of none-leaf part of partitioned table and the
	 * parent table of the inherited table is 0, we can only use the average
	 * to calculate correlation.
	 */
	bool useWeightedMean = !inh;
	sql = psprintf("select pg_catalog.gp_acquire_correlations(%d, '%s');", relOid, inh ? "t" : "f");

	Assert(Gp_role == GP_ROLE_DISPATCH);
	onerel = table_open(relOid, AccessShareLock);
	relDesc = RelationGetDescr(onerel);
	attNum = relDesc->natts;

	/* dispatch sql to segments  */
	CdbDispatchCommand(sql, DF_WITH_SNAPSHOT, &cdb_pgresults);

	/* Count the number of non-dropped cols */
	live_natts = 0;
	for (int attno = 0; attno < attNum; attno++)
	{
		Form_pg_attribute relatt = TupleDescAttr(relDesc, attno);
		correlationsIsNull[attno] = true;

		if (relatt->attisdropped)
			continue;
		live_natts++;
	}

	/* construct tupleDesc for RECORD results of gp_acquire_correlations */
	TupleDesc tupleDesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupleDesc,
						1,
						"attnum",
						INT4OID,
						-1,
						0);
	TupleDescInitEntry(tupleDesc,
						2,
						"correlation",
						FLOAT4OID,
						-1,
						0);
	TupleDescInitEntry(tupleDesc,
						3,
						"totalrows",
						INT4OID,
						-1,
						0);

	/* For RECORD results, make sure a typmod has been assigned */
	Assert(tupleDesc->tdtypeid == RECORDOID && tupleDesc->tdtypmod < 0);
	assign_record_type_typmod(tupleDesc);

	/* calculate correlations by cdb_pgresults */
	if (useWeightedMean)
		calculate_correlation_use_weighted_mean(&cdb_pgresults,
												onerel,
												tupleDesc,
												correlations,
												correlationsIsNull,
												live_natts);
	else
		calculate_correlation_use_mean(&cdb_pgresults,
										onerel,
										tupleDesc,
										correlations,
										correlationsIsNull,
										live_natts);

	cdbdisp_clearCdbPgResults(&cdb_pgresults);
	table_close(onerel, AccessShareLock);
}
