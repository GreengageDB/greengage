#include "postgres.h"


#include "access/genam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_db_role_setting.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/relcache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "arenadata_toolkit_guc.h"
#include "arenadata_toolkit_worker.h"
#include "drops_track.h"
#include "dbsize.h"
#include "file_hook.h"
#include "tf_shmem.h"
#include "track_files.h"

PG_FUNCTION_INFO_V1(tracking_register_db);
PG_FUNCTION_INFO_V1(tracking_unregister_db);
PG_FUNCTION_INFO_V1(tracking_set_snapshot_on_recovery);
PG_FUNCTION_INFO_V1(tracking_register_schema);
PG_FUNCTION_INFO_V1(tracking_unregister_schema);
PG_FUNCTION_INFO_V1(tracking_set_relkinds);
PG_FUNCTION_INFO_V1(tracking_set_relstorages);
PG_FUNCTION_INFO_V1(tracking_is_segment_initialized);
PG_FUNCTION_INFO_V1(tracking_trigger_initial_snapshot);
PG_FUNCTION_INFO_V1(tracking_is_initial_snapshot_triggered);
PG_FUNCTION_INFO_V1(tracking_get_track);
PG_FUNCTION_INFO_V1(tracking_track_version);

/*
 * Tuple description for result of tracking_get_track function.
 */
#define GET_TRACK_TUPDESC_LEN 9
#define Anum_track_relid ((AttrNumber) 0)
#define Anum_track_name ((AttrNumber) 1)
#define Anum_track_relfilenode ((AttrNumber) 2)
#define Anum_track_size ((AttrNumber) 3)
#define Anum_track_state ((AttrNumber) 4)
#define Anum_track_gp_segment_id ((AttrNumber) 5)
#define Anum_track_gp_segment_relnamespace ((AttrNumber) 6)
#define Anum_track_gp_segment_relkind ((AttrNumber) 7)
#define Anum_track_gp_segment_relstorage ((AttrNumber) 8)

/*
 * Macros for string constants, which are used during work with GUCs
 */
#define TRACKING_SCHEMAS_PREFIX "arenadata_toolkit.tracking_schemas="
#define TRACKING_RELSTORAGES_PREFIX "arenadata_toolkit.tracking_relstorages="
#define TRACKING_RELKINDS_PREFIX "arenadata_toolkit.tracking_relkinds="

/* Preserved state among the calls of tracking_get_track */
typedef struct
{
	Relation	pg_class_rel;	/* pg_class relation */
	SysScanDesc scan;			/* for scans of system table */
}	tf_main_func_state_t;

/*
 * Main state during tracking_get_track_main call. Stores
 * copy of shared Bloom and tracking filtering parameters.
 */
typedef struct
{
	bloom_t    *bloom;			/* local copy of shared bloom */

	List	   *drops;			/* drop list for current db */
	ListCell   *next_drop;
	uint64		relkinds;		/* tracking relkinds */
	uint64		relstorages;	/* tracking relstorages */
	List	   *schema_oids;	/* tracking schemas */
}	tf_get_global_state_t;

static tf_get_global_state_t tf_get_global_state = {0};

static bool callbackRegistered = false;
static uint32 CurrentVersion = InvalidVersion;

static bool isExecutorExplainMode = false;
ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
ExecutorEnd_hook_type next_ExecutorEnd_hook = NULL;

static inline void
tf_check_shmem_error(void)
{
	if (tf_shared_state == NULL)
		ereport(ERROR,
				(errmsg("Failed to access shared memory due to wrong extension initialization"),
				 errhint("Load extension's code through shared_preload_library configuration")));
}

static inline Oid
get_dbid(Oid dbid)
{
	return (dbid == InvalidOid) ? MyDatabaseId : dbid;
}

static uint32
track_bump_version(uint32 ver)
{
	ver++;
	if (ver == InvalidVersion || ver == ControlVersion)
		return StartVersion;

	return ver;
}

/*
 * If transaction called tracking_track_version commits, we
 * can bump the track version, what leads to consistency with
 * state on segments. In case of abort version on master differs from
 * segment's and during track acquisition the previous
 * filter is used on segments.
 */
static void
xact_end_version_callback(XactEvent event, void *arg)
{
	bloom_op_ctx_t ctx = bloom_set_get_entry(MyDatabaseId, LW_SHARED, LW_EXCLUSIVE);

	if (ctx.entry)
	{
		if (event == XACT_EVENT_COMMIT)
			ctx.entry->master_version = track_bump_version(ctx.entry->master_version);
		pg_atomic_clear_flag(&ctx.entry->capture_in_progress);
	}

	bloom_set_release(&ctx);

	callbackRegistered = false;
	CurrentVersion = InvalidVersion;
	isExecutorExplainMode = false;
}

static void
xact_end_track_callback(XactEvent event, void *arg)
{
	tf_get_global_state.bloom = NULL;
	tf_get_global_state.drops = NIL;
	tf_get_global_state.next_drop = NULL;
	tf_get_global_state.relkinds = 0;
	tf_get_global_state.relstorages = 0;
	tf_get_global_state.schema_oids = NIL;
}

static List *
split_string_to_list(const char *input)
{
	List	   *result = NIL;
	char	   *input_copy;
	char	   *token;

	if (input == NULL)
		return NIL;

	input_copy = pstrdup(input);

	token = strtok(input_copy, ",");

	while (token != NULL)
	{
		if (*token != '\0')
		{
			result = lappend(result, pstrdup(token));
		}

		token = strtok(NULL, ",");
	}

	pfree(input_copy);

	return result;
}

/*
 * Tracked relkinds and relstorage types
 * are coded into 64 bits via ascii offsets.
 */
static uint64
list_to_bits(const char *input)
{
	char	   *input_copy;
	char	   *token;
	uint64		bits = 0;

	if (input == NULL)
		return 0;

	input_copy = pstrdup(input);

	token = strtok(input_copy, ",");

	while (token != NULL)
	{
		if (*token != '\0')
			bits |= (1ULL << (*token - 'A'));

		token = strtok(NULL, ",");
	}

	pfree(input_copy);

	return bits;
}

static void
get_filters_from_guc()
{
	Relation	rel;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	char	   *current_schemas = NULL;
	char	   *current_relkinds = NULL;
	char	   *current_relstorages = NULL;
	List	   *schema_names = NIL;
	ListCell   *lc;

	rel = heap_open(DbRoleSettingRelationId, RowExclusiveLock);
	ScanKeyInit(&skey[0],
				Anum_pg_db_role_setting_setdatabase,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(MyDatabaseId));

	/*
	 * Lookup for not role specific configuration
	 */
	ScanKeyInit(&skey[1],
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true, NULL, 2, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		str_datum;

		str_datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
								 RelationGetDescr(rel), &isnull);
		if (!isnull)
		{
			ArrayType  *array;
			Datum	   *elems = NULL;
			bool	   *nulls = NULL;
			int			nelems;

			array = DatumGetArrayTypeP(str_datum);
			deconstruct_array(array, TEXTOID, -1, false, 'i',
							  &elems, &nulls, &nelems);
			for (int i = 0; i < nelems; i++)
			{
				if (nulls[i])
					continue;

				char	   *str = TextDatumGetCString(elems[i]);

				if (strncmp(str,
							TRACKING_SCHEMAS_PREFIX,
							sizeof(TRACKING_SCHEMAS_PREFIX) - 1) == 0)
				{
					current_schemas = pstrdup(str + sizeof(TRACKING_SCHEMAS_PREFIX) - 1);
				}
				else if (strncmp(str,
								 TRACKING_RELSTORAGES_PREFIX,
								 sizeof(TRACKING_RELSTORAGES_PREFIX) - 1) == 0)
				{
					current_relstorages = pstrdup(str + sizeof(TRACKING_RELSTORAGES_PREFIX) - 1);
				}
				else if (strncmp(str,
								 TRACKING_RELKINDS_PREFIX,
								 sizeof(TRACKING_RELKINDS_PREFIX) - 1) == 0)
				{
					current_relkinds = pstrdup(str + sizeof(TRACKING_RELKINDS_PREFIX) - 1);
				}

				pfree(str);
			}

			if (elems)
				pfree(elems);
			if (nulls)
				pfree(nulls);
		}
	}
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	if (current_schemas)
		schema_names = split_string_to_list(current_schemas);
	else
		schema_names = split_string_to_list(DEFAULT_TRACKED_SCHEMAS);
	if (current_relstorages)
		tf_get_global_state.relstorages = list_to_bits(current_relstorages);
	else
		tf_get_global_state.relstorages = list_to_bits(DEFAULT_TRACKED_REL_STORAGES);
	if (current_relkinds)
		tf_get_global_state.relkinds = list_to_bits(current_relkinds);
	else
		tf_get_global_state.relkinds = list_to_bits(DEFAULT_TRACKED_REL_KINDS);

	foreach(lc, schema_names)
	{
		Oid			nspOid;
		char	   *name = (char *) lfirst(lc);

		nspOid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum(name));

		if (!OidIsValid(nspOid))
		{
			elog(DEBUG1, "[tracking_get_track] schema \"%s\" does not exist", name);
			continue;
		}

		tf_get_global_state.schema_oids = lappend_oid(tf_get_global_state.schema_oids, nspOid);
	}

	if (schema_names)
		pfree(schema_names);
}


static bool
schema_is_tracked(Oid schema)
{
	ListCell   *lc;

	if (tf_get_global_state.schema_oids == NIL)
		return false;

	foreach(lc, tf_get_global_state.schema_oids)
	{
		Oid			tracked_schema = lfirst_oid(lc);

		if (tracked_schema == schema)
			return true;
	}

	return false;
}

static bool
kind_is_tracked(char type, uint64 allowed_kinds)
{
	return (allowed_kinds & (1ULL << (type - 'A'))) != 0;
}

/*
 * Main function for relation size track acquisition.
 */
Datum
tracking_get_track(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	tf_main_func_state_t *state;
	HeapTuple	result;
	Datum		datums[GET_TRACK_TUPDESC_LEN];
	bool		nulls[GET_TRACK_TUPDESC_LEN] = {0};
	uint32		version = PG_GETARG_INT64(0);

	tf_check_shmem_error();

	if (version == InvalidVersion)
		ereport(ERROR,
				(errmsg("Can't perform tracking for database %u properly due to internal error", MyDatabaseId)));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();

		RegisterXactCallbackOnce(xact_end_track_callback, NULL);

		oldcontext = MemoryContextSwitchTo(CurTransactionContext);

		bloom_op_ctx_t bloom_ctx = bloom_set_get_entry(MyDatabaseId, LW_SHARED, LW_EXCLUSIVE);

		if (bloom_ctx.entry == NULL)
		{
			bloom_set_release(&bloom_ctx);
			ereport(ERROR,
					(errcode(ERRCODE_GP_COMMAND_ERROR),
					 errmsg("database %u is not tracked", MyDatabaseId),
					 errhint("Call 'arenadata_toolkit.tracking_register_db()'"
							 "to enable tracking")));
		}

		/*
		 * If current bloom's version differs from incoming, we suppose that
		 * the rollback of previous track acquisition have occured. In this
		 * situation we merge previous filter to current active filter.
		 *
		 * If the ControlVersion comes, it means that track is acquired
		 * several times in the same transaction. And the same filter is used
		 * in this situation.
		 */
		if (version != ControlVersion && version != bloom_ctx.entry->work_version)
		{
			bloom_merge_internal(&bloom_ctx.entry->bloom);
		}

		/*
		 * This block handles 2 scenarios:
		 * 1. First track acquisition in transaction:
		 *  - Copy current active bloom filter to local array.
		 *  - Switch active bloom filter to preserve the state, which has just
		 *    been copied.
		 *  - Clear active filter.
		 *  - Increment current version.
		 * 2. Subsequent track acquisition in same transaction (ControlVersion)
		 *  - Temporarily switch to previous filter state
		 *  - Copy switched bloom filter to local array
		 *  - Switch back to active filter
		 *  - Keep existing current version
		 */
		if (tf_get_global_state.bloom == NULL)
		{
			tf_get_global_state.bloom = palloc(full_bloom_size(bloom_size));
			bloom_init(bloom_size, tf_get_global_state.bloom);

			if (version == ControlVersion)
			{
				bloom_switch_current(&bloom_ctx.entry->bloom);
			}

			bloom_copy(tf_get_global_state.bloom, &bloom_ctx.entry->bloom);
			bloom_switch_current(&bloom_ctx.entry->bloom);

			if (version != ControlVersion)
			{
				bloom_clear(&bloom_ctx.entry->bloom);
				bloom_ctx.entry->work_version = track_bump_version(version);
			}
		}

		bloom_set_release(&bloom_ctx);

		/* initial snapshot shouldn't return drops */
		if (!tf_get_global_state.bloom->is_set_all)
		{
			tf_get_global_state.drops = drops_track_move(MyDatabaseId);
			tf_get_global_state.next_drop = list_head(tf_get_global_state.drops);
		}

		/*
		 * Let's retrieve tracking information.
		 */
		get_filters_from_guc();

		/* emit warning only at coordinator */
		if ((tf_get_global_state.relstorages == 0 ||
			tf_get_global_state.relkinds == 0 ||
			tf_get_global_state.schema_oids == NIL) &&
			IS_QUERY_DISPATCHER())
			ereport(WARNING,
					(errmsg("One of the tracking parameters (schemas,"
							"relkinds, relstorages) for database %u is empty.", MyDatabaseId)));

		MemoryContextSwitchTo(oldcontext);

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		funcctx->tuple_desc = CreateTemplateTupleDesc(GET_TRACK_TUPDESC_LEN, false);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_relid + 1, "relid", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_name + 1, "name", NAMEOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_relfilenode + 1, "relfilenode", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_size + 1, "size", INT8OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_state + 1, "state", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_gp_segment_id + 1, "gp_segment_id", INT4OID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_gp_segment_relnamespace + 1, "relnamespace", OIDOID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_gp_segment_relkind + 1, "relkind", CHAROID, -1, 0);
		TupleDescInitEntry(funcctx->tuple_desc, Anum_track_gp_segment_relstorage + 1, "relstorage", CHAROID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

		state = (tf_main_func_state_t *) palloc0(sizeof(tf_main_func_state_t));
		funcctx->user_fctx = (void *) state;

		state->pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
		state->scan = systable_beginscan(state->pg_class_rel, InvalidOid, false, NULL, 0, NULL);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = funcctx->user_fctx;

	while (true)
	{
		Oid			filenode;
		Oid			relnamespace;
		char		relkind;
		char		relstorage;
		HeapTuple	pg_class_tuple;
		Form_pg_class relp;
		int64		size;

		if (!state->scan)
			break;

		pg_class_tuple = systable_getnext(state->scan);

		if (!HeapTupleIsValid(pg_class_tuple))
		{
			systable_endscan(state->scan);
			heap_close(state->pg_class_rel, AccessShareLock);
			state->scan = NULL;
			state->pg_class_rel = NULL;
			break;
		}

		datums[Anum_track_gp_segment_relkind] = heap_getattr(pg_class_tuple,
															 Anum_pg_class_relkind,
															 RelationGetDescr(state->pg_class_rel),
															 &nulls[Anum_track_gp_segment_relkind]);
		relkind = DatumGetChar(datums[Anum_track_gp_segment_relkind]);

		if (!kind_is_tracked(relkind, tf_get_global_state.relkinds))
			continue;

		datums[Anum_track_gp_segment_relstorage] = heap_getattr(pg_class_tuple,
																Anum_pg_class_relstorage,
																RelationGetDescr(state->pg_class_rel),
																&nulls[Anum_track_gp_segment_relstorage]);
		relstorage = DatumGetChar(datums[Anum_track_gp_segment_relstorage]);

		if (!kind_is_tracked(relstorage, tf_get_global_state.relstorages))
			continue;

		datums[Anum_track_gp_segment_relnamespace] = heap_getattr(pg_class_tuple,
																  Anum_pg_class_relnamespace,
																  RelationGetDescr(state->pg_class_rel),
																  &nulls[Anum_track_gp_segment_relnamespace]);
		relnamespace = DatumGetObjectId(datums[Anum_track_gp_segment_relnamespace]);

		if (!schema_is_tracked(relnamespace))
			continue;

		datums[Anum_track_relid] = ObjectIdGetDatum(HeapTupleGetOid(pg_class_tuple));

		datums[Anum_track_name] = heap_getattr(pg_class_tuple,
											   Anum_pg_class_relname,
											   RelationGetDescr(state->pg_class_rel),
											   &nulls[Anum_track_name]);

		datums[Anum_track_relfilenode] = heap_getattr(pg_class_tuple,
													  Anum_pg_class_relfilenode,
													  RelationGetDescr(state->pg_class_rel),
													  &nulls[Anum_track_relfilenode]);
		filenode = DatumGetObjectId(datums[Anum_track_relfilenode]);

		if (nulls[Anum_track_relfilenode])
			continue;

		/* Bloom filter check */
		if (!bloom_isset(tf_get_global_state.bloom, filenode))
			continue;

		relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);
		size = dbsize_calc_size(relp);
		datums[Anum_track_size] = Int64GetDatum(size);
		datums[Anum_track_state] = CharGetDatum(tf_get_global_state.bloom->is_set_all ? 'i' : 'a');
		datums[Anum_track_gp_segment_id] = Int32GetDatum(GpIdentity.segindex);

		result = heap_form_tuple(funcctx->tuple_desc, datums, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result));
	}

	while (true)
	{
		Oid			filenode;

		if (!tf_get_global_state.next_drop)
			break;

		filenode = lfirst_oid(tf_get_global_state.next_drop);
		tf_get_global_state.next_drop = lnext(tf_get_global_state.next_drop);

		nulls[Anum_track_relid] = true;
		nulls[Anum_track_name] = true;
		datums[Anum_track_relfilenode] = filenode;
		datums[Anum_track_size] = Int64GetDatum(0);
		datums[Anum_track_state] = CharGetDatum('d');
		datums[Anum_track_gp_segment_id] = Int32GetDatum(GpIdentity.segindex);
		nulls[Anum_track_gp_segment_relnamespace] = true;
		nulls[Anum_track_gp_segment_relkind] = true;
		nulls[Anum_track_gp_segment_relstorage] = true;

		result = heap_form_tuple(funcctx->tuple_desc, datums, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result));
	}

	if (tf_get_global_state.bloom)
	{
		pfree(tf_get_global_state.bloom);
		tf_get_global_state.bloom = NULL;
	}

	if (tf_get_global_state.schema_oids)
	{
		pfree(tf_get_global_state.schema_oids);
		tf_get_global_state.schema_oids = NIL;
	}

	SRF_RETURN_DONE(funcctx);
}

static void
track_db(Oid dbid, bool reg)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		AlterDatabaseSetStmt stmt;
		VariableSetStmt v_stmt;
		A_Const		aconst =
		{.type = T_A_Const,.val = {.type = T_String,.val.str = reg ? "t" : "f"}};

		stmt.type = T_AlterDatabaseSetStmt;
		stmt.dbname = get_database_name(dbid);

		if (stmt.dbname == NULL)
			ereport(ERROR,
			(errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

		stmt.setstmt = &v_stmt;

		v_stmt.type = T_VariableSetStmt;
		v_stmt.kind = VAR_SET_VALUE;
		v_stmt.name = "arenadata_toolkit.tracking_is_db_tracked";
		v_stmt.args = lappend(NIL, &aconst);
		v_stmt.is_local = false;

		tf_guc_unlock();

		AlterDatabaseSet(&stmt);

		tf_guc_unlock();
		/* Will set the GUC in caller session only on coordinator */
		SetConfigOption("arenadata_toolkit.tracking_is_db_tracked", reg ? "t" : "f",
						PGC_SUSET, PGC_S_DATABASE);
	}

	if (!reg)
		bloom_set_unbind(dbid);
	else if (!bloom_set_bind(dbid))
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] exceeded maximum number of tracked databases")));
}

static bool
is_initialized()
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	bool		all_inited = true;

	if (pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized))
		return false;

	CdbDispatchCommand("select * from arenadata_toolkit.tracking_is_segment_initialized()", 0, &cdb_pgresults);

	for (int i = 0; i < cdb_pgresults.numResults; i++)
	{
		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("Failed to check segments status")));
		}
		else
		{
			int32		segindex = 0;
			bool		is_initialized = false;

			segindex = atoi(PQgetvalue(pgresult, 0, 0));
			is_initialized = strcmp(PQgetvalue(pgresult, 0, 1), "t") == 0;

			elog(LOG, "[arenadata_toolkit] tracking_register_db initialization check"
			  " segindex: %d, is_initialized: %d", segindex, is_initialized);

			if (!is_initialized)
			{
				all_inited = false;
				break;
			}
		}
	}

	if (cdb_pgresults.numResults > 0)
		cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return all_inited;
}

/*
 * Registers current (if dbid is 0) or specific database as tracked by arenadata_toolkit tables tracking.
 * Dispatches call to segments by itself. Binds a bloom filter to the registered database if possible.
 */
Datum
tracking_register_db(PG_FUNCTION_ARGS)
{
	Oid			dbid = get_dbid(PG_GETARG_OID(0));

	tf_check_shmem_error();

	if (Gp_role != GP_ROLE_DISPATCH && IS_QUERY_DISPATCHER())
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_register_db outside query dispatcher")));
	}

	if (Gp_role == GP_ROLE_DISPATCH && !is_initialized())
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] Cannot register database before workers initialize tracking"),
				 errhint("Wait arenadata_toolkit.tracking_worker_naptime_sec and try again")));

	elog(LOG, "[arenadata_toolkit] registering database %u for tracking", dbid);

	track_db(dbid, true);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd =
		psprintf("select arenadata_toolkit.tracking_register_db(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);

		pfree(cmd);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Stop tracking given database and unbind from bloom.
 */
Datum
tracking_unregister_db(PG_FUNCTION_ARGS)
{
	Oid			dbid = get_dbid(PG_GETARG_OID(0));

	tf_check_shmem_error();

	if (Gp_role != GP_ROLE_DISPATCH && IS_QUERY_DISPATCHER())
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_unregister_db outside query dispatcher")));
	}

	if (Gp_role == GP_ROLE_DISPATCH && !is_initialized())
		ereport(ERROR,
				(errmsg("[arenadata_toolkit] Cannot unregister database before workers initialize tracking"),
				 errhint("Wait arenadata_toolkit.tracking_worker_naptime_sec and try again")));

	elog(LOG, "[arenadata_toolkit] unregistering database %u from tracking", dbid);

	track_db(dbid, false);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd =
		psprintf("select arenadata_toolkit.tracking_unregister_db(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);

		pfree(cmd);
	}

	PG_RETURN_BOOL(true);
}

Datum
tracking_set_snapshot_on_recovery(PG_FUNCTION_ARGS)
{
	bool		set = PG_GETARG_BOOL(0);
	Oid			dbid = get_dbid(PG_GETARG_OID(1));

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_set_snapshot_on_recovery outside query dispatcher")));
	}

	A_Const		aconst =
	{.type = T_A_Const,.val = {.type = T_String,.val.str = set ? "t" : "f"}};

	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);
	stmt.setstmt = &v_stmt;

	if (stmt.dbname == NULL)
		ereport(ERROR,
		   (errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	v_stmt.type = T_VariableSetStmt;
	v_stmt.kind = VAR_SET_VALUE;
	v_stmt.name = "arenadata_toolkit.tracking_snapshot_on_recovery";
	v_stmt.args = lappend(NIL, &aconst);
	v_stmt.is_local = false;

	tf_guc_unlock();

	AlterDatabaseSet(&stmt);

	/* Will set the GUC in caller session only on coordinator */
	tf_guc_unlock();
	SetConfigOption("arenadata_toolkit.tracking_snapshot_on_recovery", set ? "t" : "f",
					PGC_SUSET, PGC_S_DATABASE);

	PG_RETURN_BOOL(true);
}

/* Helper function to add or remove schema from configuration string */
static char *
add_or_remove_schema(const char *schema_string, const char *schemaName, bool add)
{
	StringInfoData buf;
	char	   *token;
	char	   *str;
	bool		found = false;

	initStringInfo(&buf);

	/*
	 * consider NULL value as a need for applying operation
	 * to default schema set
	 */
	if (schema_string == NULL)
	{
		schema_string = DEFAULT_TRACKED_SCHEMAS;
	}

	/*
	 * If string is empty, we can only add
	 */
	if (schema_string[0] == '\0' && !add)
	{
		pfree(buf.data);
		return NULL;
	}

	if (schema_string && schema_string[0] != '\0')
	{
		str = pstrdup(schema_string);
		token = strtok(str, ",");
		while (token != NULL)
		{
			if (strcmp(token, schemaName) == 0)
			{
				found = true;
				if (add)
				{
					appendStringInfo(&buf, "%s,", token);
				}
			}
			else
			{
				appendStringInfo(&buf, "%s,", token);
			}
			token = strtok(NULL, ",");
		}
		pfree(str);
	}

	if (add && !found)
	{
		appendStringInfo(&buf, "%s,", schemaName);
	}

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	if (buf.len == 0)
	{
		pfree(buf.data);
		return NULL;
	}

	return buf.data;
}

static void
track_schema(const char *schemaName, Oid dbid, bool reg)
{
	Relation	rel;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	char	   *current_schemas = NULL;
	char	   *new_schemas = NULL;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	rel = heap_open(DbRoleSettingRelationId, RowExclusiveLock);
	ScanKeyInit(&skey[0],
				Anum_pg_db_role_setting_setdatabase,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dbid));

	/*
	 * Lookup for not role specific configuration
	 */
	ScanKeyInit(&skey[1],
				Anum_pg_db_role_setting_setrole,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(InvalidOid));
	scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true, NULL, 2, skey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		str_datum;

		str_datum = heap_getattr(tuple, Anum_pg_db_role_setting_setconfig,
								 RelationGetDescr(rel), &isnull);
		if (!isnull)
		{
			ArrayType  *array;
			Datum	   *elems = NULL;
			int			nelems;

			array = DatumGetArrayTypeP(str_datum);
			deconstruct_array(array, TEXTOID, -1, false, 'i',
							  &elems, NULL, &nelems);
			for (int i = 0; i < nelems; i++)
			{
				char	   *str = TextDatumGetCString(elems[i]);

				if (strncmp(str, TRACKING_SCHEMAS_PREFIX,
					sizeof(TRACKING_SCHEMAS_PREFIX) - 1) == 0)
				{
					current_schemas = pstrdup(str + sizeof(TRACKING_SCHEMAS_PREFIX) - 1);
					break;
				}
				pfree(str);
			}

			if (elems)
				pfree(elems);
		}
	}
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	new_schemas = add_or_remove_schema(current_schemas, schemaName, reg);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);

	if (stmt.dbname == NULL)
		ereport(ERROR,
		   (errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	stmt.setstmt = &v_stmt;

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_schemas";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = new_schemas;
	arg.location = -1;

	if (new_schemas == NULL)
	{
		/*
		 * If new_schemas is NULL, we're removing the last schema, that should
		 * lead to empty result set during track acquisition. But we anyway
		 * need to store an empty string to distinguish state when the GUC has
		 * default value and when the get_track() is supposed to
		 * filter out all schemas.
		 */
		arg.val.val.str = pstrdup("");
	}

	v_stmt.kind = VAR_SET_VALUE;
	v_stmt.args = list_make1(&arg);

	tf_guc_unlock();

	AlterDatabaseSet(&stmt);

	/* Will set the GUC in caller session only on coordinator */
	tf_guc_unlock();
	SetConfigOption("arenadata_toolkit.tracking_schemas",
					new_schemas ? new_schemas : "",
					PGC_SUSET, PGC_S_DATABASE);

	if (current_schemas)
		pfree(current_schemas);
	if (new_schemas)
		pfree(new_schemas);
}

Datum
tracking_register_schema(PG_FUNCTION_ARGS)
{
	const char *schema_name = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = get_dbid(PG_GETARG_OID(1));

	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errmsg("Cannot execute tracking_register_schema outside query dispatcher")));

	if (schema_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema does not exist")));

	if (!SearchSysCacheExists1(NAMESPACENAME, CStringGetDatum(schema_name)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema %s does not exist", schema_name)));

	elog(LOG, "[arenadata_toolkit] registering schema %s in database %u for tracking", schema_name, dbid);

	track_schema(schema_name, dbid, true);

	PG_RETURN_BOOL(true);
}

Datum
tracking_unregister_schema(PG_FUNCTION_ARGS)
{
	const char *schema_name = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = get_dbid(PG_GETARG_OID(1));

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_unregister_schema outside query dispatcher")));
	}

	if (schema_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema does not exist")));

	if (!SearchSysCacheExists1(NAMESPACENAME, CStringGetDatum(schema_name)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %s does not exist", schema_name)));

	elog(LOG, "[arenadata_toolkit] registering schema %s in database %u for tracking", schema_name, dbid);

	track_schema(schema_name, dbid, false);

	PG_RETURN_BOOL(true);
}

static bool
is_valid_relkind(char relkind)
{
	switch (relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_VIEW:
		case RELKIND_COMPOSITE_TYPE:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_UNCATALOGED:
		case RELKIND_MATVIEW:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
			return true;
		default:
			return false;
	}
}

Datum
tracking_set_relkinds(PG_FUNCTION_ARGS)
{
	char	   *relkinds_str = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = get_dbid(PG_GETARG_OID(1));
	char	   *token;
	char	   *str_copy;
	bool		seen_relkinds[256] = {false};
	StringInfoData buf;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_set_relkinds outside query dispatcher")));
	}

	if (relkinds_str == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relkinds argument cannot be NULL")));

	initStringInfo(&buf);
	str_copy = pstrdup(relkinds_str);
	token = strtok(str_copy, ",");
	while (token != NULL)
	{
		if (strlen(token) != 1 || !is_valid_relkind(token[0]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid relkind: %s", token),
					 errhint("Valid relkinds are: 'r', 'i', 'S', 't', 'v', 'c', 'f', 'u', 'm', 'o', 'b', 'M'")));

		if (!seen_relkinds[(unsigned char) token[0]])
		{
			appendStringInfoChar(&buf, token[0]);
			appendStringInfoChar(&buf, ',');
			seen_relkinds[(unsigned char) token[0]] = true;
		}
		token = strtok(NULL, ",");
	}
	pfree(str_copy);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);
	stmt.setstmt = &v_stmt;

	if (stmt.dbname == NULL)
		ereport(ERROR,
		   (errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_relkinds";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = buf.data;
	arg.location = -1;

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	v_stmt.kind = VAR_SET_VALUE;
	v_stmt.args = list_make1(&arg);
	elog(LOG, "[arenadata_toolkit] setting relkinds %s in database %u for tracking", buf.data, dbid);

	tf_guc_unlock();

	AlterDatabaseSet(&stmt);

	/* Will set the GUC in caller session only on coordinator */
	tf_guc_unlock();
	SetConfigOption("arenadata_toolkit.tracking_relkinds",
					buf.data,
					PGC_SUSET, PGC_S_DATABASE);

	pfree(buf.data);

	PG_RETURN_BOOL(true);
}

static bool
is_valid_relstorage(char relstorage)
{
	switch (relstorage)
	{
		case RELSTORAGE_HEAP:
		case RELSTORAGE_AOROWS:
		case RELSTORAGE_AOCOLS:
		case RELSTORAGE_EXTERNAL:
		case RELSTORAGE_VIRTUAL:
		case RELSTORAGE_FOREIGN:
			return true;
		default:
			return false;
	}
}

Datum
tracking_set_relstorages(PG_FUNCTION_ARGS)
{
	char	   *relstorages_str = NameStr(*PG_GETARG_NAME(0));
	Oid			dbid = get_dbid(PG_GETARG_OID(1));
	char	   *token;
	char	   *str_copy;
	bool		seen_relstorages[256] = {false};
	StringInfoData buf;
	AlterDatabaseSetStmt stmt;
	VariableSetStmt v_stmt;
	A_Const		arg;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_set_relstorages outside query dispatcher")));
	}

	if (relstorages_str == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relstorages argument cannot be NULL")));

	initStringInfo(&buf);
	str_copy = pstrdup(relstorages_str);
	token = strtok(str_copy, ",");
	while (token != NULL)
	{
		if (strlen(token) != 1 || !is_valid_relstorage(token[0]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid relstorage type: %s", token),
			errhint("Valid relstorages are: 'h', 'x', 'a', 'v', 'c', 'f'")));

		if (!seen_relstorages[(unsigned char) token[0]])
		{
			appendStringInfoChar(&buf, token[0]);
			appendStringInfoChar(&buf, ',');
			seen_relstorages[(unsigned char) token[0]] = true;
		}
		token = strtok(NULL, ",");
	}
	pfree(str_copy);

	stmt.type = T_AlterDatabaseSetStmt;
	stmt.dbname = get_database_name(dbid);

	if (stmt.dbname == NULL)
		ereport(ERROR,
		   (errmsg("[arenadata_toolkit] database %u does not exist", dbid)));

	stmt.setstmt = &v_stmt;

	v_stmt.type = T_VariableSetStmt;
	v_stmt.name = "arenadata_toolkit.tracking_relstorages";
	v_stmt.is_local = false;

	arg.type = T_A_Const;
	arg.val.type = T_String;
	arg.val.val.str = buf.data;
	arg.location = -1;

	if (buf.len > 0 && buf.data[buf.len - 1] == ',')
	{
		buf.data[buf.len - 1] = '\0';
		buf.len--;
	}

	v_stmt.kind = VAR_SET_VALUE;
	v_stmt.args = list_make1(&arg);
	elog(LOG, "[arenadata_toolkit] setting relstorages %s in database %u for tracking", buf.data, dbid);

	tf_guc_unlock();

	AlterDatabaseSet(&stmt);

	/* Will set the GUC in caller session only on coordinator */
	tf_guc_unlock();
	SetConfigOption("arenadata_toolkit.tracking_relstorages",
					buf.data,
					PGC_SUSET, PGC_S_DATABASE);

	pfree(buf.data);

	PG_RETURN_BOOL(true);
}

Datum
tracking_trigger_initial_snapshot(PG_FUNCTION_ARGS)
{
	Oid			dbid = get_dbid(PG_GETARG_OID(0));
	bloom_op_ctx_t ctx = {0};

	tf_check_shmem_error();

	if (Gp_role != GP_ROLE_DISPATCH && IS_QUERY_DISPATCHER())
	{
		ereport(ERROR,
				(errmsg("Cannot execute tracking_trigger_initial_snapshot outside query dispatcher")));
	}

	elog(LOG, "[arenadata_toolkit] tracking_trigger_initial_snapshot dbid: %u", dbid);

	ctx = bloom_set_get_entry(MyDatabaseId, LW_SHARED, LW_EXCLUSIVE);

	if (!ctx.entry)
	{
		bloom_set_release(&ctx);
		ereport(ERROR,
		(errmsg("Failed to find corresponding filter to database %u", dbid)));
	}

	if (Gp_role == GP_ROLE_DISPATCH && !pg_atomic_unlocked_test_flag(&ctx.entry->capture_in_progress))
	{
		bloom_set_release(&ctx);
		ereport(ERROR,
		  (errmsg("Cannot modify track during track acquisition %u", dbid)));
	}

	bloom_set_all(&ctx.entry->bloom);
	bloom_set_release(&ctx);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *cmd = psprintf("select arenadata_toolkit.tracking_trigger_initial_snapshot(%u)", dbid);

		CdbDispatchCommand(cmd, 0, NULL);
	}

	PG_RETURN_BOOL(true);
}

Datum
tracking_is_initial_snapshot_triggered(PG_FUNCTION_ARGS)
{
	Oid			dbid = get_dbid(PG_GETARG_OID(0));
	bool		is_triggered = false;

	tf_check_shmem_error();

	is_triggered = bloom_set_is_all_bits_triggered(dbid);

	elog(LOG, "[arenadata_toolkit] is_initial_snapshot_triggered:%d dbid: %u", is_triggered, dbid);

	PG_RETURN_BOOL(is_triggered);
}

Datum
tracking_is_segment_initialized(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	Datum		result;

	tf_check_shmem_error();

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	tupdesc = rsi->expectedDesc;

	/* Populate an output tuple. */
	values[0] = Int32GetDatum(GpIdentity.segindex);
	values[1] = BoolGetDatum(pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_is_initialized) == false);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

static bool
is_explain_analyze(List *options)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(opt->defname, "analyze") == 0)
		{
			return defGetBoolean(opt);
		}
	}
	return false;
}

static void
explain_detector_ProcessUtility(Node *parsetree,
								const char *queryString,
								ProcessUtilityContext context,
								ParamListInfo params,
								DestReceiver *dest,
								char *completionTag)
{
	if (IsA(parsetree, ExplainStmt))
	{
		ExplainStmt *stmt = (ExplainStmt *) parsetree;

		if (!is_explain_analyze(stmt->options))
			isExecutorExplainMode = true;
	}

	if (next_ProcessUtility_hook)
		next_ProcessUtility_hook(parsetree, queryString, context, params, dest, completionTag);

	isExecutorExplainMode = false;
}

/*
 * When any query execution ends, current_version is set to control.
 * If the tracking_track_version registered transaction callback
 * and its transaction is still going, then subsequent tracking_track_version
 * calls within the transaction will return ControlVerion.
 */
static void
track_ExecutorEnd(QueryDesc *queryDesc)
{
	CurrentVersion = ControlVersion;

	if (next_ExecutorEnd_hook)
		next_ExecutorEnd_hook(queryDesc);
}

void
track_setup_executor_hooks(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = explain_detector_ProcessUtility;

	next_ExecutorEnd_hook = ExecutorEnd_hook ? ExecutorEnd_hook : standard_ExecutorEnd;
	ExecutorEnd_hook = track_ExecutorEnd;

}

void
track_uninstall_executor_hooks(void)
{
	ProcessUtility_hook = (next_ProcessUtility_hook == standard_ProcessUtility) ? NULL : next_ProcessUtility_hook;
	ExecutorEnd_hook = (next_ExecutorEnd_hook == standard_ExecutorEnd) ? NULL : next_ExecutorEnd_hook;
}

/*
 * This function should be used as argument for tracking_get_track function to
 * follow correct transaction semantics. Several calls of the function within
 * the same transaction return ControlVersion, which says tracking_get_track
 * to return previous filter state.
 */
Datum
tracking_track_version(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errmsg("Cannot acquire track using such query")));

	if (isExecutorExplainMode)
		PG_RETURN_INT64((int64) InvalidVersion);

	tf_check_shmem_error();

	if (!callbackRegistered)
	{
		RegisterXactCallbackOnce(xact_end_version_callback, NULL);
		callbackRegistered = true;

		bloom_op_ctx_t ctx = bloom_set_get_entry(MyDatabaseId, LW_SHARED, LW_EXCLUSIVE);

		if (!ctx.entry)
		{
			bloom_set_release(&ctx);

			ereport(ERROR,
					(errcode(ERRCODE_GP_COMMAND_ERROR),
							errmsg("database %u is not tracked", MyDatabaseId),
							errhint("Call 'arenadata_toolkit.tracking_register_db()'"
									"to enable tracking")));
		}
		else if (!pg_atomic_test_set_flag(&ctx.entry->capture_in_progress))
		{
			bloom_set_release(&ctx);
			ereport(ERROR,
					(errcode(ERRCODE_GP_COMMAND_ERROR),
					errmsg("Track for database %u is being acquired in other transaction", MyDatabaseId)));
		}

		CurrentVersion = ctx.entry->master_version;
		bloom_set_release(&ctx);
	}

	PG_RETURN_INT64((int64) CurrentVersion);
}
