/*-------------------------------------------------------------------------
 *
 * flightsql_fdw.cpp
 *    Standard Arrow Flight SQL foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "access/table.h"
#include "access/xact.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include <errno.h>
#include <limits.h>
#include <string.h>

extern List *untransformRelOptions(Datum options);

PG_FUNCTION_INFO_V1(flightsql_fdw_handler);
PG_FUNCTION_INFO_V1(flightsql_fdw_validator);

Datum flightsql_fdw_handler(PG_FUNCTION_ARGS);
Datum flightsql_fdw_validator(PG_FUNCTION_ARGS);

}

enum FlightSqlScanPrivateIndex
{
	FLIGHTSQL_PRIVATE_QUERY,
	FLIGHTSQL_PRIVATE_FLIGHT_INFO,
	FLIGHTSQL_PRIVATE_PROJECTED_ATTRS,
	FLIGHTSQL_PRIVATE_PROJECT_ALL
};

typedef struct FlightSqlPlanState
{
	double		rows;
} FlightSqlPlanState;

typedef struct FlightSqlDispatchContext
{
	plan_tree_base_prefix base;
	PlannedStmt *stmt;
	QueryDesc  *query_desc;
	bool		found;
	bool		prepare;
} FlightSqlDispatchContext;

typedef struct FlightSqlModifyState
{
	char	   *url;
	char	   *table_name;
	char	   *schema_name;
	char	   *catalog_name;
	List	   *target_attrs;
	int			batch_rows;
	int			max_batch_bytes;
	int			timeout_ms;
	bool		verify_ingested_rows;
	char	   *transaction_id;
	char	   *write_routing_mode;
	char	   *serialized_mpp_plan;
	ArrowFlightSecurityOptions security_options;
	ArrowFlightSqlMppRoute mpp_route;
	void	   *writer_state;
} FlightSqlModifyState;

enum FlightSqlModifyPrivateIndex
{
	FLIGHTSQL_MODIFY_URL,
	FLIGHTSQL_MODIFY_TABLE,
	FLIGHTSQL_MODIFY_SCHEMA,
	FLIGHTSQL_MODIFY_CATALOG,
	FLIGHTSQL_MODIFY_TARGET_ATTRS,
	FLIGHTSQL_MODIFY_BATCH_ROWS,
	FLIGHTSQL_MODIFY_MAX_BATCH_BYTES,
	FLIGHTSQL_MODIFY_TIMEOUT_MS,
	FLIGHTSQL_MODIFY_VERIFY_INGESTED_ROWS,
	FLIGHTSQL_MODIFY_TLS_CA_FILE,
	FLIGHTSQL_MODIFY_TLS_CLIENT_CERT_FILE,
	FLIGHTSQL_MODIFY_TLS_CLIENT_KEY_FILE,
	FLIGHTSQL_MODIFY_AUTH_TOKEN_FILE,
	FLIGHTSQL_MODIFY_TRANSACTION_MODE,
	FLIGHTSQL_MODIFY_TRANSACTION_ID,
	FLIGHTSQL_MODIFY_WRITE_ROUTING_MODE,
	FLIGHTSQL_MODIFY_MPP_PLAN,
	FLIGHTSQL_MODIFY_NUM_ITEMS
};

typedef struct FlightSqlRemoteTransaction
{
	char	   *url;
	char	   *transaction_id;
	ArrowFlightSecurityOptions security_options;
	SubTransactionId subtransaction_id;
	bool		finished;
	struct FlightSqlRemoteTransaction *next;
} FlightSqlRemoteTransaction;

typedef struct FlightSqlRemotePlan
{
	QueryDesc  *owner_query_desc;
	char	   *url;
	char	   *serialized_plan;
	int			timeout_ms;
	int			max_plan_bytes;
	ArrowFlightSecurityOptions security_options;
	SubTransactionId subtransaction_id;
	bool		finished;
	struct FlightSqlRemotePlan *next;
} FlightSqlRemotePlan;

static void flightsql_GetForeignRelSize(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid);
static void flightsql_GetForeignPaths(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid);
static ForeignScan *flightsql_GetForeignPlan(PlannerInfo *root,
											 RelOptInfo *baserel,
											 Oid foreigntableid,
											 ForeignPath *best_path,
											 List *tlist,
											 List *scan_clauses,
											 Plan *outer_plan);
static void flightsql_PrepareForeignScanForDispatch(ForeignScan *node,
													 Oid foreigntableid);
static void flightsql_PrepareForeignModifyForDispatch(ModifyTable *node,
													   int target_index,
													   Oid foreigntableid,
													   QueryDesc *query_desc);
static void flightsql_ExecutorStart(QueryDesc *query_desc, int eflags);
static void flightsql_ExecutorFinish(QueryDesc *query_desc);
static bool flightsql_plan_contains_scan(PlannedStmt *stmt);
static void flightsql_prepare_plan_for_dispatch(QueryDesc *query_desc);
static bool flightsql_prepare_plan_walker(Node *node,
										   FlightSqlDispatchContext *context);
static bool flightsql_is_relation(Oid relid);
static void flightsql_validate_mpp_contract(Oid foreigntableid);
static void flightsql_validate_transaction_mode(const char *mode);
static void flightsql_validate_write_routing_mode(const char *mode);
static void flightsql_validate_write_capabilities(
	const char *mode, const ArrowFlightSqlCapabilities *capabilities);
static void flightsql_register_remote_transaction(
	const char *url, const char *transaction_id,
	const ArrowFlightSecurityOptions *security);
static void flightsql_xact_callback(XactEvent event, void *arg);
static void flightsql_subxact_callback(SubXactEvent event,
										SubTransactionId my_subid,
										SubTransactionId parent_subid,
										void *arg);
static void flightsql_finish_remote_transaction(
	FlightSqlRemoteTransaction *transaction, bool commit,
	bool error_on_failure);
static void flightsql_register_remote_plan(
	QueryDesc *owner_query_desc, const char *url,
	const char *serialized_plan,
	const char *plan_id, int timeout_ms, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security);
static void flightsql_finish_remote_plan(
	FlightSqlRemotePlan *plan, bool complete, bool error_on_failure);
static void flightsql_complete_query_plans(QueryDesc *query_desc);
static void flightsql_ensure_xact_callbacks(void);
static char *flightsql_generate_operation_id(void);
static void flightsql_BeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *flightsql_IterateForeignScan(ForeignScanState *node);
static void flightsql_ReScanForeignScan(ForeignScanState *node);
static void flightsql_EndForeignScan(ForeignScanState *node);
static void flightsql_ExplainForeignScan(ForeignScanState *node,
										 ExplainState *es);
static List *flightsql_PlanForeignModify(PlannerInfo *root,
										 ModifyTable *plan,
										 Index result_relation,
										 int subplan_index);
static void flightsql_BeginForeignModify(ModifyTableState *mtstate,
										 ResultRelInfo *rinfo,
										 List *fdw_private,
										 int subplan_index, int eflags);
static TupleTableSlot *flightsql_ExecForeignInsert(EState *estate,
												  ResultRelInfo *rinfo,
												  TupleTableSlot *slot,
												  TupleTableSlot *plan_slot);
static void flightsql_EndForeignModify(EState *estate,
									   ResultRelInfo *rinfo);
static void flightsql_BeginForeignInsert(ModifyTableState *mtstate,
										 ResultRelInfo *rinfo);
static void flightsql_EndForeignInsert(EState *estate,
									   ResultRelInfo *rinfo);
static int flightsql_IsForeignRelUpdatable(Relation rel);
static void flightsql_ExplainForeignModify(ModifyTableState *mtstate,
										   ResultRelInfo *rinfo,
										   List *fdw_private,
										   int subplan_index,
										   ExplainState *es);
static bool flightsql_is_valid_option(const char *option, Oid catalog);
static char *flightsql_get_option(List *options, const char *name);
static char *flightsql_get_merged_option(List *table_options,
										 List *server_options,
										 const char *name);
static char *flightsql_get_option_or_default(List *options,
											 const char *name,
											 const char *default_value);
static int flightsql_get_merged_int_option(
									 List *table_options,
									 List *server_options,
									 const char *name, int default_value,
									 int min_value, int max_value);
static bool flightsql_parse_bool(const char *value, const char *name);
static void flightsql_validate_endpoint_location_allowlist(
	const char *value, bool tls_enabled);
static ArrowFlightSecurityOptions flightsql_security_options(
	List *server_options);
static char *flightsql_connection_url(Oid foreigntableid);
static bool flightsql_predicate_pushdown_enabled(Oid foreigntableid);
static List *flightsql_projected_attrs(RelOptInfo *baserel,
									   Index scan_relid,
									   List *local_exprs);
static char *flightsql_build_query(Oid foreigntableid,
								   List *projected_attrs,
								   List *remote_exprs,
								   Index scan_relid,
								   bool *project_all);
static void flightsql_init_projection(ArrowFlightSqlFdwExecState *state,
									  TupleDesc tupdesc,
									  List *projected_attrs,
									  bool project_all);
static void flightsql_validate_local_types(Oid foreigntableid);
static bool flightsql_type_supported(Oid typid);

static ExecutorStart_hook_type next_ExecutorStart_hook = NULL;
static ExecutorFinish_hook_type next_ExecutorFinish_hook = NULL;
static bool flightsql_executor_hook_registered = false;
static bool flightsql_xact_callback_registered = false;
static FlightSqlRemoteTransaction *flightsql_remote_transactions = NULL;
static FlightSqlRemotePlan *flightsql_remote_plans = NULL;

extern "C" Datum
flightsql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	if (!flightsql_executor_hook_registered)
	{
		next_ExecutorStart_hook = ExecutorStart_hook;
		ExecutorStart_hook = flightsql_ExecutorStart;
		next_ExecutorFinish_hook = ExecutorFinish_hook;
		ExecutorFinish_hook = flightsql_ExecutorFinish;
		flightsql_executor_hook_registered = true;
	}

	routine->GetForeignRelSize = flightsql_GetForeignRelSize;
	routine->GetForeignPaths = flightsql_GetForeignPaths;
	routine->GetForeignPlan = flightsql_GetForeignPlan;
	routine->BeginForeignScan = flightsql_BeginForeignScan;
	routine->IterateForeignScan = flightsql_IterateForeignScan;
	routine->ReScanForeignScan = flightsql_ReScanForeignScan;
	routine->EndForeignScan = flightsql_EndForeignScan;
	routine->ExplainForeignScan = flightsql_ExplainForeignScan;
	routine->PlanForeignModify = flightsql_PlanForeignModify;
	routine->BeginForeignModify = flightsql_BeginForeignModify;
	routine->ExecForeignInsert = flightsql_ExecForeignInsert;
	routine->EndForeignModify = flightsql_EndForeignModify;
	routine->BeginForeignInsert = flightsql_BeginForeignInsert;
	routine->EndForeignInsert = flightsql_EndForeignInsert;
	routine->IsForeignRelUpdatable = flightsql_IsForeignRelUpdatable;
	routine->ExplainForeignModify = flightsql_ExplainForeignModify;

	PG_RETURN_POINTER(routine);
}

extern "C" Datum
flightsql_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *lc;
	bool		has_table_name = false;
	bool		has_cert = false;
	bool		has_key = false;
	bool		has_tls_file = false;
	bool		tls_enabled = false;
	const char *endpoint_location_allowlist = NULL;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const char *value;

		if (!flightsql_is_valid_option(def->defname, catalog))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\" for flightsql_fdw",
							def->defname)));

		value = defGetString(def);
		if (strcmp(def->defname, "host") == 0 ||
			strcmp(def->defname, "table_name") == 0 ||
			strcmp(def->defname, "schema_name") == 0 ||
			strcmp(def->defname, "catalog_name") == 0 ||
			strcmp(def->defname, "column_name") == 0 ||
			strcmp(def->defname, "tls_ca_file") == 0 ||
			strcmp(def->defname, "tls_client_cert_file") == 0 ||
			strcmp(def->defname, "tls_client_key_file") == 0 ||
			strcmp(def->defname, "auth_token_file") == 0 ||
			strcmp(def->defname, "endpoint_location_allowlist") == 0)
		{
			if (value == NULL || value[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("flightsql_fdw option \"%s\" must not be empty",
								def->defname)));
		}

		if (strcmp(def->defname, "host") == 0 &&
			strlen(value) > AF_MAX_HOST_LEN)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("flightsql_fdw host is too long")));
		else if (strcmp(def->defname, "port") == 0)
			(void) af_parse_int_option_value(value, "port", 1, 65535);
		else if (strcmp(def->defname, "timeout_ms") == 0)
			(void) af_parse_int_option_value(value, "timeout_ms", -1,
											 INT_MAX);
		else if (strcmp(def->defname, "max_endpoints") == 0)
			(void) af_parse_int_option_value(value, "max_endpoints", 1,
											 INT_MAX);
		else if (strcmp(def->defname, "max_plan_bytes") == 0)
			(void) af_parse_int_option_value(value, "max_plan_bytes", 1024,
											 INT_MAX);
		else if (strcmp(def->defname, "batch_rows") == 0)
			(void) af_parse_int_option_value(value, "batch_rows", 1,
											 INT_MAX);
		else if (strcmp(def->defname, "max_batch_bytes") == 0)
			(void) af_parse_int_option_value(value, "max_batch_bytes", 0,
											 INT_MAX);
		else if (strcmp(def->defname, "rows") == 0)
		{
			char	   *endptr;
			double		rows;

			errno = 0;
			rows = strtod(value, &endptr);
			if (errno != 0 || endptr == value || *endptr != '\0' ||
				rows <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid rows value \"%s\" for flightsql_fdw",
								value)));
		}
		else if (strcmp(def->defname, "ingest_row_count_check") == 0)
		{
			if (strcmp(value, "exact") != 0 &&
				strcmp(value, "off") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid ingest_row_count_check value \"%s\"",
								value),
						 errhint("Use \"exact\" or \"off\".")));
		}
		else if (strcmp(def->defname, "write_transaction_mode") == 0)
			flightsql_validate_transaction_mode(value);
		else if (strcmp(def->defname, "write_routing_mode") == 0)
			flightsql_validate_write_routing_mode(value);
		else if (strcmp(def->defname, "tls") == 0)
			tls_enabled = flightsql_parse_bool(value, "tls");
		else if (strcmp(def->defname,
						"endpoint_location_allowlist") == 0)
			endpoint_location_allowlist = value;
		else if (strcmp(def->defname, "predicate_pushdown") == 0)
			(void) flightsql_parse_bool(value, "predicate_pushdown");
		else if (strcmp(def->defname, "table_name") == 0)
			has_table_name = true;
		else if (strcmp(def->defname, "tls_client_cert_file") == 0)
		{
			has_cert = true;
			has_tls_file = true;
		}
		else if (strcmp(def->defname, "tls_client_key_file") == 0)
		{
			has_key = true;
			has_tls_file = true;
		}
		else if (strcmp(def->defname, "tls_ca_file") == 0 ||
				 strcmp(def->defname, "auth_token_file") == 0)
			has_tls_file = true;
	}

	if (catalog == ForeignTableRelationId && !has_table_name)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("flightsql_fdw foreign table requires option \"table_name\"")));

	if (has_cert != has_key)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("flightsql_fdw options \"tls_client_cert_file\" "
						"and \"tls_client_key_file\" must be set together")));

	if (catalog == ForeignServerRelationId && has_tls_file && !tls_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("flightsql_fdw TLS/auth file options require tls=true")));

	if (catalog == ForeignServerRelationId &&
		endpoint_location_allowlist != NULL)
		flightsql_validate_endpoint_location_allowlist(
			endpoint_location_allowlist, tls_enabled);

	PG_RETURN_VOID();
}

static void
flightsql_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
							Oid foreigntableid)
{
	FlightSqlPlanState *state;
	ForeignTable *table;
	char	   *rows;

	(void) root;
	flightsql_validate_mpp_contract(foreigntableid);
	flightsql_validate_local_types(foreigntableid);

	state = (FlightSqlPlanState *) palloc0(sizeof(*state));
	table = GetForeignTable(foreigntableid);
	rows = flightsql_get_option(table->options, "rows");
	state->rows = rows == NULL ? 1000.0 : strtod(rows, NULL);
	baserel->fdw_private = state;
	baserel->rows = state->rows;
}

static void
flightsql_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
						  Oid foreigntableid)
{
	Cost		startup_cost;
	Cost		total_cost;
	ForeignPath *path;
	ForeignTable *table;

	startup_cost = baserel->baserestrictcost.startup;
	total_cost = startup_cost +
		(cpu_tuple_cost + baserel->baserestrictcost.per_tuple) *
		baserel->rows;
	path = create_foreignscan_path(root, baserel, NULL, baserel->rows,
								   startup_cost, total_cost, NIL,
								   baserel->lateral_relids, NULL, NIL);

	table = GetForeignTable(foreigntableid);
	if (table->exec_location != FTEXECLOCATION_ALL_SEGMENTS)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("flightsql_fdw requires mpp_execute='all segments'")));

	CdbPathLocus_MakeStrewn(&path->path.locus, getgpsegmentCount());
	path->path.motionHazard = false;
	path->path.rescannable = false;
	path->path.sameslice_relids = baserel->relids;
	add_path(baserel, (Path *) path);
}

static ForeignScan *
flightsql_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid, ForeignPath *best_path,
						 List *tlist, List *scan_clauses, Plan *outer_plan)
{
	List	   *projected_attrs;
	List	   *fdw_private;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	ListCell   *lc;
	char	   *query;
	bool		project_all;
	bool		predicate_pushdown;

	(void) root;
	(void) best_path;

	predicate_pushdown =
		flightsql_predicate_pushdown_enabled(foreigntableid);
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		List	   *conjuncts;
		ListCell   *conjunct_cell;

		if (rinfo->pseudoconstant)
			continue;
		conjuncts = make_ands_implicit(rinfo->clause);
		foreach(conjunct_cell, conjuncts)
		{
			Expr	   *conjunct = (Expr *) lfirst(conjunct_cell);

			if (predicate_pushdown &&
				af_flightsql_predicate_is_safe(
					conjunct, baserel->relid,
					rinfo->security_level > 0))
				remote_exprs = lappend(remote_exprs, conjunct);
			else
				local_exprs = lappend(local_exprs, conjunct);
		}
	}

	projected_attrs = flightsql_projected_attrs(baserel, baserel->relid,
												local_exprs);
	if (projected_attrs == NIL)
	{
		Relation	rel = table_open(foreigntableid, NoLock);
		TupleDesc	tupdesc = RelationGetDescr(rel);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			if (!TupleDescAttr(tupdesc, i)->attisdropped)
			{
				projected_attrs = list_make1(makeInteger(i + 1));
				break;
			}
		}
		table_close(rel, NoLock);
	}
	query = flightsql_build_query(foreigntableid, projected_attrs,
								  remote_exprs, baserel->relid,
								  &project_all);
	fdw_private = list_make4(makeString(query),
							 makeString(pstrdup("")),
							 projected_attrs,
							 makeInteger(project_all ? 1 : 0));

	return make_foreignscan(tlist, local_exprs, baserel->relid, NIL,
							fdw_private, NIL, remote_exprs, outer_plan);
}

static void
flightsql_ExecutorStart(QueryDesc *query_desc, int eflags)
{
	if (Gp_role == GP_ROLE_DISPATCH &&
		(eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0 &&
		flightsql_plan_contains_scan(query_desc->plannedstmt))
	{
		PlannedStmt *stmt =
			(PlannedStmt *) copyObjectImpl(query_desc->plannedstmt);

		query_desc->plannedstmt = stmt;
		flightsql_prepare_plan_for_dispatch(query_desc);
	}

	if (next_ExecutorStart_hook != NULL)
		next_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);
}

static void
flightsql_ExecutorFinish(QueryDesc *query_desc)
{
	if (next_ExecutorFinish_hook != NULL)
		next_ExecutorFinish_hook(query_desc);
	else
		standard_ExecutorFinish(query_desc);

	if (Gp_role == GP_ROLE_DISPATCH)
		flightsql_complete_query_plans(query_desc);
}

static bool
flightsql_plan_contains_scan(PlannedStmt *stmt)
{
	FlightSqlDispatchContext context;

	exec_init_plan_tree_base(&context.base, stmt);
	context.stmt = stmt;
	context.query_desc = NULL;
	context.found = false;
	context.prepare = false;
	(void) flightsql_prepare_plan_walker((Node *) stmt->planTree, &context);
	return context.found;
}

static void
flightsql_prepare_plan_for_dispatch(QueryDesc *query_desc)
{
	FlightSqlDispatchContext context;
	PlannedStmt *stmt = query_desc->plannedstmt;

	exec_init_plan_tree_base(&context.base, stmt);
	context.stmt = stmt;
	context.query_desc = query_desc;
	context.found = false;
	context.prepare = true;
	(void) flightsql_prepare_plan_walker((Node *) stmt->planTree, &context);
}

static bool
flightsql_prepare_plan_walker(Node *node, FlightSqlDispatchContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ModifyTable))
	{
		ModifyTable *modify = (ModifyTable *) node;
		ListCell   *relation_cell;
		ListCell   *private_cell;
		int			target_index = 0;

		if (list_length(modify->resultRelations) !=
			list_length(modify->fdwPrivLists))
			elog(ERROR, "flightsql_fdw: inconsistent modify plan state");

		forboth(relation_cell, modify->resultRelations,
				private_cell, modify->fdwPrivLists)
		{
			Index		result_relation = lfirst_int(relation_cell);
			RangeTblEntry *rte =
				rt_fetch(result_relation, context->stmt->rtable);

			(void) private_cell;
				if (rte->rtekind == RTE_RELATION &&
					flightsql_is_relation(rte->relid))
				{
					context->found = true;
					if (context->prepare)
						flightsql_PrepareForeignModifyForDispatch(
							modify, target_index, rte->relid,
							context->query_desc);
				}
			target_index++;
		}
	}

	if (IsA(node, ForeignScan))
	{
		ForeignScan *scan = (ForeignScan *) node;

		if (scan->scan.scanrelid > 0)
		{
			RangeTblEntry *rte =
				rt_fetch(scan->scan.scanrelid, context->stmt->rtable);

			if (rte->rtekind == RTE_RELATION)
			{
				if (flightsql_is_relation(rte->relid))
				{
					context->found = true;
					if (context->prepare)
						flightsql_PrepareForeignScanForDispatch(scan,
															  rte->relid);
				}
			}
		}
	}

	return plan_tree_walker(node,
							(bool (*)()) flightsql_prepare_plan_walker,
							context, true);
}

static bool
flightsql_is_relation(Oid relid)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
		return false;

	table = GetForeignTable(relid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	return strcmp(wrapper->fdwname, "flightsql_fdw") == 0;
}

static void
flightsql_PrepareForeignScanForDispatch(ForeignScan *node,
										Oid foreigntableid)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	ArrowFlightSecurityOptions security =
		flightsql_security_options(server->options);
	char	   *url = flightsql_connection_url(foreigntableid);
	char	   *max_endpoints_value =
		flightsql_get_option(server->options, "max_endpoints");
	char	   *max_plan_bytes_value =
		flightsql_get_option(server->options, "max_plan_bytes");
	int			max_endpoints = max_endpoints_value == NULL ?
		AF_FLIGHT_SQL_DEFAULT_MAX_ENDPOINTS :
		af_parse_int_option_value(max_endpoints_value, "max_endpoints", 1,
								  INT_MAX);
	int			max_plan_bytes = max_plan_bytes_value == NULL ?
		AF_FLIGHT_SQL_DEFAULT_MAX_PLAN_BYTES :
		af_parse_int_option_value(max_plan_bytes_value, "max_plan_bytes",
								  1024, INT_MAX);
	const char *query;
	char	   *serialized;
	ArrowFlightSqlCapabilities capabilities;

	flightsql_validate_mpp_contract(foreigntableid);
	if (list_length(node->fdw_private) != 4)
		elog(ERROR, "flightsql_fdw: invalid private scan state");

	query = strVal(list_nth(node->fdw_private, FLIGHTSQL_PRIVATE_QUERY));
	af_flightsql_get_capabilities(url, &security, &capabilities);
	serialized = af_flightsql_execute_query(url, query, max_endpoints,
											max_plan_bytes, &security);
	(void) list_nth_replace(node->fdw_private,
						   FLIGHTSQL_PRIVATE_FLIGHT_INFO,
						   makeString(serialized));
}

static void
flightsql_PrepareForeignModifyForDispatch(ModifyTable *node,
										  int target_index,
										  Oid foreigntableid,
										  QueryDesc *query_desc)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	ArrowFlightSecurityOptions security =
		flightsql_security_options(server->options);
	List	   *fdw_private =
		(List *) list_nth(node->fdwPrivLists, target_index);
	const char *mode;
	const char *url;
	const char *routing_mode;
	int			timeout_ms;
	ArrowFlightSqlCapabilities capabilities;

	flightsql_validate_mpp_contract(foreigntableid);
	if (list_length(fdw_private) != FLIGHTSQL_MODIFY_NUM_ITEMS)
		elog(ERROR, "flightsql_fdw: invalid private modify state");

	mode =
		strVal(list_nth(fdw_private,
						FLIGHTSQL_MODIFY_TRANSACTION_MODE));
	url = strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_URL));
	routing_mode =
		strVal(list_nth(fdw_private,
						FLIGHTSQL_MODIFY_WRITE_ROUTING_MODE));
	timeout_ms =
		intVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_TIMEOUT_MS));
	flightsql_validate_transaction_mode(mode);
	flightsql_validate_write_routing_mode(routing_mode);
	af_flightsql_get_capabilities(url, &security, &capabilities);
	flightsql_validate_write_capabilities(mode, &capabilities);

	if (strcmp(routing_mode, AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED) == 0 &&
		!af_flightsql_mpp_action_supported(url, timeout_ms, &security))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Flight SQL server does not support planned MPP ingest"),
				 errdetail("Required actions: %s, %s, and %s.",
						   AF_FLIGHT_SQL_MPP_CREATE_ACTION,
						   AF_FLIGHT_SQL_MPP_COMPLETE_ACTION,
						   AF_FLIGHT_SQL_MPP_ABORT_ACTION)));

	if (strcmp(mode, AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED) == 0)
	{
		char	   *transaction_id =
			af_flightsql_begin_transaction(url, &security);

		flightsql_register_remote_transaction(
			url, transaction_id, &security);
		(void) list_nth_replace(
			fdw_private, FLIGHTSQL_MODIFY_TRANSACTION_ID,
			makeString(transaction_id));
	}

	if (strcmp(routing_mode, AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED) == 0)
	{
		Relation	rel = table_open(foreigntableid, NoLock);
		List	   *target_attrs =
			(List *) list_nth(fdw_private,
							  FLIGHTSQL_MODIFY_TARGET_ATTRS);
		int			schema_len;
		char	   *schema_fingerprint;
		char	   *schema_ipc =
			af_flightsql_writer_schema(
				rel, target_attrs, &schema_len, &schema_fingerprint);
		char	   *operation_id = flightsql_generate_operation_id();
		char	   *plan_id;
		bool		cluster_transaction;
		char	   *max_plan_bytes_value =
			flightsql_get_option(server->options, "max_plan_bytes");
		int			max_plan_bytes = max_plan_bytes_value == NULL ?
			AF_FLIGHT_SQL_DEFAULT_MAX_PLAN_BYTES :
			af_parse_int_option_value(
				max_plan_bytes_value, "max_plan_bytes", 1024, INT_MAX);
		char	   *transaction_id =
			strVal(list_nth(fdw_private,
							FLIGHTSQL_MODIFY_TRANSACTION_ID));
		char	   *serialized_plan;

		table_close(rel, NoLock);
		serialized_plan =
			af_flightsql_mpp_create_plan(
				url, operation_id,
				strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_CATALOG)),
				strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_SCHEMA)),
				strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_TABLE)),
				mode, transaction_id, schema_ipc, schema_len,
				schema_fingerprint, getgpsegmentCount(), timeout_ms,
				max_plan_bytes, &security, &plan_id,
				&cluster_transaction);
		if (strcmp(mode,
				   AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED) == 0 &&
			!cluster_transaction)
			elog(ERROR,
				 "flightsql_fdw: required MPP transaction was not cluster scoped");

		flightsql_register_remote_plan(
			query_desc, url, serialized_plan, plan_id, timeout_ms,
			max_plan_bytes, &security);
		(void) list_nth_replace(
			fdw_private, FLIGHTSQL_MODIFY_MPP_PLAN,
			makeString(serialized_plan));
	}
}

static void
flightsql_BeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *scan = (ForeignScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	ForeignTable *table;
	ForeignServer *server;
	ArrowFlightSqlFdwExecState *state;
	List	   *projected_attrs;
	bool		project_all;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;
	if (rel == NULL)
		elog(ERROR, "flightsql_fdw: scan relation is not available");
	if (list_length(scan->fdw_private) != 4)
		elog(ERROR, "flightsql_fdw: invalid private scan state");

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	state = (ArrowFlightSqlFdwExecState *) palloc0(sizeof(*state));
	state->url = flightsql_connection_url(RelationGetRelid(rel));
	state->serialized_flight_info =
		pstrdup(strVal(list_nth(scan->fdw_private,
							  FLIGHTSQL_PRIVATE_FLIGHT_INFO)));
	state->all_segments =
		(table->exec_location == FTEXECLOCATION_ALL_SEGMENTS);
	state->security_options = flightsql_security_options(server->options);
	projected_attrs = (List *) list_nth(scan->fdw_private,
									   FLIGHTSQL_PRIVATE_PROJECTED_ATTRS);
	project_all =
		intVal(list_nth(scan->fdw_private,
						FLIGHTSQL_PRIVATE_PROJECT_ALL)) != 0;
	flightsql_init_projection(state, RelationGetDescr(rel), projected_attrs,
							  project_all);

	if (!(state->all_segments && Gp_role == GP_ROLE_DISPATCH) &&
		state->serialized_flight_info[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("flightsql_fdw scan was not prepared on the dispatcher")));

	node->fdw_state = state;
}

static TupleTableSlot *
flightsql_IterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ArrowFlightSqlFdwExecState *state =
		(ArrowFlightSqlFdwExecState *) node->fdw_state;

	ExecClearTuple(slot);
	if (state == NULL)
		return slot;
	if (state->all_segments && Gp_role == GP_ROLE_DISPATCH)
		return slot;

	return af_flightsql_stream_next_slot(node->ss.ss_currentRelation,
										 state->url,
										 state->serialized_flight_info,
										 &state->flight_state,
										 slot,
										 state->project_all,
										 state->projected_attrs,
										 &state->security_options);
}

static void
flightsql_ReScanForeignScan(ForeignScanState *node)
{
	ArrowFlightSqlFdwExecState *state =
		(ArrowFlightSqlFdwExecState *) node->fdw_state;

	if (state != NULL && state->flight_state != NULL)
	{
		af_flightsql_stream_close(state->flight_state);
		state->flight_state = NULL;
	}
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("flightsql_fdw does not support rescanning a FlightInfo")));
}

static void
flightsql_EndForeignScan(ForeignScanState *node)
{
	ArrowFlightSqlFdwExecState *state =
		(ArrowFlightSqlFdwExecState *) node->fdw_state;

	if (state != NULL && state->flight_state != NULL)
		af_flightsql_stream_close(state->flight_state);
	node->fdw_state = NULL;
}

static void
flightsql_ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *scan = (ForeignScan *) node->ss.ps.plan;

	if (list_length(scan->fdw_private) == 4)
		ExplainPropertyText("Flight SQL query",
							strVal(list_nth(scan->fdw_private,
										  FLIGHTSQL_PRIVATE_QUERY)),
							es);
}

static List *
flightsql_PlanForeignModify(PlannerInfo *root, ModifyTable *plan,
							Index result_relation, int subplan_index)
{
	RangeTblEntry *rte = planner_rt_fetch(result_relation, root);
	ForeignTable *table;
	ForeignServer *server;
	Relation	rel;
	TupleDesc	tupdesc;
	List	   *target_attrs = NIL;
	List	   *result = NIL;

	(void) subplan_index;
	if (plan->operation != CMD_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("flightsql_fdw supports only INSERT for foreign table writes")));

	flightsql_validate_local_types(rte->relid);
	flightsql_validate_mpp_contract(rte->relid);
	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);
	rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);
	for (int attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		if (!TupleDescAttr(tupdesc, attnum - 1)->attisdropped)
			target_attrs = lappend_int(target_attrs, attnum);
	}
	table_close(rel, NoLock);

	result = lappend(result,
					 makeString(flightsql_connection_url(rte->relid)));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option(table->options,
											  "table_name"))));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  table->options, "schema_name", ""))));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  table->options, "catalog_name", ""))));
	result = lappend(result, target_attrs);
	result = lappend(
		result,
		makeInteger(flightsql_get_merged_int_option(
						table->options, server->options, "batch_rows",
						AF_DEFAULT_BATCH_ROWS, 1, INT_MAX)));
	result = lappend(
		result,
		makeInteger(flightsql_get_merged_int_option(
						table->options, server->options, "max_batch_bytes",
						AF_DEFAULT_MAX_BATCH_BYTES, 0, INT_MAX)));
	result = lappend(
		result,
		makeInteger(flightsql_get_merged_int_option(
						table->options, server->options, "timeout_ms",
						-1, -1, INT_MAX)));
	{
		char	   *row_count_check =
			flightsql_get_merged_option(table->options, server->options,
									   "ingest_row_count_check");

		result = lappend(
			result,
			makeInteger(row_count_check == NULL ||
						strcmp(row_count_check, "exact") == 0));
	}
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  server->options, "tls_ca_file", ""))));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  server->options,
							  "tls_client_cert_file", ""))));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  server->options,
							  "tls_client_key_file", ""))));
	result = lappend(
		result,
		makeString(pstrdup(flightsql_get_option_or_default(
							  server->options, "auth_token_file", ""))));
	{
		char	   *transaction_mode =
			flightsql_get_merged_option(
				table->options, server->options,
				"write_transaction_mode");

		if (transaction_mode == NULL)
			transaction_mode =
				(char *) AF_FLIGHT_SQL_WRITE_TRANSACTION_AUTO_COMMIT;
		flightsql_validate_transaction_mode(transaction_mode);
		result = lappend(result, makeString(pstrdup(transaction_mode)));
	}
	result = lappend(result, makeString(pstrdup("")));
	{
		char	   *routing_mode =
			flightsql_get_merged_option(
				table->options, server->options,
				"write_routing_mode");

		if (routing_mode == NULL)
			routing_mode =
				(char *) AF_FLIGHT_SQL_WRITE_ROUTING_ORIGIN;
		flightsql_validate_write_routing_mode(routing_mode);
		result = lappend(result, makeString(pstrdup(routing_mode)));
	}
	result = lappend(result, makeString(pstrdup("")));
	return result;
}

static void
flightsql_BeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo,
							 List *fdw_private, int subplan_index, int eflags)
{
	FlightSqlModifyState *state;

	(void) mtstate;
	(void) subplan_index;
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;
	if (list_length(fdw_private) != FLIGHTSQL_MODIFY_NUM_ITEMS)
		elog(ERROR, "flightsql_fdw: invalid private modify state");

	state = (FlightSqlModifyState *) palloc0(sizeof(*state));
	state->url =
		pstrdup(strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_URL)));
	state->table_name =
		pstrdup(strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_TABLE)));
	state->schema_name =
		pstrdup(strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_SCHEMA)));
	state->catalog_name =
		pstrdup(strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_CATALOG)));
	state->target_attrs =
		(List *) list_nth(fdw_private, FLIGHTSQL_MODIFY_TARGET_ATTRS);
	state->batch_rows =
		intVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_BATCH_ROWS));
	state->max_batch_bytes =
		intVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_MAX_BATCH_BYTES));
	state->timeout_ms =
		intVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_TIMEOUT_MS));
	state->verify_ingested_rows =
		intVal(list_nth(fdw_private,
						FLIGHTSQL_MODIFY_VERIFY_INGESTED_ROWS)) != 0;
	state->security_options.tls_ca_file =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_TLS_CA_FILE)));
	state->security_options.tls_client_cert_file =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_TLS_CLIENT_CERT_FILE)));
	state->security_options.tls_client_key_file =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_TLS_CLIENT_KEY_FILE)));
	state->security_options.auth_token_file =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_AUTH_TOKEN_FILE)));
	state->transaction_id =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_TRANSACTION_ID)));
	state->write_routing_mode =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_WRITE_ROUTING_MODE)));
	state->serialized_mpp_plan =
		pstrdup(strVal(list_nth(fdw_private,
							   FLIGHTSQL_MODIFY_MPP_PLAN)));
	if (strcmp(state->write_routing_mode,
			   AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED) == 0 &&
		Gp_role != GP_ROLE_DISPATCH)
	{
		if (state->serialized_mpp_plan[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("Flight SQL MPP ingest plan was not prepared on the dispatcher")));
		af_flightsql_mpp_select_route(
			state->serialized_mpp_plan, GpIdentity.segindex,
			getgpsegmentCount(), &state->mpp_route);
		state->url = state->mpp_route.url;
	}
	rinfo->ri_FdwState = state;
}

static TupleTableSlot *
flightsql_ExecForeignInsert(EState *estate, ResultRelInfo *rinfo,
							TupleTableSlot *slot,
							TupleTableSlot *plan_slot)
{
	FlightSqlModifyState *state =
		(FlightSqlModifyState *) rinfo->ri_FdwState;

	(void) estate;
	(void) plan_slot;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("flightsql_fdw ingest state is not initialized")));
	if (Gp_role == GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Flight SQL ingest requires QE segment execution")));

	if (state->writer_state == NULL)
	{
		PG_TRY();
		{
			state->writer_state =
				af_flightsql_writer_open(
					rinfo->ri_RelationDesc, state->target_attrs,
					state->url, state->table_name, state->schema_name,
					state->catalog_name, state->batch_rows,
					state->max_batch_bytes, state->timeout_ms,
					state->verify_ingested_rows,
					state->transaction_id,
					&state->security_options,
					strcmp(
						state->write_routing_mode,
						AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED) == 0 ?
					&state->mpp_route : NULL);
			af_resource_attach(&state->writer_state);
		}
		PG_CATCH();
		{
			if (state->writer_state != NULL)
			{
				af_flightsql_writer_abort(state->writer_state);
				state->writer_state = NULL;
			}
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	PG_TRY();
	{
		af_flightsql_writer_append(state->writer_state, slot);
	}
	PG_CATCH();
	{
		af_flightsql_writer_abort(state->writer_state);
		state->writer_state = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();
	return slot;
}

static void
flightsql_EndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
	FlightSqlModifyState *state =
		(FlightSqlModifyState *) rinfo->ri_FdwState;

	(void) estate;
	if (state != NULL && state->writer_state != NULL)
	{
		af_flightsql_writer_finish(state->writer_state);
		state->writer_state = NULL;
	}
	rinfo->ri_FdwState = NULL;
}

static void
flightsql_BeginForeignInsert(ModifyTableState *mtstate,
							 ResultRelInfo *rinfo)
{
	(void) mtstate;
	(void) rinfo;
}

static void
flightsql_EndForeignInsert(EState *estate, ResultRelInfo *rinfo)
{
	flightsql_EndForeignModify(estate, rinfo);
}

static int
flightsql_IsForeignRelUpdatable(Relation rel)
{
	(void) rel;
	return (1 << CMD_INSERT);
}

static void
flightsql_ExplainForeignModify(ModifyTableState *mtstate,
							   ResultRelInfo *rinfo,
							   List *fdw_private,
							   int subplan_index,
							   ExplainState *es)
{
	(void) mtstate;
	(void) rinfo;
	(void) subplan_index;
	if (!es->verbose ||
		list_length(fdw_private) != FLIGHTSQL_MODIFY_NUM_ITEMS)
		return;

	ExplainPropertyText(
		"Flight SQL Ingest Table",
		strVal(list_nth(fdw_private, FLIGHTSQL_MODIFY_TABLE)), es);
	ExplainPropertyText(
		"Flight SQL Ingest Mode", "append", es);
	ExplainPropertyText(
		"Flight SQL Transaction Mode",
		strVal(list_nth(fdw_private,
						FLIGHTSQL_MODIFY_TRANSACTION_MODE)), es);
	ExplainPropertyText(
		"Flight SQL Write Routing",
		strVal(list_nth(fdw_private,
						FLIGHTSQL_MODIFY_WRITE_ROUTING_MODE)), es);
}

static void
flightsql_validate_mpp_contract(Oid foreigntableid)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	int			segment_count = getgpsegmentCount();

	if (table->exec_location != FTEXECLOCATION_ALL_SEGMENTS)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("flightsql_fdw requires mpp_execute='all segments'")));

	if (server->num_segments != segment_count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("flightsql_fdw num_segments (%d) does not match "
						"the current Greengage segment count (%d)",
						server->num_segments, segment_count),
				 errhint("Remove num_segments from the foreign server or set it to %d.",
						 segment_count)));
}

static void
flightsql_validate_transaction_mode(const char *mode)
{
	if (mode == NULL ||
		(strcmp(mode, AF_FLIGHT_SQL_WRITE_TRANSACTION_AUTO_COMMIT) != 0 &&
		 strcmp(mode, AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED) != 0))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid write_transaction_mode value \"%s\"",
						mode == NULL ? "" : mode),
				 errhint("Use \"auto_commit\" or \"required\".")));
}

static void
flightsql_validate_write_routing_mode(const char *mode)
{
	if (mode == NULL ||
		(strcmp(mode, AF_FLIGHT_SQL_WRITE_ROUTING_ORIGIN) != 0 &&
		 strcmp(mode, AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED) != 0))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid write_routing_mode value \"%s\"",
						mode == NULL ? "" : mode),
				 errhint("Use \"origin\" or \"planned\".")));
}

static void
flightsql_validate_write_capabilities(
	const char *mode, const ArrowFlightSqlCapabilities *capabilities)
{
	const char *server_name =
		capabilities->server_name == NULL ||
		capabilities->server_name[0] == '\0' ?
		"unknown" : capabilities->server_name;
	const char *server_version =
		capabilities->server_version == NULL ||
		capabilities->server_version[0] == '\0' ?
		"unknown" : capabilities->server_version;

	if (capabilities->bulk_ingestion ==
		AF_FLIGHT_SQL_CAPABILITY_UNSUPPORTED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Flight SQL server does not support bulk ingestion"),
				 errdetail("Server: %s %s.", server_name, server_version)));

	if (strcmp(mode, AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED) != 0)
		return;

	if (capabilities->bulk_ingestion !=
		AF_FLIGHT_SQL_CAPABILITY_SUPPORTED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("write_transaction_mode=required needs explicitly "
						"advertised Flight SQL bulk ingestion support"),
				 errdetail("Server: %s %s.", server_name, server_version)));

	if (capabilities->ingest_transactions !=
		AF_FLIGHT_SQL_CAPABILITY_SUPPORTED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("write_transaction_mode=required needs Flight SQL ingest transaction support"),
				 errdetail("Server: %s %s.", server_name, server_version)));

	if (capabilities->transaction_support <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("write_transaction_mode=required needs the Flight SQL transaction API"),
				 errdetail("Server: %s %s.", server_name, server_version)));

	if (capabilities->sql_transactions !=
		AF_FLIGHT_SQL_CAPABILITY_SUPPORTED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("write_transaction_mode=required needs SQL transaction support"),
				 errdetail("Server: %s %s.", server_name, server_version)));
}

static void
flightsql_register_remote_transaction(
	const char *url, const char *transaction_id,
	const ArrowFlightSecurityOptions *security)
{
	MemoryContext old_context;
	FlightSqlRemoteTransaction *transaction;

	if (transaction_id == NULL || transaction_id[0] == '\0')
		elog(ERROR, "flightsql_fdw received an empty transaction id");

	old_context = MemoryContextSwitchTo(TopTransactionContext);
	transaction =
		(FlightSqlRemoteTransaction *) palloc0(sizeof(*transaction));
	transaction->url = pstrdup(url);
	transaction->transaction_id = pstrdup(transaction_id);
	transaction->security_options.tls_ca_file =
		pstrdup(security->tls_ca_file == NULL ? "" :
				security->tls_ca_file);
	transaction->security_options.tls_client_cert_file =
		pstrdup(security->tls_client_cert_file == NULL ? "" :
				security->tls_client_cert_file);
	transaction->security_options.tls_client_key_file =
		pstrdup(security->tls_client_key_file == NULL ? "" :
				security->tls_client_key_file);
	transaction->security_options.auth_token_file =
		pstrdup(security->auth_token_file == NULL ? "" :
				security->auth_token_file);
	transaction->security_options.endpoint_location_allowlist =
		pstrdup(security->endpoint_location_allowlist == NULL ? "" :
				security->endpoint_location_allowlist);
	transaction->subtransaction_id = GetCurrentSubTransactionId();
	transaction->next = flightsql_remote_transactions;
	flightsql_remote_transactions = transaction;
	MemoryContextSwitchTo(old_context);

	flightsql_ensure_xact_callbacks();
}

static void
flightsql_finish_remote_transaction(
	FlightSqlRemoteTransaction *transaction, bool commit,
	bool error_on_failure)
{
	char	   *error;

	if (transaction == NULL || transaction->finished)
		return;

	error =
		af_flightsql_end_transaction(
			transaction->url, transaction->transaction_id, commit,
			&transaction->security_options);
	if (error == NULL)
	{
		transaction->finished = true;
		return;
	}

	if (error_on_failure)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not %s Flight SQL transaction",
						commit ? "commit" : "rollback"),
				 errdetail_internal("%s", error)));

	ereport(WARNING,
			(errmsg("could not rollback Flight SQL transaction"),
			 errdetail_internal("%s", error)));
	transaction->finished = true;
}

static void
flightsql_register_remote_plan(
	QueryDesc *owner_query_desc, const char *url,
	const char *serialized_plan,
	const char *plan_id, int timeout_ms, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security)
{
	MemoryContext old_context;
	FlightSqlRemotePlan *plan;

	if (owner_query_desc == NULL || serialized_plan == NULL ||
		serialized_plan[0] == '\0' || plan_id == NULL ||
		plan_id[0] == '\0')
		elog(ERROR, "flightsql_fdw received an invalid MPP plan");

	old_context = MemoryContextSwitchTo(TopTransactionContext);
	plan = (FlightSqlRemotePlan *) palloc0(sizeof(*plan));
	plan->owner_query_desc = owner_query_desc;
	plan->url = pstrdup(url);
	plan->serialized_plan = pstrdup(serialized_plan);
	plan->timeout_ms = timeout_ms;
	plan->max_plan_bytes = max_plan_bytes;
	plan->security_options.tls_ca_file =
		pstrdup(security->tls_ca_file == NULL ? "" :
				security->tls_ca_file);
	plan->security_options.tls_client_cert_file =
		pstrdup(security->tls_client_cert_file == NULL ? "" :
				security->tls_client_cert_file);
	plan->security_options.tls_client_key_file =
		pstrdup(security->tls_client_key_file == NULL ? "" :
				security->tls_client_key_file);
	plan->security_options.auth_token_file =
		pstrdup(security->auth_token_file == NULL ? "" :
				security->auth_token_file);
	plan->security_options.endpoint_location_allowlist =
		pstrdup(security->endpoint_location_allowlist == NULL ? "" :
				security->endpoint_location_allowlist);
	plan->subtransaction_id = GetCurrentSubTransactionId();
	plan->next = flightsql_remote_plans;
	flightsql_remote_plans = plan;
	MemoryContextSwitchTo(old_context);

	flightsql_ensure_xact_callbacks();
}

static void
flightsql_finish_remote_plan(
	FlightSqlRemotePlan *plan, bool complete, bool error_on_failure)
{
	char	   *error;

	if (plan == NULL || plan->finished)
		return;

	error = complete ?
		af_flightsql_mpp_complete_plan(
			plan->url, plan->serialized_plan, plan->timeout_ms,
			plan->max_plan_bytes, &plan->security_options) :
		af_flightsql_mpp_abort_plan(
			plan->url, plan->serialized_plan, plan->timeout_ms,
			plan->max_plan_bytes, &plan->security_options);
	if (error == NULL)
	{
		plan->finished = true;
		return;
	}

	if (error_on_failure)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not %s Flight SQL MPP ingest plan",
						complete ? "complete" : "abort"),
				 errdetail_internal("%s", error)));

	ereport(WARNING,
			(errmsg("could not abort Flight SQL MPP ingest plan"),
			 errdetail_internal("%s", error)));
	plan->finished = true;
}

static void
flightsql_complete_query_plans(QueryDesc *query_desc)
{
	FlightSqlRemotePlan *plan;

	for (plan = flightsql_remote_plans;
		 plan != NULL;
		 plan = plan->next)
	{
		if (plan->owner_query_desc == query_desc && !plan->finished)
			flightsql_finish_remote_plan(plan, true, true);
	}
}

static void
flightsql_ensure_xact_callbacks(void)
{
	if (!flightsql_xact_callback_registered)
	{
		RegisterXactCallback(flightsql_xact_callback, NULL);
		RegisterSubXactCallback(flightsql_subxact_callback, NULL);
		flightsql_xact_callback_registered = true;
	}
}

static char *
flightsql_generate_operation_id(void)
{
	unsigned char bytes[16];

	if (!pg_strong_random(bytes, sizeof(bytes)))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate Flight SQL MPP operation id")));

	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	return psprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
		"%02x%02x%02x%02x%02x%02x",
		bytes[0], bytes[1], bytes[2], bytes[3],
		bytes[4], bytes[5], bytes[6], bytes[7],
		bytes[8], bytes[9], bytes[10], bytes[11],
		bytes[12], bytes[13], bytes[14], bytes[15]);
}

static void
flightsql_xact_callback(XactEvent event, void *arg)
{
	FlightSqlRemoteTransaction *transaction;
	FlightSqlRemotePlan *plan;

	(void) arg;
	if (flightsql_remote_transactions == NULL &&
		flightsql_remote_plans == NULL)
		return;

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			for (plan = flightsql_remote_plans;
				 plan != NULL;
				 plan = plan->next)
			{
				if (!plan->finished)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_ERROR),
							 errmsg("Flight SQL MPP ingest plan was not completed")));
			}
			for (transaction = flightsql_remote_transactions;
				 transaction != NULL;
				 transaction = transaction->next)
				flightsql_finish_remote_transaction(
					transaction, true, true);
			break;

		case XACT_EVENT_PRE_PREPARE:
			if (flightsql_remote_transactions != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot prepare a Greengage transaction with an active Flight SQL transaction"),
						 errhint("Commit or roll back the transaction without PREPARE TRANSACTION.")));
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			for (plan = flightsql_remote_plans;
				 plan != NULL;
				 plan = plan->next)
				flightsql_finish_remote_plan(plan, false, false);
			for (transaction = flightsql_remote_transactions;
				 transaction != NULL;
				 transaction = transaction->next)
				flightsql_finish_remote_transaction(
					transaction, false, false);
			flightsql_remote_transactions = NULL;
			flightsql_remote_plans = NULL;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			flightsql_remote_transactions = NULL;
			flightsql_remote_plans = NULL;
			break;
	}
}

static void
flightsql_subxact_callback(SubXactEvent event,
						   SubTransactionId my_subid,
						   SubTransactionId parent_subid,
						   void *arg)
{
	FlightSqlRemoteTransaction **link = &flightsql_remote_transactions;
	FlightSqlRemotePlan **plan_link = &flightsql_remote_plans;

	(void) arg;
	while (*plan_link != NULL)
	{
		FlightSqlRemotePlan *plan = *plan_link;

		if (plan->subtransaction_id != my_subid)
		{
			plan_link = &plan->next;
			continue;
		}

		if (event == SUBXACT_EVENT_COMMIT_SUB)
		{
			plan->subtransaction_id = parent_subid;
			plan_link = &plan->next;
		}
		else if (event == SUBXACT_EVENT_ABORT_SUB)
		{
			flightsql_finish_remote_plan(plan, false, false);
			*plan_link = plan->next;
		}
		else
			plan_link = &plan->next;
	}

	while (*link != NULL)
	{
		FlightSqlRemoteTransaction *transaction = *link;

		if (transaction->subtransaction_id != my_subid)
		{
			link = &transaction->next;
			continue;
		}

		if (event == SUBXACT_EVENT_COMMIT_SUB)
		{
			transaction->subtransaction_id = parent_subid;
			link = &transaction->next;
		}
		else if (event == SUBXACT_EVENT_ABORT_SUB)
		{
			flightsql_finish_remote_transaction(
				transaction, false, false);
			*link = transaction->next;
		}
		else
			link = &transaction->next;
	}
}

static bool
flightsql_is_valid_option(const char *option, Oid catalog)
{
	if (catalog == ForeignServerRelationId)
		return strcmp(option, "host") == 0 ||
			strcmp(option, "port") == 0 ||
			strcmp(option, "tls") == 0 ||
			strcmp(option, "tls_ca_file") == 0 ||
			strcmp(option, "tls_client_cert_file") == 0 ||
			strcmp(option, "tls_client_key_file") == 0 ||
			strcmp(option, "auth_token_file") == 0 ||
			strcmp(option, "endpoint_location_allowlist") == 0 ||
			strcmp(option, "timeout_ms") == 0 ||
			strcmp(option, "max_endpoints") == 0 ||
			strcmp(option, "max_plan_bytes") == 0 ||
			strcmp(option, "batch_rows") == 0 ||
			strcmp(option, "max_batch_bytes") == 0 ||
			strcmp(option, "ingest_row_count_check") == 0 ||
			strcmp(option, "write_transaction_mode") == 0 ||
			strcmp(option, "write_routing_mode") == 0 ||
			strcmp(option, "predicate_pushdown") == 0 ||
			strcmp(option, "mpp_execute") == 0;

	if (catalog == ForeignTableRelationId)
		return strcmp(option, "table_name") == 0 ||
			strcmp(option, "schema_name") == 0 ||
			strcmp(option, "catalog_name") == 0 ||
			strcmp(option, "rows") == 0 ||
			strcmp(option, "batch_rows") == 0 ||
			strcmp(option, "max_batch_bytes") == 0 ||
			strcmp(option, "ingest_row_count_check") == 0 ||
			strcmp(option, "write_transaction_mode") == 0 ||
			strcmp(option, "write_routing_mode") == 0 ||
			strcmp(option, "predicate_pushdown") == 0;

	if (catalog == AttributeRelationId)
		return strcmp(option, "column_name") == 0;

	return false;
}

static char *
flightsql_get_option(List *options, const char *name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}
	return NULL;
}

static char *
flightsql_get_merged_option(List *table_options, List *server_options,
							const char *name)
{
	char	   *value = flightsql_get_option(table_options, name);

	return value == NULL ? flightsql_get_option(server_options, name) : value;
}

static char *
flightsql_get_option_or_default(List *options, const char *name,
								const char *default_value)
{
	char	   *value = flightsql_get_option(options, name);

	return value == NULL ? (char *) default_value : value;
}

static int
flightsql_get_merged_int_option(List *table_options, List *server_options,
								const char *name, int default_value,
								int min_value, int max_value)
{
	char	   *value = flightsql_get_merged_option(table_options,
												  server_options,
												  name);

	return value == NULL ?
		default_value :
		af_parse_int_option_value(value, name, min_value, max_value);
}

static bool
flightsql_parse_bool(const char *value, const char *name)
{
	bool		result;

	if (!parse_bool(value, &result))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid boolean value \"%s\" for flightsql_fdw option \"%s\"",
						value, name)));
	return result;
}

static void
flightsql_validate_endpoint_location_allowlist(const char *value,
											   bool tls_enabled)
{
	const char *expected_scheme = tls_enabled ?
		"grpc+tls://" : "grpc+tcp://";
	const Size	expected_scheme_len = strlen(expected_scheme);
	const char *entry = value;

	if (strlen(value) > AF_MAX_ENDPOINT_LOCATION_ALLOWLIST_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("flightsql_fdw endpoint_location_allowlist is too "
						"long")));

	for (;;)
	{
		const char *end = strchr(entry, ',');
		Size		entry_len = end == NULL ? strlen(entry) : end - entry;
		const char *authority;
		const char *port_separator;
		const char *port_text;
		char	   *port_end;
		long		port;

		if (entry_len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("flightsql_fdw endpoint_location_allowlist contains "
							"an empty entry")));

		for (Size index = 0; index < entry_len; index++)
		{
			unsigned char ch = (unsigned char) entry[index];

			if (ch <= 0x20 || ch == 0x7f)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("flightsql_fdw endpoint_location_allowlist "
								"contains whitespace or control characters")));
		}

		if (entry_len <= expected_scheme_len ||
			strncmp(entry, expected_scheme, expected_scheme_len) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid endpoint location in flightsql_fdw "
							"endpoint_location_allowlist"),
					 errdetail("Every entry must use the \"%s\" scheme.",
							   expected_scheme)));

		authority = entry + expected_scheme_len;
		if (memchr(authority, '/', entry_len - expected_scheme_len) != NULL ||
			memchr(authority, '?', entry_len - expected_scheme_len) != NULL ||
			memchr(authority, '#', entry_len - expected_scheme_len) != NULL ||
			memchr(authority, '@', entry_len - expected_scheme_len) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid endpoint location in flightsql_fdw "
							"endpoint_location_allowlist"),
					 errdetail("Endpoint locations must contain only a host "
							   "and an explicit port.")));

		if (*authority == '[')
		{
			const char *closing_bracket =
				(const char *) memchr(authority, ']',
									 entry_len - expected_scheme_len);

			if (closing_bracket == NULL || closing_bracket == authority + 1 ||
				closing_bracket + 1 >= entry + entry_len ||
				closing_bracket[1] != ':')
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid endpoint location in flightsql_fdw "
								"endpoint_location_allowlist"),
						 errdetail("IPv6 endpoint locations must use "
								   "\"[host]:port\" syntax.")));
			port_separator = closing_bracket + 1;
		}
		else
		{
			const char *cursor;

			port_separator = NULL;
			for (cursor = authority; cursor < entry + entry_len; cursor++)
			{
				if (*cursor == ':')
				{
					if (port_separator != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid endpoint location in "
										"flightsql_fdw "
										"endpoint_location_allowlist"),
								 errdetail("IPv6 endpoint locations must use "
										   "\"[host]:port\" syntax.")));
					port_separator = cursor;
				}
			}
			if (port_separator == NULL || port_separator == authority ||
				port_separator + 1 >= entry + entry_len)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid endpoint location in flightsql_fdw "
								"endpoint_location_allowlist"),
						 errdetail("Endpoint locations must use "
								   "\"host:port\" syntax.")));
		}

		port_text = port_separator + 1;
		errno = 0;
		port = strtol(port_text, &port_end, 10);
		if (errno != 0 || port_end != entry + entry_len ||
			port < 1 || port > 65535)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid endpoint port in flightsql_fdw "
							"endpoint_location_allowlist")));

		if (end == NULL)
			break;
		entry = end + 1;
	}
}

static ArrowFlightSecurityOptions
flightsql_security_options(List *server_options)
{
	ArrowFlightSecurityOptions result;

	memset(&result, 0, sizeof(result));
	result.tls_ca_file =
		flightsql_get_option(server_options, "tls_ca_file");
	result.tls_client_cert_file =
		flightsql_get_option(server_options, "tls_client_cert_file");
	result.tls_client_key_file =
		flightsql_get_option(server_options, "tls_client_key_file");
	result.auth_token_file =
		flightsql_get_option(server_options, "auth_token_file");
	result.endpoint_location_allowlist =
		flightsql_get_option(server_options, "endpoint_location_allowlist");
	return result;
}

static char *
flightsql_connection_url(Oid foreigntableid)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	char	   *host = flightsql_get_option(server->options, "host");
	char	   *port = flightsql_get_option(server->options, "port");
	char	   *tls = flightsql_get_option(server->options, "tls");
	char	   *timeout = flightsql_get_option(server->options, "timeout_ms");
	StringInfoData url;
	bool		has_query = false;

	if (host == NULL || port == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("flightsql_fdw server requires options \"host\" and \"port\"")));

	initStringInfo(&url);
	appendStringInfo(&url, AF_SCHEME "%s:%s/flightsql", host, port);
	if (tls != NULL)
	{
		appendStringInfo(&url, "?tls=%s", tls);
		has_query = true;
	}
	if (timeout != NULL)
		appendStringInfo(&url, "%ctimeout_ms=%s", has_query ? '&' : '?',
						 timeout);
	return url.data;
}

static bool
flightsql_predicate_pushdown_enabled(Oid foreigntableid)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);
	char	   *value = flightsql_get_merged_option(table->options,
												   server->options,
												   "predicate_pushdown");

	return value == NULL ?
		true :
		flightsql_parse_bool(value, "predicate_pushdown");
}

static List *
flightsql_projected_attrs(RelOptInfo *baserel, Index scan_relid,
						  List *local_exprs)
{
	Bitmapset  *attrs_used = NULL;
	List	   *result = NIL;
	ListCell   *lc;
	int			member = -1;

	pull_varattnos((Node *) baserel->reltarget->exprs, scan_relid,
				   &attrs_used);
	foreach(lc, local_exprs)
		pull_varattnos((Node *) lfirst(lc), scan_relid, &attrs_used);

	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		bms_free(attrs_used);
		return list_make1(makeInteger(AF_FDW_PROJECT_ALL));
	}

	while ((member = bms_next_member(attrs_used, member)) >= 0)
	{
		AttrNumber	attnum =
			(AttrNumber) (member + FirstLowInvalidHeapAttributeNumber);

		if (attnum > 0)
			result = lappend(result, makeInteger(attnum));
	}
	bms_free(attrs_used);
	return result;
}

static char *
flightsql_build_query(Oid foreigntableid, List *projected_attrs,
					  List *remote_exprs, Index scan_relid,
					  bool *project_all)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	Relation	rel = table_open(foreigntableid, NoLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);
	char	   *table_name =
		flightsql_get_option(table->options, "table_name");
	char	   *schema_name =
		flightsql_get_option(table->options, "schema_name");
	char	   *catalog_name =
		flightsql_get_option(table->options, "catalog_name");
	StringInfoData sql;
	bool		first = true;
	bool		select_all = list_length(projected_attrs) == 1 &&
		intVal(linitial(projected_attrs)) == AF_FDW_PROJECT_ALL;
	int			readable_columns = 0;
	ListCell   *lc;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			readable_columns++;
	}
	if (!select_all && list_length(projected_attrs) == readable_columns)
		select_all = true;

	initStringInfo(&sql);
	appendStringInfoString(&sql, "SELECT ");
	*project_all = select_all;

	if (select_all)
	{
		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
			List	   *column_options;
			char	   *column_name;

			if (attr->attisdropped)
				continue;
			column_options = GetForeignColumnOptions(foreigntableid, i + 1);
			column_name = flightsql_get_option(column_options, "column_name");
			if (column_name == NULL)
				column_name = NameStr(attr->attname);
			if (!first)
				appendStringInfoString(&sql, ", ");
			appendStringInfo(&sql, "%s AS %s", quote_identifier(column_name),
							 quote_identifier(NameStr(attr->attname)));
			first = false;
		}
	}
	else
	{
		foreach(lc, projected_attrs)
		{
			int			attnum = intVal(lfirst(lc));
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
			List	   *column_options =
				GetForeignColumnOptions(foreigntableid, attnum);
			char	   *column_name =
				flightsql_get_option(column_options, "column_name");

			if (column_name == NULL)
				column_name = NameStr(attr->attname);
			if (!first)
				appendStringInfoString(&sql, ", ");
			appendStringInfo(&sql, "%s AS %s", quote_identifier(column_name),
							 quote_identifier(NameStr(attr->attname)));
			first = false;
		}
	}

	if (first)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("flightsql_fdw foreign table has no readable columns")));

	appendStringInfoString(&sql, " FROM ");
	if (catalog_name != NULL)
		appendStringInfo(&sql, "%s.", quote_identifier(catalog_name));
	if (schema_name != NULL)
		appendStringInfo(&sql, "%s.", quote_identifier(schema_name));
	appendStringInfoString(&sql, quote_identifier(table_name));
	af_flightsql_append_predicates(&sql, foreigntableid, scan_relid,
								   remote_exprs);
	table_close(rel, NoLock);
	return sql.data;
}

static void
flightsql_init_projection(ArrowFlightSqlFdwExecState *state,
						   TupleDesc tupdesc, List *projected_attrs,
						   bool project_all)
{
	ListCell   *lc;

	state->project_all = project_all;
	state->projected_nattrs = tupdesc->natts;
	state->projected_attrs =
		(bool *) palloc0(sizeof(bool) * tupdesc->natts);
	if (project_all)
	{
		for (int i = 0; i < tupdesc->natts; i++)
			state->projected_attrs[i] =
				!TupleDescAttr(tupdesc, i)->attisdropped;
		return;
	}

	foreach(lc, projected_attrs)
	{
		int			attnum = intVal(lfirst(lc));

		if (attnum > 0 && attnum <= tupdesc->natts)
			state->projected_attrs[attnum - 1] = true;
	}
}

static void
flightsql_validate_local_types(Oid foreigntableid)
{
	Relation	rel = table_open(foreigntableid, NoLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;
		if (!flightsql_type_supported(attr->atttypid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" has unsupported Greengage type %s for flightsql_fdw",
							NameStr(attr->attname),
							format_type_be(attr->atttypid))));
	}
	table_close(rel, NoLock);
}

static bool
flightsql_type_supported(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case BYTEAOID:
		case UUIDOID:
		case INTERVALOID:
		case TIMEOID:
		case CASHOID:
		case INETOID:
		case CIDROID:
		case MACADDROID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case JSONOID:
		case JSONBOID:
			return true;
		default:
			return af_type_is_enum(typid);
	}
}
