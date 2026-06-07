/*-------------------------------------------------------------------------
 *
 * arrowflight_fdw.cpp
 *    Readable arrowflight_fdw planner and executor callbacks.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"

#include <errno.h>
#include <limits.h>
#include <string.h>

extern List *untransformRelOptions(Datum options);

PG_FUNCTION_INFO_V1(arrowflight_fdw_handler);
PG_FUNCTION_INFO_V1(arrowflight_fdw_validator);

Datum arrowflight_fdw_handler(PG_FUNCTION_ARGS);
Datum arrowflight_fdw_validator(PG_FUNCTION_ARGS);

}

static void af_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
									 Oid foreigntableid);
static void af_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreigntableid);
static ForeignScan *af_fdw_GetForeignPlan(PlannerInfo *root,
									  RelOptInfo *baserel,
									  Oid foreigntableid,
									  ForeignPath *best_path,
									  List *tlist,
									  List *scan_clauses,
									  Plan *outer_plan);
static void af_fdw_BeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *af_fdw_IterateForeignScan(ForeignScanState *node);
static void af_fdw_ReScanForeignScan(ForeignScanState *node);
static void af_fdw_EndForeignScan(ForeignScanState *node);
static bool af_fdw_is_valid_option(const char *option, Oid catalog);
static char *af_fdw_get_option(List *options, const char *name);
static char *af_fdw_get_merged_option(List *table_options,
									  List *server_options,
									  const char *name);
static ArrowFlightSecurityOptions af_fdw_security_options(List *server_options);
static void af_fdw_validate_security_options(List *options, Oid catalog);
static void af_fdw_validate_path_option(const char *path);
static bool af_fdw_parse_bool_option_value(const char *value,
										   const char *name);
static bool af_fdw_path_has_segments_component(const char *path);
static char *af_fdw_build_read_path(const char *path, bool append_segments);
static void af_fdw_append_query_option(StringInfo url,
									   bool *has_query,
									   const char *name,
									   const char *value);
static void af_fdw_append_missing_query_option(StringInfo url,
											  bool *has_query,
											  const char *original_url,
											  const char *name,
											  const char *value);
static char *af_fdw_append_default_query_options(const char *url,
												 List *table_options,
												 List *server_options,
												 bool relative_read);
static char *af_fdw_build_read_url(ForeignTable *table);
static ArrowFlightFdwPlanState *af_fdw_get_plan_state(Oid foreigntableid);
static List *af_fdw_build_projected_attrs(RelOptInfo *baserel,
									  Index scan_relid,
									  List *scan_clauses);
static void af_fdw_init_projected_attrs(ArrowFlightFdwExecState *exec_state,
										TupleDesc tupdesc,
										List *fdw_private);

extern "C" Datum
arrowflight_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = af_fdw_GetForeignRelSize;
	routine->GetForeignPaths = af_fdw_GetForeignPaths;
	routine->GetForeignPlan = af_fdw_GetForeignPlan;
	routine->BeginForeignScan = af_fdw_BeginForeignScan;
	routine->IterateForeignScan = af_fdw_IterateForeignScan;
	routine->ReScanForeignScan = af_fdw_ReScanForeignScan;
	routine->EndForeignScan = af_fdw_EndForeignScan;
	routine->PlanForeignModify = af_fdw_PlanForeignModify;
	routine->BeginForeignModify = af_fdw_BeginForeignModify;
	routine->ExecForeignInsert = af_fdw_ExecForeignInsert;
	routine->EndForeignModify = af_fdw_EndForeignModify;
	routine->BeginForeignInsert = af_fdw_BeginForeignInsert;
	routine->EndForeignInsert = af_fdw_EndForeignInsert;
	routine->IsForeignRelUpdatable = af_fdw_IsForeignRelUpdatable;
	routine->ExplainForeignModify = af_fdw_ExplainForeignModify;

	PG_RETURN_POINTER(routine);
}

extern "C" Datum
arrowflight_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!af_fdw_is_valid_option(def->defname, catalog))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\" for arrowflight_fdw",
							def->defname),
					 errhint("Use server options such as \"host\" and \"port\", and table options such as \"path\", \"url\", and \"rows\".")));

		if (strcmp(def->defname, "url") == 0)
		{
			const char *url = defGetString(def);
			char		projection_pushdown[16];

			if (url == NULL || !af_has_scheme(url))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid Arrow Flight FDW URL"),
						 errhint("Use an arrowflight:// URL.")));

			(void) af_get_url_int_option(url, "timeout_ms", -1, -1,
										 INT_MAX);
			(void) af_get_url_int_option(url, "max_batch_bytes",
										  AF_DEFAULT_MAX_BATCH_BYTES, 0,
										  INT_MAX);
			(void) af_get_url_int_option(url, "retry_count", 0, 0,
										  AF_MAX_RETRY_COUNT);
			(void) af_get_url_int_option(url, "retry_backoff_ms", 100, 0,
										  AF_MAX_RETRY_BACKOFF_MS);
			if (af_get_url_option(url, "projection_pushdown",
								  projection_pushdown,
								  sizeof(projection_pushdown)))
			{
				af_validate_projection_pushdown(projection_pushdown);
				if (strcmp(projection_pushdown,
						   AF_PROJECTION_PUSHDOWN_REQUIRE) == 0 &&
					!af_get_url_bool_option(url, "use_get_flight_info", false))
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("projection_pushdown=require requires use_get_flight_info=true")));
			}
		}
		else if (strcmp(def->defname, "path") == 0)
			af_fdw_validate_path_option(defGetString(def));
		else if (strcmp(def->defname, "write_mode") == 0)
			af_validate_fdw_write_mode(defGetString(def));
		else if (strcmp(def->defname, "operation_metadata") == 0)
			af_validate_fdw_operation_metadata(defGetString(def));
		else if (strcmp(def->defname, "batch_rows") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "batch_rows", 1, INT_MAX);
		else if (strcmp(def->defname, "max_batch_bytes") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "max_batch_bytes", 0, INT_MAX);
		else if (strcmp(def->defname, "timeout_ms") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "timeout_ms", -1, INT_MAX);
		else if (strcmp(def->defname, "retry_count") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "retry_count", 0,
											 AF_MAX_RETRY_COUNT);
		else if (strcmp(def->defname, "retry_backoff_ms") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "retry_backoff_ms", 0,
											 AF_MAX_RETRY_BACKOFF_MS);
		else if (strcmp(def->defname, "host") == 0)
		{
			const char *host = defGetString(def);

			if (host == NULL || *host == '\0' ||
				strlen(host) > AF_MAX_HOST_LEN)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid host value for arrowflight_fdw")));
		}
		else if (strcmp(def->defname, "port") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "port", 1, 65535);
		else if (strcmp(def->defname, "tls_ca_file") == 0 ||
				 strcmp(def->defname, "tls_client_cert_file") == 0 ||
				 strcmp(def->defname, "tls_client_key_file") == 0 ||
				 strcmp(def->defname, "auth_token_file") == 0)
		{
			const char *value = defGetString(def);

			if (value == NULL || *value == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid empty value for arrowflight_fdw option \"%s\"",
								def->defname)));
		}
		else if (strcmp(def->defname, "tls") == 0 ||
				 strcmp(def->defname, "use_get_flight_info") == 0 ||
				 strcmp(def->defname, "insert_dist_by_key") == 0)
			(void) defGetBoolean(def);
		else if (strcmp(def->defname, "insert_dist_by_key_weight") == 0)
			(void) af_parse_int_option_value(defGetString(def),
											 "insert_dist_by_key_weight",
											 0, INT_MAX);
		else if (strcmp(def->defname, "flight_endpoint_policy") == 0)
			af_validate_endpoint_policy(defGetString(def));
		else if (strcmp(def->defname, "projection_pushdown") == 0)
			af_validate_projection_pushdown(defGetString(def));
		else if (strcmp(def->defname, "rows") == 0)
		{
			const char *value = defGetString(def);
			char	   *endptr;
			double		rows;

			errno = 0;
			rows = strtod(value, &endptr);
			if (errno != 0 || endptr == value || *endptr != '\0' ||
				rows <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("invalid rows value \"%s\" for arrowflight_fdw",
								value)));
		}
	}

	af_fdw_validate_security_options(options_list, catalog);

	PG_RETURN_VOID();
}

static void
af_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid)
{
	ArrowFlightFdwPlanState *fdw_private;

	(void) root;

	fdw_private = af_fdw_get_plan_state(foreigntableid);
	baserel->fdw_private = (void *) fdw_private;
	baserel->rows = fdw_private->rows;
}

static void
af_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	Cost		startup_cost;
	Cost		total_cost;
	ForeignPath *pathnode;
	ForeignTable *table;

	startup_cost = baserel->baserestrictcost.startup;
	total_cost = startup_cost +
		(cpu_tuple_cost + baserel->baserestrictcost.per_tuple) *
		baserel->rows;

	pathnode = create_foreignscan_path(root,
									   baserel,
									   NULL,
									   baserel->rows,
									   startup_cost,
									   total_cost,
									   NIL,
									   baserel->lateral_relids,
									   NULL,
									   NIL);
	table = GetForeignTable(foreigntableid);
	if (table->exec_location == FTEXECLOCATION_ALL_SEGMENTS)
		CdbPathLocus_MakeStrewn(&pathnode->path.locus,
								getgpsegmentCount());
	else
		pathnode->path.locus = cdbpathlocus_from_baserel(root, baserel);
	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = false;
	pathnode->path.sameslice_relids = baserel->relids;

	add_path(baserel, (Path *) pathnode);
}

static ForeignScan *
af_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
					  Oid foreigntableid, ForeignPath *best_path,
					  List *tlist, List *scan_clauses, Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;
	List	   *projected_attrs;

	(void) root;
	(void) foreigntableid;
	(void) best_path;

	projected_attrs = af_fdw_build_projected_attrs(baserel, scan_relid,
												  scan_clauses);
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,
							projected_attrs,
							NIL,
							NIL,
							outer_plan);
}

static void
af_fdw_BeginForeignScan(ForeignScanState *node, int eflags)
{
	ArrowFlightFdwPlanState *plan_state;
	ArrowFlightFdwExecState *exec_state;
	Relation	rel = node->ss.ss_currentRelation;
	ForeignTable *table;
	ForeignScan *scan_plan = (ForeignScan *) node->ss.ps.plan;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (rel == NULL)
		elog(ERROR, "arrowflight_fdw: scan relation is not available");

	plan_state = af_fdw_get_plan_state(RelationGetRelid(rel));
	table = GetForeignTable(RelationGetRelid(rel));

	exec_state = (ArrowFlightFdwExecState *) palloc0(sizeof(*exec_state));
	exec_state->url = pstrdup(plan_state->url);
	exec_state->security_options = plan_state->security_options;
	exec_state->all_segments =
		(table->exec_location == FTEXECLOCATION_ALL_SEGMENTS);
	af_fdw_init_projected_attrs(exec_state, RelationGetDescr(rel),
								scan_plan->fdw_private);

	node->fdw_state = (void *) exec_state;
}

static TupleTableSlot *
af_fdw_IterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ArrowFlightFdwExecState *exec_state =
		(ArrowFlightFdwExecState *) node->fdw_state;

	ExecClearTuple(slot);

	if (exec_state == NULL)
		return slot;

	if (exec_state->all_segments && Gp_role == GP_ROLE_DISPATCH)
		return slot;

	return af_flight_stream_next_slot(node->ss.ss_currentRelation,
									 exec_state->url,
									 &exec_state->flight_state,
									 slot,
									 exec_state->project_all,
									 exec_state->projected_attrs,
									 &exec_state->security_options);
}

static void
af_fdw_ReScanForeignScan(ForeignScanState *node)
{
	ArrowFlightFdwExecState *exec_state =
		(ArrowFlightFdwExecState *) node->fdw_state;

	if (exec_state == NULL)
		return;

	if (exec_state->flight_state != NULL)
	{
		af_flight_stream_close(exec_state->flight_state);
		exec_state->flight_state = NULL;
	}
}

static void
af_fdw_EndForeignScan(ForeignScanState *node)
{
	ArrowFlightFdwExecState *exec_state =
		(ArrowFlightFdwExecState *) node->fdw_state;

	if (exec_state == NULL)
		return;

	if (exec_state->flight_state != NULL)
	{
		af_flight_stream_close(exec_state->flight_state);
		exec_state->flight_state = NULL;
	}

	node->fdw_state = NULL;
}

static bool
af_fdw_is_valid_option(const char *option, Oid catalog)
{
	if (catalog == ForeignTableRelationId)
		return strcmp(option, "url") == 0 ||
			strcmp(option, "path") == 0 ||
			strcmp(option, "rows") == 0 ||
			strcmp(option, "write_mode") == 0 ||
			strcmp(option, "operation_metadata") == 0 ||
			strcmp(option, "batch_rows") == 0 ||
			strcmp(option, "max_batch_bytes") == 0 ||
			strcmp(option, "timeout_ms") == 0 ||
			strcmp(option, "retry_count") == 0 ||
			strcmp(option, "retry_backoff_ms") == 0 ||
			strcmp(option, "use_get_flight_info") == 0 ||
			strcmp(option, "flight_endpoint_policy") == 0 ||
			strcmp(option, "projection_pushdown") == 0;

	if (catalog == ForeignServerRelationId)
		return strcmp(option, "host") == 0 ||
			strcmp(option, "port") == 0 ||
			strcmp(option, "tls") == 0 ||
			strcmp(option, "tls_ca_file") == 0 ||
			strcmp(option, "tls_client_cert_file") == 0 ||
			strcmp(option, "tls_client_key_file") == 0 ||
			strcmp(option, "auth_token_file") == 0 ||
			strcmp(option, "mpp_execute") == 0 ||
			strcmp(option, "write_mode") == 0 ||
			strcmp(option, "batch_rows") == 0 ||
			strcmp(option, "max_batch_bytes") == 0 ||
			strcmp(option, "timeout_ms") == 0 ||
			strcmp(option, "retry_count") == 0 ||
			strcmp(option, "retry_backoff_ms") == 0 ||
			strcmp(option, "use_get_flight_info") == 0 ||
			strcmp(option, "flight_endpoint_policy") == 0 ||
			strcmp(option, "projection_pushdown") == 0;

	if (catalog == ForeignDataWrapperRelationId)
		return false;

	if (catalog == AttributeRelationId)
		return strcmp(option, "insert_dist_by_key") == 0 ||
			strcmp(option, "insert_dist_by_key_weight") == 0;

	return false;
}

static char *
af_fdw_get_option(List *options, const char *name)
{
	ListCell   *cell;

	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}

	return NULL;
}

static char *
af_fdw_get_merged_option(List *table_options, List *server_options,
						 const char *name)
{
	char	   *value = af_fdw_get_option(table_options, name);

	if (value != NULL)
		return value;

	return af_fdw_get_option(server_options, name);
}

static ArrowFlightSecurityOptions
af_fdw_security_options(List *server_options)
{
	ArrowFlightSecurityOptions options;

	memset(&options, 0, sizeof(options));
	options.tls_ca_file =
		af_fdw_get_option(server_options, "tls_ca_file");
	options.tls_client_cert_file =
		af_fdw_get_option(server_options, "tls_client_cert_file");
	options.tls_client_key_file =
		af_fdw_get_option(server_options, "tls_client_key_file");
	options.auth_token_file =
		af_fdw_get_option(server_options, "auth_token_file");
	if (options.tls_ca_file != NULL)
		options.tls_ca_file = pstrdup(options.tls_ca_file);
	if (options.tls_client_cert_file != NULL)
		options.tls_client_cert_file = pstrdup(options.tls_client_cert_file);
	if (options.tls_client_key_file != NULL)
		options.tls_client_key_file = pstrdup(options.tls_client_key_file);
	if (options.auth_token_file != NULL)
		options.auth_token_file = pstrdup(options.auth_token_file);

	return options;
}

static void
af_fdw_validate_security_options(List *options, Oid catalog)
{
	char	   *tls;
	char	   *ca;
	char	   *cert;
	char	   *key;
	char	   *token;

	if (catalog != ForeignServerRelationId)
		return;

	tls = af_fdw_get_option(options, "tls");
	ca = af_fdw_get_option(options, "tls_ca_file");
	cert = af_fdw_get_option(options, "tls_client_cert_file");
	key = af_fdw_get_option(options, "tls_client_key_file");
	token = af_fdw_get_option(options, "auth_token_file");

	if ((cert == NULL) != (key == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw options \"tls_client_cert_file\" and \"tls_client_key_file\" must be set together")));

	if ((ca != NULL || cert != NULL || key != NULL || token != NULL) &&
		(tls == NULL || !af_fdw_parse_bool_option_value(tls, "tls")))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw TLS/auth file options require tls=true")));
}

static void
af_fdw_validate_path_option(const char *path)
{
	StringInfoData url;
	const char *trimmed_path;
	ArrowFlightEndpoint endpoint;

	if (path == NULL || *path == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw path option must not be empty")));

	if (af_has_scheme(path))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw path option must be relative"),
				 errhint("Use option \"url\" for absolute arrowflight:// endpoints.")));

	if (strchr(path, '?') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw path option must not contain a query string"),
				 errhint("Use table or server options for connection settings.")));

	trimmed_path = path;
	while (*trimmed_path == '/')
		trimmed_path++;

	initStringInfo(&url);
	appendStringInfo(&url, AF_SCHEME "localhost:1/%s", trimmed_path);
	af_parse_flight_endpoint(url.data, &endpoint);
}

static bool
af_fdw_parse_bool_option_value(const char *value, const char *name)
{
	if (value == NULL)
		return false;

	if (pg_strcasecmp(value, "true") == 0 ||
		pg_strcasecmp(value, "on") == 0 ||
		pg_strcasecmp(value, "yes") == 0 ||
		strcmp(value, "1") == 0)
		return true;

	if (pg_strcasecmp(value, "false") == 0 ||
		pg_strcasecmp(value, "off") == 0 ||
		pg_strcasecmp(value, "no") == 0 ||
		strcmp(value, "0") == 0)
		return false;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid boolean value \"%s\" for arrowflight_fdw option \"%s\"",
					value, name)));

	return false;
}

static bool
af_fdw_path_has_segments_component(const char *path)
{
	const char *cursor;

	if (path == NULL)
		return false;

	cursor = path;
	while (*cursor == '/')
		cursor++;

	while (*cursor != '\0')
	{
		const char *next = strchr(cursor, '/');
		Size		len = next == NULL ? strlen(cursor) : (Size) (next - cursor);

		if (len == strlen("segments") &&
			strncmp(cursor, "segments", len) == 0)
			return true;

		if (next == NULL)
			break;

		cursor = next + 1;
	}

	return false;
}

static char *
af_fdw_build_read_path(const char *path, bool append_segments)
{
	StringInfoData out;

	initStringInfo(&out);
	appendStringInfoString(&out, path);

	if (!append_segments)
		return out.data;

	if (af_fdw_path_has_segments_component(path))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("arrowflight_fdw relative read path must not contain a \"segments\" component"),
				 errhint("The FDW appends the current Greengage segment count automatically; use absolute \"url\" only for direct endpoint tickets.")));

	if (out.len > 0 && out.data[out.len - 1] != '/')
		appendStringInfoChar(&out, '/');
	appendStringInfo(&out, "segments/%d", getgpsegmentCount());

	return out.data;
}

static void
af_fdw_append_query_option(StringInfo url, bool *has_query,
						   const char *name, const char *value)
{
	if (value == NULL)
		return;

	appendStringInfoChar(url, *has_query ? '&' : '?');
	appendStringInfo(url, "%s=%s", name, value);
	*has_query = true;
}

static void
af_fdw_append_missing_query_option(StringInfo url, bool *has_query,
								   const char *original_url,
								   const char *name, const char *value)
{
	char		existing[64];

	if (value == NULL)
		return;

	if (af_get_url_option(original_url, name, existing, sizeof(existing)))
		return;

	af_fdw_append_query_option(url, has_query, name, value);
}

static char *
af_fdw_append_default_query_options(const char *url,
									List *table_options,
									List *server_options,
									bool relative_read)
{
	StringInfoData out;
	bool		has_query;
	char	   *use_get_flight_info;
	char	   *flight_endpoint_policy;

	initStringInfo(&out);
	appendStringInfoString(&out, url);
	has_query = strchr(url, '?') != NULL;

	af_fdw_append_missing_query_option(&out, &has_query, url, "tls",
									   af_fdw_get_option(server_options,
														 "tls"));
	af_fdw_append_missing_query_option(&out, &has_query, url, "timeout_ms",
									   af_fdw_get_merged_option(table_options,
																server_options,
																"timeout_ms"));
	af_fdw_append_missing_query_option(&out, &has_query, url,
									   "max_batch_bytes",
									   af_fdw_get_merged_option(table_options,
																server_options,
																"max_batch_bytes"));
	af_fdw_append_missing_query_option(&out, &has_query, url, "retry_count",
									   af_fdw_get_merged_option(table_options,
																server_options,
																"retry_count"));
	af_fdw_append_missing_query_option(&out, &has_query, url,
									   "retry_backoff_ms",
									   af_fdw_get_merged_option(table_options,
																server_options,
																"retry_backoff_ms"));

	use_get_flight_info =
		af_fdw_get_merged_option(table_options, server_options,
								 "use_get_flight_info");
	if (use_get_flight_info == NULL && relative_read)
		use_get_flight_info = (char *) "true";
	af_fdw_append_missing_query_option(&out, &has_query, url,
									   "use_get_flight_info",
									   use_get_flight_info);

	flight_endpoint_policy =
		af_fdw_get_merged_option(table_options, server_options,
								 "flight_endpoint_policy");
	if (flight_endpoint_policy == NULL && relative_read)
		flight_endpoint_policy = (char *) AF_ENDPOINT_POLICY_SEGMENT_INDEX;
	af_fdw_append_missing_query_option(&out, &has_query, url,
									   "flight_endpoint_policy",
									   flight_endpoint_policy);

	af_fdw_append_missing_query_option(&out, &has_query, url,
									   "projection_pushdown",
									   af_fdw_get_merged_option(table_options,
																server_options,
																"projection_pushdown"));

	return out.data;
}

static char *
af_fdw_build_read_url(ForeignTable *table)
{
	ForeignServer *server = GetForeignServer(table->serverid);
	char	   *url = af_fdw_get_option(table->options, "url");
	char	   *path;
	char	   *host;
	char	   *port;
	char	   *use_get_flight_info;
	bool		append_segments;
	StringInfoData out;

	if (url != NULL)
		return af_fdw_append_default_query_options(url,
												   table->options,
												   server->options,
												   false);

	path = af_fdw_get_option(table->options, "path");
	if (path == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw foreign table requires option \"path\" or \"url\"")));

	host = af_fdw_get_option(server->options, "host");
	port = af_fdw_get_option(server->options, "port");
	if (host == NULL || port == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw relative path requires server options \"host\" and \"port\"")));

	use_get_flight_info =
		af_fdw_get_merged_option(table->options, server->options,
								 "use_get_flight_info");
	append_segments = use_get_flight_info == NULL ||
		af_fdw_parse_bool_option_value(use_get_flight_info,
									   "use_get_flight_info");
	path = af_fdw_build_read_path(path, append_segments);
	while (*path == '/')
		path++;

	initStringInfo(&out);
	appendStringInfo(&out, AF_SCHEME "%s:%s/%s", host, port, path);

	return af_fdw_append_default_query_options(out.data,
											  table->options,
											  server->options,
											  true);
}

static ArrowFlightFdwPlanState *
af_fdw_get_plan_state(Oid foreigntableid)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ArrowFlightFdwPlanState *state =
		(ArrowFlightFdwPlanState *) palloc0(sizeof(*state));
	char	   *rows_value;
	ArrowFlightEndpoint endpoint;

	state->url = af_fdw_build_read_url(table);
	state->security_options =
		af_fdw_security_options(GetForeignServer(table->serverid)->options);

	if (!af_has_scheme(state->url))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid Arrow Flight FDW URL"),
				 errhint("Use an arrowflight:// URL.")));

	af_parse_flight_endpoint(state->url, &endpoint);

	rows_value = af_fdw_get_option(table->options, "rows");
	state->rows = rows_value != NULL ? strtod(rows_value, NULL) : 1000.0;
	if (state->rows <= 0)
		state->rows = 1000.0;

	return state;
}

static List *
af_fdw_build_projected_attrs(RelOptInfo *baserel, Index scan_relid,
							 List *scan_clauses)
{
	Bitmapset  *attrs_used = NULL;
	List	   *projected_attrs = NIL;
	ListCell   *lc;
	int			member;
	int			i;

	if (scan_relid <= 0)
		return list_make1(makeInteger(AF_FDW_PROJECT_ALL));

	pull_varattnos((Node *) baserel->reltarget->exprs, scan_relid,
				   &attrs_used);

	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, scan_relid, &attrs_used);
	}

	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
	{
		bms_free(attrs_used);
		return list_make1(makeInteger(AF_FDW_PROJECT_ALL));
	}

	for (i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++)
	{
		if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			bms_free(attrs_used);
			return list_make1(makeInteger(AF_FDW_PROJECT_ALL));
		}
	}

	member = -1;
	while ((member = bms_next_member(attrs_used, member)) >= 0)
	{
		AttrNumber	attnum =
			(AttrNumber) (member + FirstLowInvalidHeapAttributeNumber);

		if (attnum > 0)
			projected_attrs = lappend(projected_attrs, makeInteger(attnum));
	}

	bms_free(attrs_used);
	return projected_attrs;
}

static void
af_fdw_init_projected_attrs(ArrowFlightFdwExecState *exec_state,
							TupleDesc tupdesc, List *fdw_private)
{
	ListCell   *lc;
	int			i;

	exec_state->projected_nattrs = tupdesc->natts;
	exec_state->projected_attrs =
		(bool *) palloc0(sizeof(bool) * exec_state->projected_nattrs);

	foreach(lc, fdw_private)
	{
		Node	   *node = (Node *) lfirst(lc);
		int			attnum;

		if (!IsA(node, Integer))
			elog(ERROR, "arrowflight_fdw: invalid projection metadata");

		attnum = intVal(node);
		if (attnum == AF_FDW_PROJECT_ALL)
		{
			exec_state->project_all = true;
			break;
		}

		if (attnum <= 0 || attnum > tupdesc->natts)
			elog(ERROR, "arrowflight_fdw: invalid projected attribute %d",
				 attnum);

		if (!exec_state->projected_attrs[attnum - 1])
		{
			exec_state->projected_attrs[attnum - 1] = true;
			exec_state->projected_attr_count++;
		}
	}

	if (exec_state->project_all)
	{
		exec_state->projected_attr_count = 0;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			exec_state->projected_attrs[i] = true;
			exec_state->projected_attr_count++;
		}
	}
}
