/*-------------------------------------------------------------------------
 *
 * arrowflight_fdw_write.cpp
 *	  Writable FDW planning skeleton for Arrow Flight DoPut.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "access/table.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"

}

#include <limits.h>
#include <string.h>

typedef struct ArrowFlightFdwModifyState
{
	char	   *operation_id;
	char	   *url;
	char	   *dataset;
	char	   *write_mode;
	char	   *operation_metadata;
	List	   *target_attrs;
	int			batch_rows;
	int			max_batch_bytes;
	int			timeout_ms;
	int			retry_count;
	int			retry_backoff_ms;
	ArrowFlightSecurityOptions security_options;
	bool		stream_opened;
	void	   *writer_state;
} ArrowFlightFdwModifyState;

typedef enum ArrowFlightFdwModifyPrivateIndex
{
	AF_FDW_MODIFY_OPERATION_ID = 0,
	AF_FDW_MODIFY_URL,
	AF_FDW_MODIFY_DATASET,
	AF_FDW_MODIFY_WRITE_MODE,
	AF_FDW_MODIFY_OPERATION_METADATA,
	AF_FDW_MODIFY_TARGET_ATTRS,
	AF_FDW_MODIFY_BATCH_ROWS,
	AF_FDW_MODIFY_MAX_BATCH_BYTES,
	AF_FDW_MODIFY_TIMEOUT_MS,
	AF_FDW_MODIFY_RETRY_COUNT,
	AF_FDW_MODIFY_RETRY_BACKOFF_MS,
	AF_FDW_MODIFY_TLS_CA_FILE,
	AF_FDW_MODIFY_TLS_CLIENT_CERT_FILE,
	AF_FDW_MODIFY_TLS_CLIENT_KEY_FILE,
	AF_FDW_MODIFY_AUTH_TOKEN_FILE,
	AF_FDW_MODIFY_NUM_ITEMS
} ArrowFlightFdwModifyPrivateIndex;

static char *af_fdw_get_option(List *options, const char *name);
static char *af_fdw_get_merged_option(List *table_options,
									  List *server_options,
									  const char *name);
static char *af_fdw_get_option_or_default(List *options, const char *name,
										  const char *default_value);
static int	af_fdw_get_merged_int_option_or_default(List *table_options,
												   List *server_options,
												   const char *name,
												   int default_value,
												   int min_value,
												   int max_value);
static void af_fdw_append_query_option(StringInfo url,
									   bool *has_query,
									   const char *name,
									   const char *value);
static void af_fdw_append_missing_query_option(StringInfo url,
											  bool *has_query,
											  const char *original_url,
											  const char *name,
											  const char *value);
static char *af_fdw_append_write_default_query_options(const char *url,
													   List *server_options);
static char *af_fdw_build_write_url(ForeignTable *table,
									ForeignServer *server);
static char *af_fdw_derive_dataset_from_url(const char *url);
static char *af_fdw_derive_dataset_from_ticket(const char *ticket);
static char *af_fdw_make_operation_id(Oid relid, int subplan_index);
static bool af_fdw_write_type_supported(Form_pg_attribute attr,
										const char **reason);
static ArrowFlightFdwModifyState *af_fdw_create_modify_state(List *fdw_private);
static char *af_fdw_modify_private_string(List *fdw_private,
										  ArrowFlightFdwModifyPrivateIndex index);
static char *af_fdw_modify_private_optional_string(List *fdw_private,
												  ArrowFlightFdwModifyPrivateIndex index);
static int	af_fdw_modify_private_int(List *fdw_private,
									  ArrowFlightFdwModifyPrivateIndex index);

List *
af_fdw_PlanForeignModify(PlannerInfo *root, ModifyTable *plan,
						 Index resultRelation, int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	ForeignTable *table;
	ForeignServer *server;
	char	   *url;
	char	   *dataset;
	char	   *write_mode;
	char	   *operation_metadata;
	char	   *operation_id;
	List	   *target_attrs = NIL;
	List	   *fdw_private = NIL;
	TupleDesc	tupdesc;
	int			attnum;

	if (operation != CMD_INSERT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("arrowflight_fdw supports only INSERT for foreign table writes")));

	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);

	url = af_fdw_build_write_url(table, server);
	dataset = af_fdw_derive_dataset_from_url(url);
	af_validate_fdw_dataset(dataset);

	write_mode = af_fdw_get_merged_option(table->options, server->options,
										  "write_mode");
	if (write_mode == NULL)
		write_mode = (char *) AF_FDW_WRITE_MODE_STAGING;
	af_validate_fdw_write_mode(write_mode);
	operation_metadata =
		af_fdw_get_option_or_default(table->options, "operation_metadata", "");
	af_validate_fdw_operation_metadata(operation_metadata);

	rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);
	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (attr->attisdropped)
			continue;

		const char *unsupported_reason = NULL;

		if (!af_fdw_write_type_supported(attr, &unsupported_reason))
		{
			char	   *attname = pstrdup(NameStr(attr->attname));
			Oid			typid = attr->atttypid;

			table_close(rel, NoLock);
			if (unsupported_reason != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("column \"%s\" has unsupported type oid %u for Arrow Flight FDW write",
								attname, typid),
						 errdetail("%s", unsupported_reason)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("column \"%s\" has unsupported type oid %u for Arrow Flight FDW write",
								attname, typid)));
		}

		target_attrs = lappend_int(target_attrs, attnum);
	}

	operation_id = af_fdw_make_operation_id(rte->relid, subplan_index);

	fdw_private = lappend(fdw_private, makeString(operation_id));
	fdw_private = lappend(fdw_private, makeString(pstrdup(url)));
	fdw_private = lappend(fdw_private, makeString(pstrdup(dataset)));
	fdw_private = lappend(fdw_private, makeString(pstrdup(write_mode)));
	fdw_private = lappend(fdw_private, makeString(pstrdup(operation_metadata)));
	fdw_private = lappend(fdw_private, target_attrs);
	fdw_private =
		lappend(fdw_private,
				makeInteger(af_fdw_get_merged_int_option_or_default(
								table->options, server->options, "batch_rows",
								AF_DEFAULT_BATCH_ROWS, 1, INT_MAX)));
	fdw_private =
		lappend(fdw_private,
				makeInteger(af_fdw_get_merged_int_option_or_default(
								table->options, server->options,
								"max_batch_bytes",
								AF_DEFAULT_MAX_BATCH_BYTES, 0, INT_MAX)));
	fdw_private =
		lappend(fdw_private,
				makeInteger(af_fdw_get_merged_int_option_or_default(
								table->options, server->options,
								"timeout_ms", -1, -1,
								INT_MAX)));
	fdw_private =
		lappend(fdw_private,
				makeInteger(af_fdw_get_merged_int_option_or_default(
								table->options, server->options,
								"retry_count", 0, 0,
								AF_MAX_RETRY_COUNT)));
	fdw_private =
		lappend(fdw_private,
				makeInteger(af_fdw_get_merged_int_option_or_default(
								table->options, server->options,
								"retry_backoff_ms", 100, 0,
								AF_MAX_RETRY_BACKOFF_MS)));
	fdw_private =
		lappend(fdw_private,
				makeString(pstrdup(af_fdw_get_option_or_default(
									 server->options, "tls_ca_file", ""))));
	fdw_private =
		lappend(fdw_private,
				makeString(pstrdup(af_fdw_get_option_or_default(
									 server->options,
									 "tls_client_cert_file", ""))));
	fdw_private =
		lappend(fdw_private,
				makeString(pstrdup(af_fdw_get_option_or_default(
									 server->options,
									 "tls_client_key_file", ""))));
	fdw_private =
		lappend(fdw_private,
				makeString(pstrdup(af_fdw_get_option_or_default(
									 server->options, "auth_token_file",
									 ""))));

	table_close(rel, NoLock);
	return fdw_private;
}

void
af_fdw_BeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo,
						  List *fdw_private, int subplan_index, int eflags)
{
	(void) mtstate;
	(void) subplan_index;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	rinfo->ri_FdwState = af_fdw_create_modify_state(fdw_private);
}

TupleTableSlot *
af_fdw_ExecForeignInsert(EState *estate, ResultRelInfo *rinfo,
						 TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	ArrowFlightFdwModifyState *state =
		(ArrowFlightFdwModifyState *) rinfo->ri_FdwState;

	(void) estate;
	(void) planSlot;

	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("arrowflight_fdw write state is not initialized")));

	if (Gp_role == GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Arrow Flight FDW write requires QE segment execution"),
				 errdetail("operation_id=%s dataset=%s", state->operation_id,
						   state->dataset)));

	if (!state->stream_opened)
	{
		PG_TRY();
		{
			state->writer_state =
				af_fdw_writer_open(rinfo->ri_RelationDesc,
								   state->target_attrs,
								   state->operation_id,
								   state->url,
								   state->dataset,
								   state->write_mode,
								   state->operation_metadata,
								   state->batch_rows,
								   state->max_batch_bytes,
								   state->timeout_ms,
								   state->retry_count,
								   state->retry_backoff_ms,
								   &state->security_options);
			state->stream_opened = true;
		}
		PG_CATCH();
		{
			if (state->writer_state != NULL)
			{
				af_fdw_writer_abort(state->writer_state);
				state->writer_state = NULL;
			}
			state->stream_opened = false;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	PG_TRY();
	{
		af_fdw_writer_append(state->writer_state, slot);
	}
	PG_CATCH();
	{
		if (state->writer_state != NULL)
		{
			af_fdw_writer_abort(state->writer_state);
			state->writer_state = NULL;
		}
		state->stream_opened = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return slot;
}

void
af_fdw_EndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
	ArrowFlightFdwModifyState *state =
		(ArrowFlightFdwModifyState *) rinfo->ri_FdwState;

	(void) estate;

	if (state != NULL && state->writer_state != NULL)
	{
		af_fdw_writer_finish(state->writer_state);
		state->writer_state = NULL;
		state->stream_opened = false;
	}

	rinfo->ri_FdwState = NULL;
}

void
af_fdw_BeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo)
{
	(void) mtstate;
	(void) rinfo;
}

void
af_fdw_EndForeignInsert(EState *estate, ResultRelInfo *rinfo)
{
	af_fdw_EndForeignModify(estate, rinfo);
}

int
af_fdw_IsForeignRelUpdatable(Relation rel)
{
	(void) rel;

	return (1 << CMD_INSERT);
}

void
af_fdw_ExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo,
							List *fdw_private, int subplan_index,
							ExplainState *es)
{
	(void) mtstate;
	(void) rinfo;
	(void) subplan_index;

	if (!es->verbose)
		return;

	ExplainPropertyText("Arrow Flight Endpoint",
						af_fdw_modify_private_string(fdw_private,
													 AF_FDW_MODIFY_URL),
						es);
	ExplainPropertyText("Arrow Flight Dataset",
						af_fdw_modify_private_string(fdw_private,
													 AF_FDW_MODIFY_DATASET),
						es);
	ExplainPropertyText("Arrow Flight Write Mode",
						af_fdw_modify_private_string(fdw_private,
													 AF_FDW_MODIFY_WRITE_MODE),
						es);
	ExplainPropertyInteger("Arrow Flight Batch Rows",
						   NULL,
						   af_fdw_modify_private_int(fdw_private,
													 AF_FDW_MODIFY_BATCH_ROWS),
						   es);
	ExplainPropertyInteger("Arrow Flight Max Batch Bytes",
						   NULL,
						   af_fdw_modify_private_int(fdw_private,
													 AF_FDW_MODIFY_MAX_BATCH_BYTES),
						   es);
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

static char *
af_fdw_get_option_or_default(List *options, const char *name,
							 const char *default_value)
{
	char	   *value = af_fdw_get_option(options, name);

	return value != NULL ? value : (char *) default_value;
}

static int
af_fdw_get_merged_int_option_or_default(List *table_options,
										List *server_options,
										const char *name,
										int default_value,
										int min_value,
										int max_value)
{
	char	   *value =
		af_fdw_get_merged_option(table_options, server_options, name);

	if (value == NULL)
		return default_value;

	return af_parse_int_option_value(value, name, min_value, max_value);
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
af_fdw_append_write_default_query_options(const char *url, List *server_options)
{
	StringInfoData out;
	bool		has_query;

	initStringInfo(&out);
	appendStringInfoString(&out, url);
	has_query = strchr(url, '?') != NULL;

	af_fdw_append_missing_query_option(&out, &has_query, url, "tls",
									   af_fdw_get_option(server_options,
														 "tls"));

	return out.data;
}

static char *
af_fdw_build_write_url(ForeignTable *table, ForeignServer *server)
{
	char	   *url = af_fdw_get_option(table->options, "url");
	char	   *path;
	char	   *host;
	char	   *port;
	StringInfoData out;
	ArrowFlightEndpoint endpoint;

	if (url != NULL)
	{
		url = af_fdw_append_write_default_query_options(url,
													   server->options);
		if (!af_has_scheme(url))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
					 errmsg("invalid Arrow Flight FDW URL"),
					 errhint("Use an arrowflight:// URL.")));
		af_parse_flight_endpoint(url, &endpoint);
		return url;
	}

	host = af_fdw_get_option(server->options, "host");
	port = af_fdw_get_option(server->options, "port");
	if (host == NULL || port == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw INSERT requires option \"url\" or server options \"host\" and \"port\"")));

	path = af_fdw_get_option(table->options, "path");
	if (path == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw INSERT requires table option \"path\" or \"url\"")));
	while (*path == '/')
		path++;

	initStringInfo(&out);
	appendStringInfo(&out, AF_SCHEME "%s:%s/%s", host, port, path);
	url = af_fdw_append_write_default_query_options(out.data,
												   server->options);
	af_parse_flight_endpoint(url, &endpoint);

	return url;
}

static char *
af_fdw_derive_dataset_from_url(const char *url)
{
	ArrowFlightEndpoint endpoint;

	af_parse_flight_endpoint(url, &endpoint);
	return af_fdw_derive_dataset_from_ticket(endpoint.ticket);
}

static char *
af_fdw_derive_dataset_from_ticket(const char *ticket)
{
	const char *start = ticket;
	const char *component_start = NULL;
	const char *component_end = NULL;
	char	   *dataset;

	if (ticket == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw INSERT could not derive dataset from endpoint path")));

	while (*start == '/')
		start++;

	if (strncmp(start, "write/", strlen("write/")) == 0)
	{
		component_start = start + strlen("write/");
		component_end = strchr(component_start, '/');
	}
	else if (strncmp(start, "dataset/", strlen("dataset/")) == 0)
	{
		component_start = start + strlen("dataset/");
		component_end = strchr(component_start, '/');
	}
	else
	{
		component_end = start + strlen(start);
		while (component_end > start && component_end[-1] == '/')
			component_end--;
		component_start = component_end;
		while (component_start > start && component_start[-1] != '/')
			component_start--;
	}

	if (component_end == NULL)
		component_end = component_start + strlen(component_start);

	if (component_start == NULL || component_end <= component_start)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("arrowflight_fdw INSERT could not derive dataset from endpoint path"),
				 errhint("Use path like \"<dataset>\".")));

	dataset = pnstrdup(component_start, component_end - component_start);
	af_validate_fdw_dataset(dataset);
	return dataset;
}

static char *
af_fdw_make_operation_id(Oid relid, int subplan_index)
{
	return psprintf("afw-%d-%d-%u-%d-%d",
					gp_session_id,
					gp_command_count,
					relid,
					subplan_index,
					MyProcPid);
}

static bool
af_fdw_write_type_supported(Form_pg_attribute attr, const char **reason)
{
	Oid			typid;

	if (reason != NULL)
		*reason = NULL;
	if (attr == NULL)
		return false;

	typid = attr->atttypid;
	switch (typid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case BYTEAOID:
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
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
			return true;
		case NUMERICOID:
		{
			int32		typmod;
			int32		precision;
			int32		scale;

			typmod = attr->atttypmod;
			if (typmod < (int32) VARHDRSZ)
			{
				if (reason != NULL)
					*reason = "constrained numeric(p,s) is required";
				return false;
			}

			typmod -= VARHDRSZ;
			precision = (typmod >> 16) & 0xffff;
			scale = typmod & 0xffff;
			if (precision < 1 || precision > 38 || scale < 0 || scale > precision)
			{
				if (reason != NULL)
					*reason = "numeric precision must be between 1 and 38 and scale must be between 0 and precision";
				return false;
			}
			return true;
		}
		default:
			return af_type_uses_text_exchange(typid);
	}
}

static ArrowFlightFdwModifyState *
af_fdw_create_modify_state(List *fdw_private)
{
	ArrowFlightFdwModifyState *state;

	if (list_length(fdw_private) != AF_FDW_MODIFY_NUM_ITEMS)
		elog(ERROR, "arrowflight_fdw: invalid write metadata");

	state = (ArrowFlightFdwModifyState *) palloc0(sizeof(*state));
	state->operation_id =
		pstrdup(af_fdw_modify_private_string(fdw_private,
											 AF_FDW_MODIFY_OPERATION_ID));
	state->url =
		pstrdup(af_fdw_modify_private_string(fdw_private,
											 AF_FDW_MODIFY_URL));
	state->dataset =
		pstrdup(af_fdw_modify_private_string(fdw_private,
											 AF_FDW_MODIFY_DATASET));
	state->write_mode =
		pstrdup(af_fdw_modify_private_string(fdw_private,
											 AF_FDW_MODIFY_WRITE_MODE));
	state->operation_metadata =
		pstrdup(af_fdw_modify_private_string(fdw_private,
											 AF_FDW_MODIFY_OPERATION_METADATA));
	state->target_attrs =
		(List *) list_nth(fdw_private, AF_FDW_MODIFY_TARGET_ATTRS);
	state->batch_rows =
		af_fdw_modify_private_int(fdw_private, AF_FDW_MODIFY_BATCH_ROWS);
	state->max_batch_bytes =
		af_fdw_modify_private_int(fdw_private, AF_FDW_MODIFY_MAX_BATCH_BYTES);
	state->timeout_ms =
		af_fdw_modify_private_int(fdw_private, AF_FDW_MODIFY_TIMEOUT_MS);
	state->retry_count =
		af_fdw_modify_private_int(fdw_private, AF_FDW_MODIFY_RETRY_COUNT);
	state->retry_backoff_ms =
		af_fdw_modify_private_int(fdw_private, AF_FDW_MODIFY_RETRY_BACKOFF_MS);
	state->security_options.tls_ca_file =
		af_fdw_modify_private_optional_string(fdw_private,
											  AF_FDW_MODIFY_TLS_CA_FILE);
	state->security_options.tls_client_cert_file =
		af_fdw_modify_private_optional_string(fdw_private,
											  AF_FDW_MODIFY_TLS_CLIENT_CERT_FILE);
	state->security_options.tls_client_key_file =
		af_fdw_modify_private_optional_string(fdw_private,
											  AF_FDW_MODIFY_TLS_CLIENT_KEY_FILE);
	state->security_options.auth_token_file =
		af_fdw_modify_private_optional_string(fdw_private,
											  AF_FDW_MODIFY_AUTH_TOKEN_FILE);

	return state;
}

static char *
af_fdw_modify_private_string(List *fdw_private,
							 ArrowFlightFdwModifyPrivateIndex index)
{
	Node	   *node;

	if (list_length(fdw_private) <= (int) index)
		elog(ERROR, "arrowflight_fdw: missing write metadata");

	node = (Node *) list_nth(fdw_private, index);
	if (!IsA(node, String))
		elog(ERROR, "arrowflight_fdw: invalid write string metadata");

	return strVal(node);
}

static char *
af_fdw_modify_private_optional_string(List *fdw_private,
									  ArrowFlightFdwModifyPrivateIndex index)
{
	char	   *value = af_fdw_modify_private_string(fdw_private, index);

	if (value == NULL || *value == '\0')
		return NULL;

	return pstrdup(value);
}

static int
af_fdw_modify_private_int(List *fdw_private,
						  ArrowFlightFdwModifyPrivateIndex index)
{
	Node	   *node;

	if (list_length(fdw_private) <= (int) index)
		elog(ERROR, "arrowflight_fdw: missing write metadata");

	node = (Node *) list_nth(fdw_private, index);
	if (!IsA(node, Integer))
		elog(ERROR, "arrowflight_fdw: invalid write integer metadata");

	return intVal(node);
}
