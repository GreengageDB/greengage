/*
 *	pg_upgrade_support.c
 *
 *	server-side functions to set backend global variables
 *	to control oid and relfilenode assignment, and do other special
 *	hacks needed for pg_upgrade.
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	contrib/pg_upgrade_support/pg_upgrade_support.c
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/transam.h"
#include "catalog/binary_upgrade.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_resqueuecapability.h"
#include "catalog/gp_configuration_history.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteHandler.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

/* THIS IS USED ONLY FOR PG >= 9.0 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern PGDLLIMPORT Oid binary_upgrade_next_toast_pg_type_oid;

extern PGDLLIMPORT Oid binary_upgrade_next_toast_pg_class_oid;

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

PG_FUNCTION_INFO_V1(set_next_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_array_pg_type_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_type_oid);

PG_FUNCTION_INFO_V1(set_next_heap_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_index_pg_class_oid);
PG_FUNCTION_INFO_V1(set_next_toast_pg_class_oid);

PG_FUNCTION_INFO_V1(set_next_pg_enum_oid);
PG_FUNCTION_INFO_V1(set_next_pg_authid_oid);

PG_FUNCTION_INFO_V1(create_empty_extension);

PG_FUNCTION_INFO_V1(set_next_pg_namespace_oid);

PG_FUNCTION_INFO_V1(set_preassigned_oids);
PG_FUNCTION_INFO_V1(set_next_preassigned_tablespace_oid);

PG_FUNCTION_INFO_V1(view_has_anyarray_casts);
PG_FUNCTION_INFO_V1(view_has_unknown_casts);
PG_FUNCTION_INFO_V1(view_has_removed_operators);
PG_FUNCTION_INFO_V1(view_has_removed_functions);
PG_FUNCTION_INFO_V1(view_has_removed_types);
PG_FUNCTION_INFO_V1(view_has_changed_function_signatures);
PG_FUNCTION_INFO_V1(get_removed_tables);
PG_FUNCTION_INFO_V1(get_removed_columns);

typedef struct RemovedTablesWalkerContext RemovedTablesWalkerContext;
typedef struct RemovedColumnsWalkerContext RemovedColumnsWalkerContext;
typedef struct RemovedColumnStatic RemovedColumnStatic;
typedef struct RemovedColumnDynamic  RemovedColumnDynamic;
typedef struct RemovedFunctionDynamic RemovedFunctionDynamic;
typedef struct ReportedColumn ReportedColumn;

static Oid get_function(const char *name, const Oid *args, int args_count, Oid namespace);
static Oid get_type(const char *name, Oid namespace);

static Query *get_matview_query(Relation matview);

static bool check_node_anyarray_walker(Node *node, void *context);
static bool check_node_unknown_walker(Node *node, void *context);
static bool check_node_removed_operators_walker(Node *node, void *context);
static bool check_node_removed_functions_walker(Node *node, void *context);
static bool check_node_removed_types_walker(Node *node, void *context);
static bool check_node_changed_function_signatures_walker(Node *node, void *context);
static void report_removed_table(RemovedTablesWalkerContext *context, Oid reloid);
static void check_and_report_removed_table(RemovedTablesWalkerContext *context, Oid reloid);
static bool check_node_removed_tables_walker(Node *node, void *context);
static void report_removed_column(RemovedColumnsWalkerContext *context, Oid reloid, int attnum);
static bool check_and_report_removed_columns(RemovedColumnsWalkerContext *context, Oid reloid, int attnum);
static bool check_node_removed_columns_walker(Node *node, RemovedColumnsWalkerContext *context);

/*
 * Some objects are treated as 'dynamic', because they are present in the database
 * by default, but live outside of 'pg_catalog', inside schemas like
 * 'gp_toolkit' and 'information_schema', which can be dropped using
 * 'DROP SCHEMA ... CASCADE'. This means that we should check for their absence.
 * Moreover, they can be recreated from respective sql scripts after that,
 * changing OIDs of the objects. So, we need to ask the database for their OIDs first.
 */

struct RemovedTablesWalkerContext
{
	List *removedTables;
};

struct RemovedColumnsWalkerContext
{
	List *rtableStack;
	List *removedColumns;
	bool inside_whole_row_reference;
};

struct RemovedColumnStatic
{
	Oid reloid;
	int attnum;
};

struct RemovedColumnDynamic
{
	const char *relnamespace;
	const char *relname;
	int attnum;
};

struct RemovedFunctionDynamic
{
	const char *pronamespace;
	const char *name;
	const Oid  *args;
	const int   args_count;
};

struct ReportedColumn
{
	Oid  attrelid;
	int  attnum;
	bool comes_from_whole_row_reference;
};

/* Lists of objects removed from Greenage 7 */
static Oid pg_resgroup_check_move_query_oids[]    = {23, 26};
static Oid __gp_remove_ao_entry_from_cache_oids[] = {26};
static Oid __gp_get_ao_entry_from_cache_oids[]    = {26};

static RemovedFunctionDynamic removed_functions_dynamic[] =
{
	{"gp_toolkit",  "pg_resgroup_check_move_query",     pg_resgroup_check_move_query_oids,     2},
	{"gp_toolkit",  "__gp_remove_ao_entry_from_cache",  __gp_remove_ao_entry_from_cache_oids,  1},
	{"gp_toolkit",  "__gp_get_ao_entry_from_cache",     __gp_get_ao_entry_from_cache_oids,     1},

};
static const int num_removed_functions_dynamic = sizeof(removed_functions_dynamic) / sizeof(RemovedFunctionDynamic);

static Oid __gp_aocsseg_oids[]         = {2205};
static Oid __gp_aocsseg_history_oids[] = {2205};
static Oid __gp_aoseg_oids[]           = {2205};
static Oid __gp_aoseg_history_oids[]   = {2205};

static RemovedFunctionDynamic functions_with_changed_signatures_dynamic[] =
{
	{"gp_toolkit",  "__gp_aocsseg",          __gp_aocsseg_oids,          1},
	{"gp_toolkit",  "__gp_aocsseg_history",  __gp_aocsseg_history_oids,  1},
	{"gp_toolkit",  "__gp_aoseg",            __gp_aoseg_oids,            1},
	{"gp_toolkit",  "__gp_aoseg_history",    __gp_aoseg_history_oids,    1}
};
static const int num_functions_with_changed_signatures_dynamic = sizeof(functions_with_changed_signatures_dynamic) / sizeof(RemovedFunctionDynamic);


static const Oid removed_tables_static[] =
{
	5010,  /* pg_catalog.pg_partition */
	11786, /* pg_catalog.pg_partition_columns */
	9903,  /* pg_catalog.pg_partition_encoding */
	5011,  /* pg_catalog.pg_partition_rule */
	11782, /* pg_catalog.pg_partitions */
	11789, /* pg_catalog.pg_partition_templates */
	11796  /* pg_catalog.pg_stat_partition_operations */
};
static const int num_removed_tables_static = sizeof(removed_tables_static) / sizeof(Oid);

/* Assuming that all of these tables live inside gp_toolkit schema */
static char *removed_tables_dynamic[] =
{
	"gp_size_of_partition_and_indexes_disk",
	"__gp_user_data_tables"
};
static const int num_removed_tables_dynamic = sizeof(removed_tables_dynamic) / sizeof(char*);


static const RemovedColumnStatic removed_columns_static[] =
{
	{11636,              6}, /* pg_catalog.pg_roles.rolcatupdate */
	{11639,              5}, /* pg_catalog.pg_shadow.usecatupd  */
	{11645,              5}, /* pg_catalog.pg_user.usecatupd */
	{11758,             11}, /* pg_catalog.pg_stat_replication.sent_location */
	{11758,             12}, /* pg_catalog.pg_stat_replication.write_location */
	{11758,             13}, /* pg_catalog.pg_stat_replication.flush_location */
	{11758,             14}, /* pg_catalog.pg_stat_replication.replay_location */
	{11755,             15}, /* pg_catalog.pg_stat_activity.waiting */
	{11755,             20}, /* pg_catalog.pg_stat_activity.waiting_reason */
	{11755,             23}, /* pg_catalog.pg_stat_activity.rsgqueueduration */
	{12345,             4},  /* pg_catalog.gp_distributed_log.distributed_id */
	{12339,             2},  /* pg_catalog.gp_distributed_xacts.distributed_id */
	{11764,             14}, /* pg_catalog.gp_stat_replication.flush_location */
	{11764,             15}, /* pg_catalog.gp_stat_replication.replay_location */
	{11764,             12}, /* pg_catalog.gp_stat_replication.sent_location */
	{11764,             13}, /* pg_catalog.gp_stat_replication.write_location */
	{6439,              -2}, /* pg_catalog.pg_resgroupcapability.oid */
	{GpConfigHistoryRelationId,     Anum_gp_configuration_history_desc},
	{ProcedureRelationId,           Anum_pg_proc_protransform},
	{ProcedureRelationId,           Anum_pg_proc_proisagg},
	{ProcedureRelationId,           Anum_pg_proc_proiswindow},
	{ProcedureRelationId,           Anum_pg_proc_prodataaccess},
	{AccessMethodRelationId,        Anum_pg_am_ambeginscan},
	{AccessMethodRelationId,        Anum_pg_am_ambuild},
	{AccessMethodRelationId,        Anum_pg_am_ambuildempty},
	{AccessMethodRelationId,        Anum_pg_am_ambulkdelete},
	{AccessMethodRelationId,        Anum_pg_am_amcanbackward},
	{AccessMethodRelationId,        Anum_pg_am_amcanmulticol},
	{AccessMethodRelationId,        Anum_pg_am_amcanorder},
	{AccessMethodRelationId,        Anum_pg_am_amcanorderbyop},
	{AccessMethodRelationId,        Anum_pg_am_amcanreturn},
	{AccessMethodRelationId,        Anum_pg_am_amcanunique},
	{AccessMethodRelationId,        Anum_pg_am_amclusterable},
	{AccessMethodRelationId,        Anum_pg_am_amcostestimate},
	{AccessMethodRelationId,        Anum_pg_am_amendscan},
	{AccessMethodRelationId,        Anum_pg_am_amgetbitmap},
	{AccessMethodRelationId,        Anum_pg_am_amgettuple},
	{AccessMethodRelationId,        Anum_pg_am_aminsert},
	{AccessMethodRelationId,        Anum_pg_am_amkeytype},
	{AccessMethodRelationId,        Anum_pg_am_ammarkpos},
	{AccessMethodRelationId,        Anum_pg_am_amoptionalkey},
	{AccessMethodRelationId,        Anum_pg_am_amoptions},
	{AccessMethodRelationId,        Anum_pg_am_ampredlocks},
	{AccessMethodRelationId,        Anum_pg_am_amrescan},
	{AccessMethodRelationId,        Anum_pg_am_amrestrpos},
	{AccessMethodRelationId,        Anum_pg_am_amsearcharray},
	{AccessMethodRelationId,        Anum_pg_am_amsearchnulls},
	{AccessMethodRelationId,        Anum_pg_am_amstorage},
	{AccessMethodRelationId,        Anum_pg_am_amstrategies},
	{AccessMethodRelationId,        Anum_pg_am_amsupport},
	{AccessMethodRelationId,        Anum_pg_am_amvacuumcleanup},
	{AppendOnlyRelationId,          Anum_pg_appendonly_blkdiridxid},
	{AppendOnlyRelationId,          Anum_pg_appendonly_blocksize},
	{AppendOnlyRelationId,          Anum_pg_appendonly_checksum},
	{AppendOnlyRelationId,          Anum_pg_appendonly_columnstore},
	{AppendOnlyRelationId,          Anum_pg_appendonly_compresslevel},
	{AppendOnlyRelationId,          Anum_pg_appendonly_compresstype},
	{AppendOnlyRelationId,          Anum_pg_appendonly_safefswritesize},
	{AppendOnlyRelationId,          Anum_pg_appendonly_visimapidxid},
	{AttrDefaultRelationId,         Anum_pg_attrdef_adsrc},
	{AuthIdRelationId,              Anum_pg_authid_rolcatupdate},
	{RelationRelationId,            Anum_pg_class_relhasoids},
	{RelationRelationId,            Anum_pg_class_relhaspkey},
	{RelationRelationId,            Anum_pg_class_relstorage},
	{ConstraintRelationId,          Anum_pg_constraint_consrc},
	{CompressionRelationId,         -2 /* oid */},
	{ExtTableRelationId,            -8 /* gp_segment_id */},
	{ExtTableRelationId,            -7 /* tableoid */},
	{ExtTableRelationId,            -6 /* cmax */},
	{ExtTableRelationId,            -5 /* xmax */},
	{ExtTableRelationId,            -4 /* cmin */},
	{ExtTableRelationId,            -3 /* xmin */},
	{ExtTableRelationId,            -1 /* ctid*/},
	{ResQueueCapabilityRelationId,  -2 /* oid */}
};
static const int num_removed_columns_static = sizeof(removed_columns_static) / sizeof(RemovedColumnStatic);

static const RemovedColumnDynamic removed_columns_dynamic[] =
{
	{"gp_toolkit", "gp_locks_on_resqueue",            9  /* lorwaiting */},
	{"gp_toolkit", "gp_resgroup_config",              4  /* cpu_rate_limit */},
	{"gp_toolkit", "gp_resgroup_config",              8  /* memory_auditor */},
	{"gp_toolkit", "gp_resgroup_config",              6  /* memory_shared_quota */},
	{"gp_toolkit", "gp_resgroup_config",              7  /* memory_spill_ratio */},
	{"gp_toolkit", "gp_resgroup_status",              8  /* cpu_usage */},
	{"gp_toolkit", "gp_resgroup_status",              9  /* memory_usage */},
	{"gp_toolkit", "gp_resgroup_status",              1  /* rsgname */ },
	{"gp_toolkit", "gp_resgroup_status_per_host",     4  /* cpu */ },
	{"gp_toolkit", "gp_resgroup_status_per_host",     6  /* memory_available */ },
	{"gp_toolkit", "gp_resgroup_status_per_host",     8  /* memory_quota_available */},
	{"gp_toolkit", "gp_resgroup_status_per_host",     7  /* memory_quota_used */},
	{"gp_toolkit", "gp_resgroup_status_per_host",     10 /* memory_shared_available */},
	{"gp_toolkit", "gp_resgroup_status_per_host",     9  /* memory_shared_used */},
	{"gp_toolkit", "gp_resgroup_status_per_host",     5  /* memory_used */},
	{"gp_toolkit", "gp_resgroup_status_per_host",     1  /* rsgname */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  5  /* cpu */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  3  /* hostname */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  7  /* memory_available */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  9  /* memory_quota_available */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  8  /* memory_quota_used */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  11 /* memory_shared_available */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  10 /* memory_shared_used */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  6  /* memory_used */},
	{"gp_toolkit", "gp_resgroup_status_per_segment",  1  /* rsgname */},
	{"gp_toolkit", "__gp_user_tables",                9  /* autrelstorage */ },
	{"gp_toolkit", "__gp_user_data_tables_readable",  9  /* autrelstorage */ },
	{"information_schema", "routines",                65 /* result_cast_character_set_name */},
	{"information_schema", "routines",                43 /* sql_data_access */},
	{"gp_toolkit", "__gp_log_master_ext",             -8 /* gp_segment_id */},
	{"gp_toolkit", "__gp_log_master_ext",             -7 /* tableoid */},
	{"gp_toolkit", "__gp_log_master_ext",             -6 /* cmax */},
	{"gp_toolkit", "__gp_log_master_ext",             -5 /* xmax */},
	{"gp_toolkit", "__gp_log_master_ext",             -4 /* cmin */},
	{"gp_toolkit", "__gp_log_master_ext",             -3 /* xmin */},
	{"gp_toolkit", "__gp_log_master_ext",             -1 /* ctid */}
};
static const int num_removed_columns_dynamic = sizeof(removed_columns_dynamic) / sizeof(RemovedColumnDynamic);


static const char* removed_types_gp_toolkit[] =
{
	"gp_size_of_partition_and_indexes_disk",
	"__gp_user_data_tables"
};
static const int num_removed_types_gp_toolkit = sizeof(removed_types_gp_toolkit) / sizeof (char*);

/*
 * Helper function like get_view_query, but for materialized views.
 * It works similarly to ExecRefreshMatView.
 */
static Query *
get_matview_query(Relation matview)
{
	RewriteRule *rule;
	List	    *actions;

	Assert(matview->rd_rel->relkind == RELKIND_MATVIEW);

	if (matview->rd_rel->relhasrules == false ||
		matview->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matview));

	if (matview->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matview));

	rule = matview->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matview));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matview));

	return (Query *) linitial(rule->actions);
}

Datum
set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);
	Oid			typnamespaceoid = PG_GETARG_OID(1);
	char	   *typname = GET_STR(PG_GETARG_TEXT_P(2));

	AddPreassignedOidFromBinaryUpgrade(typoid, TypeRelationId, typname,
						typnamespaceoid, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);
	Oid			typnamespaceoid = PG_GETARG_OID(1);
	char	   *typname = GET_STR(PG_GETARG_TEXT_P(2));

	AddPreassignedOidFromBinaryUpgrade(typoid, TypeRelationId, typname,
						typnamespaceoid, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);
	Oid			typnamespaceoid = PG_GETARG_OID(1);
	char	   *typname = GET_STR(PG_GETARG_TEXT_P(2));

	binary_upgrade_next_toast_pg_type_oid++;

	AddPreassignedOidFromBinaryUpgrade(typoid, TypeRelationId, typname,
						typnamespaceoid, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));

	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));

	binary_upgrade_next_toast_pg_class_oid++;

	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnamespace = PG_GETARG_OID(1);
	char	   *relname = GET_STR(PG_GETARG_TEXT_P(2));

	AddPreassignedOidFromBinaryUpgrade(reloid, RelationRelationId, relname,
									   relnamespace, InvalidOid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_pg_enum_oid(PG_FUNCTION_ARGS)
{
	Oid			enumoid = PG_GETARG_OID(0);
	Oid			typeoid = PG_GETARG_OID(1);
	char	   *enumlabel = GET_STR(PG_GETARG_TEXT_P(2));

	AddPreassignedOidFromBinaryUpgrade(enumoid, EnumRelationId, enumlabel,
									   InvalidOid, typeoid, InvalidOid);

	PG_RETURN_VOID();
}

Datum
set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
	Oid			roleid = PG_GETARG_OID(0);
	char	   *rolename = GET_STR(PG_GETARG_TEXT_P(1));

	if (Gp_role == GP_ROLE_UTILITY)
	{
		AddPreassignedOidFromBinaryUpgrade(roleid, AuthIdRelationId, rolename,
										   InvalidOid, InvalidOid, InvalidOid);
	}

	PG_RETURN_VOID();
}

Datum
create_empty_extension(PG_FUNCTION_ARGS)
{
	text	   *extName = PG_GETARG_TEXT_PP(0);
	text	   *schemaName = PG_GETARG_TEXT_PP(1);
	bool		relocatable = PG_GETARG_BOOL(2);
	text	   *extVersion = PG_GETARG_TEXT_PP(3);
	Datum		extConfig;
	Datum		extCondition;
	List	   *requiredExtensions;

	if (PG_ARGISNULL(4))
		extConfig = PointerGetDatum(NULL);
	else
		extConfig = PG_GETARG_DATUM(4);

	if (PG_ARGISNULL(5))
		extCondition = PointerGetDatum(NULL);
	else
		extCondition = PG_GETARG_DATUM(5);

	requiredExtensions = NIL;
	if (!PG_ARGISNULL(6))
	{
		ArrayType  *textArray = PG_GETARG_ARRAYTYPE_P(6);
		Datum	   *textDatums;
		int			ndatums;
		int			i;

		deconstruct_array(textArray,
						  TEXTOID, -1, false, 'i',
						  &textDatums, NULL, &ndatums);
		for (i = 0; i < ndatums; i++)
		{
			text	   *txtname = DatumGetTextPP(textDatums[i]);
			char	   *extName = text_to_cstring(txtname);
			Oid			extOid = get_extension_oid(extName, false);

			requiredExtensions = lappend_oid(requiredExtensions, extOid);
		}
	}

	InsertExtensionTuple(text_to_cstring(extName),
						 GetUserId(),
					   get_namespace_oid(text_to_cstring(schemaName), false),
						 relocatable,
						 text_to_cstring(extVersion),
						 extConfig,
						 extCondition,
						 requiredExtensions);

	PG_RETURN_VOID();
}

Datum
set_next_pg_namespace_oid(PG_FUNCTION_ARGS)
{
	Oid			nspid = PG_GETARG_OID(0);
	char	   *nspname = GET_STR(PG_GETARG_TEXT_P(1));

	if (Gp_role == GP_ROLE_UTILITY)
	{
		AddPreassignedOidFromBinaryUpgrade(nspid, NamespaceRelationId, nspname,
										   InvalidOid, InvalidOid, InvalidOid);
	}

	PG_RETURN_VOID();
}

Datum
set_preassigned_oids(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *oids;
	int			nelems;
	int			i;

	deconstruct_array(array, OIDOID, sizeof(Oid), true, 'i',
					  &oids, NULL, &nelems);

	for (i = 0; i < nelems; i++)
	{
		Datum		oid = DatumGetObjectId(oids[i]);

		MarkOidPreassignedFromBinaryUpgrade(oid);
	}

	PG_RETURN_VOID();
}

Datum
set_next_preassigned_tablespace_oid(PG_FUNCTION_ARGS)
{
	Oid			tsoid = PG_GETARG_OID(0);
	char	   *objname = GET_STR(PG_GETARG_TEXT_P(1));

	if (Gp_role == GP_ROLE_UTILITY)
	{
		AddPreassignedOidFromBinaryUpgrade(tsoid, TableSpaceRelationId, objname,
		                                   InvalidOid, InvalidOid, InvalidOid);
	}

	PG_RETURN_VOID();
}

/*
 * Check for anyarray casts which may have corrupted the given view's definition
 * The corruption can result from the GPDB special handling for ANYARRAY types
 * in parse_coerce.c: coerce_type()
 */

Datum
view_has_anyarray_casts(PG_FUNCTION_ARGS)
{
	Oid			view_oid = PG_GETARG_OID(0);
	Relation 	rel = try_relation_open(view_oid, AccessShareLock, false);
	Query		*viewquery;
	bool		found;

	if (rel == NULL)
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_anyarray_walker, NULL, 0);
	}
	else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_anyarray_walker, NULL, 0);
	}
	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

static bool
check_node_anyarray_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	/*
	 * Look only at Consts since the GPDB special handling hack for ANYARRAY
	 * types is only applied to Consts. See parse_coerce.c: coerce_type()
	 */
	if (IsA(node, Const))
	{
		Const *constant = (Const *) node;
		/*
		 * Check to see if the constant has an anyarray cast. If the constant's
		 * value is NULL, disregard. This is because NULL::anyarray is a valid
		 * expression and is encountered in the pg_stats catalog view.
		 */
		return constant->consttype == ANYARRAYOID && !constant->constisnull;
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_anyarray_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_anyarray_walker,
								  context);
}

Datum
view_has_unknown_casts(PG_FUNCTION_ARGS)
{
	Oid			view_oid = PG_GETARG_OID(0);
	Relation 	rel = try_relation_open(view_oid, AccessShareLock, false);
	Query		*viewquery;
	bool		found;

	if (rel == NULL)
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_unknown_walker, NULL, 0);
	}
	else if(rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_unknown_walker, NULL, 0);
	}
	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

static bool
check_node_unknown_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	/*
	 * Look only at FuncExpr since the GPDB special handling hack for unknown
	 * types is only applied to FuncExpr. See parse_coerce.c: coerce_type()
	 */
	if (IsA(node, FuncExpr))
	{
		FuncExpr *fe = (FuncExpr *) node;
		/*
		 * Check to see if the FuncExpr has an unknown::cstring explicit cast.
		 *
		 * If it has no such cast yet, check its arguments.
		 */
		if ((fe->funcresulttype != CSTRINGOID) || !fe->args || (list_length(fe->args) != 1) || (fe->funcformat == COERCE_IMPLICIT_CAST))
			return expression_tree_walker(node, check_node_unknown_walker, context);

		Node *head = lfirst(((List *)fe->args)->head);

		if (IsA(head, Var) && ((Var *)head)->vartype == UNKNOWNOID)
			return true;
		else
			return expression_tree_walker(node, check_node_unknown_walker, context);
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_unknown_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_unknown_walker, context);
}

Datum
view_has_removed_operators(PG_FUNCTION_ARGS)
{
	Oid		  view_oid = PG_GETARG_OID(0);
	Relation  rel = try_relation_open(view_oid, AccessShareLock, false);
	Query	 *viewquery;
	bool	  found;

	if (rel == NULL)
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_operators_walker, NULL, 0);
	}
	else if(rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_operators_walker, NULL, 0);
	}

	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

static bool
check_node_removed_operators_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		Oid op_oid = ((OpExpr *)node)->opno;
		if (op_oid == 386) // int2vectoreq
			return true;

		return false;
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_removed_operators_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_removed_operators_walker, context);
}

Datum
view_has_removed_functions(PG_FUNCTION_ARGS)
{
	Oid		  view_oid = PG_GETARG_OID(0);
	Relation  rel = try_relation_open(view_oid, AccessShareLock, false);
	Query	 *viewquery;
	bool	  found;

	if (!RelationIsValid(rel))
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_functions_walker, NULL, 0);
	}
	else if(rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_functions_walker, NULL, 0);
	}

	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

/* Helper functions to get object OIDs by their signatures */
Oid
get_function(const char *name, const Oid *args, int args_count, Oid namespace)
{
	return GetSysCacheOid3(PROCNAMEARGSNSP,
						   PointerGetDatum(name),
						   PointerGetDatum(buildoidvector(args, args_count)),
						   ObjectIdGetDatum(namespace));
}

Oid
get_type(const char *name, Oid namespace)
{
	return GetSysCacheOid2(TYPENAMENSP,
						   PointerGetDatum(name),
						   ObjectIdGetDatum(namespace));
}

static bool
check_node_removed_functions_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		Oid schema_oid;
		Oid func_oid = ((FuncExpr *)node)->funcid;
		if (func_oid ==  7188 || // pg_catalog.bmbeginscan
			func_oid ==  7193 || // pg_catalog.bmbuild
			func_oid ==  7011 || // pg_catalog.bmbuildempty
			func_oid ==  7194 || // pg_catalog.bmbulkdelete
			func_oid ==  7196 || // pg_catalog.bmcostestimate
			func_oid ==  7190 || // pg_catalog.bmendscan
			func_oid ==  7051 || // pg_catalog.bmgetbitmap
			func_oid ==  7050 || // pg_catalog.bmgettuple
			func_oid ==  7187 || // pg_catalog.bminsert
			func_oid ==  7191 || // pg_catalog.bmmarkpos
			func_oid ==  7197 || // pg_catalog.bmoptions
			func_oid ==  7189 || // pg_catalog.bmrescan
			func_oid ==  7192 || // pg_catalog.bmrestrpos
			func_oid ==  7195 || // pg_catalog.bmvacuumcleanup
			func_oid ==   333 || // pg_catalog.btbeginscan
			func_oid ==   338 || // pg_catalog.btbuild
			func_oid ==   328 || // pg_catalog.btbuildempty
			func_oid ==   332 || // pg_catalog.btbulkdelete
			func_oid ==  6276 || // pg_catalog.btcanreturn
			func_oid ==  1268 || // pg_catalog.btcostestimate
			func_oid ==   335 || // pg_catalog.btendscan
			func_oid ==   636 || // pg_catalog.btgetbitmap
			func_oid ==   330 || // pg_catalog.btgettuple
			func_oid ==   331 || // pg_catalog.btinsert
			func_oid ==   336 || // pg_catalog.btmarkpos
			func_oid ==  6785 || // pg_catalog.btoptions
			func_oid ==   334 || // pg_catalog.btrescan
			func_oid ==   337 || // pg_catalog.btrestrpos
			func_oid ==   972 || // pg_catalog.btvacuumcleanup
			func_oid ==  2733 || // pg_catalog.ginbeginscan
			func_oid ==  2738 || // pg_catalog.ginbuild
			func_oid ==   325 || // pg_catalog.ginbuildempty
			func_oid ==  2739 || // pg_catalog.ginbulkdelete
			func_oid ==  6741 || // pg_catalog.gincostestimate
			func_oid ==  2735 || // pg_catalog.ginendscan
			func_oid ==  2731 || // pg_catalog.gingetbitmap
			func_oid ==  2732 || // pg_catalog.gininsert
			func_oid ==  2736 || // pg_catalog.ginmarkpos
			func_oid ==  2788 || // pg_catalog.ginoptions
			func_oid ==  2734 || // pg_catalog.ginrescan
			func_oid ==  2737 || // pg_catalog.ginrestrpos
			func_oid ==  6740 || // pg_catalog.ginvacuumcleanup
			func_oid ==   777 || // pg_catalog.gistbeginscan
			func_oid ==  2579 || // pg_catalog.gist_box_compress
			func_oid ==  2580 || // pg_catalog.gist_box_decompress
			func_oid ==   782 || // pg_catalog.gistbuild
			func_oid ==   326 || // pg_catalog.gistbuildempty
			func_oid ==   776 || // pg_catalog.gistbulkdelete
			func_oid ==   772 || // pg_catalog.gistcostestimate
			func_oid ==   779 || // pg_catalog.gistendscan
			func_oid ==   638 || // pg_catalog.gistgetbitmap
			func_oid ==   774 || // pg_catalog.gistgettuple
			func_oid ==   775 || // pg_catalog.gistinsert
			func_oid ==   780 || // pg_catalog.gistmarkpos
			func_oid ==  6787 || // pg_catalog.gistoptions
			func_oid ==   778 || // pg_catalog.gistrescan
			func_oid ==   781 || // pg_catalog.gistrestrpos
			func_oid ==  2561 || // pg_catalog.gistvacuumcleanup
			func_oid ==  5044 || // pg_catalog.gp_elog
			func_oid ==  5045 || // pg_catalog.gp_elog
			func_oid ==  9999 || // pg_catalog.gp_fault_inject
			func_oid == 12531 || // pg_catalog.gp_quicklz_compress
			func_oid == 12529 || // pg_catalog.gp_quicklz_constructor
			func_oid == 12532 || // pg_catalog.gp_quicklz_decompress
			func_oid == 12530 || // pg_catalog.gp_quicklz_destructor
			func_oid == 12533 || // pg_catalog.gp_quicklz_validator
			func_oid ==  7173 || // pg_catalog.gp_update_ao_master_stats
			func_oid ==  3696 || // pg_catalog.gtsquery_decompress
			func_oid ==   443 || // pg_catalog.hashbeginscan
			func_oid ==   448 || // pg_catalog.hashbuild
			func_oid ==   327 || // pg_catalog.hashbuildempty
			func_oid ==   442 || // pg_catalog.hashbulkdelete
			func_oid ==   438 || // pg_catalog.hashcostestimate
			func_oid ==   445 || // pg_catalog.hashendscan
			func_oid ==   637 || // pg_catalog.hashgetbitmap
			func_oid ==   440 || // pg_catalog.hashgettuple
			func_oid ==   441 || // pg_catalog.hashinsert
			func_oid ==   398 || // pg_catalog.hashint2vector
			func_oid ==   446 || // pg_catalog.hashmarkpos
			func_oid ==  6786 || // pg_catalog.hashoptions
			func_oid ==   444 || // pg_catalog.hashrescan
			func_oid ==   447 || // pg_catalog.hashrestrpos
			func_oid ==   425 || // pg_catalog.hashvacuumcleanup
			func_oid ==  3556 || // pg_catalog.inet_gist_decompress
			func_oid ==   315 || // pg_catalog.int2vectoreq
			func_oid ==  7597 || // pg_catalog.numeric2point
			func_oid ==  3157 || // pg_catalog.numeric_transform
			func_oid ==  2852 || // pg_catalog.pg_current_xlog_insert_location
			func_oid ==  2849 || // pg_catalog.pg_current_xlog_location
			func_oid ==  5024 || // pg_catalog.pg_get_partition_def
			func_oid ==  5034 || // pg_catalog.pg_get_partition_def
			func_oid ==  5025 || // pg_catalog.pg_get_partition_def
			func_oid ==  5028 || // pg_catalog.pg_get_partition_rule_def
			func_oid ==  5027 || // pg_catalog.pg_get_partition_rule_def
			func_oid ==  5037 || // pg_catalog.pg_get_partition_template_def
			func_oid ==  3073 || // pg_catalog.pg_is_xlog_replay_paused
			func_oid ==  3820 || // pg_catalog.pg_last_xlog_receive_location
			func_oid ==  3821 || // pg_catalog.pg_last_xlog_replay_location
			func_oid ==  2853 || // pg_catalog.pg_stat_get_backend_waiting
			func_oid ==  7298 || // pg_catalog.pg_stat_get_backend_waiting_reason
			func_oid ==  2848 || // pg_catalog.pg_switch_xlog
			func_oid ==  2851 || // pg_catalog.pg_xlogfile_name
			func_oid ==  2850 || // pg_catalog.pg_xlogfile_name_offset
			func_oid ==  3165 || // pg_catalog.pg_xlog_location_diff
			func_oid ==  3071 || // pg_catalog.pg_xlog_replay_pause
			func_oid ==  3072 || // pg_catalog.pg_xlog_replay_resume
			func_oid ==  3877 || // pg_catalog.range_gist_compress
			func_oid ==  3878 || // pg_catalog.range_gist_decompress
			func_oid ==  4004 || // pg_catalog.spgbeginscan
			func_oid ==  4009 || // pg_catalog.spgbuild
			func_oid ==  4010 || // pg_catalog.spgbuildempty
			func_oid ==  4011 || // pg_catalog.spgbulkdelete
			func_oid ==  4032 || // pg_catalog.spgcanreturn
			func_oid ==  4013 || // pg_catalog.spgcostestimate
			func_oid ==  4006 || // pg_catalog.spgendscan
			func_oid ==  4002 || // pg_catalog.spggetbitmap
			func_oid ==  4001 || // pg_catalog.spggettuple
			func_oid ==  4003 || // pg_catalog.spginsert
			func_oid ==  4007 || // pg_catalog.spgmarkpos
			func_oid ==  4014 || // pg_catalog.spgoptions
			func_oid ==  4005 || // pg_catalog.spgrescan
			func_oid ==  4008 || // pg_catalog.spgrestrpos
			func_oid ==  4012 || // pg_catalog.spgvacuumcleanup
			func_oid ==  3917 || // pg_catalog.timestamp_transform
			func_oid ==  3944 || // pg_catalog.time_transform
			func_oid ==  3158 || // pg_catalog.varbit_transform
			func_oid ==  3097)   // pg_catalog.varchar_transform
			return true;

		for (int i = 0; i < num_removed_functions_dynamic; ++i)
		{
			schema_oid = GetSysCacheOid(NAMESPACENAME,
										CStringGetDatum(removed_functions_dynamic[i].pronamespace),
										0, 0, 0);

			if (OidIsValid(schema_oid))
			{
				if (func_oid == get_function(removed_functions_dynamic[i].name,
											 removed_functions_dynamic[i].args,
											 removed_functions_dynamic[i].args_count,
											 schema_oid))
					return true;
			}
		}

		return false;
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_removed_functions_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_removed_functions_walker, context);
}


Datum
view_has_removed_types(PG_FUNCTION_ARGS)
{
	Oid		  view_oid = PG_GETARG_OID(0);
	Relation  rel = try_relation_open(view_oid, AccessShareLock, false);
	Query	 *viewquery;
	bool	  found;

	if (!RelationIsValid(rel))
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_types_walker, NULL, 0);
	}
	else if(rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_removed_types_walker, NULL, 0);
	}
	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

static bool
check_node_removed_types_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	if (IsA(node, Var) || IsA(node, Const))
	{
		Oid gp_toolkit_oid;
		Oid type_oid;
		if IsA(node, Var)
			type_oid = ((Var *)node)->vartype;
		else
			type_oid = ((Const *)node)->consttype;

		if (type_oid ==  1023 || // pg_catalog._abstime
			type_oid ==   702 || // pg_catalog.abstime
			type_oid == 11612 || // pg_catalog.pg_partition
			type_oid == 11787 || // pg_catalog.pg_partition_columns
			type_oid == 11617 || // pg_catalog.pg_partition_encoding
			type_oid == 11613 || // pg_catalog.pg_partition_rule
			type_oid == 11783 || // pg_catalog.pg_partitions
			type_oid == 11790 || // pg_catalog.pg_partition_templates
			type_oid == 11797 || // pg_catalog.pg_stat_partition_operations
			type_oid ==  1024 || // pg_catalog._reltime
			type_oid ==   703 || // pg_catalog.reltime
			type_oid ==   210 || // pg_catalog.smgr
			type_oid ==  1025 || // pg_catalog._tinterval
			type_oid ==   704)   // pg_catalog.tinterval
			return true;

		gp_toolkit_oid = GetSysCacheOid(NAMESPACENAME,
										CStringGetDatum("gp_toolkit"),
										0, 0, 0);

		if (OidIsValid(gp_toolkit_oid))
		{
			for (int i = 0; i < num_removed_types_gp_toolkit; i++)
			{
				if (type_oid == get_type(removed_types_gp_toolkit[i],
										 gp_toolkit_oid))
					return true;
			}
		}

		return false;
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_removed_types_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_removed_types_walker, context);
}

Datum
view_has_changed_function_signatures(PG_FUNCTION_ARGS)
{
	Oid		  view_oid = PG_GETARG_OID(0);
	Relation  rel = try_relation_open(view_oid, AccessShareLock, false);
	Query	 *viewquery;
	bool	  found;

	if (!RelationIsValid(rel))
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	if(rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		found = query_tree_walker(viewquery, check_node_changed_function_signatures_walker, NULL, 0);
	}
	else if(rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		found = query_tree_walker(viewquery, check_node_changed_function_signatures_walker, NULL, 0);
	}
	else
		found = false;

	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(found);
}

static bool
check_node_changed_function_signatures_walker(Node *node, void *context)
{
	Assert(context == NULL);

	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		Oid schema_oid;
		Oid func_oid = ((FuncExpr *)node)->funcid;
		if (func_oid ==  2335 || // pg_catalog.array_agg
			func_oid ==  2334 || // pg_catalog.array_agg_finalfn
			func_oid ==  2333 || // pg_catalog.array_agg_transfn
			func_oid ==  3484 || // pg_catalog.gin_consistent_jsonb
			func_oid ==  3487 || // pg_catalog.gin_consistent_jsonb_path
			func_oid ==  3482 || // pg_catalog.gin_extract_jsonb
			func_oid ==  3485 || // pg_catalog.gin_extract_jsonb_path
			func_oid ==  3483 || // pg_catalog.gin_extract_jsonb_query
			func_oid ==  3486 || // pg_catalog.gin_extract_jsonb_query_path
			func_oid ==  3488 || // pg_catalog.gin_triconsistent_jsonb
			func_oid ==  3489 || // pg_catalog.gin_triconsistent_jsonb_path
			func_oid ==  3921 || // pg_catalog.gin_tsquery_triconsistent
			func_oid ==  2578 || // pg_catalog.gist_box_consistent
			func_oid ==  2591 || // pg_catalog.gist_circle_consistent
			func_oid ==  2179 || // pg_catalog.gist_point_consistent
			func_oid ==  3064 || // pg_catalog.gist_point_distance
			func_oid ==  2585 || // pg_catalog.gist_poly_consistent
			func_oid ==  6036 || // pg_catalog.gp_dist_wait_status
			func_oid ==  6022 || // pg_catalog.gp_execution_segment
			func_oid ==  5035 || // pg_catalog.gp_request_fts_probe_scan
			func_oid == 11823 || // pg_catalog.gp_tablespace_segment_location
			func_oid ==  3698 || // pg_catalog.gtsquery_union
			func_oid ==  3651 || // pg_catalog.gtsvector_union
			func_oid ==  3553 || // pg_catalog.inet_gist_consistent
			func_oid ==  3554 || // pg_catalog.inet_gist_union
			func_oid ==  6225 || // pg_catalog.int4_pivot_accum
			func_oid ==  2121 || // pg_catalog.max
			func_oid ==  2137 || // pg_catalog.min
			func_oid ==  3786 || // pg_catalog.pg_create_logical_replication_slot
			func_oid ==  3779 || // pg_catalog.pg_create_physical_replication_slot
			func_oid ==  2511 || // pg_catalog.pg_cursor
			func_oid ==  3566 || // pg_catalog.pg_event_trigger_dropped_objects
			func_oid ==  3781 || // pg_catalog.pg_get_replication_slots
			func_oid ==  3839 || // pg_catalog.pg_identify_object
			func_oid ==  3445 || // pg_catalog.pg_import_system_collations
			func_oid ==  3783 || // pg_catalog.pg_logical_slot_get_binary_changes
			func_oid ==  3782 || // pg_catalog.pg_logical_slot_get_changes
			func_oid ==  3785 || // pg_catalog.pg_logical_slot_peek_binary_changes
			func_oid ==  3784 || // pg_catalog.pg_logical_slot_peek_changes
			func_oid ==  6066 || // pg_catalog.pg_resgroup_get_status
			func_oid ==  3078 || // pg_catalog.pg_sequence_parameters
			func_oid ==  2084 || // pg_catalog.pg_show_all_settings
			func_oid ==  2172 || // pg_catalog.pg_start_backup
			func_oid ==  3307 || // pg_catalog.pg_stat_file
			func_oid ==  2022 || // pg_catalog.pg_stat_get_activity
			func_oid ==  3099 || // pg_catalog.pg_stat_get_wal_senders
			func_oid ==  6226 || // pg_catalog.pivot_sum
			func_oid ==  3875 || // pg_catalog.range_gist_consistent
			func_oid ==  3876 || // pg_catalog.range_gist_union
			func_oid ==  3495 || // pg_catalog.to_regclass
			func_oid ==  3492 || // pg_catalog.to_regoper
			func_oid ==  3476 || // pg_catalog.to_regoperator
			func_oid ==  3494 || // pg_catalog.to_regproc
			func_oid ==  3479 || // pg_catalog.to_regprocedure
			func_oid ==  3493)   // pg_catalog.to_regtype
			return true;

		for (int i = 0; i < num_functions_with_changed_signatures_dynamic; i++)
		{
			schema_oid = GetSysCacheOid(NAMESPACENAME,
										CStringGetDatum(functions_with_changed_signatures_dynamic[i].pronamespace),
										0, 0, 0);

			if (OidIsValid(schema_oid))
			{
				if (func_oid == get_function(functions_with_changed_signatures_dynamic[i].name,
											 functions_with_changed_signatures_dynamic[i].args,
											 functions_with_changed_signatures_dynamic[i].args_count,
											 schema_oid))
					return true;
			}
		}

		return false;
	}
	else if (IsA(node, Query))
	{
		/* recurse into subselects and ctes */
		Query *query = (Query *) node;
		return query_tree_walker(query, check_node_changed_function_signatures_walker, context, 0);
	}

	return expression_tree_walker(node, check_node_changed_function_signatures_walker, context);
}

static void
report_removed_table(RemovedTablesWalkerContext *context, Oid reloid)
{
	Oid             already_reported_table;
	ListCell       *lc;

	/*
	 * Go thorogh already reported tables to remove
	 * duplicates
	 */
	foreach (lc, context->removedTables)
	{
		already_reported_table = lfirst_oid(lc);
		if (reloid == already_reported_table)
			return;
	}

	context->removedTables = lappend_oid(context->removedTables, reloid);
}

static void
check_and_report_removed_table(RemovedTablesWalkerContext *context, Oid reloid)
{
	int i;
	Oid gp_toolkit_oid;

	for (i = 0; i < num_removed_tables_static; i++)
	{
		if (reloid == removed_tables_static[i])
			report_removed_table(context, reloid);
	}

	gp_toolkit_oid = GetSysCacheOid(NAMESPACENAME,
									CStringGetDatum("gp_toolkit"),
									0, 0, 0);

	if (OidIsValid(gp_toolkit_oid))
	{
		for (i = 0; i < num_removed_tables_dynamic; i++)
		{
			if (reloid == get_relname_relid(removed_tables_dynamic[i], gp_toolkit_oid))
				report_removed_table(context, reloid);

		}
	}
}

static bool
check_node_removed_tables_walker(Node *node, void *context)
{
	Assert(context != NULL);

	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;
		if (rte->rtekind == RTE_RELATION)
			check_and_report_removed_table(context, rte->relid);
		return false;
	}
	else if(IsA(node, Query))
	{
		/*
		 * Recurse into (sub)queries to look for removed tables.
		 */
		return query_tree_walker((Query *) node,
								 check_node_removed_tables_walker,
								 context,
								 QTW_EXAMINE_RTES);
	}

	/*
	 * This ensures that we look for removed tables embedded inside
	 * expressions (e.g. CTEs, sublinks etc.) which can contain range tables.
	 */
	return expression_tree_walker(node, check_node_removed_tables_walker, context);
}

Datum
get_removed_tables(PG_FUNCTION_ARGS)
{
	Oid             view_oid = PG_GETARG_OID(0);
	Relation        rel;
	StringInfoData  buf;
	Oid             reported_table;
	Oid             relnamespace;
	ListCell       *lc;
	char           *nspname;
	char           *relname;
	Query		   *viewquery;
	RemovedTablesWalkerContext  context;

	rel = try_relation_open(view_oid, AccessShareLock, false);
	if (!RelationIsValid(rel))
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	context.removedTables = NIL;
	if (rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		check_node_removed_tables_walker((Node *) viewquery, &context);
	}
	else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		check_node_removed_tables_walker((Node *) viewquery, &context);
	}

	relation_close(rel, AccessShareLock);

	/*
	 * Make a single formatted string, listing all unique removed tables.
	 * It will be displayed to the user.
	 */
	initStringInfo(&buf);
	foreach (lc, context.removedTables)
	{
		reported_table = lfirst_oid(lc);
		relname = get_rel_name(reported_table);
		if (!relname)
			elog(ERROR, "cache lookup failed for relation %u", reported_table);

		relnamespace = get_rel_namespace(reported_table);
		if (!OidIsValid(relnamespace))
			elog(ERROR, "cache lookup failed for relation %u", reported_table);

		nspname = get_namespace_name(relnamespace);
		if (!nspname)
			elog(ERROR, "cache lookup failed for namespace %u", relnamespace);

		appendStringInfo(&buf, "\t%s.%s\n", nspname, relname);
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}


static void
report_removed_column(RemovedColumnsWalkerContext *context, Oid reloid, int attnum)
{
	ListCell       *lc;
	ReportedColumn *already_reported_column;
	ReportedColumn *column;

	/*
	 * Go through already reported columns in a nested loop manner
	 * to remove duplicates. This should be fast enough, because the
	 * number of removed columns is not that large
	 * (currently, 110), and most view wouldn't have all of them.
	 * But it is hard to tell without user data.
	 */
	foreach (lc, context->removedColumns)
	{
		already_reported_column = lfirst(lc);
		if (reloid == already_reported_column->attrelid &&
			attnum == already_reported_column->attnum &&
			context->inside_whole_row_reference == already_reported_column->comes_from_whole_row_reference)
			return;
	}

	column = palloc(sizeof(ReportedColumn));
	column->attrelid = reloid;
	column->attnum = attnum;
	column->comes_from_whole_row_reference = context->inside_whole_row_reference;

	context->removedColumns = lappend(context->removedColumns, column);
}

static bool
check_and_report_removed_columns(RemovedColumnsWalkerContext *context, Oid reloid, int attnum)
{
	int i;
	Oid schema_oid;
	int removed_column_attnum;

	for (i = 0; i < num_removed_columns_static; i++)
	{
		removed_column_attnum = removed_columns_static[i].attnum;
		if (reloid == removed_columns_static[i].reloid &&
			(attnum == removed_column_attnum || attnum == InvalidAttrNumber))
			report_removed_column(context, reloid, removed_column_attnum);
	}

	for (i = 0; i < num_removed_columns_dynamic; i++)
	{
		schema_oid = GetSysCacheOid(NAMESPACENAME,
									CStringGetDatum(removed_columns_dynamic[i].relnamespace),
									0, 0, 0);

		if (OidIsValid(schema_oid))
		{
			removed_column_attnum = removed_columns_dynamic[i].attnum;
			if (reloid == get_relname_relid(removed_columns_dynamic[i].relname, schema_oid) &&
				(attnum == removed_column_attnum || attnum == InvalidAttrNumber))
				report_removed_column(context, reloid, removed_column_attnum);
		}
	}

	return false;
}

/*
 * Check whether a query contains a reference to a removed column, or a whole
 * row reference to a table with removed columns.
 *
 * The first case will always cause pg_upgrade to fail, while the second is more
 * complicated. Whole row references by themselves won't cause upgrade to fail,
 * because row type will be taken from the target cluster. For example,
 * the following view won't cause any troubles:
 *
 *  CREATE VIEW view1 AS SELECT pg_class FROM pg_class;
 *
 * However, there could be another view that references specific columns from the
 * previous one:
 *
 *  CREATE VIEW view2 AS SELECT (pg_class).relhasoids FROM view1;
 *
 * and this columns may be indeed absent in the new version. Because of that,
 * conservatively report any whole row reference to any table with removed
 * columns.
 */
bool
check_node_removed_columns_walker(Node *node, RemovedColumnsWalkerContext *context)
{
	Assert(context != NULL);

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		Var           *var;
		List          *rtable;
		RangeTblEntry *rte;
		bool           save_inside_whole_row_reference;

		var = (Var *) node;
		if (var->varlevelsup >= list_length(context->rtableStack))
			elog(ERROR, "invalid varlevelsup %d", var->varlevelsup);

		rtable = (List *) list_nth(context->rtableStack, var->varlevelsup);
		if (var->varno <= 0 || var->varno > list_length(rtable))
			elog(ERROR, "invalid varno %d", var->varno);

		save_inside_whole_row_reference = context->inside_whole_row_reference;
		if (var->varattno == InvalidAttrNumber)
			context->inside_whole_row_reference = true;

		rte = (RangeTblEntry *) list_nth(rtable, var->varno - 1);
		if (rte->rtekind == RTE_RELATION)
		{
			/*
			 * It's a plain relation, simply check that Var doesn't reference
			 * removed column(s)
			 */
			check_and_report_removed_columns(context, rte->relid, var->varattno);
		}
		else if (rte->rtekind == RTE_JOIN)
		{
			/*
			 * It's a join entry, we need to recursively go through
			 * the RTE tree to get to the source entry for this attribute.
			 */
			int   i;
			List *save_rtables = context->rtableStack;

			context->rtableStack = list_copy_tail(context->rtableStack,
												  var->varlevelsup);

			if (var->varattno == InvalidAttrNumber)
			{
				/*
				 * For a whole table reference, check every column of the RTE
				 */
				for (i = 0; i < list_length(rte->joinaliasvars); i++)
					check_node_removed_columns_walker((Node *) list_nth(rte->joinaliasvars, i),
													  context);
			}
			else
			{
				/* Regular attribute */
				if (var->varattno <= 0 ||
					var->varattno > list_length(rte->joinaliasvars))
					elog(ERROR, "invalid varattno %d", var->varattno);

				check_node_removed_columns_walker((Node *) list_nth(rte->joinaliasvars,
																	var->varattno - 1),
												  context);
			}
			list_free(context->rtableStack);
			context->rtableStack = save_rtables;
		}

		/*
		 * Don't do anything special for other RTE kinds. Most notably, RTE_SUBQUERY,
		 * because subqueries will be handled when we recurse into them.
		 */
		context->inside_whole_row_reference = save_inside_whole_row_reference;
		return false;
	}
	else if (IsA(node, Query))
	{
		/*
		 * Recurse into (sub)queries to search for removed columns.
		 *
		 * Pass QTW_IGNORE_JOINALIASES to avoid recursing into a join RTE's
		 * joinaliasvars, as they always contain every unique column from
		 * the joined tables. Meaning that without this flag, each join with
		 * a table with removed columns would trigger this check.
		 * For example:
		 *
		 *  CREATE VIEW err AS SELECT jn.relname FROM (pg_class JOIN pg_namespace ON true) jn;
		 *
		 * will be erroneously reported as referencing removed columns. Legit cases like:
		 *
		 *  CREATE VIEW rte_join AS SELECT jn.relhasoids FROM (pg_class JOIN pg_namespace ON true) jn;
		 *
		 * are handled when processing Var nodes, for them (rte->rtekind == RTE_JOIN)
		 */
		Query *query = (Query *) node;
		context->rtableStack = lcons(query->rtable, context->rtableStack);
		query_tree_walker(query,
						  check_node_removed_columns_walker,
						  context,
						  QTW_IGNORE_JOINALIASES);
		context->rtableStack = list_delete_first(context->rtableStack);
		return false;
	}

	/*
	 * This ensures we look at all expressions, including entities that contain
	 * subqueries (such as CTEs and sublinks)
	 */
	return expression_tree_walker(node, check_node_removed_columns_walker, context);
}

Datum
get_removed_columns(PG_FUNCTION_ARGS)
{
	Oid			    view_oid = PG_GETARG_OID(0);
	Relation 	    rel;
	StringInfoData  buf;
	Oid             relnamespace;
	ListCell       *lc;
	char           *nspname;
	char           *relname;
	char           *attname;
	char           *comes_from_whole_row_reference_string;
	ReportedColumn *removed_column;
	Query	       *viewquery;
	RemovedColumnsWalkerContext context;

	rel = try_relation_open(view_oid, AccessShareLock, false);
	if (!RelationIsValid(rel))
		elog(ERROR, "Could not open relation file for relation oid %u", view_oid);

	context.rtableStack = NIL;
	context.removedColumns = NIL;
	context.inside_whole_row_reference = false;
	if (rel->rd_rel->relkind == RELKIND_VIEW)
	{
		viewquery = get_view_query(rel);
		check_node_removed_columns_walker((Node *) viewquery, &context);
	}
	else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
	{
		viewquery = get_matview_query(rel);
		check_node_removed_columns_walker((Node *) viewquery, &context);
	}

	relation_close(rel, AccessShareLock);

	/*
	 * Make a single formatted string, listing all unique removed columns.
	 * It will be displayed to the user.
	 */
	initStringInfo(&buf);
	foreach (lc, context.removedColumns)
	{
		removed_column = lfirst(lc);
		relname = get_rel_name(removed_column->attrelid);
		if (!relname)
			elog(ERROR, "cache lookup failed for relation %u", removed_column->attrelid);

		relnamespace = get_rel_namespace(removed_column->attrelid);
		if (!OidIsValid(relnamespace))
			elog(ERROR, "cache lookup failed for relation %u", removed_column->attrelid);

		nspname = get_namespace_name(relnamespace);
		if (!nspname)
			elog(ERROR, "cache lookup failed for namespace %u", relnamespace);

		attname = get_attname(removed_column->attrelid, removed_column->attnum);
		if (!attname)
			elog(ERROR, "cache lookup failed for attribute %d for relation %u",
				 removed_column->attnum, removed_column->attrelid);

		comes_from_whole_row_reference_string = "";
		if (removed_column->comes_from_whole_row_reference)
			comes_from_whole_row_reference_string = "(comes from a whole row reference)";

		appendStringInfo(&buf, "\t%s.%s.%s %s\n", nspname, relname, attname,
						 comes_from_whole_row_reference_string);

		pfree(relname);
		pfree(nspname);
		pfree(attname);
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
