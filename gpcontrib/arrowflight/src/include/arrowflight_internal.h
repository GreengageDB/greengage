/*-------------------------------------------------------------------------
 *
 * arrowflight_internal.h
 *    Shared declarations for the experimental Flight SQL extension.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ARROWFLIGHT_INTERNAL_H
#define ARROWFLIGHT_INTERNAL_H

extern "C"
{

#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "commands/explain.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "utils/rel.h"
#include "utils/resowner.h"

}

#define AF_SCHEME "arrowflight://"
#define AF_SCHEME_LEN (sizeof(AF_SCHEME) - 1)
#define AF_MAX_HOST_LEN 255
#define AF_DEFAULT_BATCH_ROWS 8192
#define AF_DEFAULT_MAX_BATCH_BYTES (4 * 1024 * 1024)
#define AF_MAX_TOKEN_BYTES 8192
#define AF_MAX_ENDPOINT_LOCATION_ALLOWLIST_BYTES 8192
#define AF_FDW_PROJECT_ALL (-1)
#define AF_FLIGHT_SQL_WRITE_TRANSACTION_AUTO_COMMIT "auto_commit"
#define AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED "required"
#define AF_FLIGHT_SQL_WRITE_ROUTING_ORIGIN "origin"
#define AF_FLIGHT_SQL_WRITE_ROUTING_PLANNED "planned"
#define AF_FLIGHT_SQL_DEFAULT_MAX_ENDPOINTS 10000
#define AF_FLIGHT_SQL_DEFAULT_MAX_PLAN_BYTES (16 * 1024 * 1024)
#define AF_FLIGHT_SQL_MAX_TRANSACTION_ID_BYTES (64 * 1024)
#define AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION 1
#define AF_FLIGHT_SQL_MPP_DEFAULT_LEASE_MS (5 * 60 * 1000)
#define AF_FLIGHT_SQL_MPP_CREATE_ACTION \
	"greengage.flight.sql.mpp_ingest.v1.create"
#define AF_FLIGHT_SQL_MPP_COMPLETE_ACTION \
	"greengage.flight.sql.mpp_ingest.v1.complete"
#define AF_FLIGHT_SQL_MPP_ABORT_ACTION \
	"greengage.flight.sql.mpp_ingest.v1.abort"
#define AF_FLIGHT_SQL_MPP_OPTION_VERSION "greengage.mpp.version"
#define AF_FLIGHT_SQL_MPP_OPTION_PLAN_ID "greengage.mpp.plan_id"
#define AF_FLIGHT_SQL_MPP_OPTION_ROUTE_TOKEN "greengage.mpp.route_token"
#define AF_FLIGHT_SQL_MPP_OPTION_SEGMENT_INDEX "greengage.mpp.segment_index"
#define AF_FLIGHT_SQL_MPP_OPTION_SEGMENT_COUNT "greengage.mpp.segment_count"
#define AF_FLIGHT_SQL_MPP_OPTION_CLIENT_OPERATION_ID \
	"greengage.mpp.client_operation_id"
#define AF_FLIGHT_SQL_MPP_OPTION_SCHEMA_FINGERPRINT \
	"greengage.mpp.schema_fingerprint"

#define AF_ARROW_TO_PG_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define AF_ARROW_TO_PG_EPOCH_USECS \
	((int64) AF_ARROW_TO_PG_EPOCH_DAYS * USECS_PER_DAY)

typedef struct ArrowFlightConnection
{
	char		host[AF_MAX_HOST_LEN + 1];
	int			port;
	bool		tls;
} ArrowFlightConnection;

typedef struct ArrowFlightSecurityOptions
{
	char	   *tls_ca_file;
	char	   *tls_client_cert_file;
	char	   *tls_client_key_file;
	char	   *auth_token_file;
	char	   *endpoint_location_allowlist;
} ArrowFlightSecurityOptions;

typedef enum ArrowFlightSqlCapabilityState
{
	AF_FLIGHT_SQL_CAPABILITY_UNKNOWN = -1,
	AF_FLIGHT_SQL_CAPABILITY_UNSUPPORTED = 0,
	AF_FLIGHT_SQL_CAPABILITY_SUPPORTED = 1
} ArrowFlightSqlCapabilityState;

typedef struct ArrowFlightSqlCapabilities
{
	char	   *server_name;
	char	   *server_version;
	ArrowFlightSqlCapabilityState bulk_ingestion;
	ArrowFlightSqlCapabilityState ingest_transactions;
	int			transaction_support;
	ArrowFlightSqlCapabilityState sql_transactions;
	ArrowFlightSqlCapabilityState cancellation;
	int			default_isolation;
	bool		default_isolation_known;
} ArrowFlightSqlCapabilities;

typedef struct ArrowFlightSqlMppRoute
{
	char	   *url;
	char	   *plan_id;
	char	   *route_token;
	char	   *client_operation_id;
	char	   *schema_fingerprint;
	char	   *worker_id;
	int			segment_index;
	int			segment_count;
} ArrowFlightSqlMppRoute;

typedef void (*ArrowFlightResourceCleanup) (void *resource);

typedef struct ArrowFlightSqlFdwExecState
{
	char	   *url;
	char	   *serialized_flight_info;
	bool		all_segments;
	bool		project_all;
	bool	   *projected_attrs;
	int			projected_nattrs;
	ArrowFlightSecurityOptions security_options;
	void	   *flight_state;
} ArrowFlightSqlFdwExecState;

bool		af_get_url_option(const char *url, const char *key, char *dst,
							  Size dstlen);
bool		af_get_url_bool_option(const char *url, const char *key,
								   bool default_value);
int			af_parse_int_option_value(const char *value, const char *key,
									  int min_value, int max_value);
int			af_get_url_int_option(const char *url, const char *key, int default_value,
								  int min_value, int max_value);
void		af_parse_flight_connection(const char *url,
									   ArrowFlightConnection *connection);
bool		af_type_uses_text_exchange(Oid typid);
bool		af_type_is_enum(Oid typid);
Datum		af_input_text_datum(Form_pg_attribute attr, const char *data, int32 len);
Datum		af_input_varchar_datum(Form_pg_attribute attr, const char *data,
								   int32 len);
char	   *af_output_text_datum(Oid typid, Datum value);
Datum		af_uuid_datum_from_bytes(const unsigned char *data, Size len);
Datum		af_interval_datum_from_parts(int32 months, int32 days,
										 int64 time_usecs);

const char *af_arrow_build_info(void);
void		af_check_arrow_flight_linkage(void);
bool		af_flightsql_predicate_is_safe(Expr *expression,
										   Index scan_relid,
										   bool require_leakproof);
void		af_flightsql_append_predicates(StringInfo buf,
										   Oid foreigntableid,
										   Index scan_relid,
										   List *expressions);
void		af_flightsql_stream_close(void *flight_state);
char	   *af_flightsql_execute_query(
	const char *url, const char *query, int max_endpoints, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options);
TupleTableSlot *af_flightsql_stream_next_slot(
	Relation rel, const char *url, const char *serialized_flight_info,
	void **flight_state, TupleTableSlot *slot, bool project_all,
	const bool *projected_attrs,
	const ArrowFlightSecurityOptions *security_options);
void		af_flightsql_get_capabilities(
	const char *url, const ArrowFlightSecurityOptions *security_options,
	ArrowFlightSqlCapabilities *capabilities);
char	   *af_flightsql_begin_transaction(
	const char *url, const ArrowFlightSecurityOptions *security_options);
char	   *af_flightsql_end_transaction(
	const char *url, const char *transaction_id, bool commit,
	const ArrowFlightSecurityOptions *security_options);
bool		af_flightsql_mpp_action_supported(
	const char *url, int timeout_ms,
	const ArrowFlightSecurityOptions *security_options);
char	   *af_flightsql_mpp_create_plan(
	const char *url, const char *client_operation_id,
	const char *catalog_name, const char *schema_name,
	const char *table_name, const char *transaction_mode,
	const char *transaction_id, const char *schema_ipc,
	int schema_ipc_len, const char *schema_fingerprint,
	int segment_count, int timeout_ms, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options,
	char **plan_id, bool *cluster_transaction);
void		af_flightsql_mpp_select_route(
	const char *serialized_plan, int segment_index, int segment_count,
	ArrowFlightSqlMppRoute *route);
char	   *af_flightsql_mpp_complete_plan(
	const char *url, const char *serialized_plan, int timeout_ms,
	int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options);
char	   *af_flightsql_mpp_abort_plan(
	const char *url, const char *serialized_plan, int timeout_ms,
	int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options);

void		af_resource_register(void *resource,
								 ArrowFlightResourceCleanup cleanup,
								 const char *description);
void		af_resource_attach(void **resource_slot);
void		af_resource_unregister(void *resource);

void	   *af_flightsql_writer_open(
	Relation rel, List *target_attrs, const char *url, const char *table_name,
	const char *schema_name, const char *catalog_name, int batch_rows,
	int max_batch_bytes, int timeout_ms, bool verify_ingested_rows,
	const char *transaction_id,
	const ArrowFlightSecurityOptions *security_options,
	const ArrowFlightSqlMppRoute *mpp_route);
char	   *af_flightsql_writer_schema(
	Relation rel, List *target_attrs, int *schema_len,
	char **schema_fingerprint);
void		af_flightsql_writer_append(void *writer_state,
									 TupleTableSlot *slot);
void		af_flightsql_writer_finish(void *writer_state);
void		af_flightsql_writer_abort(void *writer_state);

#endif							/* ARROWFLIGHT_INTERNAL_H */
