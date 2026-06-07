/*-------------------------------------------------------------------------
 *
 * arrowflight_internal.h
 *    Shared declarations for the experimental Arrow Flight extension.
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
#define AF_MAX_TICKET_LEN 1024
#define AF_MAX_RETRY_COUNT 10
#define AF_MAX_RETRY_BACKOFF_MS 60000
#define AF_DEFAULT_BATCH_ROWS 8192
#define AF_MAX_DATASET_LEN 256
#define AF_MAX_OPERATION_METADATA_LEN 8192
#define AF_DEFAULT_MAX_BATCH_BYTES (4 * 1024 * 1024)
#define AF_MAX_TOKEN_BYTES 8192
#define AF_TICKET_SEGID_PLACEHOLDER "{segid}"
#define AF_TICKET_SEGCOUNT_PLACEHOLDER "{segcount}"
#define AF_ENDPOINT_POLICY_FIRST "first"
#define AF_ENDPOINT_POLICY_SEGMENT_INDEX "segment_index"
#define AF_FDW_PROJECT_ALL (-1)
#define AF_PROJECTION_PUSHDOWN_OFF "off"
#define AF_PROJECTION_PUSHDOWN_TRY "try"
#define AF_PROJECTION_PUSHDOWN_REQUIRE "require"
#define AF_FDW_WRITE_MODE_STAGING "staging"
#define AF_FDW_WRITE_MODE_APPEND "append"

#define AF_ARROW_TO_PG_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define AF_ARROW_TO_PG_EPOCH_USECS \
	((int64) AF_ARROW_TO_PG_EPOCH_DAYS * USECS_PER_DAY)

typedef struct ArrowFlightEndpoint
{
	char		host[AF_MAX_HOST_LEN + 1];
	int			port;
	char		ticket[AF_MAX_TICKET_LEN + 1];
	bool		tls;
} ArrowFlightEndpoint;

typedef struct ArrowFlightReadCtx
{
	StringInfoData data;
	int			cursor;
	bool		eof;
	void	   *flight_state;
	ResourceOwner owner;
	struct ArrowFlightReadCtx *next;
} ArrowFlightReadCtx;

typedef struct ArrowFlightSecurityOptions
{
	char	   *tls_ca_file;
	char	   *tls_client_cert_file;
	char	   *tls_client_key_file;
	char	   *auth_token_file;
} ArrowFlightSecurityOptions;

typedef struct ArrowFlightFdwPlanState
{
	char	   *url;
	ArrowFlightSecurityOptions security_options;
	double		rows;
} ArrowFlightFdwPlanState;

typedef struct ArrowFlightFdwExecState
{
	char	   *url;
	bool		all_segments;
	bool		project_all;
	bool	   *projected_attrs;
	int			projected_nattrs;
	int			projected_attr_count;
	ArrowFlightSecurityOptions security_options;
	void	   *flight_state;
} ArrowFlightFdwExecState;

bool		af_has_scheme(const char *url);
bool		af_get_url_option(const char *url, const char *key, char *dst,
							  Size dstlen);
bool		af_get_url_bool_option(const char *url, const char *key,
								   bool default_value);
int			af_parse_int_option_value(const char *value, const char *key,
									  int min_value, int max_value);
int			af_get_url_int_option(const char *url, const char *key, int default_value,
								  int min_value, int max_value);
void		af_validate_fdw_write_mode(const char *write_mode);
void		af_validate_fdw_dataset(const char *dataset);
void		af_validate_fdw_operation_metadata(const char *metadata);
void		af_validate_endpoint_policy(const char *policy);
void		af_validate_projection_pushdown(const char *mode);
void		af_parse_flight_endpoint(const char *url, ArrowFlightEndpoint *endpoint);
void		af_expand_ticket_placeholders(ArrowFlightEndpoint *endpoint);
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
void	   *af_flight_stream_open(TupleDesc tupdesc, const char *url,
								  const char *consumer, const bool *projected_attrs,
								  const ArrowFlightSecurityOptions *security_options);
TupleTableSlot *af_flight_stream_next_slot(Relation rel, const char *url,
										   void **flight_state, TupleTableSlot *slot,
										   bool project_all, const bool *projected_attrs,
										   const ArrowFlightSecurityOptions *security_options);
void		af_flight_stream_close(void *flight_state);

void		af_append_i32(StringInfo out, int32 value);
void		af_append_i64(StringInfo out, int64 value);
int32		af_read_i32(const char *buf);
int64		af_read_i64(const char *buf);
Datum		af_decode_attr(Form_pg_attribute attr, const char *payload,
						   int32 len, bool *isnull);
void		af_unsupported_type(Oid typid, const char *attname);

void	   *af_fdw_writer_open(Relation rel, List *target_attrs,
							   const char *operation_id, const char *url,
							   const char *dataset, const char *write_mode,
							   const char *operation_metadata, int batch_rows,
							   int max_batch_bytes, int timeout_ms,
							   int retry_count, int retry_backoff_ms,
							   const ArrowFlightSecurityOptions *security_options);
void		af_fdw_writer_append(void *writer_state, TupleTableSlot *slot);
void		af_fdw_writer_finish(void *writer_state);
void		af_fdw_writer_abort(void *writer_state);

List	   *af_fdw_PlanForeignModify(PlannerInfo *root, ModifyTable *plan,
									 Index resultRelation, int subplan_index);
void		af_fdw_BeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo,
									  List *fdw_private, int subplan_index,
									  int eflags);
TupleTableSlot *af_fdw_ExecForeignInsert(EState *estate, ResultRelInfo *rinfo,
										 TupleTableSlot *slot,
										 TupleTableSlot *planSlot);
void		af_fdw_EndForeignModify(EState *estate, ResultRelInfo *rinfo);
void		af_fdw_BeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo);
void		af_fdw_EndForeignInsert(EState *estate, ResultRelInfo *rinfo);
int			af_fdw_IsForeignRelUpdatable(Relation rel);
void		af_fdw_ExplainForeignModify(ModifyTableState *mtstate,
										ResultRelInfo *rinfo, List *fdw_private,
										int subplan_index, ExplainState *es);

#endif							/* ARROWFLIGHT_INTERNAL_H */
