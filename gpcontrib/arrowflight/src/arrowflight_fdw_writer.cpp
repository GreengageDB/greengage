/*-------------------------------------------------------------------------
 *
 * arrowflight_fdw_writer.cpp
 *	  Arrow Flight DoPut writer for arrowflight_fdw INSERT.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "catalog/pg_type.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "common/int.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

#include <string.h>

}

#ifdef USE_ARROW_FLIGHT
#ifdef Abs
#undef Abs
#endif

#include <arrow/api.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_time.h>
#include <arrow/buffer.h>
#include <arrow/flight/api.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/decimal.h>

#include <cerrno>
#include <cstdlib>
#include <exception>
#include <cstdint>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#define AF_PG_TO_ARROW_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define AF_PG_TO_ARROW_EPOCH_USECS \
	((int64) AF_PG_TO_ARROW_EPOCH_DAYS * USECS_PER_DAY)

typedef enum ArrowFlightFdwWriterColumnKind
{
	AF_WRITER_BOOL,
	AF_WRITER_INT2,
	AF_WRITER_INT4,
	AF_WRITER_INT8,
	AF_WRITER_FLOAT4,
	AF_WRITER_FLOAT8,
	AF_WRITER_NUMERIC,
	AF_WRITER_STRING,
	AF_WRITER_BYTEA,
	AF_WRITER_TEXT_EXCHANGE,
	AF_WRITER_UUID,
	AF_WRITER_INTERVAL,
	AF_WRITER_TIME,
	AF_WRITER_MONEY,
	AF_WRITER_INET,
	AF_WRITER_CIDR,
	AF_WRITER_MACADDR,
	AF_WRITER_ENUM,
	AF_WRITER_DATE,
	AF_WRITER_TIMESTAMP,
	AF_WRITER_TIMESTAMPTZ
} ArrowFlightFdwWriterColumnKind;

typedef struct ArrowFlightFdwWriterColumn
{
	int			attnum;
	Oid			typid;
	std::string name;
	ArrowFlightFdwWriterColumnKind kind;
	std::shared_ptr<arrow::DataType> arrow_type;
	std::unique_ptr<arrow::ArrayBuilder> builder;
} ArrowFlightFdwWriterColumn;

class ArrowFlightFdwWriterState
{
public:
	std::string operation_id;
	std::string url;
	std::string dataset;
	std::string write_mode;
	std::string operation_metadata;
	std::string endpoint;
	std::string descriptor;
	std::string stream_id;
	std::string tls_ca_file;
	std::string tls_client_cert_file;
	std::string tls_client_key_file;
	std::string auth_token_file;
	arrow::flight::FlightCallOptions call_options;
	std::unique_ptr<arrow::flight::FlightClient> client;
	std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
	std::unique_ptr<arrow::flight::FlightMetadataReader> reader;
	std::shared_ptr<arrow::Schema> schema;
	std::vector<ArrowFlightFdwWriterColumn> columns;
	int			batch_rows = AF_DEFAULT_BATCH_ROWS;
	int			max_batch_bytes = AF_DEFAULT_MAX_BATCH_BYTES;
	int			retry_count = 0;
	int			retry_backoff_ms = 100;
	int			attempt = 0;
	int64		rows_in_batch = 0;
	int64		estimated_batch_bytes = 0;
	int64		total_rows = 0;
	int64		batches = 0;
	std::string server_ack_transfer_id;
	int64		server_ack_rows = -1;
	int64		server_ack_bytes = -1;
	int64		server_ack_batches = -1;
	bool		server_final_ack = false;
	bool		finalized = false;
	bool		tls_enabled = false;
	bool		auth_enabled = false;
};

template <typename T>
static T af_writer_value_or_throw(arrow::Result<T> result,
								  const char *context);
static void af_writer_status_or_throw(const arrow::Status& status,
									  const char *context);
static std::shared_ptr<arrow::DataType> af_writer_arrow_type(Form_pg_attribute attr,
															 ArrowFlightFdwWriterColumnKind *kind);
static bool af_writer_numeric_typmod(Form_pg_attribute attr,
									 int32 *precision, int32 *scale);
static std::unique_ptr<arrow::ArrayBuilder> af_writer_make_builder(
									const ArrowFlightFdwWriterColumn *column);
static void af_writer_init_columns(ArrowFlightFdwWriterState *state,
								   Relation rel, List *target_attrs);
static void af_writer_reset_builders(ArrowFlightFdwWriterState *state);
static std::shared_ptr<arrow::Schema> af_writer_build_schema(
								   ArrowFlightFdwWriterState *state,
								   Relation rel);
static std::shared_ptr<arrow::KeyValueMetadata> af_writer_build_metadata(
								   ArrowFlightFdwWriterState *state,
								   Relation rel);
static void af_writer_append_static_metadata(arrow::KeyValueMetadata *metadata,
											 const char *operation_metadata);
static std::string af_writer_batch_metadata(ArrowFlightFdwWriterState *state,
											bool final, int64 rows);
static void af_writer_connect(ArrowFlightFdwWriterState *state);
static arrow::flight::FlightClientOptions af_writer_client_options(
									ArrowFlightFdwWriterState *state);
static void af_writer_apply_auth_header(ArrowFlightFdwWriterState *state);
static std::string af_writer_read_file(const std::string& path,
									   const char *label);
static std::string af_writer_read_token(const std::string& path);
static int32 af_writer_date_to_arrow_days(
									const ArrowFlightFdwWriterColumn *column,
									Datum value);
static int64 af_writer_timestamp_to_arrow_usecs(
									const ArrowFlightFdwWriterColumn *column,
									Datum value);
static void af_writer_append_attr(ArrowFlightFdwWriterColumn *column,
								  Datum value, bool isnull,
								  int64 *estimated_bytes);
static void af_writer_flush(ArrowFlightFdwWriterState *state);
static void af_writer_drain_metadata(ArrowFlightFdwWriterState *state);
static bool af_writer_metadata_has_line(std::shared_ptr<arrow::Buffer> metadata,
										const char *key,
										const char *value);
static bool af_writer_metadata_value(std::shared_ptr<arrow::Buffer> metadata,
									 const char *key, std::string *value);
static bool af_writer_parse_int64(const std::string& value, int64 *out);
static void af_writer_parse_ack_metadata(ArrowFlightFdwWriterState *state,
										 std::shared_ptr<arrow::Buffer> metadata);
static std::string af_writer_action_body(ArrowFlightFdwWriterState *state,
										 const char *action_type);
static void af_writer_do_action(ArrowFlightFdwWriterState *state,
								const char *action_type);
static void af_writer_abort_operation(ArrowFlightFdwWriterState *state);
static std::string af_writer_context(ArrowFlightFdwWriterState *state);
static void af_writer_delete(ArrowFlightFdwWriterState *state);

void *
af_fdw_writer_open(Relation rel, List *target_attrs,
				   const char *operation_id, const char *url,
				   const char *dataset, const char *write_mode,
				   const char *operation_metadata, int batch_rows,
				   int max_batch_bytes, int timeout_ms,
				   int retry_count, int retry_backoff_ms,
				   const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightFdwWriterState> state;

	try
	{
		ArrowFlightEndpoint endpoint;

		if (GpIdentity.segindex < 0)
			throw std::runtime_error("Arrow Flight FDW write requires QE segment execution");

		CHECK_FOR_INTERRUPTS();

		af_parse_flight_endpoint(url, &endpoint);
		state = std::make_unique<ArrowFlightFdwWriterState>();
		state->operation_id = operation_id == nullptr ? "" : operation_id;
		state->url = url == nullptr ? "" : url;
		state->dataset = dataset == nullptr ? "" : dataset;
		state->write_mode = write_mode == nullptr ? "" : write_mode;
		state->tls_enabled = endpoint.tls;
		if (security_options != nullptr)
		{
			state->tls_ca_file = security_options->tls_ca_file == nullptr ?
				"" : security_options->tls_ca_file;
			state->tls_client_cert_file =
				security_options->tls_client_cert_file == nullptr ?
				"" : security_options->tls_client_cert_file;
			state->tls_client_key_file =
				security_options->tls_client_key_file == nullptr ?
				"" : security_options->tls_client_key_file;
			state->auth_token_file =
				security_options->auth_token_file == nullptr ?
				"" : security_options->auth_token_file;
		}
		state->auth_enabled = !state->auth_token_file.empty();
		if (!state->tls_enabled &&
			(!state->tls_ca_file.empty() ||
			 !state->tls_client_cert_file.empty() ||
			 !state->tls_client_key_file.empty() ||
			 state->auth_enabled))
			throw std::runtime_error("Arrow Flight TLS/auth options require tls=true");
		state->operation_metadata =
			operation_metadata == nullptr ? "" : operation_metadata;
		state->batch_rows = batch_rows;
		state->max_batch_bytes = max_batch_bytes;
		state->retry_count = retry_count;
		state->retry_backoff_ms = retry_backoff_ms;
		state->stream_id = state->operation_id + "/seg" +
			std::to_string(GpIdentity.segindex) + "/attempt0";
		state->descriptor = "af-v1/write/" + state->dataset + "/" +
			state->operation_id + "/segment/" +
			std::to_string(GpIdentity.segindex);

		arrow::Result<arrow::flight::Location> location_result =
			endpoint.tls ?
			arrow::flight::Location::ForGrpcTls(endpoint.host, endpoint.port) :
			arrow::flight::Location::ForGrpcTcp(endpoint.host, endpoint.port);
		arrow::flight::Location location =
			af_writer_value_or_throw(std::move(location_result),
									 "create Arrow Flight write location");
		state->endpoint = location.ToString();

		if (timeout_ms > 0)
			state->call_options.timeout =
				arrow::flight::TimeoutDuration((double) timeout_ms / 1000.0);

		af_writer_init_columns(state.get(), rel, target_attrs);

		int			attempt;
		std::string last_error;

		for (attempt = 0; attempt <= retry_count; attempt++)
		{
			CHECK_FOR_INTERRUPTS();
			state->attempt = attempt;
			state->stream_id = state->operation_id + "/seg" +
				std::to_string(GpIdentity.segindex) + "/attempt" +
				std::to_string(attempt);
			state->schema = af_writer_build_schema(state.get(), rel);
			state->call_options.headers.clear();
			state->call_options.headers.push_back(
				{"x-arrowflight-operation-id", state->operation_id});
			state->call_options.headers.push_back(
				{"x-arrowflight-stream-id", state->stream_id});
			state->call_options.headers.push_back(
				{"x-arrowflight-segment-index",
				 std::to_string(GpIdentity.segindex)});
			af_writer_apply_auth_header(state.get());

			try
			{
				state->client.reset();
				state->writer.reset();
				state->reader.reset();

				state->client =
					af_writer_value_or_throw(
						arrow::flight::FlightClient::Connect(
							location, af_writer_client_options(state.get())),
						"connect to Arrow Flight write server");
				af_writer_connect(state.get());
				break;
			}
			catch (const std::exception& ex)
			{
				last_error = ex.what();
				state->client.reset();
				state->writer.reset();
				state->reader.reset();

				if (attempt >= retry_count)
					throw std::runtime_error("failed after " +
											 std::to_string(attempt + 1) +
											 " attempt(s): " + last_error);

				if (retry_backoff_ms > 0)
					pg_usleep((long) retry_backoff_ms * 1000L);
			}
		}

		return state.release();
	}
	catch (const std::exception& ex)
	{
		std::string context = af_writer_context(state.get());

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write raised unknown C++ exception")));
	}

	return NULL;
}

void
af_fdw_writer_append(void *writer_state, TupleTableSlot *slot)
{
	ArrowFlightFdwWriterState *state =
		static_cast<ArrowFlightFdwWriterState *>(writer_state);

	if (state == nullptr)
		elog(ERROR, "arrowflight_fdw writer is not initialized");

	try
	{
		CHECK_FOR_INTERRUPTS();

		for (auto& column : state->columns)
		{
			bool		isnull = false;
			Datum		value = slot_getattr(slot, column.attnum, &isnull);

			af_writer_append_attr(&column, value, isnull,
								  &state->estimated_batch_bytes);
		}

		state->rows_in_batch++;
		state->total_rows++;

		if (state->rows_in_batch >= state->batch_rows ||
			(state->max_batch_bytes > 0 &&
			 state->estimated_batch_bytes >= state->max_batch_bytes))
			af_writer_flush(state);
	}
	catch (const std::exception& ex)
	{
		std::string context = af_writer_context(state);

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write raised unknown C++ exception")));
	}
}

void
af_fdw_writer_finish(void *writer_state)
{
	ArrowFlightFdwWriterState *state =
		static_cast<ArrowFlightFdwWriterState *>(writer_state);

	if (state == nullptr)
		return;

	try
	{
		CHECK_FOR_INTERRUPTS();
		af_writer_flush(state);

		if (state->writer != nullptr)
		{
			af_writer_status_or_throw(
				state->writer->WriteMetadata(
					arrow::Buffer::FromString(
						af_writer_batch_metadata(state, true, 0))),
				"write Arrow Flight final metadata");
			af_writer_status_or_throw(state->writer->DoneWriting(),
									  "finish Arrow Flight DoPut stream");
		}

		af_writer_drain_metadata(state);
		af_writer_do_action(state, "FinalizeOperation");
		state->finalized = true;
		af_writer_delete(state);
	}
	catch (const std::exception& ex)
	{
		std::string context = af_writer_context(state);

		if (state != nullptr && !state->finalized)
			af_writer_abort_operation(state);
		af_writer_delete(state);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		af_writer_delete(state);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight FDW write raised unknown C++ exception")));
	}
}

void
af_fdw_writer_abort(void *writer_state)
{
	ArrowFlightFdwWriterState *state =
		static_cast<ArrowFlightFdwWriterState *>(writer_state);

	if (state != nullptr && !state->finalized)
		af_writer_abort_operation(state);
	af_writer_delete(state);
}

template <typename T>
static T
af_writer_value_or_throw(arrow::Result<T> result, const char *context)
{
	if (!result.ok())
		throw std::runtime_error("failed to " + std::string(context) +
								 ": " + result.status().ToString());

	return std::move(result).ValueOrDie();
}

static void
af_writer_status_or_throw(const arrow::Status& status, const char *context)
{
	if (!status.ok())
		throw std::runtime_error("failed to " + std::string(context) +
								 ": " + status.ToString());
}

static std::shared_ptr<arrow::DataType>
af_writer_arrow_type(Form_pg_attribute attr,
					 ArrowFlightFdwWriterColumnKind *kind)
{
	switch (attr->atttypid)
	{
		case BOOLOID:
			*kind = AF_WRITER_BOOL;
			return arrow::boolean();
		case INT2OID:
			*kind = AF_WRITER_INT2;
			return arrow::int16();
		case INT4OID:
			*kind = AF_WRITER_INT4;
			return arrow::int32();
		case INT8OID:
			*kind = AF_WRITER_INT8;
			return arrow::int64();
		case FLOAT4OID:
			*kind = AF_WRITER_FLOAT4;
			return arrow::float32();
		case FLOAT8OID:
			*kind = AF_WRITER_FLOAT8;
			return arrow::float64();
		case NUMERICOID:
		{
			int32		precision;
			int32		scale;

			if (!af_writer_numeric_typmod(attr, &precision, &scale))
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" requires constrained numeric(p,s) with p <= 38 for Arrow Decimal128");

			*kind = AF_WRITER_NUMERIC;
			return arrow::decimal128(precision, scale);
		}
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			*kind = AF_WRITER_STRING;
			return arrow::utf8();
		case BYTEAOID:
			*kind = AF_WRITER_BYTEA;
			return arrow::binary();
		case UUIDOID:
			*kind = AF_WRITER_UUID;
			return arrow::fixed_size_binary(UUID_LEN);
		case INTERVALOID:
			*kind = AF_WRITER_INTERVAL;
			return arrow::month_day_nano_interval();
		case TIMEOID:
			*kind = AF_WRITER_TIME;
			return arrow::time64(arrow::TimeUnit::MICRO);
		case CASHOID:
			*kind = AF_WRITER_MONEY;
			return arrow::int64();
		case INETOID:
			*kind = AF_WRITER_INET;
			return arrow::fixed_size_binary(18);
		case CIDROID:
			*kind = AF_WRITER_CIDR;
			return arrow::fixed_size_binary(18);
		case MACADDROID:
			*kind = AF_WRITER_MACADDR;
			return arrow::fixed_size_binary(6);
		case DATEOID:
			*kind = AF_WRITER_DATE;
			return arrow::date32();
		case TIMESTAMPOID:
			*kind = AF_WRITER_TIMESTAMP;
			return arrow::timestamp(arrow::TimeUnit::MICRO);
		case TIMESTAMPTZOID:
			*kind = AF_WRITER_TIMESTAMPTZ;
			return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
		default:
			if (af_type_is_enum(attr->atttypid))
			{
				*kind = AF_WRITER_ENUM;
				return arrow::dictionary(arrow::int32(), arrow::utf8());
			}

			if (af_type_uses_text_exchange(attr->atttypid))
			{
				*kind = AF_WRITER_TEXT_EXCHANGE;
				return arrow::utf8();
			}

			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" has unsupported type oid " +
									 std::to_string(attr->atttypid) +
									 " for Arrow Flight FDW write");
	}
}

static bool
af_writer_numeric_typmod(Form_pg_attribute attr, int32 *precision, int32 *scale)
{
	int32		typmod;
	int32		tmp_typmod;

	if (attr == nullptr || precision == nullptr || scale == nullptr)
		return false;

	typmod = attr->atttypmod;
	if (typmod < (int32) VARHDRSZ)
		return false;

	tmp_typmod = typmod - VARHDRSZ;
	*precision = (tmp_typmod >> 16) & 0xffff;
	*scale = tmp_typmod & 0xffff;
	return *precision >= 1 && *precision <= 38 &&
		*scale >= 0 && *scale <= *precision;
}

static std::unique_ptr<arrow::ArrayBuilder>
af_writer_make_builder(const ArrowFlightFdwWriterColumn *column)
{
	if (column->kind == AF_WRITER_ENUM)
		return std::make_unique<arrow::Dictionary32Builder<arrow::StringType>>(
			arrow::utf8(), arrow::default_memory_pool());

	return af_writer_value_or_throw(
		arrow::MakeBuilder(column->arrow_type, arrow::default_memory_pool()),
		"create Arrow array builder");
}

static void
af_writer_init_columns(ArrowFlightFdwWriterState *state, Relation rel,
					   List *target_attrs)
{
	ListCell   *cell;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	foreach(cell, target_attrs)
	{
		int			attnum = lfirst_int(cell);
		Form_pg_attribute attr;
		ArrowFlightFdwWriterColumn column;

		if (attnum <= 0 || attnum > tupdesc->natts)
			throw std::runtime_error("invalid Arrow Flight FDW write target attribute");

		attr = TupleDescAttr(tupdesc, attnum - 1);
		if (attr->attisdropped)
			continue;

		column.attnum = attnum;
		column.typid = attr->atttypid;
		column.name = NameStr(attr->attname);
		column.arrow_type = af_writer_arrow_type(attr, &column.kind);
		column.builder = af_writer_make_builder(&column);
		state->columns.push_back(std::move(column));
	}
}

static void
af_writer_reset_builders(ArrowFlightFdwWriterState *state)
{
	for (auto& column : state->columns)
		column.builder = af_writer_make_builder(&column);
}

static std::shared_ptr<arrow::Schema>
af_writer_build_schema(ArrowFlightFdwWriterState *state, Relation rel)
{
	std::vector<std::shared_ptr<arrow::Field>> fields;

	for (const auto& column : state->columns)
		fields.push_back(arrow::field(column.name, column.arrow_type));

	return arrow::schema(fields)->WithMetadata(
		af_writer_build_metadata(state, rel));
}

static std::shared_ptr<arrow::KeyValueMetadata>
af_writer_build_metadata(ArrowFlightFdwWriterState *state, Relation rel)
{
	auto metadata = std::make_shared<arrow::KeyValueMetadata>();

	metadata->Append("af.protocol.version", "1");
	metadata->Append("af.operation.id", state->operation_id);
	metadata->Append("af.operation.type", "insert");
	metadata->Append("af.operation.mode", state->write_mode);
	metadata->Append("af.dataset", state->dataset);
	metadata->Append("af.relation.oid",
					 std::to_string(RelationGetRelid(rel)));
	metadata->Append("af.relation.name", RelationGetRelationName(rel));
	metadata->Append("af.segment.index",
					 std::to_string(GpIdentity.segindex));
	metadata->Append("af.segment.count",
					 std::to_string(getgpsegmentCount()));
	metadata->Append("af.stream.id", state->stream_id);
	metadata->Append("af.stream.attempt", std::to_string(state->attempt));
	metadata->Append("af.session.id", std::to_string(gp_session_id));
	metadata->Append("af.command.count", std::to_string(gp_command_count));
	metadata->Append("af.pid", std::to_string(MyProcPid));
	af_writer_append_static_metadata(metadata.get(),
									 state->operation_metadata.c_str());

	return metadata;
}

static void
af_writer_append_static_metadata(arrow::KeyValueMetadata *metadata,
								 const char *operation_metadata)
{
	const char *pos;

	if (operation_metadata == nullptr || operation_metadata[0] == '\0')
		return;

	pos = operation_metadata;
	while (*pos != '\0')
	{
		const char *entry_end = pos;
		const char *eq;
		std::string key;
		std::string value;

		while (*entry_end != '\0' && *entry_end != ',' &&
			   *entry_end != ';')
			entry_end++;

		eq = (const char *) memchr(pos, '=', entry_end - pos);
		if (eq == nullptr)
			break;

		key.assign(pos, eq - pos);
		value.assign(eq + 1, entry_end - eq - 1);
		if (key.rfind("static.", 0) == 0)
			key = "af." + key;
		metadata->Append(key, value);

		pos = entry_end;
		if (*pos == ',' || *pos == ';')
			pos++;
	}
}

static std::string
af_writer_batch_metadata(ArrowFlightFdwWriterState *state, bool final,
						 int64 rows)
{
	std::string metadata;

	metadata += "af.operation.id=" + state->operation_id + "\n";
	metadata += "af.stream.id=" + state->stream_id + "\n";
	metadata += "af.segment.index=" + std::to_string(GpIdentity.segindex) + "\n";
	metadata += "af.batch.index=" + std::to_string(state->batches) + "\n";
	metadata += "af.batch.rows=" + std::to_string(rows) + "\n";
	metadata += "af.batch.final=" + std::string(final ? "true" : "false") + "\n";
	return metadata;
}

static void
af_writer_connect(ArrowFlightFdwWriterState *state)
{
	arrow::flight::FlightDescriptor descriptor =
		arrow::flight::FlightDescriptor::Path(
			{"af-v1", "write", state->dataset, state->operation_id,
			 "segment", std::to_string(GpIdentity.segindex)});

	arrow::flight::FlightClient::DoPutResult result =
		af_writer_value_or_throw(
			state->client->DoPut(state->call_options, descriptor,
								 state->schema),
			"open Arrow Flight DoPut stream");

	state->writer = std::move(result.writer);
	state->reader = std::move(result.reader);
}

static arrow::flight::FlightClientOptions
af_writer_client_options(ArrowFlightFdwWriterState *state)
{
	arrow::flight::FlightClientOptions options =
		arrow::flight::FlightClientOptions::Defaults();

	if (state == nullptr || !state->tls_enabled)
		return options;

	if (!state->tls_ca_file.empty())
		options.tls_root_certs =
			af_writer_read_file(state->tls_ca_file, "tls_ca_file");

	if (state->tls_client_cert_file.empty() !=
		state->tls_client_key_file.empty())
		throw std::runtime_error("tls_client_cert_file and tls_client_key_file must be set together");

	if (!state->tls_client_cert_file.empty())
	{
		options.cert_chain =
			af_writer_read_file(state->tls_client_cert_file,
								"tls_client_cert_file");
		options.private_key =
			af_writer_read_file(state->tls_client_key_file,
								"tls_client_key_file");
	}

	return options;
}

static void
af_writer_apply_auth_header(ArrowFlightFdwWriterState *state)
{
	if (state == nullptr || state->auth_token_file.empty())
		return;

	if (!state->tls_enabled)
		throw std::runtime_error("auth_token_file requires tls=true");

	state->call_options.headers.push_back(
		{"authorization",
		 "Bearer " + af_writer_read_token(state->auth_token_file)});
}

static std::string
af_writer_read_file(const std::string& path, const char *label)
{
	std::ifstream file(path, std::ios::binary);

	if (!file)
		throw std::runtime_error(std::string("could not read Arrow Flight ") +
								 label + " file");

	std::ostringstream out;
	out << file.rdbuf();
	return out.str();
}

static std::string
af_writer_read_token(const std::string& path)
{
	std::string token = af_writer_read_file(path, "auth_token");

	while (!token.empty() &&
		   (token.back() == '\n' || token.back() == '\r'))
		token.pop_back();

	if (token.empty())
		throw std::runtime_error("Arrow Flight auth_token_file is empty");

	if (token.size() > AF_MAX_TOKEN_BYTES)
		throw std::runtime_error("Arrow Flight auth_token_file is too large");

	for (unsigned char ch : token)
	{
		if (ch < 0x20 || ch == 0x7f)
			throw std::runtime_error("Arrow Flight auth_token_file contains control characters");
	}

	return token;
}

static int32
af_writer_date_to_arrow_days(const ArrowFlightFdwWriterColumn *column,
							 Datum value)
{
	DateADT	date = DatumGetDateADT(value);
	int64	arrow_days;

	if (DATE_NOT_FINITE(date))
		throw std::runtime_error("column \"" + column->name +
								 "\" date infinity cannot be represented as Arrow date32");

	if (!IS_VALID_DATE(date))
		throw std::runtime_error("column \"" + column->name +
								 "\" date value is out of Greengage range");

	arrow_days = (int64) date + AF_PG_TO_ARROW_EPOCH_DAYS;
	if (arrow_days < PG_INT32_MIN || arrow_days > PG_INT32_MAX)
		throw std::runtime_error("column \"" + column->name +
								 "\" date value is out of Arrow date32 range");

	return (int32) arrow_days;
}

static int64
af_writer_timestamp_to_arrow_usecs(const ArrowFlightFdwWriterColumn *column,
								   Datum value)
{
	Timestamp	timestamp = DatumGetTimestamp(value);
	int64		arrow_usecs;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		throw std::runtime_error("column \"" + column->name +
								 "\" timestamp infinity cannot be represented as Arrow timestamp");

	if (!IS_VALID_TIMESTAMP(timestamp))
		throw std::runtime_error("column \"" + column->name +
								 "\" timestamp value is out of Greengage range");

	if (pg_add_s64_overflow(timestamp, AF_PG_TO_ARROW_EPOCH_USECS,
							&arrow_usecs))
		throw std::runtime_error("column \"" + column->name +
								 "\" timestamp value is out of Arrow timestamp range");

	return arrow_usecs;
}

static void
af_writer_append_attr(ArrowFlightFdwWriterColumn *column, Datum value,
					  bool isnull, int64 *estimated_bytes)
{
	if (isnull)
	{
		af_writer_status_or_throw(column->builder->AppendNull(),
								  "append Arrow null");
		if (estimated_bytes != nullptr)
			*estimated_bytes += 1;
		return;
	}

	switch (column->kind)
	{
		case AF_WRITER_BOOL:
			af_writer_status_or_throw(
				static_cast<arrow::BooleanBuilder *>(column->builder.get())
				->Append(DatumGetBool(value)),
				"append Arrow bool");
			if (estimated_bytes != nullptr)
				*estimated_bytes += 1;
			break;
		case AF_WRITER_INT2:
			af_writer_status_or_throw(
				static_cast<arrow::Int16Builder *>(column->builder.get())
				->Append(DatumGetInt16(value)),
				"append Arrow int2");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int16);
			break;
		case AF_WRITER_INT4:
			af_writer_status_or_throw(
				static_cast<arrow::Int32Builder *>(column->builder.get())
				->Append(DatumGetInt32(value)),
				"append Arrow int4");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int32);
			break;
		case AF_WRITER_INT8:
			af_writer_status_or_throw(
				static_cast<arrow::Int64Builder *>(column->builder.get())
				->Append(DatumGetInt64(value)),
				"append Arrow int8");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int64);
			break;
		case AF_WRITER_FLOAT4:
			af_writer_status_or_throw(
				static_cast<arrow::FloatBuilder *>(column->builder.get())
				->Append(DatumGetFloat4(value)),
				"append Arrow float4");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(float4);
			break;
		case AF_WRITER_FLOAT8:
			af_writer_status_or_throw(
				static_cast<arrow::DoubleBuilder *>(column->builder.get())
				->Append(DatumGetFloat8(value)),
				"append Arrow float8");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(float8);
			break;
		case AF_WRITER_NUMERIC:
		{
			char	   *numeric_text = af_output_text_datum(NUMERICOID, value);
			arrow::Decimal128 arrow_value =
				af_writer_value_or_throw(
					arrow::Decimal128::FromString(numeric_text),
					"parse Arrow decimal");

			af_writer_status_or_throw(
				static_cast<arrow::Decimal128Builder *>(column->builder.get())
				->Append(arrow_value),
				"append Arrow numeric");
			if (estimated_bytes != nullptr)
				*estimated_bytes += 16;
			pfree(numeric_text);
			break;
		}
		case AF_WRITER_STRING:
		{
			text	   *text_value = DatumGetTextPP(value);
			const char *data = VARDATA_ANY(text_value);
			int32		len = VARSIZE_ANY_EXHDR(text_value);

			af_writer_status_or_throw(
				static_cast<arrow::StringBuilder *>(column->builder.get())
				->Append(data, len),
				"append Arrow string");
			if (estimated_bytes != nullptr)
				*estimated_bytes += len + sizeof(int32);
			if ((Pointer) text_value != DatumGetPointer(value))
				pfree(text_value);
			break;
		}
		case AF_WRITER_BYTEA:
		{
			bytea	   *bytea_value = DatumGetByteaPP(value);
			const char *data = VARDATA_ANY(bytea_value);
			int32		len = VARSIZE_ANY_EXHDR(bytea_value);

			af_writer_status_or_throw(
				static_cast<arrow::BinaryBuilder *>(column->builder.get())
				->Append(reinterpret_cast<const uint8_t *>(data), len),
				"append Arrow bytea");
			if (estimated_bytes != nullptr)
				*estimated_bytes += len + sizeof(int32);
			if ((Pointer) bytea_value != DatumGetPointer(value))
				pfree(bytea_value);
			break;
		}
		case AF_WRITER_TEXT_EXCHANGE:
		{
			char	   *text_value = af_output_text_datum(column->typid, value);
			int32		len = strlen(text_value);

			af_writer_status_or_throw(
				static_cast<arrow::StringBuilder *>(column->builder.get())
				->Append(text_value, len),
				"append Arrow text-exchange value");
			if (estimated_bytes != nullptr)
				*estimated_bytes += len + sizeof(int32);
			pfree(text_value);
			break;
		}
		case AF_WRITER_UUID:
		{
			pg_uuid_t  *uuid = DatumGetUUIDP(value);

			af_writer_status_or_throw(
				static_cast<arrow::FixedSizeBinaryBuilder *>(column->builder.get())
				->Append(uuid->data),
				"append Arrow uuid");
			if (estimated_bytes != nullptr)
				*estimated_bytes += UUID_LEN;
			break;
		}
		case AF_WRITER_INTERVAL:
		{
			Interval   *interval = DatumGetIntervalP(value);

			if (interval->time > PG_INT64_MAX / 1000 ||
				interval->time < PG_INT64_MIN / 1000)
				throw std::runtime_error("interval value is out of Arrow month_day_nano_interval range");

			arrow::MonthDayNanoIntervalType::MonthDayNanos arrow_value{
				(int32_t) interval->month,
				(int32_t) interval->day,
				(int64_t) interval->time * 1000
			};

			af_writer_status_or_throw(
				static_cast<arrow::MonthDayNanoIntervalBuilder *>(column->builder.get())
				->Append(arrow_value),
				"append Arrow interval");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(arrow_value);
			break;
		}
		case AF_WRITER_TIME:
			af_writer_status_or_throw(
				static_cast<arrow::Time64Builder *>(column->builder.get())
				->Append(DatumGetTimeADT(value)),
				"append Arrow time");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int64);
			break;
		case AF_WRITER_MONEY:
			af_writer_status_or_throw(
				static_cast<arrow::Int64Builder *>(column->builder.get())
				->Append(DatumGetCash(value)),
				"append Arrow money");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int64);
			break;
		case AF_WRITER_INET:
		case AF_WRITER_CIDR:
		{
			inet	   *ip = DatumGetInetPP(value);
			unsigned char payload[18] = {0};

			payload[0] = ip_family(ip);
			payload[1] = ip_bits(ip);
			memcpy(payload + 2, ip_addr(ip), ip_addrsize(ip));
			af_writer_status_or_throw(
				static_cast<arrow::FixedSizeBinaryBuilder *>(column->builder.get())
				->Append(payload),
				"append Arrow inet/cidr");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(payload);
			if ((Pointer) ip != DatumGetPointer(value))
				pfree(ip);
			break;
		}
		case AF_WRITER_MACADDR:
		{
			macaddr    *mac = DatumGetMacaddrP(value);
			unsigned char payload[6] = {
				mac->a, mac->b, mac->c, mac->d, mac->e, mac->f
			};

			af_writer_status_or_throw(
				static_cast<arrow::FixedSizeBinaryBuilder *>(column->builder.get())
				->Append(payload),
				"append Arrow macaddr");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(payload);
			break;
		}
		case AF_WRITER_ENUM:
		{
			char	   *text_value = af_output_text_datum(column->typid, value);
			int32		len = strlen(text_value);

			af_writer_status_or_throw(
				static_cast<arrow::Dictionary32Builder<arrow::StringType> *>(column->builder.get())
				->Append(text_value, len),
				"append Arrow enum dictionary value");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int32);
			pfree(text_value);
			break;
		}
		case AF_WRITER_DATE:
			af_writer_status_or_throw(
				static_cast<arrow::Date32Builder *>(column->builder.get())
				->Append(af_writer_date_to_arrow_days(column, value)),
				"append Arrow date");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int32);
			break;
		case AF_WRITER_TIMESTAMP:
		case AF_WRITER_TIMESTAMPTZ:
			af_writer_status_or_throw(
				static_cast<arrow::TimestampBuilder *>(column->builder.get())
				->Append(af_writer_timestamp_to_arrow_usecs(column, value)),
				"append Arrow timestamp");
			if (estimated_bytes != nullptr)
				*estimated_bytes += sizeof(int64);
			break;
	}
}

static void
af_writer_flush(ArrowFlightFdwWriterState *state)
{
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	if (state->rows_in_batch == 0)
		return;

	CHECK_FOR_INTERRUPTS();

	for (auto& column : state->columns)
	{
		std::shared_ptr<arrow::Array> array;

		af_writer_status_or_throw(column.builder->Finish(&array),
								  "finish Arrow array");
		arrays.push_back(array);
	}

	std::shared_ptr<arrow::RecordBatch> batch =
		arrow::RecordBatch::Make(state->schema, state->rows_in_batch,
								 arrays);
	std::shared_ptr<arrow::Buffer> metadata =
		arrow::Buffer::FromString(
			af_writer_batch_metadata(state, false, state->rows_in_batch));

	af_writer_status_or_throw(
		state->writer->WriteWithMetadata(*batch, metadata),
		"write Arrow Flight record batch");

	state->batches++;
	state->rows_in_batch = 0;
	state->estimated_batch_bytes = 0;
	af_writer_reset_builders(state);
}

static void
af_writer_drain_metadata(ArrowFlightFdwWriterState *state)
{
	if (state->reader == nullptr)
		return;

	while (true)
	{
		std::shared_ptr<arrow::Buffer> metadata;
		arrow::Status status = state->reader->ReadMetadata(&metadata);

		if (!status.ok())
			throw std::runtime_error("failed to read Arrow Flight DoPut metadata: " +
									 status.ToString());
		if (metadata == nullptr)
			break;

		if (af_writer_metadata_has_line(metadata, "af.ack.final", "true"))
		{
			state->server_final_ack = true;
			af_writer_parse_ack_metadata(state, metadata);
		}
		CHECK_FOR_INTERRUPTS();
	}

	if (!state->server_final_ack)
		throw std::runtime_error(
			"Arrow Flight DoPut server did not return final ack");
}

static void
af_writer_parse_ack_metadata(ArrowFlightFdwWriterState *state,
							 std::shared_ptr<arrow::Buffer> metadata)
{
	std::string value;
	int64		parsed = 0;

	if (state == nullptr)
		return;

	if (af_writer_metadata_value(metadata, "af.ack.transfer_id", &value))
		state->server_ack_transfer_id = value;
	if (af_writer_metadata_value(metadata, "af.ack.rows", &value) &&
		af_writer_parse_int64(value, &parsed))
		state->server_ack_rows = parsed;
	if (af_writer_metadata_value(metadata, "af.ack.bytes", &value) &&
		af_writer_parse_int64(value, &parsed))
		state->server_ack_bytes = parsed;
	if (af_writer_metadata_value(metadata, "af.ack.batches", &value) &&
		af_writer_parse_int64(value, &parsed))
		state->server_ack_batches = parsed;
}

static bool
af_writer_metadata_has_line(std::shared_ptr<arrow::Buffer> metadata,
							const char *key,
							const char *value)
{
	std::string text;
	std::string expected;
	size_t		start = 0;

	if (metadata == nullptr || metadata->size() <= 0)
		return false;

	text.assign(reinterpret_cast<const char *>(metadata->data()),
				metadata->size());
	expected = std::string(key) + "=" + value;

	while (start <= text.size())
	{
		size_t		end = text.find('\n', start);
		std::string line =
			text.substr(start, end == std::string::npos ?
						std::string::npos : end - start);

		if (line == expected)
			return true;
		if (end == std::string::npos)
			break;
		start = end + 1;
	}

	return false;
}

static bool
af_writer_metadata_value(std::shared_ptr<arrow::Buffer> metadata,
						 const char *key, std::string *value)
{
	std::string text;
	std::string prefix;
	size_t		start = 0;

	if (metadata == nullptr || metadata->size() <= 0 ||
		key == nullptr || value == nullptr)
		return false;

	text.assign(reinterpret_cast<const char *>(metadata->data()),
				metadata->size());
	prefix = std::string(key) + "=";

	while (start <= text.size())
	{
		size_t		end = text.find('\n', start);
		std::string line =
			text.substr(start, end == std::string::npos ?
						std::string::npos : end - start);

		if (line.rfind(prefix, 0) == 0)
		{
			*value = line.substr(prefix.size());
			return true;
		}
		if (end == std::string::npos)
			break;
		start = end + 1;
	}

	return false;
}

static bool
af_writer_parse_int64(const std::string& value, int64 *out)
{
	char	   *end = nullptr;
	long long	parsed = 0;

	if (out == nullptr || value.empty())
		return false;

	errno = 0;
	parsed = std::strtoll(value.c_str(), &end, 10);
	if (errno != 0 || end == value.c_str() || *end != '\0')
		return false;

	*out = (int64) parsed;
	return true;
}

static std::string
af_writer_action_body(ArrowFlightFdwWriterState *state,
					  const char *action_type)
{
	std::string body;

	body += "af.protocol.version=1\n";
	body += "af.action.type=" + std::string(action_type) + "\n";
	body += "af.operation.id=" + state->operation_id + "\n";
	body += "af.operation.type=insert\n";
	body += "af.operation.mode=" + state->write_mode + "\n";
	body += "af.dataset=" + state->dataset + "\n";
	body += "af.stream.id=" + state->stream_id + "\n";
	body += "af.stream.attempt=" + std::to_string(state->attempt) + "\n";
	body += "af.segment.index=" + std::to_string(GpIdentity.segindex) + "\n";
	body += "af.segment.count=" + std::to_string(getgpsegmentCount()) + "\n";
	body += "af.rows=" + std::to_string(state->total_rows) + "\n";
	body += "af.batches=" + std::to_string(state->batches) + "\n";
	body += "af.session.id=" + std::to_string(gp_session_id) + "\n";
	body += "af.command.count=" + std::to_string(gp_command_count) + "\n";
	body += "af.pid=" + std::to_string(MyProcPid) + "\n";

	return body;
}

static void
af_writer_do_action(ArrowFlightFdwWriterState *state,
					const char *action_type)
{
	if (state->client == nullptr)
		throw std::runtime_error("Arrow Flight client is not initialized");

	CHECK_FOR_INTERRUPTS();

	arrow::flight::Action action(
		action_type,
		arrow::Buffer::FromString(af_writer_action_body(state, action_type)));
	std::unique_ptr<arrow::flight::ResultStream> results =
		af_writer_value_or_throw(
			state->client->DoAction(state->call_options, action),
			action_type);

	af_writer_status_or_throw(results->Drain(), action_type);
}

static void
af_writer_abort_operation(ArrowFlightFdwWriterState *state)
{
	if (state == nullptr || state->client == nullptr)
		return;

	try
	{
		arrow::flight::Action action(
			"AbortOperation",
			arrow::Buffer::FromString(
				af_writer_action_body(state, "AbortOperation")));
		arrow::Result<std::unique_ptr<arrow::flight::ResultStream>> result =
			state->client->DoAction(state->call_options, action);

		if (result.ok())
			(void) std::move(result).ValueOrDie()->Drain();
	}
	catch (...)
	{
	}
}

static std::string
af_writer_context(ArrowFlightFdwWriterState *state)
{
	if (state == nullptr)
		return "";

	return " (endpoint=" + state->endpoint +
		" dataset=" + state->dataset +
		" operation_id=" + state->operation_id +
		" stream_id=" + state->stream_id +
		" segment=" + std::to_string(GpIdentity.segindex) +
		" tls=" + std::string(state->tls_enabled ? "true" : "false") +
		" auth=" + std::string(state->auth_enabled ? "true" : "false") +
		" batches=" + std::to_string(state->batches) +
		" rows=" + std::to_string(state->total_rows) +
		" server_ack_transfer_id=" + state->server_ack_transfer_id +
		" server_ack_rows=" + std::to_string(state->server_ack_rows) +
		" server_ack_bytes=" + std::to_string(state->server_ack_bytes) +
		" server_ack_batches=" + std::to_string(state->server_ack_batches) +
		")";
}

static void
af_writer_delete(ArrowFlightFdwWriterState *state)
{
	if (state == nullptr)
		return;

	delete state;
}

#else							/* !USE_ARROW_FLIGHT */

void *
af_fdw_writer_open(Relation rel, List *target_attrs,
				   const char *operation_id, const char *url,
				   const char *dataset, const char *write_mode,
				   const char *operation_metadata, int batch_rows,
				   int max_batch_bytes, int timeout_ms,
				   int retry_count, int retry_backoff_ms,
				   const ArrowFlightSecurityOptions *security_options)
{
	(void) rel;
	(void) target_attrs;
	(void) operation_id;
	(void) url;
	(void) dataset;
	(void) write_mode;
	(void) operation_metadata;
	(void) batch_rows;
	(void) max_batch_bytes;
	(void) timeout_ms;
	(void) retry_count;
	(void) retry_backoff_ms;
	(void) security_options;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Arrow Flight FDW write requires Arrow Flight support"),
			 errhint("Build the arrowflight extension with USE_ARROW_FLIGHT=1 or with_arrow_flight=yes.")));

	return NULL;
}

void
af_fdw_writer_append(void *writer_state, TupleTableSlot *slot)
{
	(void) writer_state;
	(void) slot;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Arrow Flight FDW write requires Arrow Flight support")));
}

void
af_fdw_writer_finish(void *writer_state)
{
	(void) writer_state;
}

void
af_fdw_writer_abort(void *writer_state)
{
	(void) writer_state;
}

#endif							/* USE_ARROW_FLIGHT */
