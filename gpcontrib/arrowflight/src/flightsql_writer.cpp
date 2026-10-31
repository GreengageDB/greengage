/*-------------------------------------------------------------------------
 *
 * flightsql_writer.cpp
 *	  Arrow Flight SQL bulk-ingest writer.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "catalog/pg_type.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "common/base64.h"
#include "common/int.h"
#include "common/sha2.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
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
#include <arrow/flight/sql/client.h>
#include <arrow/ipc/writer.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/decimal.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <exception>
#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#define AF_PG_TO_ARROW_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define AF_PG_TO_ARROW_EPOCH_USECS \
	((int64) AF_PG_TO_ARROW_EPOCH_DAYS * USECS_PER_DAY)

typedef enum FlightSqlWriterColumnKind
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
} FlightSqlWriterColumnKind;

typedef struct FlightSqlWriterColumn
{
	int			attnum;
	Oid			typid;
	std::string name;
	FlightSqlWriterColumnKind kind;
	std::shared_ptr<arrow::DataType> arrow_type;
	std::unique_ptr<arrow::ArrayBuilder> builder;
} FlightSqlWriterColumn;

class ArrowFlightSqlBatchReader : public arrow::RecordBatchReader
{
public:
	struct QueueStats
	{
		int64		queued_bytes;
		int64		peak_queued_bytes;
		int64		wait_nanos;
	};

	ArrowFlightSqlBatchReader(std::shared_ptr<arrow::Schema> schema,
							  int64 max_queued_bytes)
		: schema_(std::move(schema)),
		  max_queued_bytes_(max_queued_bytes)
	{
	}

	std::shared_ptr<arrow::Schema>
	schema() const override
	{
		return schema_;
	}

	arrow::Status
	ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override
	{
		std::unique_lock<std::mutex> lock(mutex_);

		ready_.wait(lock, [this]() {
			return !batches_.empty() || finished_ || !status_.ok();
		});

		if (!status_.ok())
			return status_;
		if (batches_.empty())
		{
			*batch = nullptr;
			return arrow::Status::OK();
		}

		QueuedBatch queued = std::move(batches_.front());

		batches_.pop_front();
		queued_bytes_ -= queued.bytes;
		*batch = std::move(queued.batch);
		space_.notify_all();
		return arrow::Status::OK();
	}

	void
	Push(const std::shared_ptr<arrow::RecordBatch>& batch)
	{
		int64		bytes = arrow::util::TotalBufferSize(*batch);
		std::unique_lock<std::mutex> lock(mutex_);
		const auto wait_started = std::chrono::steady_clock::now();
		bool		waited = false;

		if (bytes > max_queued_bytes_)
			throw std::runtime_error(
				"Flight SQL completed batch exceeds the queue byte limit");

		while (status_.ok() && !finished_ &&
			   queued_bytes_ + bytes > max_queued_bytes_)
		{
			waited = true;
			space_.wait_for(lock, std::chrono::milliseconds(100));
			lock.unlock();
			CHECK_FOR_INTERRUPTS();
			lock.lock();
		}

		if (!status_.ok())
			throw std::runtime_error("Flight SQL ingest failed: " +
									 status_.ToString());
		if (finished_)
			throw std::runtime_error("Flight SQL ingest reader is closed");
		if (waited)
			wait_nanos_ +=
				std::chrono::duration_cast<std::chrono::nanoseconds>(
					std::chrono::steady_clock::now() - wait_started).count();

		batches_.push_back({batch, bytes});
		queued_bytes_ += bytes;
		peak_queued_bytes_ =
			std::max(peak_queued_bytes_, queued_bytes_);
		ready_.notify_one();
	}

	QueueStats
	GetQueueStats()
	{
		std::lock_guard<std::mutex> lock(mutex_);

		return {queued_bytes_, peak_queued_bytes_, wait_nanos_};
	}

	void
	Finish()
	{
		std::lock_guard<std::mutex> lock(mutex_);

		finished_ = true;
		ready_.notify_all();
		space_.notify_all();
	}

	void
	Fail(const arrow::Status& status)
	{
		std::lock_guard<std::mutex> lock(mutex_);

		if (status_.ok())
			status_ = status;
		finished_ = true;
		ready_.notify_all();
		space_.notify_all();
	}

private:
	struct QueuedBatch
	{
		std::shared_ptr<arrow::RecordBatch> batch;
		int64		bytes;
	};

	std::shared_ptr<arrow::Schema> schema_;
	int64		max_queued_bytes_;
	int64		queued_bytes_ = 0;
	int64		peak_queued_bytes_ = 0;
	int64		wait_nanos_ = 0;
	std::deque<QueuedBatch> batches_;
	std::mutex	mutex_;
	std::condition_variable ready_;
	std::condition_variable space_;
	arrow::Status status_ = arrow::Status::OK();
	bool		finished_ = false;
};

class FlightSqlWriterState
{
public:
	std::string url;
	std::string table_name;
	std::string schema_name;
	std::string catalog_name;
	std::string transaction_id;
	std::unordered_map<std::string, std::string> ingest_options;
	std::string endpoint;
	std::string tls_ca_file;
	std::string tls_client_cert_file;
	std::string tls_client_key_file;
	std::string auth_token_file;
	arrow::flight::FlightCallOptions call_options;
	arrow::StopSource ingest_stop_source;
	std::shared_ptr<arrow::flight::FlightClient> client;
	std::shared_ptr<arrow::Schema> schema;
	std::vector<FlightSqlWriterColumn> columns;
	std::shared_ptr<ArrowFlightSqlBatchReader> ingest_reader;
	std::thread ingest_thread;
	std::mutex ingest_result_mutex;
	arrow::Status ingest_status = arrow::Status::OK();
	int64		ingested_rows = -1;
	bool		verify_ingested_rows = true;
	int			batch_rows = AF_DEFAULT_BATCH_ROWS;
	int			max_batch_bytes = AF_DEFAULT_MAX_BATCH_BYTES;
	int64		rows_in_batch = 0;
	int64		estimated_batch_bytes = 0;
	int64		total_rows = 0;
	int64		batches = 0;
	bool		finalized = false;
	bool		tls_enabled = false;
	bool		auth_enabled = false;
	bool		mpp_planned = false;
	std::string mpp_worker_id;
};

template <typename T>
static T af_writer_value_or_throw(arrow::Result<T> result,
								  const char *context);
static void af_writer_status_or_throw(const arrow::Status& status,
									  const char *context);
static std::shared_ptr<arrow::DataType> af_writer_arrow_type(Form_pg_attribute attr,
															 FlightSqlWriterColumnKind *kind);
static bool af_writer_numeric_typmod(Form_pg_attribute attr,
									 int32 *precision, int32 *scale);
static std::unique_ptr<arrow::ArrayBuilder> af_writer_make_builder(
									const FlightSqlWriterColumn *column);
static void af_writer_init_columns(FlightSqlWriterState *state,
								   Relation rel, List *target_attrs,
								   bool use_remote_names);
static void af_writer_reset_builders(FlightSqlWriterState *state);
static std::shared_ptr<arrow::Schema> af_writer_build_schema(
								   FlightSqlWriterState *state,
								   Relation rel);
static arrow::flight::FlightClientOptions af_writer_client_options(
									FlightSqlWriterState *state);
static void af_writer_apply_auth_header(FlightSqlWriterState *state);
static std::string af_writer_read_file(const std::string& path,
									   const char *label);
static std::string af_writer_read_token(const std::string& path);
static int32 af_writer_date_to_arrow_days(
									const FlightSqlWriterColumn *column,
									Datum value);
static int64 af_writer_timestamp_to_arrow_usecs(
									const FlightSqlWriterColumn *column,
									Datum value);
static void af_writer_append_attr(FlightSqlWriterColumn *column,
								  Datum value, bool isnull,
								  int64 *estimated_bytes);
static void af_writer_append(FlightSqlWriterState *state,
							 TupleTableSlot *slot);
static void af_writer_flush(FlightSqlWriterState *state);
static std::string af_writer_context(FlightSqlWriterState *state);
static void af_writer_delete(FlightSqlWriterState *state);
static void af_writer_resource_cleanup(void *resource);
static std::string af_flightsql_decode_transaction_id(
	const char *encoded_transaction_id);
static void af_flightsql_start_ingest(FlightSqlWriterState *state);
static void af_flightsql_join_ingest(FlightSqlWriterState *state);

void *
af_flightsql_writer_open(
	Relation rel, List *target_attrs, const char *url, const char *table_name,
	const char *schema_name, const char *catalog_name, int batch_rows,
	int max_batch_bytes, int timeout_ms, bool verify_ingested_rows,
	const char *transaction_id,
	const ArrowFlightSecurityOptions *security_options,
	const ArrowFlightSqlMppRoute *mpp_route)
{
	std::unique_ptr<FlightSqlWriterState> state;

	try
	{
		ArrowFlightConnection connection;
		arrow::Result<arrow::flight::Location> location_result;

		if (GpIdentity.segindex < 0)
			throw std::runtime_error("Flight SQL ingest requires QE segment execution");
		if (table_name == nullptr || table_name[0] == '\0')
			throw std::runtime_error("Flight SQL ingest table name is empty");

		CHECK_FOR_INTERRUPTS();
		af_parse_flight_connection(url, &connection);
		state = std::make_unique<FlightSqlWriterState>();
		state->url = url == nullptr ? "" : url;
		state->table_name = table_name;
		state->schema_name = schema_name == nullptr ? "" : schema_name;
		state->catalog_name = catalog_name == nullptr ? "" : catalog_name;
		if (transaction_id != nullptr && transaction_id[0] != '\0')
			state->transaction_id =
				af_flightsql_decode_transaction_id(transaction_id);
		if (mpp_route != nullptr)
		{
			state->mpp_planned = true;
			state->mpp_worker_id =
				mpp_route->worker_id == nullptr ?
				"" : mpp_route->worker_id;
			state->ingest_options[AF_FLIGHT_SQL_MPP_OPTION_VERSION] =
				std::to_string(AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION);
			state->ingest_options[AF_FLIGHT_SQL_MPP_OPTION_PLAN_ID] =
				mpp_route->plan_id;
			state->ingest_options[AF_FLIGHT_SQL_MPP_OPTION_ROUTE_TOKEN] =
				mpp_route->route_token;
			state->ingest_options[AF_FLIGHT_SQL_MPP_OPTION_SEGMENT_INDEX] =
				std::to_string(mpp_route->segment_index);
			state->ingest_options[AF_FLIGHT_SQL_MPP_OPTION_SEGMENT_COUNT] =
				std::to_string(mpp_route->segment_count);
			state->ingest_options[
				AF_FLIGHT_SQL_MPP_OPTION_CLIENT_OPERATION_ID] =
				mpp_route->client_operation_id;
			state->ingest_options[
				AF_FLIGHT_SQL_MPP_OPTION_SCHEMA_FINGERPRINT] =
				mpp_route->schema_fingerprint;
		}
		state->batch_rows = batch_rows;
		state->max_batch_bytes = max_batch_bytes;
		state->verify_ingested_rows = verify_ingested_rows;
		state->tls_enabled = connection.tls;
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
			throw std::runtime_error("Flight SQL TLS/auth options require tls=true");

		location_result = connection.tls ?
			arrow::flight::Location::ForGrpcTls(connection.host,
											   connection.port) :
			arrow::flight::Location::ForGrpcTcp(connection.host,
											   connection.port);
		arrow::flight::Location location =
			af_writer_value_or_throw(std::move(location_result),
									 "create Flight SQL ingest location");
		state->endpoint = location.ToString();
		if (timeout_ms > 0)
			state->call_options.timeout =
				arrow::flight::TimeoutDuration((double) timeout_ms / 1000.0);
		state->call_options.stop_token = state->ingest_stop_source.token();
		af_writer_apply_auth_header(state.get());

		af_writer_init_columns(state.get(), rel, target_attrs, true);
		state->schema = af_writer_build_schema(state.get(), rel);
		state->client =
			af_writer_value_or_throw(
				arrow::flight::FlightClient::Connect(
					location, af_writer_client_options(state.get())),
				"connect to Flight SQL ingest server");

		int64		queue_limit = max_batch_bytes > 0 ?
			(int64) max_batch_bytes * 2 :
			(int64) AF_DEFAULT_MAX_BATCH_BYTES * 2;

		state->ingest_reader =
			std::make_shared<ArrowFlightSqlBatchReader>(state->schema,
														queue_limit);
		af_flightsql_start_ingest(state.get());
		af_resource_register(state.get(), af_writer_resource_cleanup,
							 "Flight SQL write");
		return state.release();
	}
	catch (const std::exception& ex)
	{
		if (state != nullptr)
		{
			state->ingest_stop_source.RequestStop(
				arrow::Status::Cancelled("ingest open failed"));
			if (state->ingest_reader != nullptr)
				state->ingest_reader->Fail(
					arrow::Status::Cancelled("ingest open failed"));
			af_flightsql_join_ingest(state.get());
		}
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Flight SQL ingest failed: %s", ex.what())));
	}
	catch (...)
	{
		if (state != nullptr)
		{
			state->ingest_stop_source.RequestStop(
				arrow::Status::Cancelled("ingest open failed"));
			if (state->ingest_reader != nullptr)
				state->ingest_reader->Fail(
					arrow::Status::Cancelled("ingest open failed"));
			af_flightsql_join_ingest(state.get());
		}
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Flight SQL ingest raised unknown C++ exception")));
	}

	return NULL;
}

static void
af_writer_append(FlightSqlWriterState *state, TupleTableSlot *slot)
{
	if (state == nullptr)
		elog(ERROR, "Flight SQL writer is not initialized");

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
				 errmsg("Flight SQL ingest failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Flight SQL ingest raised unknown C++ exception")));
	}
}

void
af_flightsql_writer_append(void *writer_state, TupleTableSlot *slot)
{
	FlightSqlWriterState *state =
		static_cast<FlightSqlWriterState *>(writer_state);

	af_writer_append(state, slot);
}

void
af_flightsql_writer_finish(void *writer_state)
{
	FlightSqlWriterState *state =
		static_cast<FlightSqlWriterState *>(writer_state);

	if (state == nullptr)
		return;

	try
	{
		CHECK_FOR_INTERRUPTS();
		af_writer_flush(state);
		state->ingest_reader->Finish();
		af_flightsql_join_ingest(state);
		if (!state->ingest_status.ok())
			throw std::runtime_error(state->ingest_status.ToString());
		if (state->verify_ingested_rows &&
			state->ingested_rows >= 0 &&
			state->ingested_rows != state->total_rows)
			throw std::runtime_error(
				"Flight SQL server reported " +
				std::to_string(state->ingested_rows) +
				" ingested rows, sent " +
				std::to_string(state->total_rows));

		state->finalized = true;
		{
			std::string context = af_writer_context(state);

			elog(DEBUG1, "Flight SQL ingest completed%s",
				 context.c_str());
		}
		af_writer_delete(state);
	}
	catch (const std::exception& ex)
	{
		state->ingest_stop_source.RequestStop(
			arrow::Status::Cancelled("ingest finish failed"));
		if (state->ingest_reader != nullptr)
			state->ingest_reader->Fail(
				arrow::Status::Cancelled("ingest finish failed"));
		af_flightsql_join_ingest(state);
		std::string context = af_writer_context(state);

		af_writer_delete(state);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Flight SQL ingest failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		state->ingest_stop_source.RequestStop(
			arrow::Status::Cancelled("ingest finish failed"));
		if (state->ingest_reader != nullptr)
			state->ingest_reader->Fail(
				arrow::Status::Cancelled("ingest finish failed"));
		af_flightsql_join_ingest(state);
		af_writer_delete(state);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Flight SQL ingest raised unknown C++ exception")));
	}
}

void
af_flightsql_writer_abort(void *writer_state)
{
	FlightSqlWriterState *state =
		static_cast<FlightSqlWriterState *>(writer_state);

	if (state == nullptr)
		return;
	state->ingest_stop_source.RequestStop(
		arrow::Status::Cancelled("Greengage statement aborted"));
	if (state->ingest_reader != nullptr)
		state->ingest_reader->Fail(
			arrow::Status::Cancelled("Greengage statement aborted"));
	af_flightsql_join_ingest(state);
	af_writer_delete(state);
}

char *
af_flightsql_writer_schema(
	Relation rel, List *target_attrs, int *schema_len,
	char **schema_fingerprint)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		FlightSqlWriterState state;
		std::shared_ptr<arrow::Buffer> serialized;
		pg_sha256_ctx sha;
		uint8		digest[PG_SHA256_DIGEST_LENGTH];
		static const char hex[] = "0123456789abcdef";
		char	   *result;
		char	   *fingerprint;

		if (rel == NULL || schema_len == NULL ||
			schema_fingerprint == NULL)
			throw std::runtime_error(
				"Flight SQL schema serialization arguments are invalid");

		af_writer_init_columns(&state, rel, target_attrs, true);
		state.schema = af_writer_build_schema(&state, rel);
		serialized = af_writer_value_or_throw(
			arrow::ipc::SerializeSchema(*state.schema),
			"serialize Flight SQL ingest schema");
		if (serialized == nullptr || serialized->size() <= 0 ||
			serialized->size() > INT_MAX)
			throw std::runtime_error(
				"serialized Flight SQL ingest schema has invalid size");

		result = (char *) palloc((Size) serialized->size());
		memcpy(result, serialized->data(), (Size) serialized->size());
		*schema_len = (int) serialized->size();

		pg_sha256_init(&sha);
		pg_sha256_update(
			&sha, reinterpret_cast<const uint8 *>(serialized->data()),
			(Size) serialized->size());
		pg_sha256_final(&sha, digest);

		fingerprint =
			(char *) palloc(PG_SHA256_DIGEST_STRING_LENGTH);
		for (int i = 0; i < PG_SHA256_DIGEST_LENGTH; i++)
		{
			fingerprint[i * 2] = hex[digest[i] >> 4];
			fingerprint[i * 2 + 1] = hex[digest[i] & 0x0f];
		}
		fingerprint[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';
		*schema_fingerprint = fingerprint;
		return result;
	}
	catch (const std::exception& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not serialize Flight SQL ingest schema: %s",
						ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not serialize Flight SQL ingest schema: unknown C++ exception")));
	}
#else
	(void) rel;
	(void) target_attrs;
	(void) schema_len;
	(void) schema_fingerprint;
	af_check_arrow_flight_linkage();
#endif

	return NULL;
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
					 FlightSqlWriterColumnKind *kind)
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
									 " for Flight SQL ingest");
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
af_writer_make_builder(const FlightSqlWriterColumn *column)
{
	if (column->kind == AF_WRITER_ENUM)
		return std::make_unique<arrow::Dictionary32Builder<arrow::StringType>>(
			arrow::utf8(), arrow::default_memory_pool());

	return af_writer_value_or_throw(
		arrow::MakeBuilder(column->arrow_type, arrow::default_memory_pool()),
		"create Arrow array builder");
}

static void
af_writer_init_columns(FlightSqlWriterState *state, Relation rel,
					   List *target_attrs, bool use_remote_names)
{
	ListCell   *cell;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	foreach(cell, target_attrs)
	{
		int			attnum = lfirst_int(cell);
		Form_pg_attribute attr;
		FlightSqlWriterColumn column;

		if (attnum <= 0 || attnum > tupdesc->natts)
			throw std::runtime_error("invalid Flight SQL ingest target attribute");

		attr = TupleDescAttr(tupdesc, attnum - 1);
		if (attr->attisdropped)
			continue;

		column.attnum = attnum;
		column.typid = attr->atttypid;
		column.name = NameStr(attr->attname);
		if (use_remote_names)
		{
			List	   *column_options =
				GetForeignColumnOptions(RelationGetRelid(rel), attnum);
			ListCell   *option_cell;

			foreach(option_cell, column_options)
			{
				DefElem    *def = (DefElem *) lfirst(option_cell);

				if (strcmp(def->defname, "column_name") == 0)
				{
					column.name = defGetString(def);
					break;
				}
			}
		}
		column.arrow_type = af_writer_arrow_type(attr, &column.kind);
		column.builder = af_writer_make_builder(&column);
		state->columns.push_back(std::move(column));
	}
}

static void
af_writer_reset_builders(FlightSqlWriterState *state)
{
	for (auto& column : state->columns)
		column.builder = af_writer_make_builder(&column);
}

static std::shared_ptr<arrow::Schema>
af_writer_build_schema(FlightSqlWriterState *state, Relation rel)
{
	std::vector<std::shared_ptr<arrow::Field>> fields;

	(void) rel;
	for (const auto& column : state->columns)
		fields.push_back(arrow::field(column.name, column.arrow_type));

	return arrow::schema(fields);
}

static void
af_flightsql_start_ingest(FlightSqlWriterState *state)
{
	if (state == nullptr || state->client == nullptr ||
		state->ingest_reader == nullptr)
		throw std::runtime_error("Flight SQL ingest state is not initialized");

	state->ingest_thread = std::thread([state]() {
		try
		{
			arrow::flight::sql::FlightSqlClient sql_client(state->client);
			arrow::flight::sql::TableDefinitionOptions definition_options{
				arrow::flight::sql::TableDefinitionOptionsTableNotExistOption::kFail,
				arrow::flight::sql::TableDefinitionOptionsTableExistsOption::kAppend
			};
			std::optional<std::string> schema =
				state->schema_name.empty() ?
				std::nullopt :
				std::optional<std::string>(state->schema_name);
			std::optional<std::string> catalog =
				state->catalog_name.empty() ?
				std::nullopt :
				std::optional<std::string>(state->catalog_name);
			arrow::Result<int64_t> result =
				state->transaction_id.empty() ?
				sql_client.ExecuteIngest(
					state->call_options, state->ingest_reader,
					definition_options, state->table_name, schema,
					catalog, false,
					arrow::flight::sql::no_transaction(),
					state->ingest_options) :
				sql_client.ExecuteIngest(
					state->call_options, state->ingest_reader,
					definition_options, state->table_name, schema,
					catalog, false,
					arrow::flight::sql::Transaction(
						state->transaction_id),
					state->ingest_options);
			std::lock_guard<std::mutex> lock(state->ingest_result_mutex);

			if (result.ok())
				state->ingested_rows = std::move(result).ValueOrDie();
			else
			{
				state->ingest_status = result.status();
				state->ingest_reader->Fail(state->ingest_status);
			}
		}
		catch (const std::exception& ex)
		{
			std::lock_guard<std::mutex> lock(state->ingest_result_mutex);

			state->ingest_status =
				arrow::Status::UnknownError(ex.what());
			state->ingest_reader->Fail(state->ingest_status);
		}
		catch (...)
		{
			std::lock_guard<std::mutex> lock(state->ingest_result_mutex);

			state->ingest_status =
				arrow::Status::UnknownError(
					"unknown Flight SQL ingest exception");
			state->ingest_reader->Fail(state->ingest_status);
		}
	});
}

static void
af_flightsql_join_ingest(FlightSqlWriterState *state)
{
	if (state != nullptr && state->ingest_thread.joinable())
		state->ingest_thread.join();
}

static arrow::flight::FlightClientOptions
af_writer_client_options(FlightSqlWriterState *state)
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
af_writer_apply_auth_header(FlightSqlWriterState *state)
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
af_writer_date_to_arrow_days(const FlightSqlWriterColumn *column,
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
af_writer_timestamp_to_arrow_usecs(const FlightSqlWriterColumn *column,
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
af_writer_append_attr(FlightSqlWriterColumn *column, Datum value,
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
af_writer_flush(FlightSqlWriterState *state)
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

	state->ingest_reader->Push(batch);
	state->batches++;
	state->rows_in_batch = 0;
	state->estimated_batch_bytes = 0;
	af_writer_reset_builders(state);
}

static std::string
af_writer_context(FlightSqlWriterState *state)
{
	if (state == nullptr)
		return "";

	ArrowFlightSqlBatchReader::QueueStats queue_stats{0, 0, 0};

	if (state->ingest_reader != nullptr)
		queue_stats = state->ingest_reader->GetQueueStats();

	return " (endpoint=" + state->endpoint +
		" table=" + state->table_name +
		" schema=" + state->schema_name +
		" catalog=" + state->catalog_name +
		" transactional=" +
		std::string(state->transaction_id.empty() ? "false" : "true") +
		" routing=" +
		std::string(state->mpp_planned ? "planned" : "origin") +
		" worker=" +
		(state->mpp_worker_id.empty() ? "origin" : state->mpp_worker_id) +
		" segment=" + std::to_string(GpIdentity.segindex) +
		" tls=" + std::string(state->tls_enabled ? "true" : "false") +
		" auth=" + std::string(state->auth_enabled ? "true" : "false") +
		" batches=" + std::to_string(state->batches) +
		" rows=" + std::to_string(state->total_rows) +
		" queue_bytes=" + std::to_string(queue_stats.queued_bytes) +
		" queue_peak_bytes=" +
		std::to_string(queue_stats.peak_queued_bytes) +
		" queue_wait_nanos=" +
		std::to_string(queue_stats.wait_nanos) +
		")";
}

static void
af_writer_delete(FlightSqlWriterState *state)
{
	if (state == nullptr)
		return;

	af_resource_unregister(state);
	if (state->ingest_reader != nullptr)
		state->ingest_reader->Finish();
	af_flightsql_join_ingest(state);
	delete state;
}

static std::string
af_flightsql_decode_transaction_id(const char *encoded_transaction_id)
{
	int			encoded_len;
	int			decoded_capacity;
	int			decoded_len;
	std::string transaction_id;

	if (encoded_transaction_id == nullptr ||
		encoded_transaction_id[0] == '\0')
		throw std::runtime_error("Flight SQL transaction id is empty");

	encoded_len = strlen(encoded_transaction_id);
	decoded_capacity = pg_b64_dec_len(encoded_len);
	if (decoded_capacity > AF_FLIGHT_SQL_MAX_TRANSACTION_ID_BYTES)
		throw std::runtime_error(
			"Flight SQL transaction id exceeds the supported size");
	transaction_id.resize(decoded_capacity);
	decoded_len = pg_b64_decode(encoded_transaction_id, encoded_len,
								transaction_id.data());
	if (decoded_len <= 0)
		throw std::runtime_error(
			"Flight SQL transaction id is not valid base64");
	transaction_id.resize(decoded_len);
	return transaction_id;
}

static void
af_writer_resource_cleanup(void *resource)
{
	FlightSqlWriterState *state =
		static_cast<FlightSqlWriterState *>(resource);

	try
	{
		if (state != nullptr)
		{
			state->ingest_stop_source.RequestStop(
				arrow::Status::Cancelled(
					"Greengage resource owner released"));
			if (state->ingest_reader != nullptr)
				state->ingest_reader->Fail(
					arrow::Status::Cancelled(
						"Greengage resource owner released"));
			af_flightsql_join_ingest(state);
		}
		delete state;
	}
	catch (...)
	{
	}
}

#else							/* !USE_ARROW_FLIGHT */

void *
af_flightsql_writer_open(
	Relation rel, List *target_attrs, const char *url, const char *table_name,
	const char *schema_name, const char *catalog_name, int batch_rows,
	int max_batch_bytes, int timeout_ms, bool verify_ingested_rows,
	const char *transaction_id,
	const ArrowFlightSecurityOptions *security_options,
	const ArrowFlightSqlMppRoute *mpp_route)
{
	(void) rel;
	(void) target_attrs;
	(void) url;
	(void) table_name;
	(void) schema_name;
	(void) catalog_name;
	(void) batch_rows;
	(void) max_batch_bytes;
	(void) timeout_ms;
	(void) verify_ingested_rows;
	(void) transaction_id;
	(void) security_options;
	(void) mpp_route;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Flight SQL ingest requires Arrow Flight SQL support"),
			 errhint("Build the arrowflight extension with USE_ARROW_FLIGHT=1 or with_arrow_flight=yes.")));

	return NULL;
}

char *
af_flightsql_writer_schema(
	Relation rel, List *target_attrs, int *schema_len,
	char **schema_fingerprint)
{
	(void) rel;
	(void) target_attrs;
	(void) schema_len;
	(void) schema_fingerprint;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Flight SQL ingest requires Arrow Flight SQL support")));
	return NULL;
}

void
af_flightsql_writer_append(void *writer_state, TupleTableSlot *slot)
{
	(void) writer_state;
	(void) slot;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Flight SQL ingest requires Arrow Flight SQL support")));
}

void
af_flightsql_writer_finish(void *writer_state)
{
	(void) writer_state;
}

void
af_flightsql_writer_abort(void *writer_state)
{
	(void) writer_state;
}

#endif							/* USE_ARROW_FLIGHT */
