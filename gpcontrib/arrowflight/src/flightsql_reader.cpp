/*-------------------------------------------------------------------------
 *
 * flightsql_reader.cpp
 *    Apache Arrow Flight read client and Arrow-to-Greengage decoding.
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
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/inet.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

#include <limits.h>
#include <string.h>

}

#ifdef USE_ARROW_FLIGHT
#ifdef Abs
#undef Abs
#endif

#include <arrow/array/array_binary.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_primitive.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/client.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/config.h>

#include <chrono>
#include <exception>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

class ArrowFlightStreamState;

template <typename T>
static T af_arrow_value_or_throw(arrow::Result<T> result, const char *context);
static void af_arrow_status_or_throw(const arrow::Status& status,
									 const char *context);
static void af_arrow_throw_unsupported_type(Form_pg_attribute attr);
static uint64 af_profile_now_us(void);
static void af_profile_emit(ArrowFlightStreamState *state);
static void af_profile_add_us(uint64 *target, uint64 start_us);
static int af_find_projected_attr_by_name(TupleDesc tupdesc,
								 const bool *projected_attrs,
								 const std::string& name);
static void af_validate_arrow_schema(ArrowFlightStreamState *state,
									 TupleDesc tupdesc,
									 const std::shared_ptr<arrow::Schema>& schema,
									 const bool *projected_attrs);
static void af_validate_arrow_attr(Form_pg_attribute attr,
								   const std::shared_ptr<arrow::DataType>& type);
static void af_validate_arrow_timestamp_timezone(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::DataType>& type);
static bool af_arrow_type_is_string_like(
								 const std::shared_ptr<arrow::DataType>& type);
static bool af_arrow_type_is_fixed_binary(
								 const std::shared_ptr<arrow::DataType>& type,
								 int32 byte_width);
static bool af_arrow_type_is_enum_dictionary(
								 const std::shared_ptr<arrow::DataType>& type);
static std::string_view af_arrow_string_view(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static std::string_view af_arrow_enum_label_view(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_uuid_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_interval_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_numeric_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_binary_to_bytea_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_time_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_date_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_inet_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static Datum af_arrow_macaddr_to_datum(
								 Form_pg_attribute attr,
								 const std::shared_ptr<arrow::Array>& array,
								 int64 rownum);
static arrow::flight::FlightClientOptions af_arrow_client_options(
								 ArrowFlightStreamState *state);
static void af_arrow_apply_auth_header(ArrowFlightStreamState *state);
static void af_arrow_parse_endpoint_location_allowlist(
									 ArrowFlightStreamState *state,
									 const std::string& value);
static bool af_arrow_endpoint_location_allowed(
									 const ArrowFlightStreamState *state,
									 const arrow::flight::Location& location);
static std::string af_arrow_read_file(const std::string& path,
									  const char *label);
static std::string af_arrow_read_token(const std::string& path);
static std::string af_format_flight_context(ArrowFlightStreamState *state);
static void af_flightsql_cancel_info(
							 std::shared_ptr<arrow::flight::FlightClient> client,
							 arrow::flight::FlightCallOptions call_options,
							 std::unique_ptr<arrow::flight::FlightInfo> info);
static void af_flightsql_initialize_state(
								 ArrowFlightStreamState *state,
								 TupleDesc tupdesc, const char *url,
								 const ArrowFlightSecurityOptions *security_options);
static bool af_flightsql_open_next_endpoint(
								 ArrowFlightStreamState *state,
								 const bool *projected_attrs);
static TupleTableSlot *af_stream_next_slot(
								 void **flight_state, TupleTableSlot *slot,
								 bool project_all, const bool *projected_attrs);
static Datum af_arrow_attr_to_datum(Form_pg_attribute attr,
									const std::shared_ptr<arrow::Array>& array,
									int64 rownum, bool *isnull,
									ArrowFlightStreamState *state);
static int64 af_arrow_timestamp_to_pg_usecs(Form_pg_attribute attr,
											int64 value,
											arrow::TimeUnit::type unit);
static void af_flightsql_stream_resource_cleanup(void *resource);

struct FlightSqlCapabilitiesCacheEntry
{
	std::string server_name;
	std::string server_version;
	int			bulk_ingestion = AF_FLIGHT_SQL_CAPABILITY_UNKNOWN;
	int			ingest_transactions = AF_FLIGHT_SQL_CAPABILITY_UNKNOWN;
	int			transaction_support = AF_FLIGHT_SQL_CAPABILITY_UNKNOWN;
	int			sql_transactions = AF_FLIGHT_SQL_CAPABILITY_UNKNOWN;
	int			cancellation = AF_FLIGHT_SQL_CAPABILITY_UNKNOWN;
	int			default_isolation = 0;
	bool		default_isolation_known = false;
};

static std::unordered_map<std::string, FlightSqlCapabilitiesCacheEntry>
	flightsql_capability_cache;

static std::unique_ptr<ArrowFlightStreamState> af_flightsql_connect(
	const char *url, const ArrowFlightSecurityOptions *security_options);
static std::string af_flightsql_capability_cache_key(
	const char *url, const ArrowFlightSecurityOptions *security_options);
static FlightSqlCapabilitiesCacheEntry af_flightsql_discover_capabilities(
	const char *url, const ArrowFlightSecurityOptions *security_options);
static void af_flightsql_fetch_sql_info(
	ArrowFlightStreamState *state, const std::vector<int>& info_ids,
	FlightSqlCapabilitiesCacheEntry *capabilities);
static void af_flightsql_parse_sql_info_table(
	const std::shared_ptr<arrow::Table>& table,
	FlightSqlCapabilitiesCacheEntry *capabilities);
static void af_flightsql_copy_capabilities(
	const FlightSqlCapabilitiesCacheEntry& source,
	ArrowFlightSqlCapabilities *target);
static char *af_flightsql_begin_transaction_impl(
	const char *url, const ArrowFlightSecurityOptions *security_options);
static std::string af_flightsql_end_transaction_impl(
	const char *url, const char *transaction_id, bool commit,
	const ArrowFlightSecurityOptions *security_options);

class ArrowFlightStreamState
{
public:
	~ArrowFlightStreamState()
	{
		af_profile_emit(this);
		if (reader != nullptr)
			reader->Cancel();
		if (flight_sql_mode && !flight_sql_complete &&
			flight_info != nullptr && origin_client != nullptr)
			af_flightsql_cancel_info(origin_client, call_options,
									 std::move(flight_info));
	}

	TupleDesc	tupdesc = nullptr;
	arrow::flight::FlightCallOptions call_options;
	int			max_batch_bytes = AF_DEFAULT_MAX_BATCH_BYTES;
	std::string endpoint;
	std::string descriptor;
	size_t		ticket_bytes = 0;
	std::string tls_ca_file;
	std::string tls_client_cert_file;
	std::string tls_client_key_file;
	std::string auth_token_file;
	std::string endpoint_location_allowlist;
	int			segment_index = -1;
	int			endpoint_index = -1;
	std::shared_ptr<arrow::flight::FlightClient> client;
	std::shared_ptr<arrow::flight::FlightClient> origin_client;
	std::unique_ptr<arrow::flight::FlightStreamReader> reader;
	std::unique_ptr<arrow::flight::FlightInfo> flight_info;
	arrow::flight::Location origin_location;
	std::vector<arrow::flight::Location> allowed_endpoint_locations;
	std::vector<int> assigned_endpoint_indexes;
	size_t		next_assigned_endpoint = 0;
	std::string current_client_location;
	std::shared_ptr<arrow::RecordBatch> batch;
	std::vector<int> attr_batch_indexes;
	int			expected_batch_columns = -1;
	int64		next_row = 0;
	bool		projection_pushdown_requested = false;
	bool		projection_pushdown_required = false;
	bool		tls_enabled = false;
	bool		auth_enabled = false;
	bool		flight_sql_mode = false;
	bool		flight_sql_complete = false;
	bool		profile_enabled = false;
	bool		profile_reported = false;
	std::string profile_consumer;
	std::string profile_label;
	uint64		open_total_us = 0;
	uint64		connect_us = 0;
	uint64		get_flight_info_us = 0;
	uint64		endpoint_connect_us = 0;
	uint64		doget_us = 0;
	uint64		get_schema_us = 0;
	uint64		validate_schema_us = 0;
	uint64		next_us = 0;
	uint64		fdw_decode_us = 0;
	uint64		slot_store_us = 0;
	uint64		varlena_us = 0;
	int64		next_calls = 0;
	int64		batches = 0;
	int64		rows = 0;
	int64		fdw_rows = 0;
	int64		varlena_values = 0;
	int64		varlena_bytes = 0;
};

static void
af_flightsql_stream_resource_cleanup(void *resource)
{
	try
	{
		delete static_cast<ArrowFlightStreamState *>(resource);
	}
	catch (...)
	{
	}
}

static void
af_flightsql_cancel_info(
	std::shared_ptr<arrow::flight::FlightClient> client,
	arrow::flight::FlightCallOptions call_options,
	std::unique_ptr<arrow::flight::FlightInfo> info)
{
	if (client == nullptr || info == nullptr)
		return;

	try
	{
		arrow::flight::CancelFlightInfoRequest request(std::move(info));
		arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

		call_options.timeout = arrow::flight::TimeoutDuration(5.0);
		(void) sql_client.CancelFlightInfo(call_options, request);
	}
	catch (...)
	{
	}
}

static uint64
af_profile_now_us(void)
{
	using clock = std::chrono::steady_clock;

	return (uint64)
		std::chrono::duration_cast<std::chrono::microseconds>(
			clock::now().time_since_epoch()).count();
}

static void
af_profile_add_us(uint64 *target, uint64 start_us)
{
	if (target != nullptr && start_us > 0)
		*target += af_profile_now_us() - start_us;
}

static void
af_profile_emit(ArrowFlightStreamState *state)
{
	if (state == nullptr || !state->profile_enabled ||
		state->profile_reported)
		return;

	state->profile_reported = true;
	ereport(NOTICE,
			(errmsg("flightsql_profile consumer=%s label=%s segment=%d endpoint_index=%d rows=%ld batches=%ld next_calls=%ld open_total_us=%llu connect_us=%llu get_flight_info_us=%llu endpoint_connect_us=%llu doget_us=%llu get_schema_us=%llu validate_schema_us=%llu next_us=%llu fdw_decode_us=%llu slot_store_us=%llu varlena_us=%llu fdw_rows=%ld varlena_values=%ld varlena_bytes=%ld",
				 state->profile_consumer.empty() ? "unknown" :
				 state->profile_consumer.c_str(),
				 state->profile_label.empty() ? "default" :
				 state->profile_label.c_str(),
				 state->segment_index,
				 state->endpoint_index,
				 (long) state->rows,
				 (long) state->batches,
				 (long) state->next_calls,
				 (unsigned long long) state->open_total_us,
				 (unsigned long long) state->connect_us,
				 (unsigned long long) state->get_flight_info_us,
				 (unsigned long long) state->endpoint_connect_us,
				 (unsigned long long) state->doget_us,
				 (unsigned long long) state->get_schema_us,
				 (unsigned long long) state->validate_schema_us,
				 (unsigned long long) state->next_us,
				 (unsigned long long) state->fdw_decode_us,
				 (unsigned long long) state->slot_store_us,
				 (unsigned long long) state->varlena_us,
				 (long) state->fdw_rows,
				 (long) state->varlena_values,
				 (long) state->varlena_bytes)));
}

static void
af_flightsql_initialize_state(
	ArrowFlightStreamState *state, TupleDesc tupdesc, const char *url,
	const ArrowFlightSecurityOptions *security_options)
{
	ArrowFlightConnection connection;
	int			timeout_ms;
	int			max_batch_bytes;
	arrow::Result<arrow::flight::Location> location_result;

	if (state == nullptr)
		throw std::runtime_error("Flight SQL stream state is not initialized");

	af_parse_flight_connection(url, &connection);
	timeout_ms = af_get_url_int_option(url, "timeout_ms", -1, -1, INT_MAX);
	max_batch_bytes = af_get_url_int_option(url, "max_batch_bytes",
											AF_DEFAULT_MAX_BATCH_BYTES,
											0, INT_MAX);
	location_result = connection.tls ?
		arrow::flight::Location::ForGrpcTls(connection.host,
											connection.port) :
		arrow::flight::Location::ForGrpcTcp(connection.host,
											connection.port);

	state->tupdesc = tupdesc;
	state->max_batch_bytes = max_batch_bytes;
	state->origin_location =
		af_arrow_value_or_throw(std::move(location_result),
								"create Flight SQL location");
	state->endpoint = state->origin_location.ToString();
	state->segment_index = GpIdentity.segindex;
	state->tls_enabled = connection.tls;
	state->profile_consumer = "flightsql_fdw";
	state->profile_label = "statement_query";
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
		state->endpoint_location_allowlist =
			security_options->endpoint_location_allowlist == nullptr ?
			"" : security_options->endpoint_location_allowlist;
	}
	state->auth_enabled = !state->auth_token_file.empty();
	if (!state->tls_enabled &&
		(!state->tls_ca_file.empty() ||
		 !state->tls_client_cert_file.empty() ||
		 !state->tls_client_key_file.empty() ||
		 state->auth_enabled))
		throw std::runtime_error("Flight SQL TLS/auth options require tls=true");
	if (timeout_ms > 0)
		state->call_options.timeout =
			arrow::flight::TimeoutDuration((double) timeout_ms / 1000.0);
	af_arrow_parse_endpoint_location_allowlist(
		state, state->endpoint_location_allowlist);
	af_arrow_apply_auth_header(state);
}

static bool
af_flightsql_open_next_endpoint(ArrowFlightStreamState *state,
								 const bool *projected_attrs)
{
	if (state == nullptr || state->flight_info == nullptr)
		throw std::runtime_error("Flight SQL FlightInfo is not initialized");

	state->reader.reset();
	state->batch.reset();
	state->next_row = 0;

	while (state->next_assigned_endpoint <
		   state->assigned_endpoint_indexes.size())
	{
		int			endpoint_index =
			state->assigned_endpoint_indexes[state->next_assigned_endpoint++];
		const arrow::flight::FlightEndpoint& endpoint =
			state->flight_info->endpoints()[endpoint_index];
		std::vector<arrow::flight::Location> locations = endpoint.locations;
		std::string last_error;

		if (endpoint.ticket.ticket.empty())
			throw std::runtime_error("Flight SQL returned an empty ticket for endpoint " +
									 std::to_string(endpoint_index));
		if (locations.empty())
			locations.push_back(state->origin_location);

		for (const arrow::flight::Location& advertised_location : locations)
		{
			arrow::flight::Location location =
				advertised_location.Equals(
					arrow::flight::Location::ReuseConnection()) ?
				state->origin_location : advertised_location;
			const std::string location_text = location.ToString();

			if (state->tls_enabled && location.scheme() != "grpc+tls")
			{
				last_error =
					"Flight SQL endpoint attempted to downgrade a TLS connection";
				continue;
			}
			if (!af_arrow_endpoint_location_allowed(state, location))
			{
				last_error =
					"Flight SQL endpoint location is not allowed: " +
					location_text;
				continue;
			}

			CHECK_FOR_INTERRUPTS();
			try
			{
				if (state->client == nullptr ||
					state->current_client_location != location_text)
				{
					bool		origin =
						location.Equals(state->origin_location);
					bool		previous_tls = state->tls_enabled;

					state->tls_enabled =
						location.scheme() == "grpc+tls";
					arrow::flight::FlightClientOptions client_options =
						af_arrow_client_options(state);
					state->tls_enabled = previous_tls;
					state->client =
						af_arrow_value_or_throw(
							arrow::flight::FlightClient::Connect(
								location, client_options),
							"connect to Flight SQL endpoint");
					state->current_client_location = location_text;
					if (origin)
						state->origin_client = state->client;
				}

				state->reader =
					af_arrow_value_or_throw(
						state->client->DoGet(state->call_options,
											 endpoint.ticket),
						"open Flight SQL DoGet stream");
				std::shared_ptr<arrow::Schema> schema =
					af_arrow_value_or_throw(state->reader->GetSchema(),
											"read Flight SQL stream schema");

				state->endpoint = location_text;
				state->endpoint_index = endpoint_index;
				state->ticket_bytes = endpoint.ticket.ticket.size();
				af_validate_arrow_schema(state, state->tupdesc, schema,
										 projected_attrs);
				return true;
			}
			catch (const std::exception& ex)
			{
				last_error = ex.what();
				state->reader.reset();
				state->client.reset();
				state->current_client_location.clear();
			}
		}

		throw std::runtime_error("could not open Flight SQL endpoint " +
								 std::to_string(endpoint_index) +
								 ": " + last_error);
	}

	state->flight_sql_complete = true;
	return false;
}

static void *
af_flightsql_stream_open(
	TupleDesc tupdesc, const char *url, const char *serialized_flight_info,
	bool project_all, const bool *projected_attrs,
	const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightStreamState> state =
		std::make_unique<ArrowFlightStreamState>();
	int			encoded_len;
	int			decoded_capacity;
	int			decoded_len;
	std::string decoded;

	af_check_arrow_flight_linkage();
	if (serialized_flight_info == nullptr ||
		serialized_flight_info[0] == '\0')
		throw std::runtime_error("Flight SQL serialized FlightInfo is empty");

	af_flightsql_initialize_state(state.get(), tupdesc, url,
								  security_options);
	state->flight_sql_mode = true;
	state->projection_pushdown_requested = !project_all;
	state->projection_pushdown_required = !project_all;
	state->descriptor = "CommandStatementQuery";

	encoded_len = strlen(serialized_flight_info);
	decoded_capacity = pg_b64_dec_len(encoded_len);
	decoded.resize(decoded_capacity);
	decoded_len = pg_b64_decode(serialized_flight_info, encoded_len,
								decoded.data());
	if (decoded_len < 0)
		throw std::runtime_error("Flight SQL serialized FlightInfo is not valid base64");
	decoded.resize(decoded_len);
	af_arrow_status_or_throw(
		arrow::flight::FlightInfo::Deserialize(
			decoded, &state->flight_info),
		"deserialize Flight SQL FlightInfo");

	if (GpIdentity.segindex < 0)
		throw std::runtime_error("Flight SQL endpoint assignment requires a QE segment id");

	const int segment_count = getgpsegmentCount();
	const std::vector<arrow::flight::FlightEndpoint>& endpoints =
		state->flight_info->endpoints();

	for (size_t i = 0; i < endpoints.size(); i++)
	{
		if ((int) (i % segment_count) == GpIdentity.segindex)
			state->assigned_endpoint_indexes.push_back((int) i);
	}

	arrow::ipc::DictionaryMemo dictionary_memo;
	std::shared_ptr<arrow::Schema> schema =
		af_arrow_value_or_throw(
			state->flight_info->GetSchema(&dictionary_memo),
			"read Flight SQL FlightInfo schema");
	if (schema != nullptr)
		af_validate_arrow_schema(state.get(), tupdesc, schema,
								 projected_attrs);

	(void) af_flightsql_open_next_endpoint(state.get(), projected_attrs);
	af_resource_register(state.get(), af_flightsql_stream_resource_cleanup,
						 "Flight SQL read");
	return state.release();
}

static char *
af_flightsql_execute_query_impl(
	const char *url, const char *query, int max_endpoints, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightStreamState> state =
		std::make_unique<ArrowFlightStreamState>();
	std::string serialized;
	int			encoded_capacity;
	int			encoded_len;
	char	   *encoded;

	af_check_arrow_flight_linkage();
	if (query == nullptr || query[0] == '\0')
		throw std::runtime_error("Flight SQL query is empty");

	af_flightsql_initialize_state(state.get(), nullptr, url,
								  security_options);
	state->descriptor = "CommandStatementQuery";
	arrow::flight::FlightClientOptions client_options =
		af_arrow_client_options(state.get());
	state->client =
		af_arrow_value_or_throw(
			arrow::flight::FlightClient::Connect(state->origin_location,
												 client_options),
			"connect to Flight SQL server");
	state->origin_client = state->client;
	state->current_client_location = state->origin_location.ToString();

	CHECK_FOR_INTERRUPTS();
	arrow::flight::sql::FlightSqlClient sql_client(state->client);
	std::unique_ptr<arrow::flight::FlightInfo> info =
		af_arrow_value_or_throw(
			sql_client.Execute(state->call_options, query),
			"execute Flight SQL statement query");
	CHECK_FOR_INTERRUPTS();

	if (info == nullptr)
		throw std::runtime_error("Flight SQL query returned no FlightInfo");
	if (info->endpoints().size() > (size_t) max_endpoints)
	{
		std::string message =
			"Flight SQL query returned " +
			std::to_string(info->endpoints().size()) +
			" endpoints, limit is " + std::to_string(max_endpoints);

		af_flightsql_cancel_info(state->client, state->call_options,
								 std::move(info));
		throw std::runtime_error(message);
	}

	arrow::Status serialize_status = info->SerializeToString(&serialized);

	if (!serialize_status.ok())
	{
		std::string message =
			"failed to serialize Flight SQL FlightInfo: " +
			serialize_status.ToString();

		af_flightsql_cancel_info(state->client, state->call_options,
								 std::move(info));
		throw std::runtime_error(message);
	}
	if (serialized.size() > INT_MAX)
	{
		af_flightsql_cancel_info(state->client, state->call_options,
								 std::move(info));
		throw std::runtime_error("Flight SQL FlightInfo is too large");
	}

	encoded_capacity = pg_b64_enc_len((int) serialized.size());
	if (encoded_capacity > max_plan_bytes)
	{
		std::string message =
			"serialized Flight SQL FlightInfo requires " +
			std::to_string(encoded_capacity) +
			" plan bytes, limit is " + std::to_string(max_plan_bytes);

		af_flightsql_cancel_info(state->client, state->call_options,
								 std::move(info));
		throw std::runtime_error(message);
	}
	encoded = (char *) palloc(encoded_capacity + 1);
	encoded_len = pg_b64_encode(serialized.data(), (int) serialized.size(),
								encoded);
	encoded[encoded_len] = '\0';
	return encoded;
}

template <typename T>
static T
af_arrow_value_or_throw(arrow::Result<T> result, const char *context)
{
	if (!result.ok())
	{
		const std::string status = result.status().ToString();

		throw std::runtime_error("failed to " + std::string(context) +
								 ": " + status);
	}

	return std::move(result).ValueOrDie();
}

static void
af_arrow_status_or_throw(const arrow::Status& status, const char *context)
{
	if (!status.ok())
	{
		const std::string status_text = status.ToString();

		throw std::runtime_error("failed to " + std::string(context) +
								 ": " + status_text);
	}
}

static void
af_arrow_throw_unsupported_type(Form_pg_attribute attr)
{
	throw std::runtime_error("column \"" + std::string(NameStr(attr->attname)) +
							 "\" has unsupported type oid " +
							 std::to_string(attr->atttypid) +
							 " for Arrow Flight FDW");
}

static void
af_validate_arrow_schema(ArrowFlightStreamState *state,
						 TupleDesc tupdesc,
						 const std::shared_ptr<arrow::Schema>& schema,
						 const bool *projected_attrs)
{
	int			i;

	if (schema == nullptr)
		throw std::runtime_error("Arrow Flight stream returned no schema");

	if (state == nullptr)
		throw std::runtime_error("Arrow Flight stream state is not initialized");

	state->attr_batch_indexes.assign(tupdesc->natts, -1);
	state->expected_batch_columns = schema->num_fields();

	if (schema->num_fields() == tupdesc->natts)
	{
		if (state->projection_pushdown_required)
			throw std::runtime_error(
				"projected Flight SQL query returned the full remote schema");

		for (i = 0; i < tupdesc->natts; i++)
		{
			state->attr_batch_indexes[i] = i;

			if (projected_attrs != nullptr && !projected_attrs[i])
				continue;

			af_validate_arrow_attr(TupleDescAttr(tupdesc, i),
								   schema->field(i)->type());
		}
		return;
	}

	if (!state->projection_pushdown_requested)
		throw std::runtime_error("Arrow Flight stream has " +
								 std::to_string(schema->num_fields()) +
								 " columns, external table expects " +
								 std::to_string(tupdesc->natts));

	for (int field_index = 0; field_index < schema->num_fields(); field_index++)
	{
		const std::string& name = schema->field(field_index)->name();
		int			attr_index =
			af_find_projected_attr_by_name(tupdesc, projected_attrs, name);

		if (attr_index < 0)
			throw std::runtime_error("Arrow Flight projected stream returned unexpected column \"" +
									 name + "\"");

		if (state->attr_batch_indexes[attr_index] >= 0)
			throw std::runtime_error("Arrow Flight projected stream returned duplicate column \"" +
									 name + "\"");

		state->attr_batch_indexes[attr_index] = field_index;
		af_validate_arrow_attr(TupleDescAttr(tupdesc, attr_index),
							   schema->field(field_index)->type());
	}

	for (i = 0; i < tupdesc->natts; i++)
	{
		if (TupleDescAttr(tupdesc, i)->attisdropped)
			continue;

		if (projected_attrs != nullptr && projected_attrs[i] &&
			state->attr_batch_indexes[i] < 0)
			throw std::runtime_error("Arrow Flight projected stream did not return requested column \"" +
									 std::string(NameStr(TupleDescAttr(tupdesc, i)->attname)) +
									 "\"");
	}
}

static int
af_find_projected_attr_by_name(TupleDesc tupdesc, const bool *projected_attrs,
							   const std::string& name)
{
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;

		if (projected_attrs != nullptr && !projected_attrs[i])
			continue;

		if (name == NameStr(attr->attname))
			return i;
	}

	return -1;
}

static void
af_validate_arrow_attr(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::DataType>& type)
{
	if (attr->attisdropped)
		return;

	if (type == nullptr)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has no Arrow type");

	switch (attr->atttypid)
	{
		case BOOLOID:
			if (type->id() == arrow::Type::BOOL)
				return;
			break;
		case INT2OID:
			if (type->id() == arrow::Type::INT16)
				return;
			break;
		case INT4OID:
			if (type->id() == arrow::Type::INT32)
				return;
			break;
		case INT8OID:
			if (type->id() == arrow::Type::INT64)
				return;
			break;
		case FLOAT4OID:
			if (type->id() == arrow::Type::FLOAT)
				return;
			break;
		case FLOAT8OID:
			if (type->id() == arrow::Type::DOUBLE)
				return;
			break;
		case NUMERICOID:
			if (type->id() == arrow::Type::DECIMAL128)
				return;
			break;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			if (type->id() == arrow::Type::STRING ||
				type->id() == arrow::Type::LARGE_STRING)
				return;
			break;
		case BYTEAOID:
			if (type->id() == arrow::Type::BINARY ||
				type->id() == arrow::Type::LARGE_BINARY)
				return;
			break;
		case UUIDOID:
			if (af_arrow_type_is_fixed_binary(type, UUID_LEN) ||
				af_arrow_type_is_string_like(type))
				return;
			break;
		case INTERVALOID:
			if (type->id() == arrow::Type::INTERVAL_MONTH_DAY_NANO ||
				af_arrow_type_is_string_like(type))
				return;
			break;
		case TIMEOID:
			if (type->id() == arrow::Type::TIME32 ||
				type->id() == arrow::Type::TIME64)
				return;
			break;
		case CASHOID:
			if (type->id() == arrow::Type::INT64)
				return;
			break;
		case INETOID:
		case CIDROID:
			if (af_arrow_type_is_fixed_binary(type, 18))
				return;
			break;
		case MACADDROID:
			if (af_arrow_type_is_fixed_binary(type, 6))
				return;
			break;
		case DATEOID:
			if (type->id() == arrow::Type::DATE32 ||
				type->id() == arrow::Type::DATE64)
				return;
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			if (type->id() == arrow::Type::TIMESTAMP)
			{
				af_validate_arrow_timestamp_timezone(attr, type);
				return;
			}
			break;
		default:
			if (af_type_is_enum(attr->atttypid) &&
				(af_arrow_type_is_enum_dictionary(type) ||
				 af_arrow_type_is_string_like(type)))
				return;
			if (af_type_uses_text_exchange(attr->atttypid) &&
				af_arrow_type_is_string_like(type))
				return;
			af_arrow_throw_unsupported_type(attr);
			break;
	}

	throw std::runtime_error("column \"" +
							 std::string(NameStr(attr->attname)) +
							 "\" expects Greengage type oid " +
							 std::to_string(attr->atttypid) +
							 " but Arrow Flight stream returned " +
							 type->ToString());
}

static void
af_validate_arrow_timestamp_timezone(Form_pg_attribute attr,
									 const std::shared_ptr<arrow::DataType>& type)
{
	const auto timestamp_type =
		std::static_pointer_cast<arrow::TimestampType>(type);
	const std::string& timezone = timestamp_type->timezone();

	if (attr->atttypid == TIMESTAMPOID)
	{
		if (timezone.empty())
			return;

		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" maps to timestamp without time zone and requires Arrow timestamp without timezone, but Arrow type is " +
								 type->ToString());
	}

	if (attr->atttypid == TIMESTAMPTZOID)
	{
		if (timezone.empty() || timezone == "UTC" || timezone == "Etc/UTC")
			return;

		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" maps to timestamptz and requires Arrow timestamp timezone UTC or empty, but Arrow type is " +
								 type->ToString());
	}
}

static bool
af_arrow_type_is_string_like(const std::shared_ptr<arrow::DataType>& type)
{
	return type != nullptr &&
		(type->id() == arrow::Type::STRING ||
		 type->id() == arrow::Type::LARGE_STRING);
}

static bool
af_arrow_type_is_fixed_binary(const std::shared_ptr<arrow::DataType>& type,
							  int32 byte_width)
{
	if (type == nullptr || type->id() != arrow::Type::FIXED_SIZE_BINARY)
		return false;

	const auto fixed_type =
		std::static_pointer_cast<arrow::FixedSizeBinaryType>(type);

	return fixed_type->byte_width() == byte_width;
}

static bool
af_arrow_type_is_enum_dictionary(const std::shared_ptr<arrow::DataType>& type)
{
	if (type == nullptr || type->id() != arrow::Type::DICTIONARY)
		return false;

	const auto dict_type =
		std::static_pointer_cast<arrow::DictionaryType>(type);

	return af_arrow_type_is_string_like(dict_type->value_type());
}

static std::string_view
af_arrow_string_view(Form_pg_attribute attr,
					 const std::shared_ptr<arrow::Array>& array,
					 int64 rownum)
{
	if (array->type_id() == arrow::Type::LARGE_STRING)
	{
		const auto typed =
			std::static_pointer_cast<arrow::LargeStringArray>(array);

		return typed->GetView(rownum);
	}

	if (array->type_id() == arrow::Type::STRING)
	{
		const auto typed =
			std::static_pointer_cast<arrow::StringArray>(array);

		return typed->GetView(rownum);
	}

	throw std::runtime_error("column \"" +
							 std::string(NameStr(attr->attname)) +
							 "\" expects Arrow utf8 or large_utf8 but stream returned " +
							 array->type()->ToString());
}

static std::string_view
af_arrow_enum_label_view(Form_pg_attribute attr,
						 const std::shared_ptr<arrow::Array>& array,
						 int64 rownum)
{
	if (array->type_id() != arrow::Type::DICTIONARY)
		return af_arrow_string_view(attr, array, rownum);

	const auto dict_array =
		std::static_pointer_cast<arrow::DictionaryArray>(array);
	const int64 dictionary_index = dict_array->GetValueIndex(rownum);
	const std::shared_ptr<arrow::Array>& dictionary = dict_array->dictionary();

	if (dictionary_index < 0 || dictionary_index >= dictionary->length())
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has invalid Arrow dictionary index " +
								 std::to_string(dictionary_index));
	if (dictionary->IsNull(dictionary_index))
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has null Arrow dictionary value");

	return af_arrow_string_view(attr, dictionary, dictionary_index);
}

static Datum
af_arrow_uuid_to_datum(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::Array>& array,
					   int64 rownum)
{
	if (array->type_id() == arrow::Type::FIXED_SIZE_BINARY)
	{
		const auto typed =
			std::static_pointer_cast<arrow::FixedSizeBinaryArray>(array);

		return af_uuid_datum_from_bytes(typed->GetValue(rownum), UUID_LEN);
	}

	std::string_view view = af_arrow_string_view(attr, array, rownum);

	return af_input_text_datum(attr, view.data(), (int32) view.size());
}

static Datum
af_arrow_interval_to_datum(Form_pg_attribute attr,
						   const std::shared_ptr<arrow::Array>& array,
						   int64 rownum)
{
	if (array->type_id() == arrow::Type::INTERVAL_MONTH_DAY_NANO)
	{
		const auto typed =
			std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(array);
		const auto value = typed->Value(rownum);

		if (value.nanoseconds % 1000 != 0)
			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" Arrow month_day_nano_interval value is not microsecond-aligned");

		return af_interval_datum_from_parts(
			(int32) value.months, (int32) value.days,
			(int64) (value.nanoseconds / 1000));
	}

	std::string_view view = af_arrow_string_view(attr, array, rownum);

	return af_input_text_datum(attr, view.data(), (int32) view.size());
}

static Datum
af_arrow_numeric_to_datum(Form_pg_attribute attr,
						  const std::shared_ptr<arrow::Array>& array,
						  int64 rownum)
{
	if (array->type_id() != arrow::Type::DECIMAL128)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" expects Arrow decimal128 but stream returned " +
								 array->type()->ToString());

	const auto typed = std::static_pointer_cast<arrow::Decimal128Array>(array);
	std::string value = typed->FormatValue(rownum);

	return af_input_text_datum(attr, value.data(), (int32) value.size());
}

static Datum
af_arrow_binary_to_bytea_datum(Form_pg_attribute attr,
							   const std::shared_ptr<arrow::Array>& array,
							   int64 rownum)
{
	std::string_view view;

	if (array->type_id() == arrow::Type::LARGE_BINARY)
	{
		const auto typed =
			std::static_pointer_cast<arrow::LargeBinaryArray>(array);

		view = typed->GetView(rownum);
	}
	else if (array->type_id() == arrow::Type::BINARY)
	{
		const auto typed =
			std::static_pointer_cast<arrow::BinaryArray>(array);

		view = typed->GetView(rownum);
	}
	else
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" expects Arrow binary or large_binary but stream returned " +
								 array->type()->ToString());

	if (view.size() > PG_INT32_MAX)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" bytea value exceeds maximum varlena length");

	bytea	   *result = (bytea *) palloc(VARHDRSZ + view.size());

	SET_VARSIZE(result, VARHDRSZ + view.size());
	memcpy(VARDATA(result), view.data(), view.size());
	return PointerGetDatum(result);
}

static Datum
af_arrow_time_to_datum(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::Array>& array,
					   int64 rownum)
{
	int64		usecs;

	if (array->type_id() == arrow::Type::TIME64)
	{
		const auto typed =
			std::static_pointer_cast<arrow::Time64Array>(array);
		const auto type =
			std::static_pointer_cast<arrow::Time64Type>(array->type());

		usecs = (int64) typed->Value(rownum);
		if (type->unit() == arrow::TimeUnit::NANO)
		{
			if (usecs % 1000 != 0)
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" Arrow time64 value is not microsecond-aligned");
			usecs /= 1000;
		}
		else if (type->unit() != arrow::TimeUnit::MICRO)
			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" has unsupported Arrow time64 unit");
	}
	else if (array->type_id() == arrow::Type::TIME32)
	{
		const auto typed =
			std::static_pointer_cast<arrow::Time32Array>(array);
		const auto type =
			std::static_pointer_cast<arrow::Time32Type>(array->type());

		usecs = (int64) typed->Value(rownum);
		if (type->unit() == arrow::TimeUnit::SECOND)
		{
			int64		converted_usecs;

			if (pg_mul_s64_overflow(usecs, USECS_PER_SEC,
									&converted_usecs))
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" time value is out of range");
			usecs = converted_usecs;
		}
		else if (type->unit() == arrow::TimeUnit::MILLI)
		{
			int64		converted_usecs;

			if (pg_mul_s64_overflow(usecs, INT64CONST(1000),
									&converted_usecs))
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" time value is out of range");
			usecs = converted_usecs;
		}
		else
			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" has unsupported Arrow time32 unit");
	}
	else
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" expects Arrow time32/time64 but stream returned " +
								 array->type()->ToString());

	if (usecs < 0 || usecs > USECS_PER_DAY)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" time value is out of range");

	return TimeADTGetDatum((TimeADT) usecs);
}

static Datum
af_arrow_date_to_datum(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::Array>& array,
					   int64 rownum)
{
	DateADT	date;
	int64	pg_days;

	if (array->type_id() == arrow::Type::DATE64)
	{
		const auto typed =
			std::static_pointer_cast<arrow::Date64Array>(array);
		int64		millis = (int64) typed->Value(rownum);

		if (millis % INT64CONST(86400000) != 0)
			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" Arrow date64 value is not day-aligned");

		pg_days = millis / INT64CONST(86400000) -
			AF_ARROW_TO_PG_EPOCH_DAYS;
	}
	else
	{
		const auto typed =
			std::static_pointer_cast<arrow::Date32Array>(array);

		pg_days = (int64) typed->Value(rownum) -
			AF_ARROW_TO_PG_EPOCH_DAYS;
	}

	if (pg_days < PG_INT32_MIN || pg_days > PG_INT32_MAX)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" date value is out of range");

	date = (DateADT) pg_days;
	if (DATE_NOT_FINITE(date) || !IS_VALID_DATE(date))
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" date value is out of range");

	return DateADTGetDatum(date);
}

static Datum
af_arrow_inet_to_datum(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::Array>& array,
					   int64 rownum)
{
	const auto typed =
		std::static_pointer_cast<arrow::FixedSizeBinaryArray>(array);
	const uint8_t *payload = typed->GetValue(rownum);
	inet	   *result = (inet *) palloc0(sizeof(inet));

	ip_family(result) = payload[0];
	if (ip_family(result) != PGSQL_AF_INET &&
		ip_family(result) != PGSQL_AF_INET6)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has invalid inet family in Arrow payload");
	ip_bits(result) = payload[1];
	if (ip_bits(result) > ip_maxbits(result))
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has invalid inet mask length in Arrow payload");
	memcpy(ip_addr(result), payload + 2, ip_addrsize(result));
	SET_INET_VARSIZE(result);
	return InetPGetDatum(result);
}

static Datum
af_arrow_macaddr_to_datum(Form_pg_attribute attr,
						  const std::shared_ptr<arrow::Array>& array,
						  int64 rownum)
{
	const auto typed =
		std::static_pointer_cast<arrow::FixedSizeBinaryArray>(array);
	const uint8_t *payload = typed->GetValue(rownum);
	macaddr    *result = (macaddr *) palloc(sizeof(macaddr));

	result->a = payload[0];
	result->b = payload[1];
	result->c = payload[2];
	result->d = payload[3];
	result->e = payload[4];
	result->f = payload[5];
	return MacaddrPGetDatum(result);
}

static arrow::flight::FlightClientOptions
af_arrow_client_options(ArrowFlightStreamState *state)
{
	arrow::flight::FlightClientOptions options =
		arrow::flight::FlightClientOptions::Defaults();

	if (state == nullptr || !state->tls_enabled)
		return options;

	if (!state->tls_ca_file.empty())
		options.tls_root_certs =
			af_arrow_read_file(state->tls_ca_file, "tls_ca_file");

	if (state->tls_client_cert_file.empty() !=
		state->tls_client_key_file.empty())
		throw std::runtime_error("tls_client_cert_file and tls_client_key_file must be set together");

	if (!state->tls_client_cert_file.empty())
	{
		options.cert_chain =
			af_arrow_read_file(state->tls_client_cert_file,
							   "tls_client_cert_file");
		options.private_key =
			af_arrow_read_file(state->tls_client_key_file,
							   "tls_client_key_file");
	}

	return options;
}

static void
af_arrow_apply_auth_header(ArrowFlightStreamState *state)
{
	if (state == nullptr || state->auth_token_file.empty())
		return;

	if (!state->tls_enabled)
		throw std::runtime_error("auth_token_file requires tls=true");

	state->call_options.headers.push_back(
		{"authorization",
		 "Bearer " + af_arrow_read_token(state->auth_token_file)});
}

static void
af_arrow_parse_endpoint_location_allowlist(ArrowFlightStreamState *state,
										   const std::string& value)
{
	size_t		start = 0;

	if (state == nullptr || value.empty())
		return;
	if (value.size() > AF_MAX_ENDPOINT_LOCATION_ALLOWLIST_BYTES)
		throw std::runtime_error(
			"Flight SQL endpoint_location_allowlist is too large");

	while (start < value.size())
	{
		size_t		end = value.find(',', start);
		std::string entry =
			value.substr(start, end == std::string::npos ?
							std::string::npos : end - start);
		arrow::flight::Location location =
			af_arrow_value_or_throw(
				arrow::flight::Location::Parse(entry),
				"parse Flight SQL endpoint_location_allowlist entry");
		const char *expected_scheme =
			state->tls_enabled ? "grpc+tls" : "grpc+tcp";

		if (location.scheme() != expected_scheme)
			throw std::runtime_error(
				"Flight SQL endpoint_location_allowlist entry uses an "
				"unexpected transport scheme");
		state->allowed_endpoint_locations.push_back(std::move(location));

		if (end == std::string::npos)
			break;
		start = end + 1;
	}
}

static bool
af_arrow_endpoint_location_allowed(
	const ArrowFlightStreamState *state,
	const arrow::flight::Location& location)
{
	if (state == nullptr)
		return false;
	if (location.Equals(state->origin_location))
		return true;

	for (const arrow::flight::Location& allowed :
		 state->allowed_endpoint_locations)
	{
		if (location.Equals(allowed))
			return true;
	}
	return false;
}

static std::string
af_arrow_read_file(const std::string& path, const char *label)
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
af_arrow_read_token(const std::string& path)
{
	std::string token = af_arrow_read_file(path, "auth_token");

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

static std::string
af_format_flight_context(ArrowFlightStreamState *state)
{
	std::string context;

	if (state == nullptr)
		return "";

	context = " (segment=" + std::to_string(state->segment_index);

	if (state->endpoint_index >= 0)
		context += ", endpoint_index=" + std::to_string(state->endpoint_index);
	if (!state->endpoint.empty())
		context += ", endpoint=" + state->endpoint;
	if (!state->descriptor.empty())
		context += ", descriptor=" + state->descriptor;
	if (state->ticket_bytes > 0)
		context += ", ticket_bytes=" + std::to_string(state->ticket_bytes);

	context += ", tls=" + std::string(state->tls_enabled ? "true" : "false");
	context += ", auth=" + std::string(state->auth_enabled ? "true" : "false");

	context += ")";
	return context;
}

static Datum
af_arrow_attr_to_datum(Form_pg_attribute attr,
					   const std::shared_ptr<arrow::Array>& array,
					   int64 rownum, bool *isnull,
					   ArrowFlightStreamState *state)
{
	if (array == nullptr)
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" has no Arrow array");

	if (array->IsNull(rownum))
	{
		*isnull = true;
		return (Datum) 0;
	}

	*isnull = false;

	switch (attr->atttypid)
	{
		case BOOLOID:
		{
			const auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);

			return BoolGetDatum(typed->Value(rownum));
		}
		case INT2OID:
		{
			const auto typed = std::static_pointer_cast<arrow::Int16Array>(array);

			return Int16GetDatum((int16) typed->Value(rownum));
		}
		case INT4OID:
		{
			const auto typed = std::static_pointer_cast<arrow::Int32Array>(array);

			return Int32GetDatum((int32) typed->Value(rownum));
		}
		case INT8OID:
		{
			const auto typed = std::static_pointer_cast<arrow::Int64Array>(array);

			return Int64GetDatum((int64) typed->Value(rownum));
		}
		case FLOAT4OID:
		{
			const auto typed = std::static_pointer_cast<arrow::FloatArray>(array);

			return Float4GetDatum((float4) typed->Value(rownum));
		}
		case FLOAT8OID:
		{
			const auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);

			return Float8GetDatum((float8) typed->Value(rownum));
		}
		case NUMERICOID:
			return af_arrow_numeric_to_datum(attr, array, rownum);
		case TEXTOID:
		{
			std::string_view view = af_arrow_string_view(attr, array, rownum);

			if (view.size() > PG_INT32_MAX)
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" value exceeds maximum varlena length");

			if (state != nullptr && state->profile_enabled)
			{
				uint64		phase_start_us = af_profile_now_us();
				Datum		result =
					PointerGetDatum(
						cstring_to_text_with_len(view.data(),
												 (int) view.size()));

				af_profile_add_us(&state->varlena_us, phase_start_us);
				state->varlena_values++;
				state->varlena_bytes += (int64) view.size();
				return result;
			}

			return PointerGetDatum(
				cstring_to_text_with_len(view.data(), (int) view.size()));
		}
		case VARCHAROID:
		{
			std::string_view view = af_arrow_string_view(attr, array, rownum);

			if (view.size() > PG_INT32_MAX)
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" value exceeds maximum varlena length");

			if (state != nullptr && state->profile_enabled)
			{
				uint64		phase_start_us = af_profile_now_us();
				Datum		result = af_input_varchar_datum(attr, view.data(),
															 (int32) view.size());

				af_profile_add_us(&state->varlena_us, phase_start_us);
				state->varlena_values++;
				state->varlena_bytes += (int64) view.size();
				return result;
			}

			return af_input_varchar_datum(attr, view.data(),
										  (int32) view.size());
		}
		case BPCHAROID:
		{
			std::string_view view = af_arrow_string_view(attr, array, rownum);

			if (view.size() > PG_INT32_MAX)
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" value exceeds maximum varlena length");

			if (state != nullptr && state->profile_enabled)
			{
				uint64		phase_start_us = af_profile_now_us();
				Datum		result = af_input_text_datum(attr, view.data(),
														  (int32) view.size());

				af_profile_add_us(&state->varlena_us, phase_start_us);
				state->varlena_values++;
				state->varlena_bytes += (int64) view.size();
				return result;
			}

			return af_input_text_datum(attr, view.data(), (int32) view.size());
		}
		case UUIDOID:
			return af_arrow_uuid_to_datum(attr, array, rownum);
		case INTERVALOID:
			return af_arrow_interval_to_datum(attr, array, rownum);
		case BYTEAOID:
			return af_arrow_binary_to_bytea_datum(attr, array, rownum);
		case TIMEOID:
			return af_arrow_time_to_datum(attr, array, rownum);
		case CASHOID:
		{
			const auto typed = std::static_pointer_cast<arrow::Int64Array>(array);

			return CashGetDatum((Cash) typed->Value(rownum));
		}
		case INETOID:
		case CIDROID:
			return af_arrow_inet_to_datum(attr, array, rownum);
		case MACADDROID:
			return af_arrow_macaddr_to_datum(attr, array, rownum);
		case DATEOID:
			return af_arrow_date_to_datum(attr, array, rownum);
		case TIMESTAMPOID:
		{
			const auto typed =
				std::static_pointer_cast<arrow::TimestampArray>(array);
			const auto type =
				std::static_pointer_cast<arrow::TimestampType>(array->type());

			return TimestampGetDatum(
				(Timestamp) af_arrow_timestamp_to_pg_usecs(
					attr, (int64) typed->Value(rownum), type->unit()));
		}
		case TIMESTAMPTZOID:
		{
			const auto typed =
				std::static_pointer_cast<arrow::TimestampArray>(array);
			const auto type =
				std::static_pointer_cast<arrow::TimestampType>(array->type());

			return TimestampTzGetDatum(
				(TimestampTz) af_arrow_timestamp_to_pg_usecs(
					attr, (int64) typed->Value(rownum), type->unit()));
		}
		default:
			if (af_type_is_enum(attr->atttypid))
			{
				std::string_view view = af_arrow_enum_label_view(attr, array,
																rownum);

				if (view.size() > PG_INT32_MAX)
					throw std::runtime_error("column \"" +
											 std::string(NameStr(attr->attname)) +
											 "\" value exceeds maximum input length");

				return af_input_text_datum(attr, view.data(),
										   (int32) view.size());
			}

			if (af_type_uses_text_exchange(attr->atttypid))
			{
				std::string_view view = af_arrow_string_view(attr, array,
															rownum);

				if (view.size() > PG_INT32_MAX)
					throw std::runtime_error("column \"" +
											 std::string(NameStr(attr->attname)) +
											 "\" value exceeds maximum input length");

				return af_input_text_datum(attr, view.data(),
										   (int32) view.size());
			}

			af_arrow_throw_unsupported_type(attr);
			break;
	}

	return (Datum) 0;
}

static int64
af_arrow_timestamp_to_pg_usecs(Form_pg_attribute attr, int64 value,
							   arrow::TimeUnit::type unit)
{
	int64		usecs;
	int64		pg_usecs;

	switch (unit)
	{
		case arrow::TimeUnit::SECOND:
			if (pg_mul_s64_overflow(value, USECS_PER_SEC, &usecs))
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" timestamp value is out of range");
			break;
		case arrow::TimeUnit::MILLI:
			if (pg_mul_s64_overflow(value, INT64CONST(1000), &usecs))
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" timestamp value is out of range");
			break;
		case arrow::TimeUnit::MICRO:
			usecs = value;
			break;
		case arrow::TimeUnit::NANO:
			if (value % INT64CONST(1000) != 0)
				throw std::runtime_error("column \"" +
										 std::string(NameStr(attr->attname)) +
										 "\" Arrow nanosecond timestamp cannot be represented exactly as Greengage microseconds");
			usecs = value / INT64CONST(1000);
			break;
		default:
			throw std::runtime_error("column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\" has unsupported Arrow timestamp unit");
	}

	if (pg_sub_s64_overflow(usecs, AF_ARROW_TO_PG_EPOCH_USECS, &pg_usecs))
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" timestamp value is out of range");

	if (!IS_VALID_TIMESTAMP(pg_usecs))
		throw std::runtime_error("column \"" +
								 std::string(NameStr(attr->attname)) +
								 "\" timestamp value is out of range");

	return pg_usecs;
}

static TupleTableSlot *
af_stream_next_slot(void **flight_state, TupleTableSlot *slot,
					bool project_all, const bool *projected_attrs)
{
	ArrowFlightStreamState *state =
		static_cast<ArrowFlightStreamState *>(*flight_state);
	TupleDesc	tupdesc = state->tupdesc;

	for (;;)
	{
		if (state->reader == nullptr)
		{
			if (state->flight_sql_mode && !state->flight_sql_complete &&
				af_flightsql_open_next_endpoint(state, projected_attrs))
				continue;

			af_resource_unregister(state);
			delete state;
			*flight_state = NULL;
			return slot;
		}

		if (state->batch == nullptr ||
			state->next_row >= state->batch->num_rows())
		{
			uint64		phase_start_us;

			CHECK_FOR_INTERRUPTS();
			phase_start_us = af_profile_now_us();
			arrow::flight::FlightStreamChunk chunk =
				af_arrow_value_or_throw(state->reader->Next(),
										"read Arrow Flight record batch");
			if (state->profile_enabled)
			{
				af_profile_add_us(&state->next_us, phase_start_us);
				state->next_calls++;
			}
			CHECK_FOR_INTERRUPTS();

			if (chunk.data == nullptr)
			{
				state->reader.reset();
				state->batch.reset();
				if (state->flight_sql_mode &&
					af_flightsql_open_next_endpoint(state, projected_attrs))
					continue;

				state->flight_sql_complete = state->flight_sql_mode;
				af_resource_unregister(state);
				delete state;
				*flight_state = NULL;
				return slot;
			}

			if (chunk.data->num_columns() != state->expected_batch_columns)
				throw std::runtime_error("Arrow Flight record batch has " +
										 std::to_string(chunk.data->num_columns()) +
										 " columns, stream schema expects " +
										 std::to_string(state->expected_batch_columns));

			state->batch = chunk.data;
			state->next_row = 0;
			if (state->profile_enabled)
				state->batches++;
		}

		if (state->next_row < state->batch->num_rows())
			break;
	}

	uint64		decode_start_us = af_profile_now_us();

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
			continue;
		}

		if (!project_all && !projected_attrs[i])
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
			continue;
		}

		if (i >= (int) state->attr_batch_indexes.size() ||
			state->attr_batch_indexes[i] < 0)
			throw std::runtime_error("Arrow Flight stream has no batch column for projected foreign table column \"" +
									 std::string(NameStr(attr->attname)) +
									 "\"");

		slot->tts_values[i] =
			af_arrow_attr_to_datum(
				attr,
				state->batch->column(state->attr_batch_indexes[i]),
				state->next_row, &slot->tts_isnull[i], state);
	}
	if (state->profile_enabled)
		af_profile_add_us(&state->fdw_decode_us, decode_start_us);

	state->next_row++;
	if (state->profile_enabled)
	{
		uint64		slot_start_us = af_profile_now_us();
		TupleTableSlot *stored = ExecStoreVirtualTuple(slot);

		af_profile_add_us(&state->slot_store_us, slot_start_us);
		state->rows++;
		state->fdw_rows++;
		return stored;
	}

	return ExecStoreVirtualTuple(slot);
}

static std::unique_ptr<ArrowFlightStreamState>
af_flightsql_connect(
	const char *url, const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightStreamState> state =
		std::make_unique<ArrowFlightStreamState>();

	af_check_arrow_flight_linkage();
	af_flightsql_initialize_state(state.get(), nullptr, url, security_options);
	arrow::flight::FlightClientOptions client_options =
		af_arrow_client_options(state.get());

	state->client =
		af_arrow_value_or_throw(
			arrow::flight::FlightClient::Connect(state->origin_location,
												 client_options),
			"connect to Flight SQL server");
	state->origin_client = state->client;
	state->current_client_location = state->origin_location.ToString();
	return state;
}

static std::string
af_flightsql_capability_cache_key(
	const char *url, const ArrowFlightSecurityOptions *security_options)
{
	std::string key = url == nullptr ? "" : url;

	if (security_options == nullptr)
		return key;

	const char *values[] = {
		security_options->tls_ca_file,
		security_options->tls_client_cert_file,
		security_options->tls_client_key_file,
		security_options->auth_token_file,
		security_options->endpoint_location_allowlist
	};

	for (const char *value : values)
	{
		key.push_back('\0');
		if (value != nullptr)
			key.append(value);
	}
	return key;
}

static int32_t
af_flightsql_sql_info_id(const std::shared_ptr<arrow::Scalar>& scalar)
{
	if (auto value = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar))
		return value->value;
	if (auto value = std::dynamic_pointer_cast<arrow::UInt32Scalar>(scalar))
		return static_cast<int32_t>(value->value);
	if (auto value = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar))
	{
		if (value->value < INT32_MIN || value->value > INT32_MAX)
			throw std::runtime_error("Flight SQL GetSqlInfo id is out of range");
		return static_cast<int32_t>(value->value);
	}

	throw std::runtime_error("Flight SQL GetSqlInfo returned an invalid id type");
}

static std::shared_ptr<arrow::Scalar>
af_flightsql_sql_info_value(const std::shared_ptr<arrow::Scalar>& scalar)
{
	auto value = std::dynamic_pointer_cast<arrow::DenseUnionScalar>(scalar);

	if (value == nullptr || !value->is_valid || value->value == nullptr)
		throw std::runtime_error("Flight SQL GetSqlInfo returned an invalid value");
	return value->value;
}

static int
af_flightsql_sql_info_bool(const std::shared_ptr<arrow::Scalar>& scalar,
						   const char *name)
{
	auto value = std::dynamic_pointer_cast<arrow::BooleanScalar>(scalar);

	if (value == nullptr || !value->is_valid)
		throw std::runtime_error(std::string("Flight SQL GetSqlInfo ") +
								 name + " is not boolean");
	return value->value ?
		AF_FLIGHT_SQL_CAPABILITY_SUPPORTED :
		AF_FLIGHT_SQL_CAPABILITY_UNSUPPORTED;
}

static int32_t
af_flightsql_sql_info_int32(const std::shared_ptr<arrow::Scalar>& scalar,
							const char *name)
{
	auto value = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar);

	if (value == nullptr || !value->is_valid)
		throw std::runtime_error(std::string("Flight SQL GetSqlInfo ") +
								 name + " is not int32");
	return value->value;
}

static std::string
af_flightsql_sql_info_string(const std::shared_ptr<arrow::Scalar>& scalar,
							 const char *name)
{
	auto value = std::dynamic_pointer_cast<arrow::StringScalar>(scalar);

	if (value == nullptr || !value->is_valid)
		throw std::runtime_error(std::string("Flight SQL GetSqlInfo ") +
								 name + " is not UTF-8");
	return std::string(value->view());
}

static void
af_flightsql_parse_sql_info_table(
	const std::shared_ptr<arrow::Table>& table,
	FlightSqlCapabilitiesCacheEntry *capabilities)
{
	if (table == nullptr || capabilities == nullptr ||
		table->num_columns() != 2)
		throw std::runtime_error("Flight SQL GetSqlInfo returned an invalid table");

	for (int64_t row = 0; row < table->num_rows(); row++)
	{
		std::shared_ptr<arrow::Scalar> id_scalar =
			af_arrow_value_or_throw(table->column(0)->GetScalar(row),
									"read Flight SQL GetSqlInfo id");
		std::shared_ptr<arrow::Scalar> union_scalar =
			af_arrow_value_or_throw(table->column(1)->GetScalar(row),
									"read Flight SQL GetSqlInfo value");
		int32_t		id = af_flightsql_sql_info_id(id_scalar);
		std::shared_ptr<arrow::Scalar> value =
			af_flightsql_sql_info_value(union_scalar);

		switch (id)
		{
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_NAME:
				capabilities->server_name =
					af_flightsql_sql_info_string(value, "server name");
				break;
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION:
				capabilities->server_version =
					af_flightsql_sql_info_string(value, "server version");
				break;
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION:
				capabilities->transaction_support =
					af_flightsql_sql_info_int32(value,
											   "transaction support");
				break;
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_CANCEL:
				capabilities->cancellation =
					af_flightsql_sql_info_bool(value,
											  "cancellation support");
				break;
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_BULK_INGESTION:
				capabilities->bulk_ingestion =
					af_flightsql_sql_info_bool(value,
											  "bulk ingestion support");
				break;
			case arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED:
				capabilities->ingest_transactions =
					af_flightsql_sql_info_bool(
						value, "ingest transaction support");
				break;
			case arrow::flight::sql::SqlInfoOptions::SQL_DEFAULT_TRANSACTION_ISOLATION:
				capabilities->default_isolation =
					af_flightsql_sql_info_int32(
						value, "default transaction isolation");
				capabilities->default_isolation_known = true;
				break;
			case arrow::flight::sql::SqlInfoOptions::SQL_TRANSACTIONS_SUPPORTED:
				capabilities->sql_transactions =
					af_flightsql_sql_info_bool(value,
											  "SQL transaction support");
				break;
			default:
				break;
		}
	}
}

static void
af_flightsql_fetch_sql_info(
	ArrowFlightStreamState *state, const std::vector<int>& info_ids,
	FlightSqlCapabilitiesCacheEntry *capabilities)
{
	if (state == nullptr || state->client == nullptr)
		throw std::runtime_error("Flight SQL client is not initialized");

	arrow::flight::sql::FlightSqlClient sql_client(state->client);
	std::unique_ptr<arrow::flight::FlightInfo> info =
		af_arrow_value_or_throw(
			sql_client.GetSqlInfo(state->call_options, info_ids),
			"get Flight SQL server capabilities");

	if (info == nullptr || info->endpoints().empty())
		throw std::runtime_error("Flight SQL GetSqlInfo returned no endpoints");

	for (const arrow::flight::FlightEndpoint& endpoint : info->endpoints())
	{
		if (endpoint.ticket.ticket.empty())
			throw std::runtime_error("Flight SQL GetSqlInfo returned an empty ticket");

		std::shared_ptr<arrow::flight::FlightClient> client = state->client;
		if (!endpoint.locations.empty() &&
			!endpoint.locations.front().Equals(
				arrow::flight::Location::ReuseConnection()))
		{
			const arrow::flight::Location& location =
				endpoint.locations.front();
			const bool previous_tls = state->tls_enabled;
			arrow::flight::FlightClientOptions client_options =
				arrow::flight::FlightClientOptions::Defaults();

			if (previous_tls && location.scheme() != "grpc+tls")
				throw std::runtime_error(
					"Flight SQL GetSqlInfo endpoint attempted to downgrade a "
					"TLS connection");
			if (!af_arrow_endpoint_location_allowed(state, location))
				throw std::runtime_error(
					"Flight SQL GetSqlInfo endpoint location is not allowed: " +
					location.ToString());
			state->tls_enabled = location.scheme() == "grpc+tls";
			try
			{
				client_options = af_arrow_client_options(state);
			}
			catch (...)
			{
				state->tls_enabled = previous_tls;
				throw;
			}
			state->tls_enabled = previous_tls;
			client =
				af_arrow_value_or_throw(
					arrow::flight::FlightClient::Connect(
						location, client_options),
					"connect to Flight SQL GetSqlInfo endpoint");
		}

		std::unique_ptr<arrow::flight::FlightStreamReader> reader =
			af_arrow_value_or_throw(
				client->DoGet(state->call_options, endpoint.ticket),
				"read Flight SQL server capabilities");
		std::shared_ptr<arrow::Table> table =
			af_arrow_value_or_throw(
				reader->ToTable(),
				"materialize Flight SQL server capabilities");

		af_flightsql_parse_sql_info_table(table, capabilities);
	}
}

static FlightSqlCapabilitiesCacheEntry
af_flightsql_discover_capabilities(
	const char *url, const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightStreamState> state =
		af_flightsql_connect(url, security_options);
	FlightSqlCapabilitiesCacheEntry capabilities;
	const std::vector<int> all_info = {
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_NAME,
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION,
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION,
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_CANCEL,
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_BULK_INGESTION,
		arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED,
		arrow::flight::sql::SqlInfoOptions::SQL_DEFAULT_TRANSACTION_ISOLATION,
		arrow::flight::sql::SqlInfoOptions::SQL_TRANSACTIONS_SUPPORTED
	};

	try
	{
		af_flightsql_fetch_sql_info(state.get(), all_info, &capabilities);
	}
	catch (const std::exception&)
	{
		const std::vector<int> baseline_info = {
			arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_NAME,
			arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION,
			arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION,
			arrow::flight::sql::SqlInfoOptions::FLIGHT_SQL_SERVER_CANCEL
		};

		af_flightsql_fetch_sql_info(state.get(), baseline_info, &capabilities);
		for (size_t i = baseline_info.size(); i < all_info.size(); i++)
		{
			try
			{
				af_flightsql_fetch_sql_info(
					state.get(), {all_info[i]}, &capabilities);
			}
			catch (const std::exception&)
			{
				/* Older servers may not implement newer SqlInfo identifiers. */
			}
		}
	}

	return capabilities;
}

static void
af_flightsql_copy_capabilities(
	const FlightSqlCapabilitiesCacheEntry& source,
	ArrowFlightSqlCapabilities *target)
{
	if (target == nullptr)
		throw std::runtime_error("Flight SQL capability result is null");

	memset(target, 0, sizeof(*target));
	target->server_name = pstrdup(source.server_name.c_str());
	target->server_version = pstrdup(source.server_version.c_str());
	target->bulk_ingestion =
		(ArrowFlightSqlCapabilityState) source.bulk_ingestion;
	target->ingest_transactions =
		(ArrowFlightSqlCapabilityState) source.ingest_transactions;
	target->transaction_support = source.transaction_support;
	target->sql_transactions =
		(ArrowFlightSqlCapabilityState) source.sql_transactions;
	target->cancellation =
		(ArrowFlightSqlCapabilityState) source.cancellation;
	target->default_isolation = source.default_isolation;
	target->default_isolation_known = source.default_isolation_known;
}

static char *
af_flightsql_begin_transaction_impl(
	const char *url, const ArrowFlightSecurityOptions *security_options)
{
	std::unique_ptr<ArrowFlightStreamState> state =
		af_flightsql_connect(url, security_options);
	arrow::flight::sql::FlightSqlClient sql_client(state->client);
	arrow::flight::sql::Transaction transaction =
		af_arrow_value_or_throw(
			sql_client.BeginTransaction(state->call_options),
			"begin Flight SQL transaction");

	if (!transaction.is_valid())
		throw std::runtime_error(
			"Flight SQL server returned an empty transaction id");

	const std::string& transaction_id = transaction.transaction_id();
	int			encoded_capacity;
	int			encoded_len;
	char	   *encoded;

	if (transaction_id.size() > AF_FLIGHT_SQL_MAX_TRANSACTION_ID_BYTES)
		throw std::runtime_error(
			"Flight SQL transaction id exceeds the supported size");

	encoded_capacity = pg_b64_enc_len((int) transaction_id.size());
	encoded = (char *) palloc(encoded_capacity + 1);
	encoded_len = pg_b64_encode(transaction_id.data(),
								(int) transaction_id.size(), encoded);
	encoded[encoded_len] = '\0';
	return encoded;
}

static std::string
af_flightsql_end_transaction_impl(
	const char *url, const char *encoded_transaction_id, bool commit,
	const ArrowFlightSecurityOptions *security_options)
{
	try
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

		std::unique_ptr<ArrowFlightStreamState> state =
			af_flightsql_connect(url, security_options);
		arrow::flight::sql::FlightSqlClient sql_client(state->client);
		arrow::flight::sql::Transaction transaction(transaction_id);
		arrow::Status status = commit ?
			sql_client.Commit(state->call_options, transaction) :
			sql_client.Rollback(state->call_options, transaction);

		if (!status.ok())
			return status.ToString();
		return "";
	}
	catch (const std::exception& ex)
	{
		return ex.what();
	}
	catch (...)
	{
		return "unknown Flight SQL transaction exception";
	}
}

#endif /* USE_ARROW_FLIGHT */


char *
af_flightsql_execute_query(
	const char *url, const char *query, int max_endpoints, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		return af_flightsql_execute_query_impl(
			url, query, max_endpoints, max_plan_bytes, security_options);
	}
	catch (const std::exception& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL query discovery failed: %s", ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL query discovery raised unknown C++ exception")));
	}
#else
	(void) url;
	(void) query;
	(void) max_endpoints;
	(void) max_plan_bytes;
	(void) security_options;
	af_check_arrow_flight_linkage();
#endif

	return NULL;
}

void
af_flightsql_get_capabilities(
	const char *url, const ArrowFlightSecurityOptions *security_options,
	ArrowFlightSqlCapabilities *capabilities)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		std::string key =
			af_flightsql_capability_cache_key(url, security_options);
		auto		found = flightsql_capability_cache.find(key);

		if (found == flightsql_capability_cache.end())
		{
			FlightSqlCapabilitiesCacheEntry discovered =
				af_flightsql_discover_capabilities(url, security_options);

			found =
				flightsql_capability_cache.emplace(
					std::move(key), std::move(discovered)).first;
		}

		af_flightsql_copy_capabilities(found->second, capabilities);
		return;
	}
	catch (const std::exception& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL capability discovery failed: %s",
						ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL capability discovery raised unknown C++ exception")));
	}
#else
	(void) url;
	(void) security_options;
	(void) capabilities;
	af_check_arrow_flight_linkage();
#endif
}

char *
af_flightsql_begin_transaction(
	const char *url, const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		return af_flightsql_begin_transaction_impl(url, security_options);
	}
	catch (const std::exception& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not begin Flight SQL transaction: %s",
						ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("could not begin Flight SQL transaction: unknown C++ exception")));
	}
#else
	(void) url;
	(void) security_options;
	af_check_arrow_flight_linkage();
#endif

	return NULL;
}

char *
af_flightsql_end_transaction(
	const char *url, const char *transaction_id, bool commit,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	std::string error =
		af_flightsql_end_transaction_impl(
			url, transaction_id, commit, security_options);

	return error.empty() ? NULL : pstrdup(error.c_str());
#else
	(void) url;
	(void) transaction_id;
	(void) commit;
	(void) security_options;
	return pstrdup("Arrow Flight SQL support is not compiled in");
#endif
}

TupleTableSlot *
af_flightsql_stream_next_slot(
	Relation rel, const char *url, const char *serialized_flight_info,
	void **flight_state, TupleTableSlot *slot, bool project_all,
	const bool *projected_attrs,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		if (flight_state == nullptr)
			elog(ERROR, "flightsql_fdw: invalid Flight stream state pointer");

		if (*flight_state == NULL)
		{
			*flight_state =
				af_flightsql_stream_open(
					RelationGetDescr(rel), url, serialized_flight_info,
					project_all, projected_attrs, security_options);
			af_resource_attach(flight_state);
		}

		return af_stream_next_slot(flight_state, slot, project_all,
								   projected_attrs);
	}
	catch (const std::exception& ex)
	{
		ArrowFlightStreamState *state = flight_state == nullptr ? nullptr :
			static_cast<ArrowFlightStreamState *>(*flight_state);
		std::string context = af_format_flight_context(state);

		if (flight_state != nullptr && *flight_state != NULL)
		{
			af_flightsql_stream_close(state);
			*flight_state = NULL;
		}
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL read failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Flight SQL client raised unknown C++ exception")));
	}
#else
	(void) rel;
	(void) url;
	(void) serialized_flight_info;
	(void) flight_state;
	(void) slot;
	(void) project_all;
	(void) projected_attrs;
	(void) security_options;
	af_check_arrow_flight_linkage();
#endif

	return slot;
}

void
af_flightsql_stream_close(void *flight_state)
{
#ifdef USE_ARROW_FLIGHT
	ArrowFlightStreamState *state =
		static_cast<ArrowFlightStreamState *>(flight_state);

	af_resource_unregister(state);
	af_flightsql_stream_resource_cleanup(state);
#else
	(void) flight_state;
#endif
}


const char *
af_arrow_build_info(void)
{
#ifdef USE_ARROW_FLIGHT
	return "Apache Arrow Flight " ARROW_VERSION_STRING;
#else
	return "Apache Arrow Flight support is not compiled in";
#endif
}

void
af_check_arrow_flight_linkage(void)
{
#ifdef USE_ARROW_FLIGHT
	arrow::Result<arrow::flight::Location> location =
		arrow::flight::Location::ForGrpcTcp("localhost", 0);

	af_arrow_status_or_throw(location.status(),
							 "check Arrow Flight linkage");
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Apache Arrow Flight support is not compiled in"),
			 errhint("Build the arrowflight extension with USE_ARROW_FLIGHT=1 or with_arrow_flight=yes.")));
#endif
}
