/*-------------------------------------------------------------------------
 *
 * arrowflight_reader.cpp
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
#include <arrow/record_batch.h>
#include <arrow/result.h>
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
static bool af_projection_pushdown_enabled(const char *mode);
static bool af_projection_pushdown_candidate(TupleDesc tupdesc,
								 const bool *projected_attrs);
static std::string af_projected_descriptor_path(const char *descriptor_path,
								 TupleDesc tupdesc,
								 const bool *projected_attrs);
static void af_append_projection_name(std::string *out, const char *name);
static bool af_projection_name_char_allowed(unsigned char ch);
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
static arrow::flight::Ticket af_resolve_flight_info_ticket(
								 ArrowFlightStreamState *state,
								 const char *descriptor_path,
								 const char *endpoint_policy);
static arrow::flight::FlightClientOptions af_arrow_client_options(
								 ArrowFlightStreamState *state);
static void af_arrow_apply_auth_header(ArrowFlightStreamState *state);
static std::string af_arrow_read_file(const std::string& path,
									  const char *label);
static std::string af_arrow_read_token(const std::string& path);
static std::string af_format_flight_context(ArrowFlightStreamState *state);
static Datum af_arrow_attr_to_datum(Form_pg_attribute attr,
									const std::shared_ptr<arrow::Array>& array,
									int64 rownum, bool *isnull,
									ArrowFlightStreamState *state);
static int64 af_arrow_timestamp_to_pg_usecs(Form_pg_attribute attr,
											int64 value,
											arrow::TimeUnit::type unit);

class ArrowFlightStreamState
{
public:
	~ArrowFlightStreamState()
	{
		af_profile_emit(this);
		if (reader != nullptr)
			reader->Cancel();
	}

	TupleDesc	tupdesc = nullptr;
	arrow::flight::FlightCallOptions call_options;
	int			max_batch_bytes = AF_DEFAULT_MAX_BATCH_BYTES;
	std::string endpoint;
	std::string descriptor;
	std::string ticket;
	std::string tls_ca_file;
	std::string tls_client_cert_file;
	std::string tls_client_key_file;
	std::string auth_token_file;
	int			segment_index = -1;
	int			endpoint_index = -1;
	std::unique_ptr<arrow::flight::FlightClient> client;
	std::unique_ptr<arrow::flight::FlightStreamReader> reader;
	std::shared_ptr<arrow::RecordBatch> batch;
	std::vector<int> attr_batch_indexes;
	int			expected_batch_columns = -1;
	int64		next_row = 0;
	bool		projection_pushdown_requested = false;
	bool		projection_pushdown_required = false;
	bool		tls_enabled = false;
	bool		auth_enabled = false;
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
			(errmsg("arrowflight_profile consumer=%s label=%s segment=%d endpoint_index=%d rows=%ld batches=%ld next_calls=%ld open_total_us=%llu connect_us=%llu get_flight_info_us=%llu endpoint_connect_us=%llu doget_us=%llu get_schema_us=%llu validate_schema_us=%llu next_us=%llu fdw_decode_us=%llu slot_store_us=%llu varlena_us=%llu fdw_rows=%ld varlena_values=%ld varlena_bytes=%ld",
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

void *
af_flight_stream_open(TupleDesc tupdesc, const char *url,
					  const char *consumer,
					  const bool *projected_attrs,
					  const ArrowFlightSecurityOptions *security_options)
{
	ArrowFlightEndpoint endpoint;
	int			timeout_ms;
	int			max_batch_bytes;
	int			retry_count;
	int			retry_backoff_ms;
	bool		use_get_flight_info;
	char		endpoint_policy[32];
	char		projection_pushdown[16];
	char		profile_label[128];
	std::string descriptor_path;
	std::unique_ptr<ArrowFlightStreamState> state;
	uint64		open_start_us = af_profile_now_us();

	try
	{
		af_check_arrow_flight_linkage();
		af_parse_flight_endpoint(url, &endpoint);
		af_expand_ticket_placeholders(&endpoint);
		timeout_ms = af_get_url_int_option(url, "timeout_ms", -1, -1,
										   INT_MAX);
		max_batch_bytes = af_get_url_int_option(url, "max_batch_bytes",
												AF_DEFAULT_MAX_BATCH_BYTES,
												0, INT_MAX);
		retry_count = af_get_url_int_option(url, "retry_count", 0, 0,
											AF_MAX_RETRY_COUNT);
		retry_backoff_ms = af_get_url_int_option(url, "retry_backoff_ms",
												 100, 0,
												 AF_MAX_RETRY_BACKOFF_MS);
		use_get_flight_info = af_get_url_bool_option(url, "use_get_flight_info",
													 false);
		snprintf(endpoint_policy, sizeof(endpoint_policy), "%s",
				 AF_ENDPOINT_POLICY_FIRST);
		if (use_get_flight_info)
			(void) af_get_url_option(url, "flight_endpoint_policy",
									 endpoint_policy, sizeof(endpoint_policy));
		af_validate_endpoint_policy(endpoint_policy);
		snprintf(projection_pushdown, sizeof(projection_pushdown), "%s",
				 AF_PROJECTION_PUSHDOWN_OFF);
		(void) af_get_url_option(url, "projection_pushdown",
								 projection_pushdown,
								 sizeof(projection_pushdown));
		af_validate_projection_pushdown(projection_pushdown);
		if (strcmp(projection_pushdown,
				   AF_PROJECTION_PUSHDOWN_REQUIRE) == 0 &&
			!use_get_flight_info)
			throw std::runtime_error("projection_pushdown=require requires use_get_flight_info=true");
		snprintf(profile_label, sizeof(profile_label), "default");
		(void) af_get_url_option(url, "profile_label", profile_label,
								 sizeof(profile_label));

		arrow::Result<arrow::flight::Location> location_result =
			endpoint.tls ?
			arrow::flight::Location::ForGrpcTls(endpoint.host, endpoint.port) :
			arrow::flight::Location::ForGrpcTcp(endpoint.host, endpoint.port);
		arrow::flight::Location location =
			af_arrow_value_or_throw(std::move(location_result),
									"create Arrow Flight location");

		state = std::make_unique<ArrowFlightStreamState>();
		state->tupdesc = tupdesc;
		state->max_batch_bytes = max_batch_bytes;
		state->endpoint = location.ToString();
		state->segment_index = GpIdentity.segindex;
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
		state->projection_pushdown_requested =
			use_get_flight_info &&
			af_projection_pushdown_enabled(projection_pushdown) &&
			af_projection_pushdown_candidate(tupdesc, projected_attrs);
		state->projection_pushdown_required =
			state->projection_pushdown_requested &&
			strcmp(projection_pushdown,
				   AF_PROJECTION_PUSHDOWN_REQUIRE) == 0;
		descriptor_path = state->projection_pushdown_requested ?
			af_projected_descriptor_path(endpoint.ticket, tupdesc,
										 projected_attrs) :
			std::string(endpoint.ticket);
		state->descriptor = use_get_flight_info ? descriptor_path : "";
		state->profile_enabled =
			af_get_url_bool_option(url, "profile", false);
		state->profile_consumer = consumer == nullptr ? "unknown" : consumer;
		state->profile_label = profile_label;
		if (timeout_ms > 0)
			state->call_options.timeout =
				arrow::flight::TimeoutDuration((double) timeout_ms / 1000.0);
		af_arrow_apply_auth_header(state.get());
		arrow::flight::FlightClientOptions client_options =
			af_arrow_client_options(state.get());

		std::shared_ptr<arrow::Schema> schema;
		std::string last_error;
		int			attempt;

		for (attempt = 0; attempt <= retry_count; attempt++)
		{
			CHECK_FOR_INTERRUPTS();

			try
			{
				state->reader.reset();
				state->client.reset();
				state->batch.reset();
				state->next_row = 0;
				state->endpoint = location.ToString();
				state->endpoint_index = -1;
				state->ticket.clear();

				uint64 phase_start_us = af_profile_now_us();
				state->client =
					af_arrow_value_or_throw(
						arrow::flight::FlightClient::Connect(location,
															 client_options),
						"connect to Arrow Flight server");
				if (state->profile_enabled)
					af_profile_add_us(&state->connect_us, phase_start_us);

				phase_start_us = af_profile_now_us();
				arrow::flight::Ticket ticket =
					use_get_flight_info ?
					af_resolve_flight_info_ticket(state.get(),
												  descriptor_path.c_str(),
												  endpoint_policy) :
					arrow::flight::Ticket(endpoint.ticket);
				state->ticket = ticket.ticket;

				phase_start_us = af_profile_now_us();
				state->reader =
					af_arrow_value_or_throw(
						state->client->DoGet(state->call_options, ticket),
						"open Arrow Flight DoGet stream");
				if (state->profile_enabled)
					af_profile_add_us(&state->doget_us, phase_start_us);

				phase_start_us = af_profile_now_us();
				schema =
					af_arrow_value_or_throw(state->reader->GetSchema(),
											"read Arrow Flight stream schema");
				if (state->profile_enabled)
					af_profile_add_us(&state->get_schema_us, phase_start_us);
				break;
			}
			catch (const std::exception& ex)
			{
				last_error = ex.what();
				state->reader.reset();
				state->client.reset();

				if (attempt >= retry_count)
					throw std::runtime_error("failed after " +
											 std::to_string(attempt + 1) +
											 " attempt(s): " + last_error);

				if (retry_backoff_ms > 0)
					pg_usleep((long) retry_backoff_ms * 1000L);
			}
		}

		uint64 phase_start_us = af_profile_now_us();
		af_validate_arrow_schema(state.get(), tupdesc, schema,
								 projected_attrs);
		if (state->profile_enabled)
		{
			af_profile_add_us(&state->validate_schema_us, phase_start_us);
			state->open_total_us = af_profile_now_us() - open_start_us;
		}

		return state.release();
	}
	catch (const std::exception& ex)
	{
		std::string context = af_format_flight_context(state.get());

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight read failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Arrow Flight client raised unknown C++ exception")));
	}

	return NULL;
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
			throw std::runtime_error("projection_pushdown=require requested a reduced schema, but Arrow Flight stream returned full schema");

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

static bool
af_projection_pushdown_enabled(const char *mode)
{
	return mode != nullptr &&
		(strcmp(mode, AF_PROJECTION_PUSHDOWN_TRY) == 0 ||
		 strcmp(mode, AF_PROJECTION_PUSHDOWN_REQUIRE) == 0);
}

static bool
af_projection_pushdown_candidate(TupleDesc tupdesc, const bool *projected_attrs)
{
	bool		any_projected = false;
	bool		any_unprojected = false;

	if (tupdesc == nullptr || projected_attrs == nullptr)
		return false;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;

		if (projected_attrs[i])
			any_projected = true;
		else
			any_unprojected = true;
	}

	return any_projected && any_unprojected;
}

static std::string
af_projected_descriptor_path(const char *descriptor_path, TupleDesc tupdesc,
							 const bool *projected_attrs)
{
	std::string result;
	bool		first = true;

	if (descriptor_path == nullptr || descriptor_path[0] == '\0')
		throw std::runtime_error("Arrow FlightInfo descriptor is empty");

	result = descriptor_path;
	if (!result.empty() && result.back() != '/')
		result += "/";
	result += "columns/";

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped || projected_attrs == nullptr ||
			!projected_attrs[i])
			continue;

		if (!first)
			result += ",";
		af_append_projection_name(&result, NameStr(attr->attname));
		first = false;
	}

	if (first)
		throw std::runtime_error("Arrow Flight projection pushdown has no projected columns");

	return result;
}

static void
af_append_projection_name(std::string *out, const char *name)
{
	static const char hex[] = "0123456789ABCDEF";

	if (out == nullptr || name == nullptr || name[0] == '\0')
		throw std::runtime_error("Arrow Flight projection column name is empty");

	for (const unsigned char *pos = (const unsigned char *) name;
		 *pos != '\0'; pos++)
	{
		unsigned char ch = *pos;

		if (af_projection_name_char_allowed(ch))
			*out += (char) ch;
		else
		{
			*out += "%";
			*out += hex[(ch >> 4) & 0x0f];
			*out += hex[ch & 0x0f];
		}
	}
}

static bool
af_projection_name_char_allowed(unsigned char ch)
{
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '-' || ch == '.' || ch == '_' || ch == '~';
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

static arrow::flight::Ticket
af_resolve_flight_info_ticket(ArrowFlightStreamState *state,
							  const char *descriptor_path,
							  const char *endpoint_policy)
{
	if (state == nullptr || state->client == nullptr)
		throw std::runtime_error("Arrow Flight client is not initialized");

	if (descriptor_path == nullptr || descriptor_path[0] == '\0')
		throw std::runtime_error("Arrow FlightInfo descriptor is empty");

	state->descriptor = descriptor_path;

	arrow::flight::FlightDescriptor descriptor =
		arrow::flight::FlightDescriptor::Path({std::string(descriptor_path)});

	uint64 phase_start_us = af_profile_now_us();
	std::unique_ptr<arrow::flight::FlightInfo> info =
		af_arrow_value_or_throw(
			state->client->GetFlightInfo(state->call_options, descriptor),
			"get Arrow FlightInfo");
	if (state->profile_enabled)
		af_profile_add_us(&state->get_flight_info_us, phase_start_us);

	if (info == nullptr || info->endpoints().empty())
		throw std::runtime_error("Arrow FlightInfo returned no endpoints");

	const std::vector<arrow::flight::FlightEndpoint>& endpoints =
		info->endpoints();
	int			endpoint_index = 0;

	if (strcmp(endpoint_policy, AF_ENDPOINT_POLICY_FIRST) == 0)
		endpoint_index = 0;
	else if (strcmp(endpoint_policy, AF_ENDPOINT_POLICY_SEGMENT_INDEX) == 0)
	{
		if (GpIdentity.segindex < 0)
			throw std::runtime_error("Arrow Flight endpoint policy \"" +
									 std::string(endpoint_policy) +
									 "\" requires a QE segment id");

		if (GpIdentity.segindex >= (int) endpoints.size())
			throw std::runtime_error("Arrow FlightInfo returned " +
									 std::to_string(endpoints.size()) +
									 " endpoints, but segment id is " +
									 std::to_string(GpIdentity.segindex));

		endpoint_index = GpIdentity.segindex;
	}
	else
		throw std::runtime_error("unknown Arrow Flight endpoint policy \"" +
								 std::string(endpoint_policy) + "\"");

	const arrow::flight::FlightEndpoint& endpoint = endpoints[endpoint_index];

	if (!endpoint.locations.empty() &&
		!endpoint.locations.front().Equals(arrow::flight::Location::ReuseConnection()))
	{
		state->endpoint = endpoint.locations.front().ToString();
		phase_start_us = af_profile_now_us();
		arrow::flight::FlightClientOptions client_options =
			af_arrow_client_options(state);
		state->client =
			af_arrow_value_or_throw(
				arrow::flight::FlightClient::Connect(endpoint.locations.front(),
													 client_options),
				"connect to Arrow Flight endpoint");
		if (state->profile_enabled)
			af_profile_add_us(&state->endpoint_connect_us, phase_start_us);
	}

	if (endpoint.ticket.ticket.empty())
		throw std::runtime_error("Arrow FlightInfo returned an empty ticket");

	state->endpoint_index = endpoint_index;
	state->ticket = endpoint.ticket.ticket;

	return endpoint.ticket;
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
	if (!state->ticket.empty())
		context += ", ticket=" + state->ticket;

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

#endif /* USE_ARROW_FLIGHT */


TupleTableSlot *
af_flight_stream_next_slot(Relation rel, const char *url,
						   void **flight_state, TupleTableSlot *slot,
						   bool project_all, const bool *projected_attrs,
						   const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	try
	{
		ArrowFlightStreamState *state;
		TupleDesc	tupdesc;
		int			i;

		if (flight_state == nullptr)
			elog(ERROR, "arrowflight_fdw: invalid Flight stream state pointer");

		if (*flight_state == NULL)
			*flight_state = af_flight_stream_open(RelationGetDescr(rel), url,
											  "fdw", projected_attrs,
											  security_options);

		state = static_cast<ArrowFlightStreamState *>(*flight_state);
		tupdesc = state->tupdesc;

		for (;;)
		{
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

		uint64 decode_start_us = af_profile_now_us();

		for (i = 0; i < tupdesc->natts; i++)
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
				af_arrow_attr_to_datum(attr,
								   state->batch->column(state->attr_batch_indexes[i]),
								   state->next_row,
								   &slot->tts_isnull[i],
								   state);
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
	catch (const std::exception& ex)
	{
		ArrowFlightStreamState *state = flight_state == nullptr ? nullptr :
			static_cast<ArrowFlightStreamState *>(*flight_state);
		std::string context = af_format_flight_context(state);

		if (flight_state != nullptr && *flight_state != NULL)
		{
			delete state;
			*flight_state = NULL;
		}

		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Arrow Flight FDW read failed%s: %s",
						context.c_str(), ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Arrow Flight FDW client raised unknown C++ exception")));
	}
#else
	(void) rel;
	(void) url;
	(void) flight_state;
	(void) slot;
	(void) project_all;
	(void) projected_attrs;
	af_check_arrow_flight_linkage();
#endif

	return slot;
}

void
af_flight_stream_close(void *flight_state)
{
#ifdef USE_ARROW_FLIGHT
	ArrowFlightStreamState *state =
		static_cast<ArrowFlightStreamState *>(flight_state);

	delete state;
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
