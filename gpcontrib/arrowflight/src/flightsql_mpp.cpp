/*-------------------------------------------------------------------------
 *
 * flightsql_mpp.cpp
 *	  Query-scoped MPP routing for Flight SQL bulk ingest.
 *
 *-------------------------------------------------------------------------
 */

#include "arrowflight_internal.h"

extern "C"
{

#include "common/base64.h"
#include "utils/memutils.h"

#include <string.h>

}

#ifdef USE_ARROW_FLIGHT
#ifdef Abs
#undef Abs
#endif

#pragma push_macro("INFO")
#pragma push_macro("WARNING")
#pragma push_macro("ERROR")
#pragma push_macro("FATAL")
#undef INFO
#undef WARNING
#undef ERROR
#undef FATAL
#include "flightsql_mpp.pb.h"
#pragma pop_macro("FATAL")
#pragma pop_macro("ERROR")
#pragma pop_macro("WARNING")
#pragma pop_macro("INFO")

#include <arrow/buffer.h>
#include <arrow/flight/api.h>
#include <arrow/util/uri.h>

#include "flightsql_endpoint.h"

#include <chrono>
#include <cctype>
#include <fstream>
#include <limits>
#include <memory>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mpp = greengage::flight::sql::mpp::v1;

namespace
{

constexpr size_t kMaxPlanIdBytes = 1024;
constexpr size_t kMaxRouteLocationBytes = 2048;
constexpr size_t kMaxWorkerIdBytes = 255;
constexpr int64_t kMaxLeaseMs = 24LL * 60 * 60 * 1000;
constexpr int kMaxAbortTimeoutMs = 5000;

struct MppClient
{
	arrow::flight::FlightCallOptions call_options;
	std::shared_ptr<arrow::flight::FlightClient> client;
};

template <typename T>
T
ValueOrThrow(arrow::Result<T> result, const char *context)
{
	if (!result.ok())
		throw std::runtime_error(
			std::string(context) + ": " + result.status().ToString());
	return std::move(result).ValueOrDie();
}

void
StatusOrThrow(const arrow::Status& status, const char *context)
{
	if (!status.ok())
		throw std::runtime_error(
			std::string(context) + ": " + status.ToString());
}

int
AbortTimeoutMs(int timeout_ms)
{
	return timeout_ms > 0 && timeout_ms < kMaxAbortTimeoutMs ?
		timeout_ms : kMaxAbortTimeoutMs;
}

char *
CopyToPg(const std::string& value)
{
	if (value.size() >= MaxAllocSize)
		throw std::runtime_error("Flight SQL MPP result exceeds the supported size");
	char *copy = (char *) palloc_extended(value.size() + 1, MCXT_ALLOC_NO_OOM);

	if (copy == nullptr)
		throw std::bad_alloc();
	memcpy(copy, value.c_str(), value.size() + 1);
	return copy;
}

char *
ConnectionError(const char *url, ArrowFlightConnection *connection)
{
	MemoryContext context = CurrentMemoryContext;
	char *volatile error = nullptr;

	/* This PG error boundary contains only C data, never C++ owners. */
	PG_TRY();
	{
		af_parse_flight_connection(url, connection);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(context);
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		error = pstrdup(edata->message);
		FreeErrorData(edata);
	}
	PG_END_TRY();
	return error;
}

std::string
ReadFile(const std::string& path, const char *label)
{
	std::ifstream input(path, std::ios::binary);
	std::ostringstream contents;

	if (!input)
		throw std::runtime_error(
			std::string("could not open ") + label + " file");
	contents << input.rdbuf();
	if (!input.good() && !input.eof())
		throw std::runtime_error(
			std::string("could not read ") + label + " file");
	return contents.str();
}

std::string
ReadToken(const std::string& path)
{
	std::string token = ReadFile(path, "auth_token");

	while (!token.empty() &&
		   (token.back() == '\n' || token.back() == '\r'))
		token.pop_back();
	if (token.empty())
		throw std::runtime_error("Flight SQL auth_token_file is empty");
	if (token.size() > AF_MAX_TOKEN_BYTES)
		throw std::runtime_error("Flight SQL auth_token_file is too large");
	for (unsigned char ch : token)
	{
		if (ch < 0x20 || ch == 0x7f)
			throw std::runtime_error(
				"Flight SQL auth_token_file contains control characters");
	}
	return token;
}

MppClient
Connect(const ArrowFlightConnection& connection, int timeout_ms,
		const ArrowFlightSecurityOptions *security_options)
{
	arrow::flight::FlightClientOptions client_options =
		arrow::flight::FlightClientOptions::Defaults();
	MppClient	result;
	std::string ca_file;
	std::string cert_file;
	std::string key_file;
	std::string token_file;

	if (security_options != nullptr)
	{
		ca_file = security_options->tls_ca_file == nullptr ?
			"" : security_options->tls_ca_file;
		cert_file = security_options->tls_client_cert_file == nullptr ?
			"" : security_options->tls_client_cert_file;
		key_file = security_options->tls_client_key_file == nullptr ?
			"" : security_options->tls_client_key_file;
		token_file = security_options->auth_token_file == nullptr ?
			"" : security_options->auth_token_file;
	}
	if (!connection.tls &&
		(!ca_file.empty() || !cert_file.empty() || !key_file.empty() ||
		 !token_file.empty()))
		throw std::runtime_error(
			"Flight SQL TLS/auth options require tls=true");
	if (cert_file.empty() != key_file.empty())
		throw std::runtime_error(
			"tls_client_cert_file and tls_client_key_file must be set together");

	if (connection.tls)
	{
		if (!ca_file.empty())
			client_options.tls_root_certs = ReadFile(ca_file, "tls_ca_file");
		if (!cert_file.empty())
		{
			client_options.cert_chain =
				ReadFile(cert_file, "tls_client_cert_file");
			client_options.private_key =
				ReadFile(key_file, "tls_client_key_file");
		}
	}
	if (!token_file.empty())
		result.call_options.headers.push_back(
			{"authorization", "Bearer " + ReadToken(token_file)});
	if (timeout_ms > 0)
		result.call_options.timeout =
			arrow::flight::TimeoutDuration((double) timeout_ms / 1000.0);

	arrow::Result<arrow::flight::Location> location_result =
		connection.tls ?
		arrow::flight::Location::ForGrpcTls(connection.host, connection.port) :
		arrow::flight::Location::ForGrpcTcp(connection.host, connection.port);
	arrow::flight::Location location =
		ValueOrThrow(std::move(location_result),
					 "create Flight SQL MPP control location");

	result.client =
		ValueOrThrow(
			arrow::flight::FlightClient::Connect(location, client_options),
			"connect to Flight SQL MPP control endpoint");
	return result;
}

std::string
DoAction(MppClient *client, const char *action_name,
		 const std::string& request, int max_response_bytes)
{
	arrow::flight::Action action{
		action_name, arrow::Buffer::FromString(request)};
	std::unique_ptr<arrow::flight::ResultStream> stream =
		ValueOrThrow(
			client->client->DoAction(client->call_options, action),
			"start Flight SQL MPP action");
	std::unique_ptr<arrow::flight::Result> first =
		ValueOrThrow(stream->Next(), "read Flight SQL MPP action result");

	if (first == nullptr || first->body == nullptr)
		throw std::runtime_error("Flight SQL MPP action returned no result");
	if (first->body->size() <= 0 ||
		first->body->size() > max_response_bytes)
		throw std::runtime_error(
			"Flight SQL MPP action result exceeds the supported size");

	std::string response(
		reinterpret_cast<const char *>(first->body->data()),
		(Size) first->body->size());
	std::unique_ptr<arrow::flight::Result> extra =
		ValueOrThrow(stream->Next(), "finish Flight SQL MPP action result");

	if (extra != nullptr)
		throw std::runtime_error(
			"Flight SQL MPP action returned more than one result");
	StatusOrThrow(stream->Drain(), "drain Flight SQL MPP action result");
	return response;
}

std::string
EncodeBase64(const std::string& value)
{
	int			capacity = pg_b64_enc_len((int) value.size());
	std::string encoded;

	encoded.resize(capacity);
	int			length =
		pg_b64_encode(value.data(), (int) value.size(), encoded.data());

	encoded.resize(length);
	return encoded;
}

std::string
EncodeBase64Url(const std::string& value)
{
	std::string encoded = EncodeBase64(value);

	for (char& ch : encoded)
	{
		if (ch == '+')
			ch = '-';
		else if (ch == '/')
			ch = '_';
	}
	while (!encoded.empty() && encoded.back() == '=')
		encoded.pop_back();
	return encoded;
}

std::string
DecodeBase64(const char *encoded, size_t max_decoded_bytes,
			 const char *label)
{
	int			encoded_len;
	int			capacity;
	int			decoded_len;
	std::string decoded;

	if (encoded == nullptr || encoded[0] == '\0')
		throw std::runtime_error(std::string(label) + " is empty");
	if (strlen(encoded) > INT_MAX)
		throw std::runtime_error(std::string(label) + " is too large");

	encoded_len = (int) strlen(encoded);
	capacity = pg_b64_dec_len(encoded_len);
	if (capacity <= 0 || (size_t) capacity > max_decoded_bytes)
		throw std::runtime_error(std::string(label) + " is too large");
	decoded.resize(capacity);
	decoded_len =
		pg_b64_decode(encoded, encoded_len, decoded.data());
	if (decoded_len <= 0)
		throw std::runtime_error(std::string(label) + " is not valid base64");
	decoded.resize(decoded_len);
	return decoded;
}

std::string
DecodeHex(const char *encoded, size_t expected_bytes, const char *label)
{
	std::string decoded;

	if (encoded == nullptr ||
		strlen(encoded) != expected_bytes * 2)
		throw std::runtime_error(
			std::string(label) + " has invalid length");
	decoded.resize(expected_bytes);
	for (size_t i = 0; i < expected_bytes; i++)
	{
		int			hi;
		int			lo;
		unsigned char first =
			(unsigned char) encoded[i * 2];
		unsigned char second =
			(unsigned char) encoded[i * 2 + 1];

		hi = std::isdigit(first) ? first - '0' :
			std::tolower(first) - 'a' + 10;
		lo = std::isdigit(second) ? second - '0' :
			std::tolower(second) - 'a' + 10;
		if (hi < 0 || hi > 15 || lo < 0 || lo > 15)
			throw std::runtime_error(
				std::string(label) + " is not hexadecimal");
		decoded[i] = (char) ((hi << 4) | lo);
	}
	return decoded;
}

std::string
EncodeHex(const std::string& value)
{
	static const char hex[] = "0123456789abcdef";
	std::string encoded;

	encoded.resize(value.size() * 2);
	for (size_t i = 0; i < value.size(); i++)
	{
		unsigned char ch = (unsigned char) value[i];

		encoded[i * 2] = hex[ch >> 4];
		encoded[i * 2 + 1] = hex[ch & 0x0f];
	}
	return encoded;
}

std::string
RouteUrl(const std::string& location)
{
	const char *prefix;
	bool		tls;

	if (location.rfind("grpc+tcp://", 0) == 0)
	{
		prefix = "grpc+tcp://";
		tls = false;
	}
	else if (location.rfind("grpc+tls://", 0) == 0)
	{
		prefix = "grpc+tls://";
		tls = true;
	}
	else
		throw std::runtime_error(
			"Flight SQL MPP route uses an unsupported location scheme");

	std::string authority = location.substr(strlen(prefix));

	if (authority.empty() ||
		authority.find_first_of("/?#") != std::string::npos)
		throw std::runtime_error(
			"Flight SQL MPP route must contain only host and port");
	if (authority.front() == '[')
		throw std::runtime_error(
			"IPv6 Flight SQL MPP routes are not supported");
	for (unsigned char ch : authority)
	{
		if (ch <= 0x20 || ch == 0x7f)
			throw std::runtime_error(
				"Flight SQL MPP route contains invalid characters");
	}

	arrow::util::Uri uri;

	StatusOrThrow(uri.Parse(location), "parse Flight SQL MPP route location");
	if (uri.host().empty() || uri.host().size() > AF_MAX_HOST_LEN ||
		authority.find_first_of("@%") != std::string::npos)
		throw std::runtime_error("Flight SQL MPP route host is invalid");
	if (uri.port() <= 0 || uri.port() > 65535 ||
		uri.port_text().size() >= 16)
		throw std::runtime_error("Flight SQL MPP route port is invalid");

	return
		std::string(AF_SCHEME) + authority +
		(tls ? "?tls=true" : "?tls=false");
}

void
ValidatePlan(
	const mpp::CreateMppIngestPlanResponse& response,
	const char *client_operation_id, const std::string& fingerprint,
	int segment_count, bool require_cluster_transaction,
	const ArrowFlightConnection& origin,
	const ArrowFlightSecurityOptions *security_options)
{
	int64_t		now_ms =
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();

	if (response.protocol_version() != AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION)
		throw std::runtime_error(
			"Flight SQL MPP plan uses an unsupported protocol version");
	if (response.plan_id().empty() ||
		response.plan_id().size() > kMaxPlanIdBytes)
		throw std::runtime_error("Flight SQL MPP plan id has invalid size");
	if (response.expires_at_unix_ms() <= now_ms ||
		response.expires_at_unix_ms() > now_ms + kMaxLeaseMs)
		throw std::runtime_error(
			"Flight SQL MPP plan lease is invalid");
	if (response.routes_size() != segment_count)
		throw std::runtime_error(
			"Flight SQL MPP plan route count does not match Greengage segment count");
	if (require_cluster_transaction &&
		response.transaction_scope() !=
		mpp::TRANSACTION_SCOPE_CLUSTER)
		throw std::runtime_error(
			"Flight SQL MPP planned ingest requires cluster-scoped transactions");

	std::vector<bool> seen((size_t) segment_count, false);
	arrow::flight::Location origin_location = ValueOrThrow(
		origin.tls ?
		arrow::flight::Location::ForGrpcTls(origin.host, origin.port) :
		arrow::flight::Location::ForGrpcTcp(origin.host, origin.port),
		"create Flight SQL MPP origin location");
	std::vector<arrow::flight::Location> allowed_locations =
		flightsql::ParseEndpointLocationAllowlist(
			security_options == nullptr ||
			security_options->endpoint_location_allowlist == nullptr ?
			"" : security_options->endpoint_location_allowlist,
			origin.tls, AF_MAX_ENDPOINT_LOCATION_ALLOWLIST_BYTES);

	for (const mpp::IngestRoute& route : response.routes())
	{
		if (route.segment_index() >= (uint32_t) segment_count ||
			seen[route.segment_index()])
			throw std::runtime_error(
				"Flight SQL MPP plan contains duplicate or invalid segment route");
		seen[route.segment_index()] = true;
		if (route.location().empty() ||
			route.location().size() > kMaxRouteLocationBytes)
			throw std::runtime_error(
				"Flight SQL MPP route location has invalid size");
		if (route.route_token().empty() ||
			route.route_token().size() > AF_MAX_TOKEN_BYTES)
			throw std::runtime_error(
				"Flight SQL MPP route token has invalid size");
		if (route.worker_id().size() > kMaxWorkerIdBytes)
			throw std::runtime_error(
				"Flight SQL MPP worker id is too large");

		(void) RouteUrl(route.location());
		arrow::flight::Location route_location = ValueOrThrow(
			arrow::flight::Location::Parse(route.location()),
			"parse Flight SQL MPP route location");

		if ((route_location.scheme() == "grpc+tls") != origin.tls)
			throw std::runtime_error(
				"Flight SQL MPP route TLS mode differs from the configured origin");
		if (!flightsql::EndpointLocationAllowed(
				origin_location, allowed_locations, route_location))
			throw std::runtime_error(
				"Flight SQL MPP route location is not allowed: " +
				route.location());
	}

	if (client_operation_id == nullptr ||
		client_operation_id[0] == '\0' || fingerprint.empty())
		throw std::runtime_error(
			"Flight SQL MPP local plan metadata is invalid");
}

mpp::DispatchedMppIngestPlan
ParseDispatchedPlan(const char *serialized_plan)
{
	std::string decoded =
		DecodeBase64(serialized_plan,
					 MaxAllocSize,
					 "Flight SQL MPP dispatched plan");
	mpp::DispatchedMppIngestPlan plan;

	if (!plan.ParseFromArray(decoded.data(), (int) decoded.size()))
		throw std::runtime_error(
			"Flight SQL MPP dispatched plan is not valid Protobuf");
	if (!plan.has_response() ||
		plan.response().protocol_version() !=
		AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION ||
		plan.client_operation_id().empty() ||
		plan.schema_fingerprint().empty() ||
		plan.segment_count() == 0)
		throw std::runtime_error(
			"Flight SQL MPP dispatched plan is incomplete");
	return plan;
}

std::string
PlanAction(
	const ArrowFlightConnection& origin, const char *serialized_plan, int timeout_ms,
	const ArrowFlightSecurityOptions *security_options,
	const char *action_name, int max_response_bytes)
{
	mpp::DispatchedMppIngestPlan plan =
		ParseDispatchedPlan(serialized_plan);
	mpp::MppIngestPlanRequest request;
	std::string request_bytes;

	request.set_protocol_version(AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION);
	request.set_client_operation_id(plan.client_operation_id());
	request.set_plan_id(plan.response().plan_id());
	if (!request.SerializeToString(&request_bytes))
		throw std::runtime_error(
			"could not serialize Flight SQL MPP plan action");

	MppClient client = Connect(origin, timeout_ms, security_options);
	return DoAction(
		&client, action_name, request_bytes,
		max_response_bytes);
}

void
BestEffortAbortCreatedPlan(
	const ArrowFlightConnection& origin, const std::string& client_operation_id,
	const std::string& plan_id, int timeout_ms, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options)
{
	if (client_operation_id.empty() || plan_id.empty())
		return;

	try
	{
		mpp::MppIngestPlanRequest request;
		std::string request_bytes;

		request.set_protocol_version(AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION);
		request.set_client_operation_id(client_operation_id);
		request.set_plan_id(plan_id);
		if (!request.SerializeToString(&request_bytes))
			return;

		MppClient client =
			Connect(origin, AbortTimeoutMs(timeout_ms), security_options);

		(void) DoAction(
			&client, AF_FLIGHT_SQL_MPP_ABORT_ACTION, request_bytes,
			max_plan_bytes);
	}
	catch (...)
	{
	}
}

} /* namespace */
#endif /* USE_ARROW_FLIGHT */

bool
af_flightsql_mpp_action_supported(
	const char *url, int timeout_ms,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	char		error[1024];
	ArrowFlightConnection origin;

	af_parse_flight_connection(url, &origin);
	try
	{
		MppClient client = Connect(origin, timeout_ms, security_options);
		std::vector<arrow::flight::ActionType> actions =
			ValueOrThrow(
				client.client->ListActions(client.call_options),
				"list Flight SQL MPP actions");
		bool		has_create = false;
		bool		has_complete = false;
		bool		has_abort = false;

		for (const arrow::flight::ActionType& action : actions)
		{
			if (action.type == AF_FLIGHT_SQL_MPP_CREATE_ACTION)
				has_create = true;
			else if (action.type == AF_FLIGHT_SQL_MPP_COMPLETE_ACTION)
				has_complete = true;
			else if (action.type == AF_FLIGHT_SQL_MPP_ABORT_ACTION)
				has_abort = true;
		}
		return has_create && has_complete && has_abort;
	}
	catch (const std::exception& ex)
	{
		strlcpy(error, ex.what(), sizeof(error));
	}
	catch (...)
	{
		strlcpy(error, "unknown C++ exception", sizeof(error));
	}
	ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("Flight SQL MPP action discovery failed: %s", error)));
#else
	(void) url;
	(void) timeout_ms;
	(void) security_options;
	af_check_arrow_flight_linkage();
#endif

	return false;
}

char *
af_flightsql_mpp_create_plan(
	const char *url, const char *client_operation_id,
	const char *catalog_name, const char *schema_name,
	const char *table_name, const char *transaction_mode,
	const char *transaction_id, const char *schema_ipc,
	int schema_ipc_len, const char *schema_fingerprint,
	int segment_count, int timeout_ms, int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options,
	char **plan_id, bool *cluster_transaction)
{
#ifdef USE_ARROW_FLIGHT
	char		error[1024];
	ArrowFlightConnection origin;

	af_parse_flight_connection(url, &origin);
	try
	{
		if (url == nullptr || client_operation_id == nullptr ||
			table_name == nullptr || table_name[0] == '\0' ||
			schema_ipc == nullptr || schema_ipc_len <= 0 ||
			schema_fingerprint == nullptr || transaction_mode == nullptr ||
			segment_count <= 0 || plan_id == nullptr ||
			cluster_transaction == nullptr)
			throw std::runtime_error(
				"Flight SQL MPP create arguments are invalid");

		bool		required =
			strcmp(transaction_mode,
				   AF_FLIGHT_SQL_WRITE_TRANSACTION_REQUIRED) == 0;
		mpp::CreateMppIngestPlanRequest request;
		mpp::Target *target = request.mutable_target();
		std::string fingerprint =
			DecodeHex(schema_fingerprint, 32,
					  "Flight SQL MPP schema fingerprint");

		request.set_protocol_version(AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION);
		request.set_client_operation_id(client_operation_id);
		target->set_catalog(catalog_name == nullptr ? "" : catalog_name);
		target->set_schema(schema_name == nullptr ? "" : schema_name);
		target->set_table(table_name);
		target->set_temporary(false);
		request.set_arrow_schema_ipc(schema_ipc, schema_ipc_len);
		request.set_schema_fingerprint(fingerprint);
		request.set_segment_count((uint32_t) segment_count);
		request.set_transaction_mode(
			required ?
			mpp::TRANSACTION_MODE_REQUIRED :
			mpp::TRANSACTION_MODE_AUTO_COMMIT);
		if (required)
			request.set_transaction_id(
				DecodeBase64(
					transaction_id,
					AF_FLIGHT_SQL_MAX_TRANSACTION_ID_BYTES,
					"Flight SQL transaction id"));
		request.set_requested_lease_ms(
			AF_FLIGHT_SQL_MPP_DEFAULT_LEASE_MS);

		std::string request_bytes;

		if (!request.SerializeToString(&request_bytes))
			throw std::runtime_error(
				"could not serialize Flight SQL MPP create request");

		MppClient client = Connect(origin, timeout_ms, security_options);
		std::string response_bytes =
			DoAction(
				&client, AF_FLIGHT_SQL_MPP_CREATE_ACTION,
				request_bytes, max_plan_bytes);
		mpp::CreateMppIngestPlanResponse response;

		if (!response.ParseFromArray(
				response_bytes.data(), (int) response_bytes.size()))
			throw std::runtime_error(
				"Flight SQL MPP create response is not valid Protobuf");
		try
		{
			ValidatePlan(
				response, client_operation_id, fingerprint, segment_count,
				required, origin, security_options);

			mpp::DispatchedMppIngestPlan dispatched;
			std::string dispatched_bytes;

			*dispatched.mutable_response() = response;
			dispatched.set_client_operation_id(client_operation_id);
			dispatched.set_schema_fingerprint(fingerprint);
			dispatched.set_segment_count((uint32_t) segment_count);
			if (!dispatched.SerializeToString(&dispatched_bytes) ||
				dispatched_bytes.size() > (size_t) max_plan_bytes)
				throw std::runtime_error(
					"Flight SQL MPP dispatched plan exceeds the supported size");

			std::string encoded_plan = EncodeBase64(dispatched_bytes);
			std::string encoded_plan_id = EncodeBase64Url(response.plan_id());

			*plan_id = CopyToPg(encoded_plan_id);
			*cluster_transaction =
				response.transaction_scope() ==
				mpp::TRANSACTION_SCOPE_CLUSTER;
			return CopyToPg(encoded_plan);
		}
		catch (...)
		{
			BestEffortAbortCreatedPlan(
				origin, request.client_operation_id(), response.plan_id(), timeout_ms,
				max_plan_bytes, security_options);
			throw;
		}
	}
	catch (const std::exception& ex)
	{
		strlcpy(error, ex.what(), sizeof(error));
	}
	catch (...)
	{
		strlcpy(error, "unknown C++ exception", sizeof(error));
	}
	/* All C++ owners, including the caught exception, precede this longjmp. */
	ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("Flight SQL MPP plan creation failed: %s", error)));
#else
	(void) url;
	(void) client_operation_id;
	(void) catalog_name;
	(void) schema_name;
	(void) table_name;
	(void) transaction_mode;
	(void) transaction_id;
	(void) schema_ipc;
	(void) schema_ipc_len;
	(void) schema_fingerprint;
	(void) segment_count;
	(void) timeout_ms;
	(void) max_plan_bytes;
	(void) security_options;
	(void) plan_id;
	(void) cluster_transaction;
	af_check_arrow_flight_linkage();
#endif

	return NULL;
}

void
af_flightsql_mpp_select_route(
	const char *origin_url, const char *serialized_plan,
	int segment_index, int segment_count,
	const ArrowFlightSecurityOptions *security_options,
	ArrowFlightSqlMppRoute *route)
{
#ifdef USE_ARROW_FLIGHT
	char		error[1024];
	ArrowFlightConnection origin;

	af_parse_flight_connection(origin_url, &origin);
	try
	{
		if (route == nullptr || segment_index < 0 ||
			segment_count <= 0)
			throw std::runtime_error(
				"Flight SQL MPP route selection arguments are invalid");

		mpp::DispatchedMppIngestPlan plan =
			ParseDispatchedPlan(serialized_plan);

		if (plan.segment_count() != (uint32_t) segment_count ||
			plan.response().routes_size() != segment_count)
			throw std::runtime_error(
				"Flight SQL MPP dispatched plan segment count changed");
		ValidatePlan(
			plan.response(), plan.client_operation_id().c_str(),
			plan.schema_fingerprint(), segment_count, false,
			origin, security_options);

		const mpp::IngestRoute *selected = nullptr;

		for (const mpp::IngestRoute& candidate :
			 plan.response().routes())
		{
			if (candidate.segment_index() == (uint32_t) segment_index)
			{
				if (selected != nullptr)
					throw std::runtime_error(
						"Flight SQL MPP dispatched plan has duplicate routes");
				selected = &candidate;
			}
		}
		if (selected == nullptr)
			throw std::runtime_error(
				"Flight SQL MPP dispatched plan has no route for this segment");

		memset(route, 0, sizeof(*route));
		std::string url = RouteUrl(selected->location());
		std::string plan_id =
			EncodeBase64Url(plan.response().plan_id());
		std::string route_token =
			EncodeBase64Url(selected->route_token());
		std::string fingerprint =
			EncodeHex(plan.schema_fingerprint());

		route->url = CopyToPg(url);
		route->plan_id = CopyToPg(plan_id);
		route->route_token = CopyToPg(route_token);
		route->client_operation_id =
			CopyToPg(plan.client_operation_id());
		route->schema_fingerprint = CopyToPg(fingerprint);
		route->worker_id = CopyToPg(selected->worker_id());
		route->segment_index = segment_index;
		route->segment_count = segment_count;
		return;
	}
	catch (const std::exception& ex)
	{
		strlcpy(error, ex.what(), sizeof(error));
	}
	catch (...)
	{
		strlcpy(error, "unknown C++ exception", sizeof(error));
	}
	ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("Flight SQL MPP route selection failed: %s", error)));
#else
	(void) origin_url;
	(void) serialized_plan;
	(void) segment_index;
	(void) segment_count;
	(void) security_options;
	(void) route;
	af_check_arrow_flight_linkage();
#endif
}

char *
af_flightsql_mpp_complete_plan(
	const char *url, const char *serialized_plan, int timeout_ms,
	int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	char		error[1024];
	ArrowFlightConnection origin;
	char *validation_error = ConnectionError(url, &origin);

	if (validation_error != nullptr)
		return validation_error;
	try
	{
		mpp::DispatchedMppIngestPlan plan =
			ParseDispatchedPlan(serialized_plan);
		std::string response_bytes =
			PlanAction(
				origin, serialized_plan, timeout_ms, security_options,
				AF_FLIGHT_SQL_MPP_COMPLETE_ACTION, max_plan_bytes);
		mpp::CompleteMppIngestPlanResponse response;

		if (!response.ParseFromArray(
				response_bytes.data(), (int) response_bytes.size()) ||
			response.protocol_version() !=
				AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION ||
			response.plan_id() != plan.response().plan_id())
			return CopyToPg(
				"Flight SQL MPP complete response is invalid");

		std::vector<bool> seen(plan.segment_count(), false);
		std::vector<const mpp::IngestRoute *> planned_routes(
			plan.segment_count(), nullptr);
		int64_t		rows = 0;
		int64_t		batches = 0;
		int64_t		bytes = 0;

		if (response.routes_size() != (int) plan.segment_count())
			return CopyToPg(
				"Flight SQL MPP complete response has an invalid route count");

		for (const mpp::IngestRoute& route : plan.response().routes())
		{
			if (route.segment_index() >= plan.segment_count() ||
				planned_routes[route.segment_index()] != nullptr)
				return CopyToPg(
					"Flight SQL MPP dispatched plan has invalid routes");
			planned_routes[route.segment_index()] = &route;
		}

		for (const mpp::IngestRouteResult& route : response.routes())
		{
			const mpp::IngestRoute *planned_route =
				route.segment_index() < plan.segment_count() ?
				planned_routes[route.segment_index()] : nullptr;

			if (route.segment_index() >= plan.segment_count() ||
				seen[route.segment_index()] ||
				planned_route == nullptr ||
				route.worker_id() != planned_route->worker_id() ||
				route.rows() < 0 || route.batches() < 0 ||
				route.bytes() < 0 ||
				(!route.opened() &&
				 (route.rows() != 0 || route.batches() != 0 ||
				  route.bytes() != 0)) ||
				rows > std::numeric_limits<int64_t>::max() -
					route.rows() ||
				batches > std::numeric_limits<int64_t>::max() -
					route.batches() ||
				bytes > std::numeric_limits<int64_t>::max() -
					route.bytes())
			{
				return CopyToPg(
					"Flight SQL MPP complete route result is invalid");
			}
			seen[route.segment_index()] = true;
			rows += route.rows();
			batches += route.batches();
			bytes += route.bytes();
		}
		if (response.rows() != rows ||
			response.batches() != batches ||
			response.bytes() != bytes)
			return CopyToPg(
				"Flight SQL MPP complete totals are inconsistent");
		return NULL;
	}
	catch (const std::exception& ex)
	{
		strlcpy(error, ex.what(), sizeof(error));
	}
	catch (...)
	{
		strlcpy(error, "unknown Flight SQL MPP complete exception", sizeof(error));
	}
	return pstrdup(error);
#else
	(void) url;
	(void) serialized_plan;
	(void) timeout_ms;
	(void) max_plan_bytes;
	(void) security_options;
	return pstrdup("Arrow Flight SQL support is not compiled in");
#endif
}

char *
af_flightsql_mpp_abort_plan(
	const char *url, const char *serialized_plan, int timeout_ms,
	int max_plan_bytes,
	const ArrowFlightSecurityOptions *security_options)
{
#ifdef USE_ARROW_FLIGHT
	char		error[1024];
	ArrowFlightConnection origin;
	char *validation_error = ConnectionError(url, &origin);

	if (validation_error != nullptr)
		return validation_error;
	try
	{
		mpp::DispatchedMppIngestPlan plan =
			ParseDispatchedPlan(serialized_plan);
		std::string response_bytes =
			PlanAction(
				origin, serialized_plan, AbortTimeoutMs(timeout_ms), security_options,
				AF_FLIGHT_SQL_MPP_ABORT_ACTION, max_plan_bytes);
		mpp::AbortMppIngestPlanResponse response;

		if (!response.ParseFromArray(
				response_bytes.data(), (int) response_bytes.size()) ||
			response.protocol_version() !=
			AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION ||
			response.plan_id() != plan.response().plan_id())
			return CopyToPg(
				"Flight SQL MPP abort response is invalid");
		return NULL;
	}
	catch (const std::exception& ex)
	{
		strlcpy(error, ex.what(), sizeof(error));
	}
	catch (...)
	{
		strlcpy(error, "unknown Flight SQL MPP abort exception", sizeof(error));
	}
	return pstrdup(error);
#else
	(void) url;
	(void) serialized_plan;
	(void) timeout_ms;
	(void) max_plan_bytes;
	(void) security_options;
	return pstrdup("Arrow Flight SQL support is not compiled in");
#endif
}
