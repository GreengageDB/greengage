/* Standalone tests for the production MPP validator, without a PG backend. */

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

/* Include Protobuf before PostgreSQL's Min/Max/IsPowerOf2 macros. */
#include "flightsql_mpp.pb.h"
#include "../src/flightsql_mpp.cpp"

/* PG shims exercise the exported C++/ereport boundary without a backend. */
#undef vsnprintf
namespace
{
char pg_error[2048];
bool fail_pg_allocation = false;
}

extern "C"
{
MemoryContext CurrentMemoryContext = nullptr;
sigjmp_buf *PG_exception_stack = nullptr;
ErrorContextCallback *error_context_stack = nullptr;

bool
errstart(int, const char *)
{
	if (std::current_exception() != nullptr)
		throw std::runtime_error("ereport called with a live C++ exception");
	return true;
}

void errcode(int) {}

void
errmsg(const char *format, ...)
{
	va_list args;
	va_start(args, format);
	vsnprintf(pg_error, sizeof(pg_error), format, args);
	va_end(args);
}

void
errfinish(const char *, int, const char *)
{
	if (PG_exception_stack != nullptr)
		siglongjmp(*PG_exception_stack, 1);
	throw std::runtime_error(pg_error);
}

void *
palloc_extended(Size size, int flags)
{
	if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		throw std::runtime_error("C++ allocation may longjmp on OOM");
	return fail_pg_allocation ? nullptr : malloc(size);
}

char *
pstrdup(const char *value)
{
	if (std::current_exception() != nullptr)
		throw std::runtime_error("pstrdup called with a live C++ exception");
	return strdup(value);
}

ErrorData *
CopyErrorData()
{
	ErrorData *error = (ErrorData *) calloc(1, sizeof(ErrorData));
	error->message = strdup(pg_error);
	return error;
}

void FreeErrorData(ErrorData *error) { free(error->message); free(error); }
void FlushErrorState() {}
}

void
af_parse_flight_connection(const char *url, ArrowFlightConnection *origin)
{
	if (url == nullptr || strcmp(url, "arrowflight://control:9020") != 0)
		ereport(ERROR, (errmsg("invalid test origin")));
	memset(origin, 0, sizeof(*origin));
	strcpy(origin->host, "control");
	origin->port = 9020;
}

namespace
{

int checks = 0;

void
Check(bool condition, const std::string& message)
{
	if (!condition)
		throw std::runtime_error(message);
	++checks;
}

template <typename F>
void
ExpectError(F action, const std::string& expected)
{
	bool unwound = false;
	struct Guard
	{
		bool& unwound;
		~Guard() { unwound = true; }
	};

	try
	{
		Guard guard{unwound};
		action();
	}
	catch (const std::exception& ex)
	{
		Check(unwound, "exception did not unwind C++ objects");
		Check(std::string(ex.what()).find(expected) != std::string::npos,
			  "unexpected error: " + std::string(ex.what()));
		return;
	}
	throw std::runtime_error("expected error containing: " + expected);
}

mpp::CreateMppIngestPlanResponse
Plan(const std::vector<std::string>& locations)
{
	mpp::CreateMppIngestPlanResponse response;
	int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();

	response.set_protocol_version(AF_FLIGHT_SQL_MPP_PROTOCOL_VERSION);
	response.set_plan_id("test-plan");
	response.set_expires_at_unix_ms(now_ms + 60000);
	response.set_transaction_scope(mpp::TRANSACTION_SCOPE_CLUSTER);
	for (size_t i = 0; i < locations.size(); ++i)
	{
		auto *route = response.add_routes();
		route->set_segment_index(i);
		route->set_location(locations[i]);
		route->set_route_token("test-token");
		route->set_worker_id("worker-" + std::to_string(i));
	}
	return response;
}

void
TestRouteUrl()
{
	for (const std::string scheme : {"tcp", "tls"})
	{
		const std::string prefix = "grpc+" + scheme + "://";
		const std::string suffix = scheme == "tls" ? "true" : "false";

		for (const std::string authority :
			 {"worker-1:9021", "127.0.0.1:1", "worker_1:65535", "worker:0009021"})
			Check(RouteUrl(prefix + authority) ==
				  "arrowflight://" + authority + "?tls=" + suffix,
				  "valid route URL conversion changed");
		const std::string longest_host(AF_MAX_HOST_LEN, 'a');
		Check(RouteUrl(prefix + longest_host + ":9021") ==
			  "arrowflight://" + longest_host + ":9021?tls=" + suffix,
			  "maximum-length host rejected");

		std::vector<std::string> invalid = {
			"", ":9021", "worker", "worker:", "worker:0", "worker:65536",
			"worker:-1", "worker:+9021", "worker:abc", "worker:9021junk",
			"worker:999999999999999999999", "worker:0000000000009021",
			"worker:9021/", "worker:9021?tls=true", "worker:9021#fragment",
			"[::1]:9021", "::1:9021", "user@worker:9021",
			"user:password@worker:9021", "@worker:9021", "work er:9021",
			"worker:9021\n", "worker\t:9021", "work%65r:9021", "%00:9021",
			std::string(AF_MAX_HOST_LEN + 1, 'a') + ":9021",
			std::string("worker:9021\0other:9022", 22)
		};
		for (const std::string& authority : invalid)
			ExpectError([&] { (void) RouteUrl(prefix + authority); },
						"Flight SQL MPP route");
	}
	for (const std::string location :
		 {"http://worker:9021", "grpc://worker:9021", "grpc+unix:///tmp/socket",
		  "arrow-flight-reuse-connection://?"})
		ExpectError([&] { (void) RouteUrl(location); }, "unsupported location scheme");
}

void
TestPlanLocations()
{
	for (bool tls : {false, true})
	{
		ArrowFlightConnection origin{};
		strcpy(origin.host, "control");
		origin.port = 9020;
		origin.tls = tls;
		const std::string prefix = tls ? "grpc+tls://" : "grpc+tcp://";
		const std::string opposite = tls ? "grpc+tcp://" : "grpc+tls://";
		ArrowFlightSecurityOptions security{};
		std::string allowlist;
		auto validate = [&](const std::vector<std::string>& locations)
		{
			security.endpoint_location_allowlist = allowlist.data();
			ValidatePlan(Plan(locations), "operation", "fingerprint",
						 (int) locations.size(), true, origin, &security);
		};

		validate({prefix + "control:9020", prefix + "control:9020"});
		ValidatePlan(Plan({prefix + "control:9020"}), "operation", "fingerprint",
					 1, false, origin, nullptr);
		checks += 2;
		ExpectError([&] { validate({prefix + "worker:9021"}); }, "not allowed");
		ExpectError([&] { validate({prefix + "control:9021"}); }, "not allowed");
		ExpectError([&] { validate({opposite + "control:9020"}); }, "TLS mode differs");

		allowlist = prefix + "worker:9021," + prefix + "worker-2:9022";
		validate({prefix + "worker:9021", prefix + "control:9020",
				  prefix + "worker-2:9022"});
		++checks;
		ExpectError([&] { validate({prefix + "worker:9022"}); }, "not allowed");
		ExpectError([&] { validate({prefix + "worker.evil:9021"}); }, "not allowed");
		ExpectError([&] { validate({opposite + "worker:9021"}); }, "TLS mode differs");
		ExpectError([&] { validate({prefix + "worker:9021", prefix + "other:9023"}); },
					"not allowed");
		ExpectError([&] { validate({prefix + "worker:bad"}); }, "route");

		allowlist += ",";
		validate({prefix + "worker:9021"});
		++checks; /* Preserve the read parser's trailing-comma behavior. */
		allowlist = opposite + "worker:9021";
		ExpectError([&] { validate({prefix + "control:9020"}); }, "unexpected transport");
		allowlist = "http://worker:9021";
		ExpectError([&] { validate({prefix + "control:9020"}); }, "unexpected transport");
		allowlist = prefix + "worker:not-a-port";
		ExpectError([&] { validate({prefix + "control:9020"}); }, "allowlist entry");
		allowlist = std::string(AF_MAX_ENDPOINT_LOCATION_ALLOWLIST_BYTES + 1, 'a');
		ExpectError([&] { validate({prefix + "control:9020"}); }, "allowlist is too large");
	}
}

void
TestAbortTimeout()
{
	Check(AbortTimeoutMs(-1) == 5000, "negative timeout must be bounded");
	Check(AbortTimeoutMs(0) == 5000, "unlimited timeout must be bounded");
	Check(AbortTimeoutMs(1) == 1, "remaining abort budget must be respected");
	Check(AbortTimeoutMs(75) == 75, "shorter timeout must be respected");
	Check(AbortTimeoutMs(4999) == 4999, "shorter timeout must be respected");
	Check(AbortTimeoutMs(5000) == 5000, "five-second timeout changed");
	Check(AbortTimeoutMs(60000) == 5000, "longer timeout must be capped");
}

void
TestPgBoundary()
{
	const char *url = "arrowflight://control:9020";
	ArrowFlightSecurityOptions security{};
	char ca_file[] = "unused-ca";
	security.tls_ca_file = ca_file;
	ExpectError([&] { (void) af_flightsql_mpp_action_supported(url, 1, &security); },
				"action discovery failed: Flight SQL TLS/auth options require tls=true");
	ExpectError([&] {
		(void) af_flightsql_mpp_create_plan(url, nullptr, nullptr, nullptr,
			nullptr, nullptr, nullptr, nullptr, 0, nullptr, 0, 1, 1024,
			nullptr, nullptr, nullptr);
	}, "plan creation failed: Flight SQL MPP create arguments are invalid");

	mpp::DispatchedMppIngestPlan dispatched;
	*dispatched.mutable_response() = Plan({"grpc+tcp://worker:9021"});
	dispatched.set_client_operation_id("operation");
	dispatched.set_schema_fingerprint("fingerprint");
	dispatched.set_segment_count(1);
	std::string serialized = EncodeBase64(dispatched.SerializeAsString());
	ArrowFlightSqlMppRoute route{};
	ExpectError([&] {
		af_flightsql_mpp_select_route(url, serialized.c_str(), 0, 1, nullptr, &route);
	}, "route selection failed: Flight SQL MPP route location is not allowed");
	Check(route.url == nullptr, "rejected route populated output");
	char allowlist[] = "grpc+tcp://worker:9021";
	security = {};
	security.endpoint_location_allowlist = allowlist;
	af_flightsql_mpp_select_route(url, serialized.c_str(), 0, 1, &security, &route);
	Check(std::string(route.url) == "arrowflight://worker:9021?tls=false",
		  "QE did not select an allowlisted route");
	free(route.url);
	free(route.plan_id);
	free(route.route_token);
	free(route.client_operation_id);
	free(route.schema_fingerprint);
	free(route.worker_id);
	memset(&route, 0, sizeof(route));

	fail_pg_allocation = true;
	ExpectError([&] {
		af_flightsql_mpp_select_route(url, serialized.c_str(), 0, 1, &security, &route);
	}, "route selection failed:");
	fail_pg_allocation = false;
	Check(route.url == nullptr, "failed allocation populated route");

	for (auto action : {af_flightsql_mpp_complete_plan, af_flightsql_mpp_abort_plan})
	{
		char *error = action(url, "not-base64", 1, 1024, nullptr);
		Check(error != nullptr, "bad dispatched plan accepted");
		Check(std::string(error).find("base64") != std::string::npos,
			  "wrong dispatched plan error");
		free(error);
		error = action("invalid-origin", serialized.c_str(), 1, 1024, nullptr);
		Check(error != nullptr && std::string(error) == "invalid test origin",
			  "PG origin error was not converted to callback result");
		free(error);
		Check(PG_exception_stack == nullptr, "PG error boundary was not restored");
	}
}

} /* namespace */

int
main()
{
	try
	{
		TestRouteUrl();
		TestPlanLocations();
		TestAbortTimeout();
		TestPgBoundary();
		std::cout << "Flight SQL MPP route validation: " << checks << " checks passed\n";
		return 0;
	}
	catch (const std::exception& ex)
	{
		std::cerr << "Flight SQL MPP route validation failed: " << ex.what() << '\n';
		return 1;
	}
}
