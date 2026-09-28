/*-------------------------------------------------------------------------
 *
 * flightsql_endpoint.h
 *	  Pure C++ validation of advertised Flight SQL endpoint locations.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FLIGHTSQL_ENDPOINT_H
#define FLIGHTSQL_ENDPOINT_H

#include <arrow/flight/types.h>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace flightsql
{

inline std::vector<arrow::flight::Location>
ParseEndpointLocationAllowlist(const std::string& value, bool tls_enabled,
							   size_t max_bytes)
{
	std::vector<arrow::flight::Location> locations;
	size_t		start = 0;

	if (value.size() > max_bytes)
		throw std::runtime_error(
			"Flight SQL endpoint_location_allowlist is too large");

	while (start < value.size())
	{
		size_t		end = value.find(',', start);
		std::string entry =
			value.substr(start, end == std::string::npos ?
							std::string::npos : end - start);
		auto parsed = arrow::flight::Location::Parse(entry);

		if (!parsed.ok())
			throw std::runtime_error(
				"parse Flight SQL endpoint_location_allowlist entry: " +
				parsed.status().ToString());
		arrow::flight::Location location = std::move(parsed).ValueOrDie();
		const char *expected_scheme = tls_enabled ? "grpc+tls" : "grpc+tcp";

		if (location.scheme() != expected_scheme)
			throw std::runtime_error(
				"Flight SQL endpoint_location_allowlist entry uses an "
				"unexpected transport scheme");
		locations.push_back(std::move(location));

		if (end == std::string::npos)
			break;
		start = end + 1;
	}
	return locations;
}

inline bool
EndpointLocationAllowed(
	const arrow::flight::Location& origin,
	const std::vector<arrow::flight::Location>& allowed_locations,
	const arrow::flight::Location& location)
{
	if (location.Equals(origin))
		return true;
	for (const arrow::flight::Location& allowed : allowed_locations)
	{
		if (location.Equals(allowed))
			return true;
	}
	return false;
}

} /* namespace flightsql */

#endif /* FLIGHTSQL_ENDPOINT_H */
