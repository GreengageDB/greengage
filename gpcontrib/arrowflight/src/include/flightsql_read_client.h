/*-------------------------------------------------------------------------
 *
 * flightsql_read_client.h
 *    Cancellation ownership for Flight SQL read RPCs.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FLIGHTSQL_READ_CLIENT_H
#define FLIGHTSQL_READ_CLIENT_H

#include <arrow/flight/api.h>

#include <functional>
#include <memory>
#include <mutex>

/* One read RPC at a time; cancellation also applies to a subsequently opened RPC. */
class FlightSqlReadCancellation
{
public:
	class Registration
	{
	public:
		Registration(std::shared_ptr<FlightSqlReadCancellation> owner,
					 std::function<void()> cancel);
		~Registration();
		Registration(const Registration&) = delete;
		Registration& operator=(const Registration&) = delete;

	private:
		std::shared_ptr<FlightSqlReadCancellation> owner_;
	};

	void Cancel();

private:
	std::mutex mutex_;
	std::function<void()> cancel_;
	bool cancelled_ = false;
};

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>>
FlightSqlConnectReadClient(
	const arrow::flight::Location& location,
	const arrow::flight::FlightClientOptions& options,
	std::shared_ptr<FlightSqlReadCancellation> cancellation);

#endif /* FLIGHTSQL_READ_CLIENT_H */
