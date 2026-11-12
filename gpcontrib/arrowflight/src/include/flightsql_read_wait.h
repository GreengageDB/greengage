/*-------------------------------------------------------------------------
 *
 * flightsql_read_wait.h
 *    Interruptible waits for Flight SQL discovery and read RPCs.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FLIGHTSQL_READ_WAIT_H
#define FLIGHTSQL_READ_WAIT_H

#include "flightsql_read_client.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <signal.h>
#include <system_error>
#include <thread>

inline bool
FlightSqlInterruptPending()
{
	return InterruptHoldoffCount == 0 &&
		(ProcDiePending || (QueryCancelPending && QueryCancelHoldoffCount == 0));
}

/*
 * Keep PostgreSQL work on the backend thread. One signal-blocked worker runs
 * discovery/schema/batch reads; cancellation reaches its active gRPC context.
 */
class FlightSqlReadWait
{
public:
	~FlightSqlReadWait()
	{
		{
			std::lock_guard<std::mutex> lock(mutex_);

			stopping_ = true;
		}
		ready_.notify_one();
		if (worker_.joinable())
			worker_.join();
	}

	template <typename Call>
	auto Run(Call call, const std::shared_ptr<FlightSqlReadCancellation>& cancellation)
	{
		using Result = decltype(call());
		auto task = std::make_shared<std::packaged_task<Result()>>(std::move(call));
		auto result = task->get_future();

		if (!worker_.joinable())
			StartWorker();
		{
			std::lock_guard<std::mutex> lock(mutex_);

			pending_ = [task] { (*task)(); };
		}
		ready_.notify_one();
		try
		{
			while (result.wait_for(std::chrono::milliseconds(10)) !=
				   std::future_status::ready)
			{
				if (FlightSqlInterruptPending())
					cancellation->Cancel();
			}
		}
		catch (...)
		{
			/* The worker must release the caller's state before C++ unwinds. */
			cancellation->Cancel();
			result.wait();
			throw;
		}
		return result.get();
	}

private:
	void StartWorker()
	{
		sigset_t blocked;
		sigset_t previous;

		/* The helper must never run a PostgreSQL backend signal handler. */
		sigfillset(&blocked);
		int error = pthread_sigmask(SIG_SETMASK, &blocked, &previous);

		if (error != 0)
			throw std::system_error(error, std::generic_category(),
								"block signals for Flight SQL read worker");
		try
		{
			worker_ = std::thread([this] { Work(); });
		}
		catch (...)
		{
			pthread_sigmask(SIG_SETMASK, &previous, nullptr);
			throw;
		}
		pthread_sigmask(SIG_SETMASK, &previous, nullptr);
	}

	void Work()
	{
		for (;;)
		{
			std::unique_lock<std::mutex> lock(mutex_);

			ready_.wait(lock, [this] { return stopping_ || pending_ != nullptr; });
			if (stopping_)
				return;
			auto task = std::move(pending_);

			lock.unlock();
			task();
		}
	}

	std::mutex mutex_;
	std::condition_variable ready_;
	std::thread worker_;
	std::function<void()> pending_;
	bool stopping_ = false;
};

#endif /* FLIGHTSQL_READ_WAIT_H */
