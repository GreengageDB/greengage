/*
 * Minimal Apache Arrow Flight smoke server for gpcontrib/arrowflight.
 *
 * This helper is intentionally not a production arrowflightd.  It serves a
 * small set of DoGet tickets with primitive/string/date/timestamp columns so
 * arrowflight_fdw read paths can be tested end-to-end.
 */

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <array>
#include <climits>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sstream>
#include <vector>

namespace
{

arrow::Status FinishArray(arrow::ArrayBuilder *builder,
						  std::shared_ptr<arrow::Array> *out)
{
	return builder->Finish(out);
}

std::array<uint8_t, 16> UuidBytes(uint8_t last_byte)
{
	std::array<uint8_t, 16> bytes{};

	bytes[15] = last_byte;
	return bytes;
}

int GetEnvInt(const char *name, int default_value, int min_value,
			  int max_value)
{
	const char *value = std::getenv(name);
	char *end = nullptr;

	if (value == nullptr || value[0] == '\0')
	{
		return default_value;
	}

	const long parsed = std::strtol(value, &end, 10);
	if (end == value || *end != '\0' || parsed < min_value ||
		parsed > max_value)
	{
		return default_value;
	}

	return static_cast<int>(parsed);
}

int GetSmokeRows()
{
	return GetEnvInt("ARROWFLIGHT_SMOKE_ROWS", 3, 1, INT_MAX);
}

int GetSmokeSegments()
{
	return GetEnvInt("ARROWFLIGHT_SMOKE_SEGMENTS", 2, 1, INT_MAX);
}

int GetBenchRowsPerSegment()
{
	return GetEnvInt("ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT", 100000, 1,
					 INT_MAX / 4);
}

int GetBenchBatchRows()
{
	return GetEnvInt("ARROWFLIGHT_BENCH_BATCH_ROWS", 8192, 1, INT_MAX / 4);
}

int GetBenchLabelWidth()
{
	return GetEnvInt("ARROWFLIGHT_BENCH_LABEL_WIDTH", 32, 1, 1024);
}

bool GetEnvBool(const char *name, bool default_value)
{
	const char *value = std::getenv(name);

	if (value == nullptr || value[0] == '\0')
	{
		return default_value;
	}

	return std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
		   std::strcmp(value, "on") == 0 || std::strcmp(value, "yes") == 0;
}

std::string GetEnvString(const char *name, const std::string &default_value)
{
	const char *value = std::getenv(name);

	if (value == nullptr || value[0] == '\0')
	{
		return default_value;
	}

	return value;
}

uint64_t NowUsec()
{
	using clock = std::chrono::steady_clock;

	return static_cast<uint64_t>(
		std::chrono::duration_cast<std::chrono::microseconds>(
			clock::now().time_since_epoch())
			.count());
}

void LogLine(const std::string &line)
{
	static std::mutex log_mutex;
	std::lock_guard<std::mutex> guard(log_mutex);

	std::cerr << line << std::endl;
}

bool ParseSegmentTicket(const std::string &ticket, int *segid)
{
	const std::string prefix = "smoke-seg-";

	if (ticket.rfind(prefix, 0) != 0)
	{
		return false;
	}

	const char *digits = ticket.c_str() + prefix.size();
	if (*digits == '\0')
	{
		return false;
	}

	int value = 0;
	for (const char *p = digits; *p != '\0'; p++)
	{
		if (*p < '0' || *p > '9')
		{
			return false;
		}

		const int digit = *p - '0';
		if (value > (INT_MAX - digit) / 10)
		{
			return false;
		}

		value = value * 10 + digit;
	}

	*segid = value;
	return true;
}

std::vector<std::string> SplitPath(const std::string &path)
{
	std::vector<std::string> parts;
	size_t start = 0;

	while (start <= path.size())
	{
		const size_t slash = path.find('/', start);
		if (slash == std::string::npos)
		{
			parts.push_back(path.substr(start));
			break;
		}

		parts.push_back(path.substr(start, slash - start));
		start = slash + 1;
	}

	return parts;
}

std::string MakeScopedTicket(const std::string &dataset, int segid)
{
	return "af-v1/dataset/" + dataset + "/segment/" + std::to_string(segid);
}

std::string MakeWrittenTicket(const std::string &operation_id, int segid)
{
	return "af-v1/written/" + operation_id + "/segment/" +
		   std::to_string(segid);
}

bool ParseScopedTicket(const std::string &ticket, std::string *dataset,
					   int *segid)
{
	const std::vector<std::string> parts = SplitPath(ticket);

	if (parts.size() != 5 || parts[0] != "af-v1" || parts[1] != "dataset" ||
		parts[3] != "segment" || parts[2].empty())
	{
		return false;
	}

	if (!ParseSegmentTicket("smoke-seg-" + parts[4], segid))
	{
		return false;
	}

	*dataset = parts[2];
	return true;
}

bool ParseWrittenTicket(const std::string &ticket, std::string *operation_id,
						int *segid)
{
	const std::vector<std::string> parts = SplitPath(ticket);

	if (parts.size() != 5 || parts[0] != "af-v1" || parts[1] != "written" ||
		parts[3] != "segment" || parts[2].empty())
	{
		return false;
	}

	if (!ParseSegmentTicket("smoke-seg-" + parts[4], segid))
	{
		return false;
	}

	*operation_id = parts[2];
	return true;
}

bool ResolveWriteDescriptor(const arrow::flight::FlightDescriptor &request,
							std::string *dataset, std::string *operation_id,
							int *segid)
{
	if (request.type != arrow::flight::FlightDescriptor::PATH ||
		request.path.size() != 6 || request.path[0] != "af-v1" ||
		request.path[1] != "write" || request.path[2].empty() ||
		request.path[3].empty() || request.path[4] != "segment")
	{
		return false;
	}

	if (!ParseSegmentTicket("smoke-seg-" + request.path[5], segid))
	{
		return false;
	}

	*dataset = request.path[2];
	*operation_id = request.path[3];
	return true;
}

std::map<std::string, std::string>
ParseActionBody(const std::shared_ptr<arrow::Buffer> &body)
{
	std::map<std::string, std::string> values;

	if (body == nullptr || body->size() == 0)
	{
		return values;
	}

	const std::string text(reinterpret_cast<const char *>(body->data()),
						   static_cast<size_t>(body->size()));
	size_t start = 0;
	while (start <= text.size())
	{
		const size_t end = text.find('\n', start);
		const std::string line = text.substr(
			start, end == std::string::npos ? std::string::npos : end - start);
		const size_t eq = line.find('=');

		if (eq != std::string::npos && eq > 0)
		{
			values[line.substr(0, eq)] = line.substr(eq + 1);
		}

		if (end == std::string::npos)
		{
			break;
		}
		start = end + 1;
	}

	return values;
}

int64_t ParseInt64(const std::string &value, int64_t default_value)
{
	char *end = nullptr;
	const long long parsed = std::strtoll(value.c_str(), &end, 10);

	if (end == value.c_str() || *end != '\0')
	{
		return default_value;
	}

	return static_cast<int64_t>(parsed);
}

int64_t MetadataInt64(const std::map<std::string, std::string> &values,
					  const std::string &key, int64_t default_value)
{
	const auto it = values.find(key);

	if (it == values.end())
	{
		return default_value;
	}

	return ParseInt64(it->second, default_value);
}

std::string MetadataString(const std::map<std::string, std::string> &values,
						   const std::string &key)
{
	const auto it = values.find(key);

	return it == values.end() ? "" : it->second;
}

std::string SchemaMetadataString(const std::shared_ptr<arrow::Schema> &schema,
								 const std::string &key)
{
	if (schema == nullptr || schema->metadata() == nullptr ||
		!schema->metadata()->Contains(key))
	{
		return "";
	}

	arrow::Result<std::string> result = schema->metadata()->Get(key);
	return result.ok() ? result.ValueOrDie() : "";
}

int64_t SchemaMetadataInt64(const std::shared_ptr<arrow::Schema> &schema,
							const std::string &key, int64_t default_value)
{
	const std::string value = SchemaMetadataString(schema, key);

	return value.empty() ? default_value : ParseInt64(value, default_value);
}

struct WriteSegmentState
{
	int64_t rows = 0;
	int64_t batches = 0;
	bool stream_done = false;
	bool finalized = false;
	std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
};

struct WriteOperationState
{
	std::string dataset;
	int64_t expected_segments = 0;
	bool aborted = false;
	std::map<int, WriteSegmentState> segments;
};

bool IsKnownDataset(const std::string &dataset)
{
	return dataset == "smoke" || dataset == "nulls" ||
		   dataset == "multibatch" || dataset == "types" ||
		   dataset == "timestamps" || dataset == "bad_timezone" ||
		   dataset == "bad_date64_alignment" ||
		   dataset == "bad_date32_range" ||
		   dataset == "bad_time64_range" ||
		   dataset == "bad_timestamp_overflow" ||
		   dataset == "wide" || dataset == "bench" || dataset == "bench_fixed";
}

bool ParseSegmentInfoDescriptor(const std::string &descriptor, int *segid)
{
	const std::string prefix = "smoke-info-seg-";

	if (descriptor.rfind(prefix, 0) != 0)
	{
		return false;
	}

	return ParseSegmentTicket("smoke-seg-" + descriptor.substr(prefix.size()),
							  segid);
}

bool ResolveInfoDescriptor(const arrow::flight::FlightDescriptor &request,
						   std::string *ticket)
{
	int segid = -1;

	if (request.type != arrow::flight::FlightDescriptor::PATH ||
		request.path.size() != 1)
	{
		return false;
	}

	if (request.path[0] == "smoke-info")
	{
		*ticket = "smoke";
		return true;
	}

	if (ParseSegmentInfoDescriptor(request.path[0], &segid))
	{
		*ticket = "smoke-seg-" + std::to_string(segid);
		return true;
	}

	if (request.path[0] == "schema-mismatch-info")
	{
		*ticket = "schema-mismatch";
		return true;
	}

	return false;
}

bool IsMultiSegmentInfoDescriptor(
	const arrow::flight::FlightDescriptor &request)
{
	return request.type == arrow::flight::FlightDescriptor::PATH &&
		   request.path.size() == 1 &&
		   request.path[0] == "smoke-info-segments";
}

bool ResolveDatasetSegmentsDescriptor(
	const arrow::flight::FlightDescriptor &request, std::string *dataset,
	int *segments)
{
	std::vector<std::string> parts;

	if (request.type != arrow::flight::FlightDescriptor::PATH ||
		request.path.size() != 1)
	{
		return false;
	}

	parts = SplitPath(request.path[0]);
	if (parts.size() != 4 || parts[0] != "dataset" || parts[2] != "segments" ||
		parts[1].empty())
	{
		return false;
	}

	if (!ParseSegmentTicket("smoke-seg-" + parts[3], segments) ||
		*segments <= 0)
	{
		return false;
	}

	*dataset = parts[1];
	return true;
}

bool ResolveWrittenSegmentsDescriptor(
	const arrow::flight::FlightDescriptor &request, std::string *operation_id,
	int *segments)
{
	std::vector<std::string> parts;

	if (request.type != arrow::flight::FlightDescriptor::PATH ||
		request.path.size() != 1)
	{
		return false;
	}

	parts = SplitPath(request.path[0]);
	if (parts.size() != 4 || parts[0] != "written" || parts[2] != "segments" ||
		parts[1].empty())
	{
		return false;
	}

	if (!ParseSegmentTicket("smoke-seg-" + parts[3], segments) ||
		*segments <= 0)
	{
		return false;
	}

	*operation_id = parts[1];
	return true;
}

arrow::flight::FlightEndpoint MakeFlightEndpoint(const std::string &ticket)
{
	arrow::flight::FlightEndpoint endpoint;
	endpoint.ticket = arrow::flight::Ticket(ticket);
	return endpoint;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeSmokeBatch(int rows, int id_offset, const std::string &label_prefix)
{
	arrow::Int32Builder id_builder;
	arrow::StringBuilder label_builder;
	arrow::BooleanBuilder active_builder;
	arrow::DoubleBuilder amount_builder;
	arrow::Date32Builder date_builder;
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());

	constexpr int32_t kPostgresEpochUnixDays = 10957;
	constexpr int64_t kPostgresEpochUnixUsecs = 946684800000000LL;

	for (int32_t row = 1; row <= rows; row++)
	{
		ARROW_RETURN_NOT_OK(id_builder.Append(id_offset + row));
		ARROW_RETURN_NOT_OK(
			label_builder.Append(label_prefix + std::to_string(row)));
		ARROW_RETURN_NOT_OK(active_builder.Append((row % 2) == 1));
		ARROW_RETURN_NOT_OK(
			amount_builder.Append(static_cast<double>(row) + 0.5));
		ARROW_RETURN_NOT_OK(date_builder.Append(kPostgresEpochUnixDays + row));
		ARROW_RETURN_NOT_OK(
			ts_builder.Append(kPostgresEpochUnixUsecs + row * 1000000LL));
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays(6);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&label_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&active_builder, &arrays[2]));
	ARROW_RETURN_NOT_OK(FinishArray(&amount_builder, &arrays[3]));
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[4]));
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[5]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("label", arrow::utf8()),
		arrow::field("active", arrow::boolean()),
		arrow::field("amount", arrow::float64()),
		arrow::field("d", arrow::date32()),
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
	});

	return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeSmokeBatch()
{
	return MakeSmokeBatch(GetSmokeRows(), 0, "arrowflight-row-");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeSegmentSmokeBatch(int segid)
{
	return MakeSmokeBatch(
		2, segid * 100, "arrowflight-seg-" + std::to_string(segid) + "-row-");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeSchemaMismatchBatch()
{
	arrow::Int64Builder id_builder;
	arrow::StringBuilder label_builder;

	ARROW_RETURN_NOT_OK(id_builder.Append(1));
	ARROW_RETURN_NOT_OK(label_builder.Append("bad-schema"));

	std::vector<std::shared_ptr<arrow::Array>> arrays(2);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&label_builder, &arrays[1]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int64()),
		arrow::field("label", arrow::utf8()),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeNullsBatch(int segid)
{
	arrow::Int32Builder id_builder;
	arrow::StringBuilder label_builder;
	arrow::BooleanBuilder active_builder;
	arrow::DoubleBuilder amount_builder;
	arrow::Date32Builder date_builder;
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());

	constexpr int32_t kPostgresEpochUnixDays = 10957;
	constexpr int64_t kPostgresEpochUnixUsecs = 946684800000000LL;

	for (int32_t row = 1; row <= 6; row++)
	{
		if ((row % 2) == 0)
		{
			ARROW_RETURN_NOT_OK(id_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(id_builder.Append(segid * 100 + row));
		}

		if (row == 3 || row == 6)
		{
			ARROW_RETURN_NOT_OK(label_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(
				label_builder.Append("nulls-seg-" + std::to_string(segid) +
									 "-row-" + std::to_string(row)));
		}

		if (row == 2 || row == 5)
		{
			ARROW_RETURN_NOT_OK(active_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(active_builder.Append((row % 2) == 1));
		}

		if (row == 4 || row == 6)
		{
			ARROW_RETURN_NOT_OK(amount_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(
				amount_builder.Append(static_cast<double>(row) + 0.5));
		}

		if (row == 1 || row == 6)
		{
			ARROW_RETURN_NOT_OK(date_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(
				date_builder.Append(kPostgresEpochUnixDays + row));
		}

		if (row == 5 || row == 6)
		{
			ARROW_RETURN_NOT_OK(ts_builder.AppendNull());
		}
		else
		{
			ARROW_RETURN_NOT_OK(
				ts_builder.Append(kPostgresEpochUnixUsecs + row * 1000000LL));
		}
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays(6);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&label_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&active_builder, &arrays[2]));
	ARROW_RETURN_NOT_OK(FinishArray(&amount_builder, &arrays[3]));
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[4]));
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[5]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("label", arrow::utf8()),
		arrow::field("active", arrow::boolean()),
		arrow::field("amount", arrow::float64()),
		arrow::field("d", arrow::date32()),
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
	});

	return arrow::RecordBatch::Make(schema, 6, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeTypesBatch(int segid)
{
	arrow::Int32Builder id_builder;
	arrow::StringBuilder json_builder;
	arrow::StringBuilder jsonb_builder;
	arrow::FixedSizeBinaryBuilder uuid_builder(arrow::fixed_size_binary(16));
	arrow::StringBuilder int_array_builder;
	arrow::StringBuilder text_array_builder;
	arrow::Dictionary32Builder<arrow::StringType> enum_builder(
		arrow::utf8(), arrow::default_memory_pool());
	arrow::MonthDayNanoIntervalBuilder interval_builder;
	arrow::StringBuilder varchar_n_builder;
	arrow::StringBuilder varchar_builder;
	const auto uuid1 = UuidBytes(1);
	const auto uuid2 = UuidBytes(2);

	ARROW_RETURN_NOT_OK(id_builder.Append(segid * 100 + 1));
	ARROW_RETURN_NOT_OK(json_builder.Append(
		"{\"seg\":" + std::to_string(segid) + ",\"row\":1}"));
	ARROW_RETURN_NOT_OK(
		jsonb_builder.Append("{\"kind\":\"jsonb\",\"row\":1}"));
	ARROW_RETURN_NOT_OK(uuid_builder.Append(uuid1.data()));
	ARROW_RETURN_NOT_OK(int_array_builder.Append("{1,2,3}"));
	ARROW_RETURN_NOT_OK(text_array_builder.Append("{alpha,beta}"));
	ARROW_RETURN_NOT_OK(enum_builder.Append("queued"));
	ARROW_RETURN_NOT_OK(
		interval_builder.Append(arrow::MonthDayNanoIntervalType::MonthDayNanos{
			0, 1, (2LL * 60 * 60 + 3LL * 60 + 4LL) * 1000000000LL}));
	ARROW_RETURN_NOT_OK(varchar_n_builder.Append("short"));
	ARROW_RETURN_NOT_OK(varchar_builder.Append("varchar-open"));

	ARROW_RETURN_NOT_OK(id_builder.Append(segid * 100 + 2));
	ARROW_RETURN_NOT_OK(json_builder.Append(
		"{\"seg\":" + std::to_string(segid) + ",\"row\":2}"));
	ARROW_RETURN_NOT_OK(
		jsonb_builder.Append("{\"kind\":\"jsonb\",\"row\":2}"));
	ARROW_RETURN_NOT_OK(uuid_builder.Append(uuid2.data()));
	ARROW_RETURN_NOT_OK(int_array_builder.Append("{4,5}"));
	ARROW_RETURN_NOT_OK(text_array_builder.Append("{gamma,delta}"));
	ARROW_RETURN_NOT_OK(enum_builder.Append("done"));
	ARROW_RETURN_NOT_OK(
		interval_builder.Append(arrow::MonthDayNanoIntervalType::MonthDayNanos{
			0, 0, (3LL * 60 * 60 + 5LL * 60) * 1000000000LL}));
	ARROW_RETURN_NOT_OK(varchar_n_builder.Append("tiny"));
	ARROW_RETURN_NOT_OK(varchar_builder.Append("varchar-unbounded"));

	std::vector<std::shared_ptr<arrow::Array>> arrays(10);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&json_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&jsonb_builder, &arrays[2]));
	ARROW_RETURN_NOT_OK(FinishArray(&uuid_builder, &arrays[3]));
	ARROW_RETURN_NOT_OK(FinishArray(&int_array_builder, &arrays[4]));
	ARROW_RETURN_NOT_OK(FinishArray(&text_array_builder, &arrays[5]));
	ARROW_RETURN_NOT_OK(FinishArray(&enum_builder, &arrays[6]));
	ARROW_RETURN_NOT_OK(FinishArray(&interval_builder, &arrays[7]));
	ARROW_RETURN_NOT_OK(FinishArray(&varchar_n_builder, &arrays[8]));
	ARROW_RETURN_NOT_OK(FinishArray(&varchar_builder, &arrays[9]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("js", arrow::utf8()),
		arrow::field("jb", arrow::utf8()),
		arrow::field("uid", arrow::fixed_size_binary(16)),
		arrow::field("nums", arrow::utf8()),
		arrow::field("tags", arrow::utf8()),
		arrow::field("state",
					 arrow::dictionary(arrow::int32(), arrow::utf8())),
		arrow::field("duration", arrow::month_day_nano_interval()),
		arrow::field("limited", arrow::utf8()),
		arrow::field("unlimited", arrow::utf8()),
	});

	return arrow::RecordBatch::Make(schema, 2, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeMultiBatchPart(int start_id, int rows)
{
	return MakeSmokeBatch(rows, start_id - 1, "multibatch-row-");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeTimestampsBatch()
{
	arrow::Int32Builder id_builder;
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());
	arrow::TimestampBuilder tstz_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
		arrow::default_memory_pool());

	constexpr int64_t kTs1 = 946684801000000LL;
	constexpr int64_t kTs2 = 946782245000000LL;

	ARROW_RETURN_NOT_OK(id_builder.Append(1));
	ARROW_RETURN_NOT_OK(ts_builder.Append(kTs1));
	ARROW_RETURN_NOT_OK(tstz_builder.Append(kTs1));
	ARROW_RETURN_NOT_OK(id_builder.Append(2));
	ARROW_RETURN_NOT_OK(ts_builder.Append(kTs2));
	ARROW_RETURN_NOT_OK(tstz_builder.Append(kTs2));

	std::vector<std::shared_ptr<arrow::Array>> arrays(3);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&tstz_builder, &arrays[2]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
		arrow::field("tstz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
	});

	return arrow::RecordBatch::Make(schema, 2, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBadTimezoneBatch()
{
	arrow::Int32Builder id_builder;
	arrow::TimestampBuilder tstz_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO, "Europe/Moscow"),
		arrow::default_memory_pool());

	ARROW_RETURN_NOT_OK(id_builder.Append(1));
	ARROW_RETURN_NOT_OK(tstz_builder.Append(946684801000000LL));

	std::vector<std::shared_ptr<arrow::Array>> arrays(2);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&tstz_builder, &arrays[1]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field(
			"tstz", arrow::timestamp(arrow::TimeUnit::MICRO, "Europe/Moscow")),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBadDate64AlignmentBatch()
{
	arrow::Date64Builder date_builder;

	ARROW_RETURN_NOT_OK(date_builder.Append(1));

	std::vector<std::shared_ptr<arrow::Array>> arrays(1);
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[0]));

	auto schema = arrow::schema({
		arrow::field("d", arrow::date64()),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBadDate32RangeBatch()
{
	arrow::Date32Builder date_builder;

	ARROW_RETURN_NOT_OK(
		date_builder.Append(std::numeric_limits<int32_t>::max()));

	std::vector<std::shared_ptr<arrow::Array>> arrays(1);
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[0]));

	auto schema = arrow::schema({
		arrow::field("d", arrow::date32()),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBadTime64RangeBatch()
{
	arrow::Time64Builder time_builder(
		arrow::time64(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());

	ARROW_RETURN_NOT_OK(time_builder.Append(86400000001LL));

	std::vector<std::shared_ptr<arrow::Array>> arrays(1);
	ARROW_RETURN_NOT_OK(FinishArray(&time_builder, &arrays[0]));

	auto schema = arrow::schema({
		arrow::field("t", arrow::time64(arrow::TimeUnit::MICRO)),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBadTimestampOverflowBatch()
{
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::SECOND),
		arrow::default_memory_pool());

	ARROW_RETURN_NOT_OK(
		ts_builder.Append(std::numeric_limits<int64_t>::max()));

	std::vector<std::shared_ptr<arrow::Array>> arrays(1);
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[0]));

	auto schema = arrow::schema({
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::SECOND)),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeWideBatch()
{
	arrow::Int32Builder id_builder;
	arrow::StringBuilder label_builder;

	ARROW_RETURN_NOT_OK(id_builder.Append(1));
	ARROW_RETURN_NOT_OK(label_builder.Append(std::string(512, 'x')));

	std::vector<std::shared_ptr<arrow::Array>> arrays(2);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&label_builder, &arrays[1]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("label", arrow::utf8()),
	});

	return arrow::RecordBatch::Make(schema, 1, std::move(arrays));
}

std::string MakeBenchLabel(int segid, int64_t row)
{
	std::string label =
		"bench-seg-" + std::to_string(segid) + "-row-" + std::to_string(row);
	const int width = GetBenchLabelWidth();

	if (static_cast<int>(label.size()) < width)
	{
		label.append(width - label.size(), 'x');
	}

	return label;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBenchBatch(int segid, int64_t start_row, int rows, int rows_per_segment)
{
	arrow::Int32Builder id_builder;
	arrow::Int32Builder segid_builder;
	arrow::StringBuilder label_builder;
	arrow::BooleanBuilder active_builder;
	arrow::DoubleBuilder amount_builder;
	arrow::Date32Builder date_builder;
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());

	constexpr int32_t kPostgresEpochUnixDays = 10957;
	constexpr int64_t kPostgresEpochUnixUsecs = 946684800000000LL;

	for (int i = 0; i < rows; i++)
	{
		const int64_t row = start_row + i;
		const int64_t id =
			static_cast<int64_t>(segid) * rows_per_segment + row;
		const int32_t day = static_cast<int32_t>((row % 365) + 1);

		if (id > INT_MAX)
		{
			return arrow::Status::Invalid("benchmark id exceeds int32 range");
		}

		ARROW_RETURN_NOT_OK(id_builder.Append(static_cast<int32_t>(id)));
		ARROW_RETURN_NOT_OK(segid_builder.Append(segid));
		ARROW_RETURN_NOT_OK(label_builder.Append(MakeBenchLabel(segid, row)));
		ARROW_RETURN_NOT_OK(active_builder.Append((row % 2) == 1));
		ARROW_RETURN_NOT_OK(
			amount_builder.Append(static_cast<double>(row) + 0.5));
		ARROW_RETURN_NOT_OK(date_builder.Append(kPostgresEpochUnixDays + day));
		ARROW_RETURN_NOT_OK(
			ts_builder.Append(kPostgresEpochUnixUsecs + row * 1000000LL));
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays(7);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&segid_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&label_builder, &arrays[2]));
	ARROW_RETURN_NOT_OK(FinishArray(&active_builder, &arrays[3]));
	ARROW_RETURN_NOT_OK(FinishArray(&amount_builder, &arrays[4]));
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[5]));
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[6]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("segid", arrow::int32()),
		arrow::field("label", arrow::utf8()),
		arrow::field("active", arrow::boolean()),
		arrow::field("amount", arrow::float64()),
		arrow::field("d", arrow::date32()),
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
	});

	return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
MakeBenchFixedBatch(int segid, int64_t start_row, int rows,
					int rows_per_segment)
{
	arrow::Int32Builder id_builder;
	arrow::Int32Builder segid_builder;
	arrow::BooleanBuilder active_builder;
	arrow::DoubleBuilder amount_builder;
	arrow::Date32Builder date_builder;
	arrow::TimestampBuilder ts_builder(
		arrow::timestamp(arrow::TimeUnit::MICRO),
		arrow::default_memory_pool());

	constexpr int32_t kPostgresEpochUnixDays = 10957;
	constexpr int64_t kPostgresEpochUnixUsecs = 946684800000000LL;

	for (int i = 0; i < rows; i++)
	{
		const int64_t row = start_row + i;
		const int64_t id =
			static_cast<int64_t>(segid) * rows_per_segment + row;
		const int32_t day = static_cast<int32_t>((row % 365) + 1);

		if (id > INT_MAX)
		{
			return arrow::Status::Invalid("benchmark id exceeds int32 range");
		}

		ARROW_RETURN_NOT_OK(id_builder.Append(static_cast<int32_t>(id)));
		ARROW_RETURN_NOT_OK(segid_builder.Append(segid));
		ARROW_RETURN_NOT_OK(active_builder.Append((row % 2) == 1));
		ARROW_RETURN_NOT_OK(
			amount_builder.Append(static_cast<double>(row) + 0.5));
		ARROW_RETURN_NOT_OK(date_builder.Append(kPostgresEpochUnixDays + day));
		ARROW_RETURN_NOT_OK(
			ts_builder.Append(kPostgresEpochUnixUsecs + row * 1000000LL));
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays(6);
	ARROW_RETURN_NOT_OK(FinishArray(&id_builder, &arrays[0]));
	ARROW_RETURN_NOT_OK(FinishArray(&segid_builder, &arrays[1]));
	ARROW_RETURN_NOT_OK(FinishArray(&active_builder, &arrays[2]));
	ARROW_RETURN_NOT_OK(FinishArray(&amount_builder, &arrays[3]));
	ARROW_RETURN_NOT_OK(FinishArray(&date_builder, &arrays[4]));
	ARROW_RETURN_NOT_OK(FinishArray(&ts_builder, &arrays[5]));

	auto schema = arrow::schema({
		arrow::field("id", arrow::int32()),
		arrow::field("segid", arrow::int32()),
		arrow::field("active", arrow::boolean()),
		arrow::field("amount", arrow::float64()),
		arrow::field("d", arrow::date32()),
		arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
	});

	return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
MakeBenchBatches(int segid, bool fixed_width)
{
	const int rows_per_segment = GetBenchRowsPerSegment();
	const int batch_rows = GetBenchBatchRows();
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	int64_t next_row = 1;
	int remaining = rows_per_segment;

	while (remaining > 0)
	{
		const int rows = remaining < batch_rows ? remaining : batch_rows;

		std::shared_ptr<arrow::RecordBatch> batch;
		if (fixed_width)
		{
			ARROW_ASSIGN_OR_RAISE(
				batch,
				MakeBenchFixedBatch(segid, next_row, rows, rows_per_segment));
		}
		else
		{
			ARROW_ASSIGN_OR_RAISE(batch, MakeBenchBatch(segid, next_row, rows,
														rows_per_segment));
		}
		batches.push_back(batch);

		next_row += rows;
		remaining -= rows;
	}

	return batches;
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
MakeDatasetBatches(const std::string &dataset, int segid)
{
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

	if (dataset == "smoke")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeSegmentSmokeBatch(segid));
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "nulls")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeNullsBatch(segid));
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "multibatch")
	{
		ARROW_ASSIGN_OR_RAISE(auto first, MakeMultiBatchPart(1, 2));
		ARROW_ASSIGN_OR_RAISE(auto second, MakeMultiBatchPart(3, 2));
		ARROW_ASSIGN_OR_RAISE(auto third, MakeMultiBatchPart(5, 2));
		batches.push_back(first);
		batches.push_back(second);
		batches.push_back(third);
		return batches;
	}

	if (dataset == "types")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeTypesBatch(segid));
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "timestamps")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeTimestampsBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bad_timezone")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeBadTimezoneBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bad_date64_alignment")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeBadDate64AlignmentBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bad_date32_range")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeBadDate32RangeBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bad_time64_range")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeBadTime64RangeBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bad_timestamp_overflow")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeBadTimestampOverflowBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "wide")
	{
		ARROW_ASSIGN_OR_RAISE(auto batch, MakeWideBatch());
		batches.push_back(batch);
		return batches;
	}

	if (dataset == "bench")
	{
		return MakeBenchBatches(segid, false);
	}

	if (dataset == "bench_fixed")
	{
		return MakeBenchBatches(segid, true);
	}

	return arrow::Status::KeyError("unknown smoke dataset: ", dataset);
}

int64_t
CountRows(const std::vector<std::shared_ptr<arrow::RecordBatch>> &batches)
{
	int64_t rows = 0;

	for (const auto &batch : batches)
	{
		rows += batch->num_rows();
	}

	return rows;
}

std::string BenchIpcFileName(const std::string &dataset, int segid)
{
	return dataset + "_" + std::to_string(segid) + ".arrow";
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
ReadBenchIpcBatches(const std::string &root, const std::string &dataset,
					int segid)
{
	const std::filesystem::path path =
		std::filesystem::path(root) / BenchIpcFileName(dataset, segid);
	const uint64_t start_us = NowUsec();

	ARROW_ASSIGN_OR_RAISE(auto input,
						  arrow::io::ReadableFile::Open(path.string()));
	ARROW_ASSIGN_OR_RAISE(auto reader,
						  arrow::ipc::RecordBatchStreamReader::Open(input));

	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	for (;;)
	{
		std::shared_ptr<arrow::RecordBatch> batch;
		ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
		if (batch == nullptr)
		{
			break;
		}
		batches.push_back(batch);
	}

	std::cerr << "arrowflightd_profile event=ipc_read dataset=" << dataset
			  << " segment=" << segid
			  << " prebuilt=0 source=ipc rows=" << CountRows(batches)
			  << " build_us=" << (NowUsec() - start_us)
			  << " file=" << path.string() << std::endl;
	return batches;
}

arrow::Status WriteBenchIpcDataset(const std::filesystem::path &root,
								   const std::string &dataset,
								   bool fixed_width)
{
	const int segments =
		GetEnvInt("ARROWFLIGHT_BENCH_SEGMENTS", 2, 1, INT_MAX / 4);

	for (int segid = 0; segid < segments; segid++)
	{
		ARROW_ASSIGN_OR_RAISE(auto batches,
							  MakeBenchBatches(segid, fixed_width));
		if (batches.empty())
		{
			return arrow::Status::Invalid(
				"benchmark IPC dataset has no batches");
		}

		const std::filesystem::path path =
			root / BenchIpcFileName(dataset, segid);
		ARROW_ASSIGN_OR_RAISE(
			auto sink, arrow::io::FileOutputStream::Open(path.string()));
		ARROW_ASSIGN_OR_RAISE(auto writer,
							  arrow::ipc::MakeStreamWriter(
								  sink.get(), batches.front()->schema()));

		for (const auto &batch : batches)
		{
			ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
		}

		ARROW_RETURN_NOT_OK(writer->Close());
		ARROW_RETURN_NOT_OK(sink->Close());

		std::cerr << "arrowflightd_profile event=ipc_write dataset=" << dataset
				  << " segment=" << segid
				  << " prebuilt=0 source=ipc rows=" << CountRows(batches)
				  << " build_us=0 file=" << path.string() << std::endl;
	}

	return arrow::Status::OK();
}

arrow::Status WriteBenchIpcFiles(const std::string &root)
{
	std::filesystem::create_directories(root);

	ARROW_RETURN_NOT_OK(WriteBenchIpcDataset(root, "bench", false));
	ARROW_RETURN_NOT_OK(WriteBenchIpcDataset(root, "bench_fixed", true));
	return arrow::Status::OK();
}

class SmokeFlightServer : public arrow::flight::FlightServerBase
{
  public:
	SmokeFlightServer()
		: source_mode_(GetEnvString(
			  "ARROWFLIGHT_BENCH_SOURCE",
			  GetEnvBool("ARROWFLIGHT_BENCH_PREBUILD", false) ? "prebuilt"
															  : "generated")),
		  ipc_dir_(GetEnvString("ARROWFLIGHT_BENCH_IPC_DIR", ""))
	{
	}

	arrow::Status
	GetFlightInfo(const arrow::flight::ServerCallContext & /*context*/,
				  const arrow::flight::FlightDescriptor &request,
				  std::unique_ptr<arrow::flight::FlightInfo> *info) override
	{
		std::string ticket;
		std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
		std::vector<arrow::flight::FlightEndpoint> endpoints;
		int64_t total_records = -1;
		std::string dataset;
		std::string operation_id;
		int segments = 0;

		if (ResolveDatasetSegmentsDescriptor(request, &dataset, &segments))
		{
			if (!IsKnownDataset(dataset))
			{
				return arrow::Status::KeyError("unknown smoke dataset: ",
											   dataset);
			}

			for (int segid = 0; segid < segments; segid++)
			{
				endpoints.push_back(
					MakeFlightEndpoint(MakeScopedTicket(dataset, segid)));
			}

			ARROW_ASSIGN_OR_RAISE(batches, GetDatasetBatches(dataset, 0));
			total_records =
				static_cast<int64_t>(segments) * CountRows(batches);
		}
		else if (ResolveWrittenSegmentsDescriptor(request, &operation_id,
												  &segments))
		{
			total_records = 0;
			for (int segid = 0; segid < segments; segid++)
			{
				endpoints.push_back(MakeFlightEndpoint(
					MakeWrittenTicket(operation_id, segid)));
				ARROW_ASSIGN_OR_RAISE(auto segment_batches,
									  GetWrittenBatches(operation_id, segid));
				if (segid == 0)
				{
					batches = segment_batches;
				}
				total_records += CountRows(segment_batches);
			}
		}
		else if (IsMultiSegmentInfoDescriptor(request))
		{
			const int smoke_segments = GetSmokeSegments();
			for (int segid = 0; segid < smoke_segments; segid++)
			{
				endpoints.push_back(
					MakeFlightEndpoint("smoke-seg-" + std::to_string(segid)));
			}

			ARROW_ASSIGN_OR_RAISE(batches, GetDatasetBatches("smoke", 0));
			total_records =
				static_cast<int64_t>(smoke_segments) * CountRows(batches);
		}
		else if (!ResolveInfoDescriptor(request, &ticket))
		{
			return arrow::Status::KeyError("unknown smoke descriptor: ",
										   request.ToString());
		}
		else
		{
			int segid = -1;
			if (ticket == "smoke")
			{
				ARROW_ASSIGN_OR_RAISE(auto batch, MakeSmokeBatch());
				batches.push_back(batch);
			}
			else if (ParseSegmentTicket(ticket, &segid))
			{
				ARROW_ASSIGN_OR_RAISE(batches,
									  GetDatasetBatches("smoke", segid));
			}
			else if (ticket == "schema-mismatch")
			{
				ARROW_ASSIGN_OR_RAISE(auto batch, MakeSchemaMismatchBatch());
				batches.push_back(batch);
			}
			else
			{
				return arrow::Status::KeyError("unknown smoke ticket: ",
											   ticket);
			}

			endpoints.push_back(MakeFlightEndpoint(ticket));
			total_records = CountRows(batches);
		}

		ARROW_ASSIGN_OR_RAISE(
			arrow::flight::FlightInfo flight_info,
			arrow::flight::FlightInfo::Make(batches.front()->schema(), request,
											endpoints, total_records, -1));

		*info = std::make_unique<arrow::flight::FlightInfo>(
			std::move(flight_info));
		return arrow::Status::OK();
	}

	arrow::Status
	DoGet(const arrow::flight::ServerCallContext & /*context*/,
		  const arrow::flight::Ticket &request,
		  std::unique_ptr<arrow::flight::FlightDataStream> *stream) override
	{
		int segid = -1;
		std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
		std::string dataset;
		std::string operation_id;

		if (request.ticket == "smoke")
		{
			ARROW_ASSIGN_OR_RAISE(auto batch, MakeSmokeBatch());
			batches.push_back(batch);
		}
		else if (ParseSegmentTicket(request.ticket, &segid))
		{
			ARROW_ASSIGN_OR_RAISE(batches, GetDatasetBatches("smoke", segid));
		}
		else if (ParseScopedTicket(request.ticket, &dataset, &segid))
		{
			if (!IsKnownDataset(dataset))
			{
				return arrow::Status::KeyError(
					"unknown dataset in smoke ticket: ", dataset);
			}
			ARROW_ASSIGN_OR_RAISE(batches, GetDatasetBatches(dataset, segid));
		}
		else if (ParseWrittenTicket(request.ticket, &operation_id, &segid))
		{
			ARROW_ASSIGN_OR_RAISE(batches,
								  GetWrittenBatches(operation_id, segid));
		}
		else if (request.ticket == "schema-mismatch")
		{
			ARROW_ASSIGN_OR_RAISE(auto batch, MakeSchemaMismatchBatch());
			batches.push_back(batch);
		}
		else
		{
			return arrow::Status::KeyError("unknown smoke ticket: ",
										   request.ticket);
		}

		ARROW_ASSIGN_OR_RAISE(auto reader,
							  arrow::RecordBatchReader::Make(
								  batches, batches.front()->schema()));

		*stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
		return arrow::Status::OK();
	}

	arrow::Status
	DoPut(const arrow::flight::ServerCallContext & /*context*/,
		  std::unique_ptr<arrow::flight::FlightMessageReader> reader,
		  std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override
	{
		std::string dataset;
		std::string operation_id;
		int segid = -1;
		int64_t rows = 0;
		int64_t batches = 0;
		std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
		uint64_t start_us = NowUsec();

		if (!ResolveWriteDescriptor(reader->descriptor(), &dataset,
									&operation_id, &segid))
		{
			return arrow::Status::Invalid("unknown write descriptor: ",
										  reader->descriptor().ToString());
		}

		ARROW_ASSIGN_OR_RAISE(auto schema, reader->GetSchema());
		if (schema == nullptr || schema->num_fields() == 0)
		{
			return arrow::Status::Invalid("write stream has empty schema");
		}
		const int64_t expected_segments =
			SchemaMetadataInt64(schema, "af.segment.count", 0);
		const int64_t fail_after_batches =
			dataset == "events_abort"
				? 1
				: SchemaMetadataInt64(schema, "af.static.fail_after_batches",
									  0);

		while (true)
		{
			ARROW_ASSIGN_OR_RAISE(arrow::flight::FlightStreamChunk chunk,
								  reader->Next());
			if (chunk.data == nullptr)
			{
				break;
			}

			rows += chunk.data->num_rows();
			batches++;
			record_batches.push_back(chunk.data);

			auto ack = arrow::Buffer::FromString(
				"af.ack.batch.index=" + std::to_string(batches - 1) + "\n" +
				"af.ack.batch.rows=" + std::to_string(chunk.data->num_rows()) +
				"\n");
			ARROW_RETURN_NOT_OK(writer->WriteMetadata(*ack));

			if (fail_after_batches > 0 && batches >= fail_after_batches)
			{
				std::ostringstream log;

				log << "arrowflightd_write_inject_failure"
					<< " dataset=" << dataset
					<< " operation_id=" << operation_id << " segment=" << segid
					<< " rows=" << rows << " batches=" << batches;
				LogLine(log.str());
				return arrow::Status::IOError(
					"injected write failure after batch ",
					std::to_string(batches));
			}
		}

		auto final_ack = arrow::Buffer::FromString(
			"af.ack.final=true\n"
			"af.ack.rows=" +
			std::to_string(rows) + "\n" +
			"af.ack.batches=" + std::to_string(batches) + "\n");
		ARROW_RETURN_NOT_OK(writer->WriteMetadata(*final_ack));

		RecordWriteStream(dataset, operation_id, segid, expected_segments,
						  rows, batches, record_batches);

		std::ostringstream log;

		log << "arrowflightd_write_profile"
			<< " dataset=" << dataset << " operation_id=" << operation_id
			<< " segment=" << segid << " rows=" << rows
			<< " batches=" << batches << " fields=" << schema->num_fields()
			<< " elapsed_us=" << (NowUsec() - start_us);
		LogLine(log.str());

		return arrow::Status::OK();
	}

	arrow::Status
	DoAction(const arrow::flight::ServerCallContext & /*context*/,
			 const arrow::flight::Action &action,
			 std::unique_ptr<arrow::flight::ResultStream> *result) override
	{
		if (action.type != "FinalizeOperation" &&
			action.type != "AbortOperation")
		{
			return arrow::Status::NotImplemented("unknown action: ",
												 action.type);
		}

		const std::map<std::string, std::string> values =
			ParseActionBody(action.body);
		const std::string operation_id =
			MetadataString(values, "af.operation.id");
		const std::string dataset = MetadataString(values, "af.dataset");
		const int segid =
			static_cast<int>(MetadataInt64(values, "af.segment.index", -1));
		const int64_t expected_segments =
			MetadataInt64(values, "af.segment.count", 0);
		const int64_t rows = MetadataInt64(values, "af.rows", 0);
		const int64_t batches = MetadataInt64(values, "af.batches", 0);
		int64_t finalized_segments = 0;
		std::string finalize_state = "partial";

		if (operation_id.empty() || dataset.empty() || segid < 0)
		{
			return arrow::Status::Invalid("invalid write action metadata");
		}

		{
			std::lock_guard<std::mutex> guard(write_mutex_);
			WriteOperationState &operation = write_operations_[operation_id];
			operation.dataset = dataset;
			if (expected_segments > operation.expected_segments)
			{
				operation.expected_segments = expected_segments;
			}

			WriteSegmentState &segment = operation.segments[segid];
			if (rows > 0)
			{
				segment.rows = rows;
			}
			if (batches > 0)
			{
				segment.batches = batches;
			}

			if (action.type == "AbortOperation")
			{
				operation.aborted = true;
				finalize_state = "aborted";
			}
			else
			{
				segment.finalized = true;
				for (const auto &entry : operation.segments)
				{
					if (entry.second.finalized)
					{
						finalized_segments++;
					}
				}

				if (!operation.aborted && operation.expected_segments > 0 &&
					finalized_segments >= operation.expected_segments)
				{
					finalize_state = "complete";
				}
			}
		}

		std::ostringstream log;

		log << "arrowflightd_write_action"
			<< " action=" << action.type << " dataset=" << dataset
			<< " operation_id=" << operation_id << " segment=" << segid
			<< " rows=" << rows << " batches=" << batches
			<< " expected_segments=" << expected_segments
			<< " finalized_segments=" << finalized_segments
			<< " finalize_state=" << finalize_state;
		LogLine(log.str());

		std::vector<arrow::flight::Result> results;
		results.emplace_back(arrow::Buffer::FromString("af.action.ok=true\n"
													   "af.action.type=" +
													   action.type +
													   "\n"
													   "af.operation.id=" +
													   operation_id +
													   "\n"
													   "af.finalize.state=" +
													   finalize_state + "\n"));
		*result = std::make_unique<arrow::flight::SimpleResultStream>(
			std::move(results));
		return arrow::Status::OK();
	}

	arrow::Status
	ListActions(const arrow::flight::ServerCallContext & /*context*/,
				std::vector<arrow::flight::ActionType> *actions) override
	{
		actions->emplace_back(
			"FinalizeOperation",
			"Mark a staged Arrow Flight write segment complete");
		actions->emplace_back(
			"AbortOperation",
			"Mark a staged Arrow Flight write operation aborted");
		return arrow::Status::OK();
	}

  private:
	void RecordWriteStream(
		const std::string &dataset, const std::string &operation_id, int segid,
		int64_t expected_segments, int64_t rows, int64_t batches,
		const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches)
	{
		std::lock_guard<std::mutex> guard(write_mutex_);
		WriteOperationState &operation = write_operations_[operation_id];

		operation.dataset = dataset;
		if (expected_segments > operation.expected_segments)
		{
			operation.expected_segments = expected_segments;
		}

		WriteSegmentState &segment = operation.segments[segid];
		segment.rows = rows;
		segment.batches = batches;
		segment.stream_done = true;
		segment.record_batches = record_batches;
	}

	arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
	GetWrittenBatches(const std::string &operation_id, int segid)
	{
		std::lock_guard<std::mutex> guard(write_mutex_);
		auto operation_it = write_operations_.find(operation_id);

		if (operation_it == write_operations_.end())
		{
			return arrow::Status::KeyError("unknown written operation: ",
										   operation_id);
		}

		if (operation_it->second.aborted)
		{
			return arrow::Status::Invalid("written operation is aborted: ",
										  operation_id);
		}

		auto segment_it = operation_it->second.segments.find(segid);
		if (segment_it == operation_it->second.segments.end() ||
			!segment_it->second.stream_done || !segment_it->second.finalized ||
			segment_it->second.record_batches.empty())
		{
			return arrow::Status::KeyError(
				"written segment is not complete: ", operation_id, "/", segid);
		}

		return segment_it->second.record_batches;
	}

	arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
	GetDatasetBatches(const std::string &dataset, int segid)
	{
		const bool is_bench = dataset == "bench" || dataset == "bench_fixed";
		const bool cacheable = source_mode_ == "prebuilt" && is_bench;

		if (source_mode_ == "ipc" && is_bench)
		{
			if (ipc_dir_.empty())
			{
				return arrow::Status::Invalid(
					"ARROWFLIGHT_BENCH_IPC_DIR is required for IPC benchmark "
					"source");
			}

			return ReadBenchIpcBatches(ipc_dir_, dataset, segid);
		}

		if (!cacheable)
		{
			const uint64_t start_us = NowUsec();
			ARROW_ASSIGN_OR_RAISE(auto batches,
								  MakeDatasetBatches(dataset, segid));
			std::cerr << "arrowflightd_profile event=build dataset=" << dataset
					  << " segment=" << segid
					  << " prebuilt=0 source=" << source_mode_
					  << " rows=" << CountRows(batches)
					  << " build_us=" << (NowUsec() - start_us) << std::endl;
			return batches;
		}

		const std::string key = dataset + ":" + std::to_string(segid);
		{
			std::lock_guard<std::mutex> guard(cache_mutex_);
			auto it = batch_cache_.find(key);
			if (it != batch_cache_.end())
			{
				std::cerr << "arrowflightd_profile event=cache_hit dataset="
						  << dataset << " segment=" << segid
						  << " prebuilt=1 source=" << source_mode_
						  << " rows=" << CountRows(it->second) << " build_us=0"
						  << std::endl;
				return it->second;
			}
		}

		const uint64_t start_us = NowUsec();
		ARROW_ASSIGN_OR_RAISE(auto batches,
							  MakeDatasetBatches(dataset, segid));
		const uint64_t build_us = NowUsec() - start_us;

		{
			std::lock_guard<std::mutex> guard(cache_mutex_);
			batch_cache_[key] = batches;
		}

		std::cerr << "arrowflightd_profile event=build dataset=" << dataset
				  << " segment=" << segid
				  << " prebuilt=1 source=" << source_mode_
				  << " rows=" << CountRows(batches) << " build_us=" << build_us
				  << std::endl;
		return batches;
	}

	std::string source_mode_;
	std::string ipc_dir_;
	std::mutex cache_mutex_;
	std::mutex write_mutex_;
	std::map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>>
		batch_cache_;
	std::map<std::string, WriteOperationState> write_operations_;
};

} /* namespace */

int main(int argc, char **argv)
{
	int port = 8815;
	const char *program = argv[0] == nullptr ? "arrowflightd" : argv[0];
	const char *slash = std::strrchr(program, '/');

	if (slash != nullptr)
	{
		program = slash + 1;
	}

	if (argc > 1 && std::strcmp(argv[1], "--write-bench-ipc") == 0)
	{
		if (argc <= 2)
		{
			std::cerr << "usage: " << program << " --write-bench-ipc <dir>"
					  << std::endl;
			return 1;
		}

		auto status = WriteBenchIpcFiles(argv[2]);
		if (!status.ok())
		{
			std::cerr << status.ToString() << std::endl;
			return 1;
		}

		std::cerr << program << " wrote benchmark IPC files to " << argv[2]
				  << std::endl;
		return 0;
	}

	if (argc > 1)
	{
		port = std::atoi(argv[1]);
	}

	auto location_result =
		arrow::flight::Location::ForGrpcTcp("0.0.0.0", port);
	if (!location_result.ok())
	{
		std::cerr << location_result.status().ToString() << std::endl;
		return 1;
	}

	SmokeFlightServer server;
	arrow::flight::FlightServerOptions options(*location_result);
	auto status = server.Init(options);
	if (!status.ok())
	{
		std::cerr << status.ToString() << std::endl;
		return 1;
	}

	std::cerr << program << " listening on port " << server.port()
			  << std::endl;

	status = server.Serve();
	if (!status.ok())
	{
		std::cerr << status.ToString() << std::endl;
		return 1;
	}

	return 0;
}
