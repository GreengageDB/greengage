/* Standalone tests of the production read transport, without a PG backend. */

#include "../src/flightsql_read_client.cpp"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_dict.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <atomic>
#include <iostream>
#include <thread>

static void
Require(bool condition, const char *message)
{
	if (!condition)
		throw std::runtime_error(message);
}

static std::string
Field(int number, const std::string& value)
{
	std::string result;
	google::protobuf::io::StringOutputStream output(&result);
	{
		google::protobuf::io::CodedOutputStream encoded(&output);

		encoded.WriteTag((number << 3) | 2);
		encoded.WriteVarint32(value.size());
		encoded.WriteRaw(value.data(), value.size());
	}
	return result;
}

class ReadTestServer : public FlightServerBase
{
public:
	std::shared_ptr<arrow::RecordBatch> batch;

	Status GetFlightInfo(const ServerCallContext&, const FlightDescriptor& descriptor,
						 std::unique_ptr<FlightInfo> *out) override
	{
		if (descriptor.cmd == "error")
			return Status::Invalid("discovery test error");
		ARROW_ASSIGN_OR_RAISE(auto info,
			FlightInfo::Make(*batch->schema(), descriptor,
				{FlightEndpoint{Ticket{"test"}, {}, std::nullopt, ""}},
				batch->num_rows() * 2, -1));
		*out = std::make_unique<FlightInfo>(std::move(info));
		return Status::OK();
	}

	Status DoGet(const ServerCallContext&, const Ticket& ticket,
				 std::unique_ptr<FlightDataStream> *out) override
	{
		if (ticket.ticket == "error")
			return Status::Invalid("stream test error");
		ARROW_ASSIGN_OR_RAISE(auto reader,
			arrow::RecordBatchReader::Make({batch, batch}, batch->schema()));
		*out = std::make_unique<RecordBatchStream>(reader);
		return Status::OK();
	}
};

static void
TestStream()
{
	ReadTestServer server;
	arrow::StringBuilder strings;
	arrow::StringDictionaryBuilder dictionary;
	std::string large(4096, 'x');

	for (int row = 0; row < 1000; row++)
	{
		Require((row % 3 ? strings.Append(large) : strings.AppendNull()).ok(), "append string");
		Require((row % 3 ? dictionary.Append("label") : dictionary.AppendNull()).ok(), "append dictionary");
	}
	auto text = strings.Finish().ValueOrDie();
	auto labels = dictionary.Finish().ValueOrDie();
	auto schema = arrow::schema({arrow::field("text", text->type()),
		arrow::field("labels", labels->type())});

	server.batch = arrow::RecordBatch::Make(schema, 1000, {text, labels});
	auto location = Location::ForGrpcTcp("127.0.0.1", 0).ValueOrDie();
	Require(server.Init(FlightServerOptions(location)).ok(), "start test server");
	location = Location::ForGrpcTcp("127.0.0.1", server.port()).ValueOrDie();
	auto client = FlightSqlConnectReadClient(location, FlightClientOptions::Defaults(),
		std::make_shared<FlightSqlReadCancellation>()).ValueOrDie();
	FlightCallOptions options;

	options.timeout = TimeoutDuration(5);
	auto info = client->GetFlightInfo(options, FlightDescriptor::Command("test"));
	Require(info.ok(), "discover test stream");
	auto error = client->GetFlightInfo(options, FlightDescriptor::Command("error"));
	Require(error.status().IsInvalid(), "lost Arrow discovery status trailers");
	for (int attempt = 0; attempt < 3; attempt++)
	{
		auto reader = client->DoGet(options, Ticket{"test"}).ValueOrDie();

		Require(reader->GetSchema().ValueOrDie()->Equals(*schema), "wrong stream schema");
		for (int index = 0; index < 2; index++)
		{
			auto chunk = reader->Next().ValueOrDie();

			Require(chunk.data && chunk.data->Equals(*server.batch), "dictionary/null/large stream mismatch");
		}
		Require(!reader->Next().ValueOrDie().data, "unexpected trailing batch");
	}
	auto stream_error = client->DoGet(options, Ticket{"error"});
	Require(stream_error.status().IsInvalid(), "lost Arrow stream status trailers");
	Require(client->Close().ok(), "close test client");
	auto cancelled = std::make_shared<FlightSqlReadCancellation>();

	cancelled->Cancel();
	client = FlightSqlConnectReadClient(location, FlightClientOptions::Defaults(),
		cancelled).ValueOrDie();
	Require(client->GetFlightInfo(options, FlightDescriptor::Command("test")).status().IsCancelled(),
			"pre-cancelled discovery started");
	Require(client->DoGet(options, Ticket{"test"}).status().IsCancelled(),
			"pre-cancelled stream started");
	Require(client->Close().ok(), "close cancelled client");
	Require(server.Shutdown().ok(), "stop test server");
}

int
main()
{
	try
	{
		TestStream();
		const std::string body(256 * 1024, 'x');
		const std::string encoded = Field(2, "header") + Field(77, "unknown") +
			Field(3, "application") + Field(1000, body);
		for (size_t split : {size_t(0), size_t(1), size_t(3), size_t(13),
							encoded.size() / 2, encoded.size() - 1})
		{
			grpc::Slice slices[] = {grpc::Slice(encoded.data(), split),
				grpc::Slice(encoded.data() + split, encoded.size() - split)};
			grpc::ByteBuffer bytes(slices + (split == 0 ? 1 : 0), split == 0 ? 1 : 2);
			transport::FlightData data;

			Require(ReadFlightData(&bytes, &data).ok(), "FlightData decoding failed");
			Require(data.metadata->ToString() == "header", "wrong header");
			Require(data.app_metadata->ToString() == "application", "wrong application metadata");
			Require(data.body->ToString() == body, "wrong body");
			if (split == 0)
				Require(data.body->data() == slices[1].begin() + encoded.size() - body.size(),
						"single-slice body was copied");
			bytes.Clear();
			Require(data.body->ToString() == body, "body lifetime depends on ByteBuffer");
		}
		for (const auto& value : {std::string(), Field(2, "schema"), Field(3, "metadata")})
		{
			grpc::Slice slice(value);
			grpc::ByteBuffer bytes(&slice, 1);
			transport::FlightData data;

			Require(ReadFlightData(&bytes, &data).ok(), "empty body rejected");
			Require(data.body != nullptr && data.body->size() == 0, "missing default body");
		}
		for (const auto& value : {std::string("\x12\xff", 2),
				std::string("\x12\x05x", 3), std::string("\x00", 1),
				std::string("\x0f", 1), std::string("\x12\xff\xff\xff\xff\x0f", 6)})
		{
			grpc::Slice slice(value);
			grpc::ByteBuffer bytes(&slice, 1);
			transport::FlightData data;

			Require(!ReadFlightData(&bytes, &data).ok(), "malformed FlightData accepted");
		}
		for (int i = 0; i < 1000; i++)
		{
			auto cancellation = std::make_shared<FlightSqlReadCancellation>();
			std::atomic<int> calls{0};
			if (i % 2 == 0)
				cancellation->Cancel();
			auto registration = std::make_unique<FlightSqlReadCancellation::Registration>(
				cancellation, [&] { ++calls; });
			Require(i % 2 != 0 || calls == 1, "pre-registration cancellation lost");
			std::thread cancelling([&] { cancellation->Cancel(); });

			registration.reset();
			cancelling.join();
			const int detached_calls = calls;
			cancellation->Cancel();
			Require(calls == detached_calls, "detached callback invoked");
		}
		std::cout << "PASS: dictionary/null/large streams, FlightData buffers, malformed messages, cancellation lifetime (1000 races)\n";
		return 0;
	}
	catch (const std::exception& ex)
	{
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
