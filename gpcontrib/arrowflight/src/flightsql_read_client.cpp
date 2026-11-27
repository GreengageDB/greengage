/*-------------------------------------------------------------------------
 *
 * flightsql_read_client.cpp
 *    Interruptible GetFlightInfo and DoGet using Arrow's transport interface.
 *
 * PostgreSQL signals are handled by the backend, which calls Cancel(). The
 * read worker owns each gRPC context until completion, including connection
 * establishment and the first schema message. No PostgreSQL API is used here.
 *
 *-------------------------------------------------------------------------
 */

#ifdef USE_ARROW_FLIGHT

#include "flightsql_read_client.h"

#include <arrow/buffer.h>
#include <arrow/flight/transport.h>
#include <arrow/util/uri.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/generic_stub.h>

#include <climits>
#include <cstring>
#include <stdexcept>

using arrow::Status;
using namespace arrow::flight;
namespace transport = arrow::flight::internal;

FlightSqlReadCancellation::Registration::Registration(
	std::shared_ptr<FlightSqlReadCancellation> owner, std::function<void()> cancel)
	: owner_(std::move(owner))
{
	std::lock_guard<std::mutex> lock(owner_->mutex_);

	if (owner_->cancel_)
		throw std::logic_error("overlapping Flight SQL read RPCs");
	owner_->cancel_ = std::move(cancel);
	if (owner_->cancelled_)
		owner_->cancel_();
}

FlightSqlReadCancellation::Registration::~Registration()
{
	/* Serialize detachment with Cancel(), before the context is destroyed. */
	std::lock_guard<std::mutex> lock(owner_->mutex_);

	owner_->cancel_ = nullptr;
}

void
FlightSqlReadCancellation::Cancel()
{
	std::lock_guard<std::mutex> lock(mutex_);

	cancelled_ = true;
	if (cancel_)
		cancel_();
}

namespace
{
constexpr char kReadScheme[] = "greengage-flight-read";

Status
RpcStatus(const grpc::Status& status, const grpc::ClientContext& context)
{
	if (status.ok())
		return Status::OK();
	TransportStatusCode code;

	switch (status.error_code())
	{
		case grpc::StatusCode::CANCELLED: code = TransportStatusCode::kCancelled; break;
		case grpc::StatusCode::DEADLINE_EXCEEDED: code = TransportStatusCode::kTimedOut; break;
		case grpc::StatusCode::INVALID_ARGUMENT: code = TransportStatusCode::kInvalidArgument; break;
		case grpc::StatusCode::OUT_OF_RANGE: code = TransportStatusCode::kInvalidArgument; break;
		case grpc::StatusCode::NOT_FOUND: code = TransportStatusCode::kNotFound; break;
		case grpc::StatusCode::ALREADY_EXISTS: code = TransportStatusCode::kAlreadyExists; break;
		case grpc::StatusCode::UNAUTHENTICATED: code = TransportStatusCode::kUnauthenticated; break;
		case grpc::StatusCode::PERMISSION_DENIED: code = TransportStatusCode::kUnauthorized; break;
		case grpc::StatusCode::UNIMPLEMENTED: code = TransportStatusCode::kUnimplemented; break;
		case grpc::StatusCode::UNAVAILABLE: code = TransportStatusCode::kUnavailable; break;
		case grpc::StatusCode::RESOURCE_EXHAUSTED: code = TransportStatusCode::kUnavailable; break;
		case grpc::StatusCode::FAILED_PRECONDITION: code = TransportStatusCode::kUnavailable; break;
		case grpc::StatusCode::ABORTED: code = TransportStatusCode::kUnavailable; break;
		case grpc::StatusCode::INTERNAL: code = TransportStatusCode::kInternal; break;
		case grpc::StatusCode::DATA_LOSS: code = TransportStatusCode::kInternal; break;
		default: code = TransportStatusCode::kUnknown; break;
	}
	Status result = transport::TransportStatus{code, status.error_message()}.ToStatus();
	const auto& trailers = context.GetServerTrailingMetadata();
	auto trailer = [&trailers](const char *key) -> std::optional<std::string>
	{
		auto found = trailers.find(key);

		if (found == trailers.end())
			return std::nullopt;
		return std::string(found->second.data(), found->second.size());
	};
	auto arrow_code = trailer("x-arrow-status");

	if (arrow_code)
		return transport::ReconstructStatus(
			*arrow_code, result, trailer("x-arrow-status-message-bin"),
			trailer("x-arrow-status-detail-bin"), trailer("grpc-status-details-bin"),
			FlightStatusDetail::UnwrapStatus(result));
	return result;
}

/* CQ and context outlive all pending operations, even during C++ unwinding. */
class ReadRpc
{
public:
	ReadRpc(const FlightCallOptions& options,
			std::shared_ptr<FlightSqlReadCancellation> cancellation)
		: registration_(std::move(cancellation), [this] { context.TryCancel(); })
	{
		if (options.timeout.count() >= 0)
			context.set_deadline(std::chrono::time_point_cast<
				std::chrono::system_clock::duration>(
					std::chrono::system_clock::now() + options.timeout));
		for (const auto& header : options.headers)
			context.AddMetadata(header.first, header.second);
	}

	~ReadRpc()
	{
		context.TryCancel();
		queue.Shutdown();
		void *tag;
		bool ok;

		while (queue.Next(&tag, &ok))
			;
	}

	bool Wait()
	{
		void *tag;
		bool ok;

		return queue.Next(&tag, &ok) && tag == this && ok;
	}

	grpc::ClientContext context;
	grpc::CompletionQueue queue;

private:
	FlightSqlReadCancellation::Registration registration_;
};

/* Keep gRPC storage alive; large single-slice batches need no payload copy. */
class SliceBuffer : public arrow::Buffer
{
public:
	explicit SliceBuffer(grpc::Slice slice)
		: arrow::Buffer(nullptr, 0), slice_(std::move(slice))
	{
		data_ = slice_.begin();
		size_ = slice_.size();
		capacity_ = size_;
	}

private:
	grpc::Slice slice_;
};

arrow::Result<std::shared_ptr<arrow::Buffer>>
WrapBuffer(grpc::ByteBuffer *bytes)
{
	std::vector<grpc::Slice> slices;
	auto status = bytes->Dump(&slices);
	if (!status.ok())
		return Status::IOError("cannot read Flight gRPC buffer: ", status.error_message());
	if (slices.size() == 1)
		return std::make_shared<SliceBuffer>(std::move(slices.front()));
	ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(bytes->Length()));
	int64_t offset = 0;

	for (const auto& slice : slices)
	{
		std::memcpy(buffer->mutable_data() + offset, slice.begin(), slice.size());
		offset += slice.size();
	}
	return std::shared_ptr<arrow::Buffer>(std::move(buffer));
}

/*
 * Decode only the standard FlightData protobuf envelope (Flight.proto).
 * The IPC headers/body remain buffer views; Arrow decodes schemas, dictionaries
 * and RecordBatches. Protobuf handles unknown fields and malformed varints.
 */
Status
ReadFlightData(grpc::ByteBuffer *bytes, transport::FlightData *data)
{
	ARROW_ASSIGN_OR_RAISE(auto buffer, WrapBuffer(bytes));
	if (buffer->size() > INT_MAX)
		return Status::Invalid("FlightData exceeds protobuf size limit");
	google::protobuf::io::CodedInputStream input(buffer->data(), buffer->size());
	input.SetTotalBytesLimit(buffer->size());
	using Wire = google::protobuf::internal::WireFormatLite;
	uint32_t tag;

	*data = transport::FlightData{};
	while ((tag = input.ReadTag()) != 0)
	{
		const int field = Wire::GetTagFieldNumber(tag);

		if (Wire::GetTagWireType(tag) != Wire::WIRETYPE_LENGTH_DELIMITED ||
			(field != 1 && field != 2 && field != 3 && field != 1000))
		{
			if (!Wire::SkipField(&input, tag))
				return Status::Invalid("invalid FlightData field");
			continue;
		}
		uint32_t size;

		if (!input.ReadVarint32(&size) || size > INT_MAX ||
			size > buffer->size() - input.CurrentPosition())
			return Status::Invalid("invalid FlightData field length");
		auto value = arrow::SliceBuffer(buffer, input.CurrentPosition(), size);

		if (!input.Skip(size))
			return Status::Invalid("truncated FlightData field");
		switch (field)
		{
			case 1:
				data->descriptor = std::make_unique<FlightDescriptor>();
				ARROW_RETURN_NOT_OK(FlightDescriptor::Deserialize(
					std::string_view(reinterpret_cast<const char *>(value->data()), size),
					data->descriptor.get()));
				break;
			case 2: data->metadata = std::move(value); break;
			case 3: data->app_metadata = std::move(value); break;
			case 1000: data->body = std::move(value); break;
		}
	}
	if (!data->body)
		data->body = std::make_shared<arrow::Buffer>(nullptr, 0);
	return input.ConsumedEntireMessage() ? Status::OK() :
		Status::Invalid("truncated FlightData protobuf");
}

class ReadStream : public transport::ClientDataStream
{
public:
	ReadStream(const FlightCallOptions& options,
			   std::shared_ptr<FlightSqlReadCancellation> cancellation)
		: rpc_(options, std::move(cancellation)) {}

	~ReadStream() override
	{
		TryCancel();
		(void) DoFinish();
	}

	Status Open(grpc::GenericStub *stub, const Ticket& ticket)
	{
		ARROW_ASSIGN_OR_RAISE(auto request, ticket.SerializeToString());
		grpc::Slice slice(request);
		grpc::ByteBuffer bytes(&slice, 1);

		stream_ = stub->PrepareCall(&rpc_.context,
			"/arrow.flight.protocol.FlightService/DoGet", &rpc_.queue);
		stream_->StartCall(&rpc_);
		if (!rpc_.Wait())
			return DoFinish();
		stream_->Write(bytes, &rpc_);
		if (!rpc_.Wait())
			return DoFinish();
		stream_->WritesDone(&rpc_);
		if (!rpc_.Wait())
			return DoFinish();
		return Status::OK();
	}

	bool ReadData(transport::FlightData *data) override
	{
		if (finished_)
			return false;
		grpc::ByteBuffer bytes;

		stream_->Read(&bytes, &rpc_);
		if (!rpc_.Wait())
		{
			read_done_ = true;
			return false;
		}
		client_status_ = ReadFlightData(&bytes, data);
		if (!client_status_.ok())
			TryCancel();
		return client_status_.ok();
	}

	void TryCancel() override { rpc_.context.TryCancel(); }

protected:
	Status DoFinish() override
	{
		if (!finished_ && stream_)
		{
			/* An IPC error or early close must not wait for unread server data. */
			if (!read_done_)
				TryCancel();
			stream_->Finish(&status_, &rpc_);
			(void) rpc_.Wait();
			finished_ = true;
		}
		return client_status_.ok() ? RpcStatus(status_, rpc_.context) : client_status_;
	}

private:
	ReadRpc rpc_;
	std::unique_ptr<grpc::GenericClientAsyncReaderWriter> stream_;
	grpc::Status status_;
	Status client_status_;
	bool read_done_ = false;
	bool finished_ = false;
};

struct ConnectContext
{
	Location location;
	std::shared_ptr<FlightSqlReadCancellation> cancellation;
};

/* FlightClient::Connect invokes the registered factory synchronously. */
thread_local const ConnectContext *connecting = nullptr;

class ReadTransport : public transport::ClientTransport
{
public:
	explicit ReadTransport(const ConnectContext& context) : context_(context) {}

	Status Init(const FlightClientOptions& options, const Location&,
				const arrow::util::Uri&) override
	{
		if (!options.middleware.empty() || options.disable_server_verification)
			return Status::NotImplemented("Flight SQL read transport requires verified TLS and no client middleware");
		arrow::util::Uri uri;

		ARROW_RETURN_NOT_OK(uri.Parse(context_.location.ToString()));
		const auto& scheme = context_.location.scheme();
		std::shared_ptr<grpc::ChannelCredentials> credentials;

		if (scheme == "grpc+tls")
		{
			grpc::SslCredentialsOptions tls;

			tls.pem_root_certs = options.tls_root_certs;
			tls.pem_cert_chain = options.cert_chain;
			tls.pem_private_key = options.private_key;
			credentials = grpc::SslCredentials(tls);
		}
		else if (scheme == "grpc" || scheme == "grpc+tcp")
			credentials = grpc::InsecureChannelCredentials();
		else
			return Status::Invalid("unsupported Flight SQL read transport: ", scheme);
		grpc::ChannelArguments arguments;

		arguments.SetInt("grpc.initial_reconnect_backoff_ms", 100);
		arguments.SetInt("grpc.use_local_subchannel_pool", 1);
		arguments.SetMaxReceiveMessageSize(-1);
		if (!options.override_hostname.empty())
			arguments.SetSslTargetNameOverride(options.override_hostname);
		for (const auto& option : options.generic_options)
		{
			if (std::holds_alternative<int>(option.second))
				arguments.SetInt(option.first, std::get<int>(option.second));
			else
				arguments.SetString(option.first, std::get<std::string>(option.second));
		}
		channel_ = grpc::CreateCustomChannel(
			arrow::util::UriEncodeHost(uri.host()) + ":" + uri.port_text(),
			credentials, arguments);
		stub_ = std::make_unique<grpc::GenericStub>(channel_);
		options_ = options;
		return Status::OK();
	}

	Status Close() override
	{
		stub_.reset();
		channel_.reset();
		return cleanup_ ? cleanup_->Close() : Status::OK();
	}

	Status GetFlightInfo(const FlightCallOptions& options,
						 const FlightDescriptor& descriptor,
						 std::unique_ptr<FlightInfo> *info) override
	{
		ARROW_ASSIGN_OR_RAISE(auto request, descriptor.SerializeToString());
		grpc::Slice slice(request);
		grpc::ByteBuffer bytes(&slice, 1);
		grpc::ByteBuffer response;
		grpc::Status status;
		ReadRpc rpc(options, context_.cancellation);
		auto call = stub_->PrepareUnaryCall(&rpc.context,
			"/arrow.flight.protocol.FlightService/GetFlightInfo", bytes, &rpc.queue);

		call->StartCall();
		call->Finish(&response, &status, &rpc);
		if (!rpc.Wait())
			return Status::IOError("Flight SQL GetFlightInfo completion failed");
		ARROW_RETURN_NOT_OK(RpcStatus(status, rpc.context));
		ARROW_ASSIGN_OR_RAISE(auto buffer, WrapBuffer(&response));
		return FlightInfo::Deserialize(
			std::string_view(reinterpret_cast<const char *>(buffer->data()), buffer->size()), info);
	}

	Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
				 std::unique_ptr<transport::ClientDataStream> *out) override
	{
		auto stream = std::make_unique<ReadStream>(options, context_.cancellation);

		ARROW_RETURN_NOT_OK(stream->Open(stub_.get(), ticket));
		*out = std::move(stream);
		return Status::OK();
	}

	Status DoAction(const FlightCallOptions& options, const Action& action,
					std::unique_ptr<ResultStream> *results) override
	{
		/* Open a cleanup channel only when CancelFlightInfo is actually needed. */
		if (!cleanup_)
		{
			arrow::util::Uri uri;

			ARROW_RETURN_NOT_OK(uri.Parse(context_.location.ToString()));
			ARROW_ASSIGN_OR_RAISE(auto client,
				transport::GetDefaultTransportRegistry()->MakeClient(context_.location.scheme()));
			ARROW_RETURN_NOT_OK(client->Init(options_, context_.location, uri));
			cleanup_ = std::move(client);
		}
		return cleanup_->DoAction(options, action, results);
	}

private:
	ConnectContext context_;
	FlightClientOptions options_;
	std::shared_ptr<grpc::Channel> channel_;
	std::unique_ptr<grpc::GenericStub> stub_;
	std::unique_ptr<transport::ClientTransport> cleanup_;
};
} /* namespace */

arrow::Result<std::unique_ptr<FlightClient>>
FlightSqlConnectReadClient(const Location& location,
						  const FlightClientOptions& options,
						  std::shared_ptr<FlightSqlReadCancellation> cancellation)
{
	static const Status registered = transport::GetDefaultTransportRegistry()->RegisterClient(
		kReadScheme, []() -> arrow::Result<std::unique_ptr<transport::ClientTransport>>
		{
			if (!connecting)
				return Status::Invalid("Flight SQL read client has no connection context");
			return std::make_unique<ReadTransport>(*connecting);
		});

	ARROW_RETURN_NOT_OK(registered);
	ConnectContext context{location, std::move(cancellation)};
	struct Scope
	{
		const ConnectContext *previous = connecting;
		~Scope() { connecting = previous; }
	} scope;

	connecting = &context;
	ARROW_ASSIGN_OR_RAISE(auto private_location,
		Location::Parse(std::string(kReadScheme) + "://localhost"));
	return FlightClient::Connect(private_location, options);
}

#endif /* USE_ARROW_FLIGHT */
