abstract type NATSError <: Exception end

struct ProtocolError <: NATSError
    message::String
end

struct ConnectionClosedError <: NATSError
    message::String
end

struct ConnectionDrainingError <: NATSError end

struct ConnectionReconnectingError <: NATSError end

struct MaxMessagesError <: NATSError
    max_msgs::Int
end

struct ReconnectFailedError <: NATSError
    attempts::Int
    last_error::Any
end

struct ReconnectBufferExceededError <: NATSError
    limit::Int
    attempted_size::Int
end

struct NoEchoNotSupportedError <: NATSError end

struct UserInfoAlreadySetError <: NATSError end

struct TokenAlreadySetError <: NATSError end

struct ServerNotInPoolError <: NATSError
    server::String
end

struct ConnectionTimeoutError <: NATSError
    operation::String
    timeout::Float64
end

struct DrainTimeoutError <: NATSError
    timeout::Float64
end

struct ServerError <: NATSError
    message::String
end

struct StaleConnectionError <: NATSError
    message::String
end

struct PermissionViolationError <: NATSError
    message::String
end

struct AuthorizationViolationError <: NATSError
    message::String
end

struct AuthenticationExpiredError <: NATSError
    message::String
end

struct AuthenticationRevokedError <: NATSError
    message::String
end

struct AccountAuthenticationExpiredError <: NATSError
    message::String
end

struct MaxConnectionsExceededError <: NATSError
    message::String
end

struct MaxAccountConnectionsExceededError <: NATSError
    message::String
end

struct MaxSubscriptionsExceededError <: NATSError
    message::String
end

struct BadSubjectError <: NATSError
    subject::String
end

struct BadQueueNameError <: NATSError
    queue::String
end

struct BadSubscriptionError <: NATSError end

struct SyncSubscriptionRequiredError <: NATSError end

struct InvalidMsgError <: NATSError end

struct MsgNoReplyError <: NATSError end

struct NoRespondersError <: NATSError
    subject::String
end

struct MaxPayloadError <: NATSError
    max_payload::Int
    payload_size::Int
end

struct HeadersNotSupportedError <: NATSError end

struct InvalidHeaderKeyError <: NATSError
    key::String
end

struct WebSocketHeadersAlreadySetError <: NATSError end

struct UnsupportedTransportError <: NATSError
    scheme::String
    message::String
end

struct SlowConsumerError <: NATSError
    subject::String
    sid::Int
    pending::Int
    capacity::Int
    dropped::Int
    pending_bytes::Int
    bytes_limit::Int
end

struct TooManyStalledMsgsError <: NATSError
    max_pending::Int
    stall_wait::Float64
end

function Base.showerror(io::IO, err::ProtocolError)
    print(io, "NATS protocol error: ", err.message)
end

function Base.showerror(io::IO, err::ConnectionClosedError)
    print(io, "NATS connection closed: ", err.message)
end

function Base.showerror(io::IO, ::ConnectionDrainingError)
    print(io, "NATS connection draining")
end

function Base.showerror(io::IO, ::ConnectionReconnectingError)
    print(io, "NATS connection reconnecting")
end

function Base.showerror(io::IO, err::MaxMessagesError)
    print(io, "NATS maximum messages delivered")
    err.max_msgs > 0 && print(io, ": ", err.max_msgs)
end

function Base.showerror(io::IO, err::ReconnectFailedError)
    print(io, "NATS reconnect failed after ", err.attempts, " attempts")
    err.last_error === nothing || (print(io, ": "); showerror(io, err.last_error))
end

function Base.showerror(io::IO, err::ReconnectBufferExceededError)
    print(io, "NATS reconnect buffer limit ", err.limit, " exceeded by frame of ", err.attempted_size, " bytes")
end

function Base.showerror(io::IO, ::NoEchoNotSupportedError)
    print(io, "NATS no_echo is not supported by this server")
end

function Base.showerror(io::IO, ::UserInfoAlreadySetError)
    print(io, "NATS user_info_cb cannot be combined with explicit user/password credentials")
end

function Base.showerror(io::IO, ::TokenAlreadySetError)
    print(io, "NATS token_cb cannot be combined with an explicit token")
end

function Base.showerror(io::IO, err::ServerNotInPoolError)
    print(io, "NATS reconnect server ", repr(err.server), " is not in the server pool")
end

function Base.showerror(io::IO, err::ConnectionTimeoutError)
    print(io, "NATS ", err.operation, " timed out after ", err.timeout, " seconds")
end

function Base.showerror(io::IO, err::DrainTimeoutError)
    print(io, "NATS drain timed out after ", err.timeout, " seconds")
end

function Base.showerror(io::IO, err::ServerError)
    print(io, "NATS server error: ", err.message)
end

function Base.showerror(io::IO, err::StaleConnectionError)
    print(io, "NATS stale connection: ", err.message)
end

function Base.showerror(io::IO, err::PermissionViolationError)
    print(io, "NATS permissions violation: ", err.message)
end

function Base.showerror(io::IO, err::AuthorizationViolationError)
    print(io, "NATS authorization violation: ", err.message)
end

function Base.showerror(io::IO, err::AuthenticationExpiredError)
    print(io, "NATS authentication expired: ", err.message)
end

function Base.showerror(io::IO, err::AuthenticationRevokedError)
    print(io, "NATS authentication revoked: ", err.message)
end

function Base.showerror(io::IO, err::AccountAuthenticationExpiredError)
    print(io, "NATS account authentication expired: ", err.message)
end

function Base.showerror(io::IO, err::MaxConnectionsExceededError)
    print(io, "NATS maximum connections exceeded: ", err.message)
end

function Base.showerror(io::IO, err::MaxAccountConnectionsExceededError)
    print(io, "NATS maximum account connections exceeded: ", err.message)
end

function Base.showerror(io::IO, err::MaxSubscriptionsExceededError)
    print(io, "NATS maximum subscriptions exceeded: ", err.message)
end

function Base.showerror(io::IO, err::BadSubjectError)
    print(io, "NATS invalid subject ", repr(err.subject))
end

function Base.showerror(io::IO, err::BadQueueNameError)
    print(io, "NATS invalid queue name ", repr(err.queue))
end

function Base.showerror(io::IO, ::BadSubscriptionError)
    print(io, "NATS invalid subscription")
end

function Base.showerror(io::IO, ::SyncSubscriptionRequiredError)
    print(io, "NATS illegal call on an async subscription")
end

function Base.showerror(io::IO, ::InvalidMsgError)
    print(io, "NATS invalid message")
end

function Base.showerror(io::IO, ::MsgNoReplyError)
    print(io, "NATS message has no reply subject")
end

function Base.showerror(io::IO, err::NoRespondersError)
    print(io, "NATS no responders for subject ", repr(err.subject))
end

function Base.showerror(io::IO, err::MaxPayloadError)
    print(io, "NATS payload size ", err.payload_size, " exceeds server max_payload ", err.max_payload)
end

function Base.showerror(io::IO, ::HeadersNotSupportedError)
    print(io, "NATS headers are not supported by this server")
end

function Base.showerror(io::IO, err::InvalidHeaderKeyError)
    print(io, "NATS invalid header key: ", repr(err.key))
end

function Base.showerror(io::IO, ::WebSocketHeadersAlreadySetError)
    print(io, "NATS websocket_headers and websocket_headers_cb are mutually exclusive")
end

function Base.showerror(io::IO, err::UnsupportedTransportError)
    print(io, "NATS transport ", repr(err.scheme), " is not supported: ", err.message)
end

function Base.showerror(io::IO, err::SlowConsumerError)
    print(
        io,
        "NATS slow consumer on subject ",
        repr(err.subject),
        " sid ",
        err.sid,
        ": dropped ",
        err.dropped,
        " message(s), pending ",
        err.pending,
        "/",
        err.capacity,
        ", bytes ",
        err.pending_bytes,
        "/",
        err.bytes_limit,
    )
end

function Base.showerror(io::IO, err::TooManyStalledMsgsError)
    print(io, "NATS async publish stalled with ", err.max_pending, " pending message(s) for ", err.stall_wait, " seconds")
end
