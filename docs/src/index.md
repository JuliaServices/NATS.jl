# NATS.jl

`NATS.jl` is a JuliaServices NATS client designed as a Julia port of the common
`nats.go` behavior.

The public API is namespaced under `NATS`:

```julia
using NATS

conn = NATS.connect()
sub = NATS.subscribe(conn, "events.created")
NATS.publish(conn, "events.created", "hello")
msg = NATS.next_msg(sub; timeout = 1)
NATS.auto_unsubscribe(sub, 1_000)
NATS.drain(conn)
```

`NATS.request` uses a muxed inbox by default for ordinary request/reply traffic.
Use `mux = false` for protocols that deliver through an inbox subscription while
preserving a different message subject. Pass `inbox_prefix = "ACCOUNT_INBOX"`
to `NATS.connect` to customize request/reply inbox subjects for routed or
multi-account deployments; the prefix must be a literal subject prefix without
wildcards or a trailing dot.

For hand-rolled reply subjects, `NATS.new_inbox()` mirrors `nats.go`'s
`NewInbox`; `NATS.new_inbox(conn)` uses the connection's configured inbox
prefix. `NATS.publish_request` publishes with an explicit reply subject without
waiting for the response:

```julia
reply = NATS.new_inbox(conn)
sub = NATS.subscribe(conn, reply)
NATS.publish_request(conn, "events.lookup", reply, "id-1")
msg = NATS.next_msg(sub; timeout = 2)
```

Service handlers can respond directly to request messages:

```julia
sub = NATS.subscribe(conn, "events.lookup") do req
    NATS.respond(conn, req, "found"; headers = ["X-Source" => "cache"])
end

reply = NATS.request(conn, "events.lookup", "id-1"; timeout = 2)
```

When the server reports no responders, `NATS.request` throws
`NATS.NoRespondersError`. Matching `nats.go`, `NATS.next_msg` does the same
for explicit reply-inbox subscriptions that receive an empty 503 status
message.

`NATS.request_msg`, `NATS.publish_msg`, and `NATS.respond_msg` preserve message
headers from a `NATS.Msg` value. `request_msg` uses the message subject, data,
and headers, and creates the reply inbox for the request:

```julia
msg = NATS.new_msg("events.created", "hello"; headers = ["X-Event" => "created"])
NATS.publish_msg(conn, msg)
reply = NATS.request_msg(conn, msg; timeout = 2)
```

Set `headers = false` on `NATS.connect` to opt out of header-aware protocol
behavior. That disables header publishes locally and also skips the
`no_responders` CONNECT advertisement, matching `nats.go`'s dependency on
header support.

Common `nats.go/micro` service behavior lives under `NATS.Micro`. A service can
register a base endpoint, add grouped endpoints, respond with data or service
error headers, and answer the standard `$SRV.PING`, `$SRV.INFO`, and
`$SRV.STATS` monitoring subjects:

```julia
svc = NATS.Micro.add_service(
    conn;
    name = "IncrementService",
    version = "0.1.0",
    description = "Increment numbers",
    endpoint = NATS.Micro.EndpointConfig(
        subject = "echo",
        handler = req -> NATS.Micro.respond(req, NATS.Micro.payload(req)),
    ),
)

numbers = NATS.Micro.add_group(svc, "numbers")
NATS.Micro.add_endpoint!(numbers, "Increment") do req
    value = parse(Int, NATS.Micro.payload(req))
    NATS.Micro.respond(req, string(value + 1))
end

NATS.Micro.add_endpoint!(
    svc,
    "Limited";
    subject = "numbers.Limited",
    queue_group = "workers",
    pending_msg_limit = 1024,
    pending_bytes_limit = 8 * 1024 * 1024,
) do req
    NATS.Micro.respond(req, "ok")
end

reply = NATS.request(conn, "numbers.Increment", "3"; timeout = 2)
NATS.Micro.stats(svc)
NATS.Micro.stop(svc)
```

Publish, request, subscribe, and queue group helpers validate subject and queue
names before writing protocol frames. For low-level compatibility tests that
need nats.go's escape hatch, publishing validation can be disabled per
connection while still rejecting empty subjects:

```julia
conn = NATS.connect(; skip_subject_validation = true)
```

`NATS.publish`, `NATS.publish_msg`, and request helpers reject messages larger
than the connected server's `max_payload` before writing to the socket. For
header messages, the check follows `nats.go` and counts payload bytes plus
serialized header bytes.

Common CONNECT options are passed as keywords:

```julia
conn = NATS.connect(
    "nats://localhost:4222";
    name = "billing-worker-1",
    no_echo = true,
    verbose = false,
    pedantic = false,
)
```

TLS uses Reseau and verifies hostnames by default:

```julia
tls = NATS.TLSOptions(ca_file = "ca.pem")
conn = NATS.connect("tls://localhost:4222"; tls)
```

Mutual TLS client certificates use the same options. TLS version bounds can be
passed through for deployments that need an explicit handshake policy:

```julia
using Reseau

tls = NATS.TLSOptions(
    ca_file = "ca.pem",
    cert_file = "client-cert.pem",
    key_file = "client-key.pem",
    max_version = Reseau.TLS.TLS1_2_VERSION,
)
conn = NATS.connect("tls://localhost:4222"; tls)
```

WebSocket connections use HTTP.jl 2.x:

```julia
conn = NATS.connect("ws://localhost:8080")
```

Custom WebSocket handshake headers can be passed directly or generated per
connection attempt with a callback:

```julia
conn = NATS.connect(
    "ws://localhost:8080";
    websocket_headers = ["Authorization" => "Bearer token"],
)

conn = NATS.connect(
    "ws://localhost:8080";
    websocket_headers_cb = () -> ["Authorization" => "Bearer refreshed-token"],
)
```

For NATS servers behind an HTTP reverse proxy that routes WebSocket upgrades by
path, set `proxy_path`. Like `nats.go`'s `ProxyPath`, this overrides any path in
the connection URL, preserves any URL query string on the upgrade request, and
also applies to discovered WebSocket server URLs:

```julia
conn = NATS.connect("ws://localhost:8080/nats"; proxy_path = "/nats")
```

Secure WebSockets use the same Reseau TLS options:

```julia
tls = NATS.TLSOptions(ca_file = "ca.pem")
conn = NATS.connect("wss://localhost:8443"; tls)
```

Authentication supports user/password, dynamic user/password callbacks, token,
dynamic token callbacks, NKey, and JWT nonce signing. For NKey auth, either
pass a seed directly or pass a public key plus an external signer callback that
returns raw Ed25519 signature bytes:

```julia
conn = NATS.connect("nats://localhost:4222"; user_info_cb = () -> ("derek", "porkchop"))
conn = NATS.connect("nats://localhost:4222"; token_cb = () -> readchomp("token.txt"))

seed = readchomp("user.nk")
conn = NATS.connect("nats://localhost:4222"; nkey_seed = seed)

public = NATS.nkey_public_from_seed(seed)
conn = NATS.connect(
    "nats://localhost:4222";
    nkey = public,
    signature_cb = nonce -> NATS.nkey_sign(seed, nonce),
)
```

JWT auth sends the user JWT plus a nonce signature. The public key is carried by
the JWT, so the connection only needs the JWT and either a seed or callback:

```julia
conn = NATS.connect("nats://localhost:4222"; jwt = user_jwt, nkey_seed = seed)
```

Use `jwt_cb` when the JWT can refresh during reconnect. The callback is invoked
for each CONNECT payload, including reconnects after authentication expiry:

```julia
conn = NATS.connect("nats://localhost:4222"; jwt_cb = () -> readchomp("user.jwt"), nkey_seed = seed)
```

NATS credentials files are supported as either a chained `.creds` file or split
JWT and seed files:

```julia
conn = NATS.connect("nats://localhost:4222"; credentials = "user.creds")
conn = NATS.connect(
    "nats://localhost:4222";
    jwt_file = "user.jwt",
    nkey_seed_file = "user.nk",
)
```

The client also resubscribes established subscriptions after a reconnect when a
server pool is configured. Reconnect and async error callbacks can be passed as
keywords:

`custom_reconnect_delay_cb` receives the completed server-pool pass number and
returns a delay in seconds; when set, it overrides `reconnect_wait` and jitter.
`reconnect_to_server_cb` receives the current server-pool URL snapshot and the
last `NATS.ServerInfo`, and may return `(server_url, delay_seconds)` to choose a
specific reconnect target.

```julia
conn = NATS.connect(
    "nats://a.example:4222";
    servers = ["nats://b.example:4222"],
    retry_on_failed_connect = true,
    reconnect_wait = 0.5,
    reconnect_jitter = 0.1,
    reconnect_jitter_tls = 1.0,
    custom_reconnect_delay_cb = attempt -> min(0.25 * attempt, 2.0),
    reconnect_to_server_cb = (servers, _info) -> (first(servers), 0.0),
    reconnect_buffer_size = 8 * 1024 * 1024,
    reconnect_on_flusher_error = false,
    ignore_auth_error_abort = false,
    ignore_discovered_servers = false,
    connected_cb = conn -> @info "NATS connected" NATS.connected_url(conn),
    discovered_servers_cb = conn -> @info "NATS discovered servers" NATS.discovered_servers(conn),
    lame_duck_cb = conn -> @warn "NATS server entered lame duck mode" NATS.connected_url(conn),
    disconnected_cb = (conn, err) -> @warn "NATS disconnected" err,
    reconnected_cb = conn -> @info "NATS reconnected" conn.url,
    error_cb = (conn, err) -> @warn "NATS async error" err,
    reconnect_error_cb = (conn, err) -> @warn "NATS reconnect attempt failed" err,
)
```

Set `reconnect_buffer_size = -1` to disable publish buffering during reconnect.
`NATS.buffered(conn)` returns the bytes currently queued for replay.
By default, a synchronous write error is reported through `error_cb` and
`conn.async_errors` but does not force an immediate reconnect. Set
`reconnect_on_flusher_error = true` to match `nats.go`'s advanced flusher-error
policy: write failures start a reconnect when reconnects are allowed, or close
the connection when `allow_reconnect = false`.

Server `-ERR` messages are classified into typed Julia errors. Permission and
max-subscription errors are reported through `error_cb`/`conn.async_errors`
without closing the connection. Stale-connection, max-connection, and
authentication-expired errors trigger reconnect when reconnects are enabled;
if the same auth error repeats for the same reconnect server, the connection is
closed by default to avoid retrying unchanged credentials forever. Set
`ignore_auth_error_abort = true` to opt out and keep retrying according to the
normal reconnect policy. Unknown terminal server errors close the connection.
Set `permission_err_on_subscribe = true` to close denied subscriptions locally
with `NATS.PermissionViolationError`, so `NATS.next_msg(sub)` throws the server
permission error instead of timing out.

Connection observability mirrors the common `nats.go` helpers:

```julia
NATS.status(conn)
NATS.is_connected(conn)
NATS.connected_url(conn)
NATS.connected_server_id(conn)
NATS.connected_server_name(conn)
NATS.connected_server_version(conn)
NATS.connected_cluster_name(conn)
NATS.connected_client_id(conn)
NATS.connected_client_ip(conn)
NATS.connected_server_jetstream(conn)
NATS.is_system_account(conn)
NATS.auth_required(conn)
NATS.tls_required(conn)
NATS.tls_available(conn)
NATS.tls_verify(conn)
NATS.max_payload(conn)
NATS.servers(conn)
NATS.discovered_servers(conn)
NATS.num_subscriptions(conn)
NATS.buffered(conn)
NATS.last_error(conn)
NATS.rtt(conn; timeout = 2)
NATS.stats(conn)

status_ch = NATS.status_changed(conn, NATS.RECONNECTING, NATS.CONNECTED, NATS.CLOSED)
NATS.remove_status_listener!(conn, status_ch)
```

Pending `NATS.flush`, `NATS.request`, and synchronous `NATS.next_msg` calls are
released if `NATS.close(conn)` closes the connection before the server replies
or a message arrives. Set `no_callbacks_after_client_close = true` to suppress
`closed_cb` after explicit client close or drain paths; server errors and other
terminal failures still report the closed callback.

Connections also send periodic client PINGs. Tune `ping_interval` and
`max_pings_out` to control stale-connection detection; missing too many PONGs
raises `NATS.StaleConnectionError` through the normal close or reconnect path:

```julia
conn = NATS.connect(
    "nats://localhost:4222";
    ping_interval = 120,
    max_pings_out = 2,
)
```

Callback handlers can also be adjusted at runtime:

```julia
NATS.set_error_handler!(conn, (conn, sub, err) -> @warn "NATS async error" sub err)
NATS.set_reconnect_error_handler!(conn, (conn, err) -> @warn "NATS reconnect attempt failed" err)
NATS.set_connected_handler!(conn, conn -> @info "NATS connected" NATS.connected_url(conn))
NATS.set_lame_duck_handler!(conn, conn -> @warn "NATS lame duck" NATS.connected_url(conn))
NATS.set_disconnected_handler!(conn, (conn, err) -> @warn "NATS disconnected" err)
NATS.set_reconnected_handler!(conn, conn -> @info "NATS reconnected")
NATS.set_closed_handler!(conn, conn -> @info "NATS closed")
```

`error_cb` can use either the older `(conn, err)` form or the `nats.go`-style
`(conn, sub, err)` form. `sub` is the affected subscription when the client can
identify one, such as slow-consumer errors, and `nothing` otherwise.

You can force a reconnect without enabling automatic reconnect, and both
connections and individual subscriptions can be drained. Subscriptions can also
ask the server to auto-unsubscribe them after a total message count:

```julia
sub = NATS.subscribe(conn, "events.created") do msg
    @info "event" payload = NATS.payload(msg)
end

NATS.force_reconnect(conn; timeout = 2)
NATS.auto_unsubscribe(sub, 100)
NATS.drain(sub; timeout = 2)
NATS.drain(conn)
```

After an auto-unsubscribe limit is reached, `NATS.next_msg` throws
`NATS.MaxMessagesError` once buffered messages have been consumed. If multiple
tasks are blocked in `NATS.next_msg` on the same synchronous subscription, all
waiters are released when the auto-unsubscribe limit closes the subscription.
Public subscription operations on an already closed subscription, including
`NATS.drain`, pending-stat accessors, and `NATS.auto_unsubscribe`, throw
`NATS.BadSubscriptionError`, matching `nats.go`'s invalid-subscription
behavior. `NATS.unsubscribe` also matches `nats.go`'s closed-connection
precedence and throws `NATS.ConnectionClosedError` if the parent connection is
already closed.
Subscriptions created with a callback are asynchronous; calling
`NATS.next_msg` on them throws `NATS.SyncSubscriptionRequiredError` instead of
racing the callback task for the same messages.
`NATS.barrier(conn, f)` mirrors `nats.go`'s callback-subscription barrier:
`f` runs after all asynchronous callback subscriptions have processed messages
queued before the barrier call. Synchronous subscriptions do not participate,
and a closed connection throws `NATS.ConnectionClosedError`.

Drain deadline failures throw `NATS.DrainTimeoutError`; connection drain
timeouts also set `NATS.last_error(conn)` before the connection is closed.
While a connection drain is waiting for existing subscription callbacks to
finish, publishes remain allowed so callbacks can send final responses. New
subscriptions and other non-publish writes fail with `NATS.ConnectionDrainingError`.
The muxed request inbox is drained last, so callbacks that already share an
existing request mux can complete final `NATS.request` calls before close.
Calling `NATS.drain(conn)` while the connection is reconnecting closes the
connection and throws `NATS.ConnectionReconnectingError`.

Subscriptions use bounded queues. If incoming messages exceed the message or
byte pending limits, the reader drops additional messages for that subscription
and reports a `NATS.SlowConsumerError` through `conn.async_errors` and
`error_cb`; three-argument error callbacks receive the slow subscription in
their `sub` argument. Matching `nats.go`, the client reports one async error
when a subscription first enters slow-consumer state; additional drops continue to
increase `NATS.dropped(sub)` and a new async error is reported only after the
subscription has recovered and becomes slow again. For callback subscriptions,
the currently executing callback counts as pending until the handler returns.
`NATS.delivered(sub)` counts messages handed to `NATS.next_msg` or to the
callback, not messages merely queued by the reader. Synchronous subscriptions
also mirror `nats.go`: the next `NATS.next_msg(sub)` call throws
`NATS.SlowConsumerError` once before delivering any queued message, then later
`next_msg` calls can drain the buffered messages normally:

```julia
sub = NATS.subscribe(conn, "events.slow"; channel_size = 1024)

NATS.set_pending_limits(sub, 1024, 8 * 1024 * 1024)
NATS.pending(sub)          # currently queued messages
NATS.pending_bytes(sub)    # currently queued payload bytes
NATS.pending_limits(sub)   # message and byte limits
NATS.max_pending(sub)      # highest observed queued messages
NATS.max_pending_bytes(sub)
NATS.clear_max_pending!(sub)
NATS.delivered(sub)        # messages handed to next_msg or the callback
NATS.dropped(sub)          # messages dropped after a limit was exceeded
NATS.subscription_status(sub)
NATS.is_valid(sub)
NATS.is_draining(sub)
NATS.set_closed_handler!(sub, subject -> @info "subscription closed" subject)
NATS.closed_handler(sub)

status_ch = NATS.status_changed(sub, NATS.SUBSCRIPTION_SLOW_CONSUMER, NATS.SUBSCRIPTION_CLOSED)
```

Connection and subscription status listeners are bounded and nonblocking. If a
listener is not drained and its channel fills, newer matching status events may
be skipped instead of stalling the client reader or callback tasks.

Like `nats.go`, pending, max-pending, delivered, dropped, and pending-limit
helpers throw `NATS.BadSubscriptionError` after an individual subscription is
closed. If the parent connection closes first, blocked synchronous `next_msg`
calls are released with `NATS.ConnectionClosedError`, and `NATS.unsubscribe`
reports `NATS.ConnectionClosedError`. Use
`NATS.subscription_status(sub)` or `NATS.is_valid(sub)` for post-close state
inspection.

`channel_size` is still the actual in-memory storage capacity for the
subscription queue, so set it at least as high as the message pending limit you
want to allow.

JetStream helpers live under `NATS.JetStream`:

```julia
using NATS.JetStream

account = JetStream.account_info(conn)

JetStream.create_stream(conn, JetStream.StreamConfig(
    name = "EVENTS",
    subjects = ["events.*"],
    storage = "file",
    max_msgs = 100_000,
    duplicate_window = 120_000_000_000,
    subject_transform = JetStream.SubjectTransformConfig(
        source = ">",
        destination = "events.transformed.>",
    ),
    republish = JetStream.RePublish(
        source = ">",
        destination = "events.copy.>",
    ),
    consumer_limits = JetStream.StreamConsumerLimits(max_ack_pending = 1000),
    metadata = Dict("service" => "events"),
    allow_msg_ttl = true,
    allow_msg_schedules = true,
    persist_mode = "async",
))

JetStream.create_stream(conn, JetStream.StreamConfig(
    name = "EVENTS_MIRROR",
    mirror = JetStream.StreamSource(name = "EVENTS"),
    mirror_direct = true,
))

JetStream.create_or_update_stream(conn, JetStream.StreamConfig(
    name = "EVENTS",
    subjects = ["events.*"],
    description = "reconciled stream config",
))

ack = JetStream.publish(conn, "events.created", "hello";
    msg_ttl = "5m",
    retry_attempts = 2,
    retry_wait = 0.25,
)
future = JetStream.publish_async(conn, "events.created", "async hello";
    msg_id = "evt-1",
    max_pending = 256,
    stall_wait = 0.2,
    retry_attempts = 2,
    retry_wait = 0.25,
    error_cb = (conn, msg, err) -> @warn "async publish failed" subject = msg.subject err,
)
pending = JetStream.publish_async_pending(conn)
JetStream.publish_async_complete(conn; timeout = 2)
async_ack = JetStream.wait_ack(future; timeout = 2)
JetStream.cleanup_publisher(conn)
stored = JetStream.get_msg(conn, "EVENTS", ack.seq)
latest = JetStream.get_last_msg(conn, "EVENTS", "events.created")
owner = JetStream.stream_name_by_subject(conn, "events.created")
stream_details = JetStream.streams(conn; subject_filter = "events.created")
with_deleted = JetStream.stream_info(conn, "EVENTS"; deleted_details = true)

direct = JetStream.get_msg(conn, "EVENTS", ack.seq; direct = true)
direct_next = JetStream.get_msg(conn, "EVENTS", 0;
    direct = true,
    next_subject = "events.created",
)
JetStream.create_consumer(conn, "EVENTS", JetStream.ConsumerConfig(
    name = "worker",
    durable_name = "worker",
    description = "event worker",
    max_ack_pending = 1000,
    max_batch = 100,
    priority_policy = "pinned_client",
    priority_timeout = 5_000_000_000,
    priority_groups = ["primary"],
))
JetStream.create_or_update_consumer(conn, "EVENTS", JetStream.ConsumerConfig(
    name = "worker",
    durable_name = "worker",
    description = "reconciled worker",
))
info = JetStream.consumer_info(conn, "EVENTS", "worker")
names = JetStream.consumer_names(conn, "EVENTS")
consumer_details = JetStream.consumers(conn, "EVENTS")
JetStream.pause_consumer(conn, "EVENTS", "worker", "2099-01-01T00:00:00Z")
JetStream.resume_consumer(conn, "EVENTS", "worker")
JetStream.reset_consumer(conn, "EVENTS", "worker")
JetStream.reset_consumer_to_sequence(conn, "EVENTS", "worker", 42)
JetStream.unpin_consumer(conn, "EVENTS", "worker", "primary")
msg = JetStream.next_msg(conn, "EVENTS", "worker"; no_wait = true, priority_group = "primary")
meta = JetStream.metadata(msg)
JetStream.double_ack(conn, msg; timeout = 2)

msgs = JetStream.fetch(conn, "EVENTS", "worker";
    batch = 10,
    expires_ns = 5_000_000_000,
    heartbeat_ns = 1_000_000_000,
    priority_group = "primary",
)
foreach(msg -> JetStream.ack(conn, msg), msgs)
byte_limited = JetStream.fetch_bytes(conn, "EVENTS", "worker", 1_000_000)

pull = JetStream.pull_subscribe(conn, "EVENTS", "worker")
try
    msgs = JetStream.fetch(pull; batch = 10, no_wait = true, priority_group = "primary")
    byte_limited = JetStream.fetch_bytes(pull, 1_000_000; no_wait = true)
    msg = JetStream.next_msg(pull; no_wait = true, priority_group = "primary")
finally
    close(pull)
end

consumer = JetStream.consume(conn, "EVENTS", "worker"; batch = 50, priority_group = "primary")
try
    msg = JetStream.next_msg(consumer; timeout = 1)
    JetStream.ack(conn, msg)
finally
    close(consumer)
end

push = JetStream.push_subscribe(conn, "EVENTS", JetStream.ConsumerConfig(
    name = "push-worker",
    durable_name = "push-worker",
    filter_subject = "events.created",
    idle_heartbeat = 1_000_000_000,
))
push_ctx = JetStream.consume(push; channel_size = 32)
try
    msg = JetStream.next_msg(push_ctx; timeout = 1)
    JetStream.ack(conn, msg)
finally
    close(push_ctx)
end

ordered = JetStream.ordered_consumer(conn, "EVENTS";
    name_prefix = "EVENTS_ORD",
    filter_subject = "events.created",
)
try
    msgs = JetStream.fetch(ordered; batch = 10, no_wait = true)
    msg = JetStream.next_msg(ordered; timeout = 1)
finally
    close(ordered)
end

ordered_consumer = JetStream.ordered_consumer(conn, "EVENTS")
ordered_ctx = JetStream.consume(ordered_consumer; batch = 50)
try
    msg = JetStream.next_msg(ordered_ctx; timeout = 1)
finally
    close(ordered_ctx)
end

JetStream.update_consumer(conn, "EVENTS", JetStream.ConsumerConfig(
    name = "worker",
    durable_name = "worker",
    description = "updated worker",
))
JetStream.delete_consumer(conn, "EVENTS", "worker")
JetStream.delete_msg(conn, "EVENTS", ack.seq)
JetStream.purge_stream(conn, "EVENTS"; subject_filter = "events.created")
JetStream.purge_stream(conn, "EVENTS"; seq = 100)
JetStream.purge_stream(conn, "EVENTS"; keep = 10)
```

`get_msg` and `get_last_msg` return `RawStreamMsg` values with raw `data` bytes
and parsed `headers`. Direct gets require the stream to be created or updated
with `allow_direct = true`. `purge_stream` supports the common `nats.go` purge
modes: whole-stream purge, filtered purge, purge up to a sequence, and keep the
last `N` messages. `seq` and `keep` are mutually exclusive.

Reusable pull subscriptions read the consumer's configured `max_batch`,
`max_expires`, and `max_bytes` limits when they are created and reject oversized
fetch requests locally, matching `nats.go`'s pull-subscription guardrails.
Use `JetStream.consumer_info(pull_or_push)` to refresh the server-side consumer
details for a reusable subscription, or `JetStream.cached_consumer_info(...)`
when the latest locally cached value is enough.
When a pull or push subscription is created from a `ConsumerConfig`, closing the
wrapper or draining the connection also deletes that library-created JetStream
consumer. Plain connection close leaves consumers alone, matching `nats.go`.
Subscriptions bound by consumer name only detach from the NATS subscription and
leave the consumer for the caller to manage.
Queue push subscriptions use the consumer's `deliver_group`. Matching
`nats.go`, config-created queue push subscriptions reject `idle_heartbeat` and
`flow_control` because those signals cannot be routed reliably through random
queue delivery.

Missing stream or consumer management operations throw
`JetStream.StreamNotFoundError` or `JetStream.ConsumerNotFoundError`, matching
the `nats.go` distinction between resource reconciliation failures and generic
JetStream API errors.
Stream-management APIs validate stream names locally like `nats.go`; empty names
throw `JetStream.StreamNameRequiredError`, and names containing wildcards,
dots, path separators, spaces, or control whitespace throw
`JetStream.InvalidStreamNameError`.
Consumer-management and pull-request APIs validate consumer names with the same
character rules. Empty lookup/bind/delete names throw
`JetStream.ConsumerNameRequiredError`, and invalid names throw
`JetStream.InvalidConsumerNameError`; create paths fall back from an empty
configured name to a durable name or generated ephemeral name like `nats.go`.
Single-consumer `filter_subject` values are validated locally before they are
embedded in the create API subject; empty single filters and empty filter lists
are omitted like Go's `omitempty`, and `filter_subject` is mutually exclusive
with nonempty `filter_subjects`. Server-reported overlapping multi-filter
consumer configs throw `JetStream.OverlappingFilterSubjectsError`. If a server
accepts a multi-filter consumer request but does not echo the filters in the
created consumer config, the call throws
`JetStream.MultipleFilterSubjectsNotSupportedError` like `nats.go`.
Create-only stream conflicts throw `JetStream.StreamNameAlreadyInUseError` when
the server reports the `nats.go` stream-name-in-use condition.
Consumer creation and update also use the same server-side action semantics as
`nats.go`: `create_consumer` is create-only,
`update_consumer` is update-only, and `create_or_update_consumer` reconciles
either state. Conflicting create-only calls throw `JetStream.ConsumerExistsError`;
missing update-only calls throw `JetStream.ConsumerDoesNotExistError`.

Sync publishes retry temporary no-responders responses by default, matching
`nats.go`'s protection against small JetStream leadership or stream-readiness
windows; exhausted retries throw `JetStream.NoStreamResponseError`. Pending
async publish futures are failed and removed when the connection reconnects or
closes, matching `nats.go`'s rule that pub acks from an old socket are no
longer valid. `cleanup_publisher` performs the same cancellation intentionally
with `JetStream.PublisherClosedError`; the connection remains usable afterward.

For JetStream domains or imported API subjects, pass either `domain` or
`api_prefix` to management calls. Reusable pull/push subscriptions, ordered
consumers, key-value buckets, and object stores keep that API routing for later
fetches, status calls, watcher cleanup, and stream metadata operations:

```julia
JetStream.create_stream(conn, JetStream.StreamConfig(
    name = "EVENTS",
    subjects = ["events.*"],
); domain = "HUB")

account = JetStream.account_info(conn; domain = "HUB")
info = JetStream.stream_info(conn, "EVENTS"; api_prefix = "\$JS.HUB.API")
owner = JetStream.stream_name_by_subject(conn, "events.created"; domain = "HUB")
stream_details = JetStream.streams(conn; domain = "HUB")
pull = JetStream.pull_subscribe(conn, "EVENTS", "worker"; domain = "HUB")
push = JetStream.push_subscribe(conn, "EVENTS", "push-worker"; domain = "HUB")
kv = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = "FLAGS"); domain = "HUB")
objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = "ARTIFACTS"); domain = "HUB")
```

Ordered consumers are client-managed ephemeral pull consumers. They recreate
ack-none memory consumers from the last delivered stream sequence and validate
the active consumer delivery sequence from JetStream metadata. Reset retries are
bounded by `max_reset_attempts`, with `-1` available for unlimited retry loops.
Pull, push, and ordered AckNone consumer messages still expose JetStream
metadata, but `ack` and `ack_sync` reject them instead of publishing an invalid
ack.

Basic key-value helpers are also under `NATS.JetStream`:

```julia
kv = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
    bucket = "SETTINGS",
    storage = "memory",
    history = 3,
    metadata = Dict("service" => "settings"),
    republish = JetStream.RePublish(source = ">", destination = "settings.audit.>"),
))

rev = JetStream.put(kv, "feature.flag", "on")
entry = JetStream.get(kv, "feature.flag")
@assert JetStream.value_string(entry) == "on"

next_rev = JetStream.update(kv, "feature.flag", "off", rev)
old_entry = JetStream.get_revision(kv, "feature.flag", rev)
history = JetStream.history(kv, "feature.flag")
keys = JetStream.keys(kv)
feature_keys = JetStream.keys(kv, "feature.*")
selected_keys = JetStream.keys(kv, ["feature.flag", "service.*"])
status = JetStream.status(kv)
buckets = JetStream.key_value_store_names(conn)

kv = JetStream.update_key_value(conn, JetStream.KeyValueConfig(
    bucket = "SETTINGS",
    storage = "memory",
    history = 5,
    metadata = Dict("service" => "settings"),
))
kv = JetStream.create_or_update_key_value(conn, JetStream.KeyValueConfig(
    bucket = "SETTINGS",
    storage = "memory",
    history = 5,
))

sourced = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
    bucket = "COMBINED_SETTINGS",
    storage = "memory",
    sources = [
        JetStream.StreamSource(name = "SETTINGS"),
        JetStream.StreamSource(name = "KV_GLOBAL_SETTINGS"),
    ],
))
mirrored = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
    bucket = "SETTINGS_COPY",
    storage = "memory",
    mirror = JetStream.StreamSource(name = "SETTINGS"),
))

lister = JetStream.list_keys(kv, "feature.*")
try
    while true
        key = JetStream.next_key(lister; timeout = 1)
        key === nothing && break
        @info "KV key" key
    end
finally
    close(lister)
end

watcher = JetStream.watch_all(kv)
try
    while true
        update = JetStream.next_update(watcher; timeout = 1)
        update === nothing && break  # initial values loaded
    end
finally
    close(watcher)
end

JetStream.delete(kv, "feature.flag")
JetStream.purge_deletes(kv)
JetStream.create(kv, "ephemeral.flag", "on"; msg_ttl = "5m")
JetStream.purge(kv, "ephemeral.flag"; msg_ttl = "5m")
```

The current key-value surface covers basic bucket creation, update,
create-or-update, open, and delete, bucket metadata, compression,
limit-marker TTL status, put, create, update, get, get by revision, delete,
purge, `nats.go`-style key-not-found and empty-key errors, streaming key listing
with wildcard or multi-filter patterns, history, status, and bucket listing,
`nats.go`-style bucket-not-found errors for missing bucket open/update/delete
operations, bucket-level republish for change fan-out, mirror/source bucket
configs with `nats.go`-style KV stream-name normalization, create-time repair
of old discard/allow-direct bucket configs, plus key watchers with initial
markers, updates-only mode, history replay, resume-from-revision replay, and
delete filtering.
Concrete keys use the `nats.go` character set `[-/_=.A-Za-z0-9]` and reject
empty, leading-dot, trailing-dot, double-dot, space, and wildcard values; watch
filters additionally allow standalone `*` tokens and terminal `>` tokens as
valid subject-style patterns.
`create` follows `nats.go` semantics for deleted keys: if the latest revision is
a delete or purge marker, it recreates the key by using that marker revision as
the expected previous revision. `purge_deletes` removes delete and purge
markers; by default it keeps markers newer than the `nats.go` 30-minute
threshold, while `delete_markers_older_than_ns = -1` removes all current
markers. `create` accepts `msg_ttl` for per-key TTL, and purge markers also
accept `msg_ttl`; plain delete markers intentionally reject TTL, matching
`nats.go`. `get` and `get_revision` use direct stream message reads when the
bucket stream supports them, matching the `allow_direct = true` stream
configuration used by `create_key_value`.

Object stores use the `nats.go` JetStream object-store layout:

```julia
objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(
    bucket = "ARTIFACTS",
    storage = "memory",
    metadata = Dict("service" => "builds"),
))

info = JetStream.put(objects, JetStream.ObjectMeta(
    name = "report.json",
    metadata = Dict("kind" => "report"),
), "{\"ok\":true}")

data = JetStream.get_string(objects, "report.json")
stored = JetStream.get_info(objects, "report.json")
all_objects = JetStream.list(objects)
object_status = JetStream.status(objects)
buckets = JetStream.object_store_names(conn)

lister = JetStream.list_objects(objects)
try
    while true
        info = JetStream.next_object(lister; timeout = 1)
        info === nothing && break
        @info "object" name = info.name size = info.size
    end
finally
    close(lister)
end

JetStream.update_meta(objects, "report.json", JetStream.ObjectMeta(
    name = "latest-report.json",
    metadata = Dict("kind" => "report"),
))
link = JetStream.add_link(objects, "report-link", JetStream.get_info(objects, "latest-report.json"))

watcher = JetStream.watch(objects)
try
    update = JetStream.next_update(watcher; timeout = 1)
finally
    close(watcher)
end

JetStream.delete(objects, "latest-report.json")
deleted = JetStream.get_info(objects, "latest-report.json"; show_deleted = true)
JetStream.delete_object_store(conn, "ARTIFACTS")
```

The current object-store surface covers bucket creation/open/delete, put/get
bytes and strings, multi-chunk objects, digest verification, metadata, metadata
updates/renames, max-bytes and compression status, links, watchers, binary-safe
file helpers backed by streaming `IO` puts, overwrite cleanup of superseded
chunks, server-supported mirrored object-bucket streams with subject transforms,
partial-upload cleanup, delete markers, seal, `nats.go`-style object-not-found
and empty-list errors for eager object info listing, streaming object info
listing, status, bucket-not-found errors for missing object-store
open/update/delete operations, and bucket listing that ignores non-object
streams with only a matching name prefix or only matching object chunk subjects.
Object reads decode and validate the stored `SHA-256=` digest
metadata before comparing payload hashes, so malformed digest metadata and
payload mismatches fail separately. Metadata updates change the name,
description, headers, and metadata only; links are created with `add_link` or
`add_bucket_link`, and object layout options such as chunk size remain tied to
the stored object. `add_link` validates the current target object before
publishing the link, so stale metadata for a now-deleted object cannot create a
new object link. Object names follow the `nats.go` layout and are encoded for
metadata subjects, so filename-like names with spaces, dots, slashes, and other
punctuation are supported. Advanced edge cases remain roadmap items.
