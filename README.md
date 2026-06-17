# NATS.jl

JuliaServices NATS client for Julia, started as a production-oriented port of
the common `nats.go` client surface.

This package is intentionally built around:

- `Reseau.TCP` and `Reseau.TLS` for core NATS TCP/TLS transports.
- `HTTP.WebSockets` for `ws://` and `wss://` NATS transports.
- Harbor-managed NATS server containers for integration tests.
- A minimal export surface: call APIs through the `NATS` namespace.

## Common Flows

The public API is intentionally namespaced. Start with `using NATS`, then call
the client through `NATS.*` and JetStream through `NATS.JetStream.*`.

### Connect, Publish, Subscribe

```julia
using NATS

conn = NATS.connect("nats://localhost:4222"; name = "orders-api")

sub = NATS.subscribe(conn, "orders.created")
NATS.publish(conn, "orders.created", "order-123")

msg = NATS.next_msg(sub; timeout = 1)
@info "received order event" subject = msg.subject payload = NATS.payload(msg)

NATS.unsubscribe(sub)
NATS.drain(conn)
```

### Callback Subscriptions

```julia
using NATS

conn = NATS.connect("nats://localhost:4222")

sub = NATS.subscribe(conn, "orders.created") do msg
    @info "created" payload = NATS.payload(msg)
end

NATS.publish(conn, "orders.created", "order-123")
NATS.flush(conn)

NATS.drain(sub; timeout = 2)
NATS.drain(conn)
```

### Queue Groups

```julia
using NATS

conn = NATS.connect("nats://localhost:4222")

worker = NATS.subscribe(conn, "orders.process"; queue = "order-workers") do msg
    @info "processing order" payload = NATS.payload(msg)
end

NATS.publish(conn, "orders.process", "order-123")
NATS.drain(worker)
NATS.close(conn)
```

### Request/Reply

```julia
using NATS

conn = NATS.connect("nats://localhost:4222")

responder = NATS.subscribe(conn, "orders.lookup") do req
    order_id = NATS.payload(req)
    NATS.respond(conn, req, "status=paid;id=$order_id")
end

reply = NATS.request(conn, "orders.lookup", "order-123"; timeout = 2)
@info "lookup result" payload = NATS.payload(reply)

NATS.unsubscribe(responder)
NATS.drain(conn)
```

### Headers And Message Values

```julia
using NATS

conn = NATS.connect("nats://localhost:4222")

body = """{"id":"order-123"}"""
msg = NATS.new_msg("orders.created", body;
    headers = [
        "Content-Type" => "application/json",
        "Nats-Msg-Id" => "order-123-created",
    ],
)

NATS.publish_msg(conn, msg)

sub = NATS.subscribe(conn, "orders.created")
received = NATS.next_msg(sub; timeout = 1)

@info "event" content_type = NATS.header(received, "Content-Type") body = NATS.payload(received)
```

### TLS, WebSockets, And Auth

```julia
using NATS

tls = NATS.TLSOptions(
    ca_file = "ca.pem",
    cert_file = "client-cert.pem",
    key_file = "client-key.pem",
)

conn = NATS.connect("tls://nats.internal:4222"; tls)
ws = NATS.connect(
    "wss://nats.example.com:443";
    tls,
    proxy_path = "/nats",
    websocket_headers_cb = () -> [
        "Authorization" => "Bearer $(readchomp("token.txt"))",
    ],
)

user_conn = NATS.connect("nats://localhost:4222"; user = "worker", password = "secret")
token_conn = NATS.connect("nats://localhost:4222"; token_cb = () -> readchomp("nats.token"))
nkey_conn = NATS.connect("nats://localhost:4222"; nkey_seed = readchomp("user.nk"))
creds_conn = NATS.connect("nats://localhost:4222"; credentials = "user.creds")
```

### Reconnects And Observability

```julia
using NATS

conn = NATS.connect(
    "nats://nats-a:4222";
    servers = ["nats://nats-b:4222", "nats://nats-c:4222"],
    retry_on_failed_connect = true,
    reconnect_wait = 0.5,
    reconnect_jitter = 0.1,
    reconnect_buffer_size = 8 * 1024 * 1024,
    connected_cb = conn -> @info "connected" url = NATS.connected_url(conn),
    disconnected_cb = (conn, err) -> @warn "disconnected" err,
    reconnected_cb = conn -> @info "reconnected" url = NATS.connected_url(conn),
    error_cb = (conn, sub, err) -> @warn "async NATS error" subject = (sub === nothing ? nothing : sub.subject) err,
)

@info "NATS stats" stats = NATS.stats(conn) rtt = NATS.rtt(conn; timeout = 2)

status_updates = NATS.status_changed(conn, NATS.RECONNECTING, NATS.CONNECTED, NATS.CLOSED)
NATS.force_reconnect(conn; timeout = 2)
@info "next connection status" status = take!(status_updates)
NATS.remove_status_listener!(conn, status_updates)
```

### JetStream Streams And Publishing

```julia
using NATS
using NATS.JetStream

conn = NATS.connect("nats://localhost:4222")

JetStream.create_or_update_stream(conn, JetStream.StreamConfig(
    name = "ORDERS",
    subjects = ["orders.*"],
    storage = "file",
    duplicate_window = 120_000_000_000,
))

ack = JetStream.publish(conn, "orders.created", """{"id":"order-123"}""";
    msg_id = "order-123-created",
    expected_stream = "ORDERS",
)

future = JetStream.publish_async(conn, "orders.created", """{"id":"order-124"}""";
    msg_id = "order-124-created",
    max_pending = 256,
    error_cb = (conn, msg, err) -> @warn "publish failed" subject = msg.subject err,
)

async_ack = JetStream.wait_ack(future; timeout = 2)
@info "stored messages" sync_seq = ack.seq async_seq = async_ack.seq
```

### JetStream Pull Consumers

```julia
using NATS
using NATS.JetStream

conn = NATS.connect("nats://localhost:4222")

JetStream.create_or_update_stream(conn, JetStream.StreamConfig(
    name = "ORDERS",
    subjects = ["orders.*"],
    storage = "file",
))

JetStream.create_or_update_consumer(conn, "ORDERS", JetStream.ConsumerConfig(
    durable_name = "billing",
    ack_policy = "explicit",
    filter_subject = "orders.created",
))

pull = JetStream.pull_subscribe(conn, "ORDERS", "billing")
try
    for msg in JetStream.fetch(pull; batch = 10, expires_ns = 2_000_000_000)
        @info "billing event" payload = NATS.payload(msg) meta = JetStream.metadata(msg)
        JetStream.ack(conn, msg)
    end
finally
    close(pull)
end
```

### JetStream Push And Ordered Consumers

```julia
using NATS
using NATS.JetStream

conn = NATS.connect("nats://localhost:4222")

push = JetStream.push_subscribe(conn, "ORDERS", JetStream.ConsumerConfig(
    durable_name = "notifications",
    filter_subject = "orders.created",
    ack_policy = "explicit",
    idle_heartbeat = 1_000_000_000,
))

ctx = JetStream.consume(push; channel_size = 128)
try
    msg = JetStream.next_msg(ctx; timeout = 2)
    JetStream.ack(conn, msg)
finally
    close(ctx)
end

ordered = JetStream.ordered_consumer(conn, "ORDERS"; filter_subject = "orders.created")
try
    for msg in JetStream.fetch(ordered; batch = 100, no_wait = true)
        @info "ordered replay" payload = NATS.payload(msg)
    end
finally
    close(ordered)
end
```

### Key/Value Buckets

```julia
using NATS
using NATS.JetStream

conn = NATS.connect("nats://localhost:4222")

kv = JetStream.create_or_update_key_value(conn, JetStream.KeyValueConfig(
    bucket = "SETTINGS",
    storage = "file",
    history = 5,
    metadata = Dict("service" => "orders"),
))

rev = JetStream.put(kv, "feature.checkout-v2", "enabled")
entry = JetStream.get(kv, "feature.checkout-v2")
@assert JetStream.value_string(entry) == "enabled"

next_rev = JetStream.update(kv, "feature.checkout-v2", "disabled", rev)
history = JetStream.history(kv, "feature.checkout-v2")
keys = JetStream.keys(kv, "feature.*")

watcher = JetStream.watch(kv, "feature.*"; updates_only = true)
try
    JetStream.put(kv, "feature.checkout-v2", "enabled")
    update = JetStream.next_update(watcher; timeout = 2)
    @info "setting changed" key = update.key revision = update.revision value = JetStream.value_string(update)
finally
    close(watcher)
end
```

### Object Stores

```julia
using NATS
using NATS.JetStream

conn = NATS.connect("nats://localhost:4222")

objects = JetStream.create_or_update_object_store(conn, JetStream.ObjectStoreConfig(
    bucket = "ARTIFACTS",
    storage = "file",
    metadata = Dict("service" => "reports"),
))

info = JetStream.put(objects, JetStream.ObjectMeta(
    name = "daily-report.json",
    description = "Daily order summary",
    metadata = Dict("content-type" => "application/json"),
), """{"orders":42}""")

body = JetStream.get_string(objects, "daily-report.json")
stored = JetStream.get_info(objects, "daily-report.json")

JetStream.put_file(objects, "build/report.pdf"; name = "reports/daily.pdf")
JetStream.get_file(objects, "reports/daily.pdf", "downloaded-report.pdf")

for object in JetStream.list(objects)
    @info "object" name = object.name size = object.size
end
```

### Services With `NATS.Micro`

```julia
using NATS

conn = NATS.connect("nats://localhost:4222")

svc = NATS.Micro.add_service(
    conn;
    name = "OrderService",
    version = "0.1.0",
    description = "Order operations",
    endpoint = NATS.Micro.EndpointConfig(
        subject = "orders.lookup",
        handler = req -> NATS.Micro.respond(req, "order=$(NATS.Micro.payload(req));status=paid"),
    ),
)

admin = NATS.Micro.add_group(svc, "orders.admin")
NATS.Micro.add_endpoint!(admin, "Cancel"; subject = "orders.cancel") do req
    NATS.Micro.respond(req, "cancelled $(NATS.Micro.payload(req))")
end

reply = NATS.request(conn, "orders.lookup", "order-123"; timeout = 2)
stats = NATS.Micro.stats(svc)

NATS.Micro.stop(svc)
NATS.drain(conn)
```

## First Slice

The current implementation includes:

- Core protocol handshake over `nats://`, secure `tls://` with the normal
  INFO-driven TLS upgrade, and an explicit `tls_handshake_first=true` option
  for servers configured to handshake before INFO, including `nats.go`-style
  CONNECT options for client name, verbose mode, pedantic mode, and no-echo.
- `NATS.new_inbox`, `NATS.publish`, `NATS.publish_request`,
  `NATS.subscribe`, `NATS.next_msg`, `NATS.request`,
  `NATS.request_msg`, `NATS.publish_msg`, `NATS.respond`,
  `NATS.respond_msg`, `NATS.flush`,
  `NATS.auto_unsubscribe`, connection/subscription `NATS.drain`,
  `NATS.force_reconnect`, and `NATS.close`, with client-side publish/subscribe
  subject, queue-name, and custom inbox-prefix validation, typed
  bad-subscription, async-subscription, max-message terminal errors, and
  `nats.go`-style max-payload rejection before socket writes, including header
  bytes, and typed drain-state, reconnecting-drain, and drain-timeout errors.
- Connection status predicates, RTT measurement, connected-server metadata, max
  payload, active subscription counts, system-account detection,
  auth-required and TLS INFO flags, and `nats.go`-style message/byte
  statistics snapshots.
- Basic headers and status header parsing, client-side header opt-out, plus
  `nats.go`-style no-responders handling for ordinary request/reply and
  explicit reply-inbox `next_msg`.
- User/password, dynamic user/password callbacks, token, dynamic token
  callbacks, NKey seed/callback auth, JWT nonce signing, dynamic JWT callbacks
  for reconnect refresh, and NATS user credentials files.
- Server pools with reconnect/resubscribe support for established
  subscriptions, discovered-server tracking, an ignore-discovered-servers
  option, discovered-server callbacks, retry-on-failed-connect startup, plus
  explicit forced reconnect, TLS-specific reconnect jitter, custom
  reconnect-delay and reconnect-to-server callbacks, and `nats.go`-style
  repeated-auth-error reconnect abort with opt-out.
- Publish buffering during reconnect with a configurable byte cap, disabled
  buffering mode, pending-buffer byte inspection, and opt-in
  `nats.go`-style reconnect-on-flusher-error policy for broken writes.
- Connected, async error, reconnect-error, discovered-server, lame-duck-mode,
  disconnected, reconnected, and closed callbacks, with last-error inspection,
  runtime setter/getter helpers for callback handlers, `nats.go`-style
  subscription-aware async error callbacks, connection status-change
  notifications, `nats.go`-style client-close callback suppression, and
  ping-interval/max-pings-out stale connection detection.
- Typed server `-ERR` classification for stale connections, permissions,
  authentication expiry/revocation, max connections, max account connections,
  max subscriptions, and unknown terminal server errors, including reconnect on
  stale/max-connection/auth-expired conditions, aborting reconnect after a
  repeated auth error for the same server unless `ignore_auth_error_abort` is
  enabled, and opt-in `permission_err_on_subscribe` routing for denied
  subscriptions.
- Bounded subscription queues with message and byte pending limits, typed
  slow-consumer errors, max-pending counters, drop counters, delivered counters,
  callback in-flight pending accounting, closed handlers, nonblocking
  subscription status-change notifications, `nats.go`-style
  closed-subscription stat validation, and synchronous `next_msg`
  slow-consumer reporting, plus callback-subscription barriers.
- `NATS.Micro` service helpers for endpoint/group registration, request
  responses, service error headers, PING/INFO/STATS monitor subjects, stats
  reset, endpoint pending limits, queue-group inheritance/disable controls, and
  service stop.
- Uncompressed `ws://` and `wss://` WebSocket transports using
  `HTTP.WebSockets`, including fixed or callback-provided handshake headers
  with multi-value header support and reconnect-time callback refresh, plus
  `nats.go`-style proxy paths for reverse-proxy deployments and discovered
  WebSocket reconnects.
- A small `NATS.JetStream` API-call foundation for streams, typed publish
  acknowledgements, async publish futures with pending/complete/max-pending
  controls, sync publish retry on temporary no-responders, explicit publisher
  cleanup, reconnect/close cleanup of pending async publish futures,
  no-stream retry/error callbacks, duplicate-message headers,
  message metadata, account info, stream create-or-update, info/list/name lookup
  by subject, broader stream/consumer config fields, consumer
  create/create-or-update/info/list/names/update/delete with `nats.go`-style
  create/update action semantics, `nats.go`-style stream/consumer name
  validation, filter-subject validation, typed stream-not-found,
  stream-name-in-use, consumer-not-found, consumer-exists, and
  consumer-does-not-exist management errors, typed overlapping filter-subject
  errors, and `nats.go`-style rejection when a server does not support
  multiple consumer filters, per-call JetStream `domain` or `api_prefix`
  routing for management APIs, raw stream message gets including
  direct get/direct-next support, stream deleted-message details, one-shot and
  reusable pull fetch/next and byte-limited fetch with idle-heartbeat,
  priority-group, min-pending request controls, and cached consumer
  max-request guardrails, reusable subscription live/cached consumer info,
  config-created pull/push subscription consumer cleanup on wrapper close or
  connection drain, synchronous push subscriptions with queue-group delivery,
  idle-heartbeat detection, flow-control responses, and `nats.go`-style
  rejection of queue push subscriptions that request idle heartbeats or flow
  control, client-managed ordered pull consumers with fetch-vs-consume mode and
  concurrent-request guardrails, pull/push/ordered consume contexts/iterators,
  stream message get/delete/purge helpers including
  filtered, sequence, and keep purge modes, direct key-value reads,
  sync/double ack, explicit ack/NAK-with-delay/WPI/TERM-with-reason publishing,
  and AckNone consumer messages that preserve metadata while rejecting ack calls.
- Advanced stream config fields including compression, subject transforms,
  republish, mirrors, sources, external sources, placement, metadata,
  stream-level consumer limits, newer 2.11+ message TTL/scheduling/counter
  toggles, atomic/batched publish toggles, and persist mode.
- Newer consumer config fields including pause deadlines, priority policy,
  priority timeout, and priority groups, plus consumer pause/resume,
  reset-to-ack-floor/reset-to-sequence, and pinned-client unpin API calls.
- Basic JetStream key-value buckets with create/update/create-or-update/open/delete
  bucket, put, create, update, get, get by revision, delete, purge,
  `nats.go`-style create after delete or purge markers, create/purge-marker TTL,
  create-time repair of old discard/allow-direct bucket configs,
  purge-delete-marker cleanup, client-side key and watch-pattern validation,
  `nats.go`-style bucket-not-found, key-not-found, and empty-key errors,
  streaming key listing with wildcard/multi-filter support, history, status,
  and bucket listing,
  bucket metadata/compression/limit-marker status, bucket republish,
  mirror/source bucket configs, plus key watchers with initial-load markers,
  updates-only mode, resume-from-revision replay, and delete filtering.
- Core JetStream object stores with create/open/delete bucket, put/get bytes and
  strings, filename-like object names, multi-chunk objects, SHA-256 digest
  verification with digest format validation, metadata, max-bytes and
  compression status, object delete markers, metadata updates/renames, object
  and bucket links through validating link helpers, watchers, binary-safe file
  helpers backed by streaming `IO` puts, overwrite cleanup of superseded chunks,
  server-supported mirrored object-bucket streams with subject transforms,
  partial-upload cleanup, seal, `nats.go`-style object-not-found and empty-list
  errors for eager object info listing, bucket-not-found errors for missing
  object stores, streaming object info listing, status, and bucket listing that
  ignores non-object streams with only a matching name prefix or only matching
  object chunk subjects.
- Harbor integration tests against `nats:2.10.18` covering core pub/sub,
  request/reply, headers, no responders, JetStream sync/async publish, duplicate
  detection, pull fetch/ack metadata, key-value revisions, history, keys, status,
  delete markers, watchers, object stores, and domain/API-prefix routed
  JetStream management calls, TLS with hostname verification,
  mutual TLS client certificates, TLS-first handshake, WebSockets, secure WebSockets,
  WebSocket proxy paths including discovered-server reconnects,
  CONNECT client name/verbose/pedantic/no-echo behavior,
  user/password, token, NKey auth, JWT auth with a memory account resolver,
  chained credentials files, connected-server metadata including closed-state
  accessors, request mux cleanup and duplicate-reply handling,
  connection and subscription drain, draining callback
  response publishes, final muxed requests during drain, queue groups, callback
  exception reporting, muxed concurrent requests, callback signals, close
  release of pending flush, request, and synchronous `next_msg` calls,
  closed-connection API endpoint parity,
  connection status listeners, forced reconnect, reconnect/resubscribe,
  advanced stream config round-trips, consumer
  management, ordered pull consumers, `nats.go`-style slow-consumer
  notification suppression, and buffered publishes during reconnect, plus
  targeted latest-server coverage against `nats:2.14.2` for newer JetStream
  config flags, publish headers, and priority pull requests.

## Production Readiness

This is a production-oriented client for the common core NATS and JetStream
paths, not a complete clone of every `nats.go` edge. The core reader task owns
protocol demultiplexing, writes are serialized through a dedicated lock, and TLS
is delegated to Reseau with hostname-aware verification. That puts the
foundation in much better shape than the older Julia client.

Known gaps remain:

- remaining slow-consumer edge cases
- WebSocket compression
- richer ordered-consumer edge-case parity, richer key-value parity, and
  advanced object-store edge cases
- a wider port of `nats.go/test` and `nats.go/jetstream/test`, including
  WebSocket compression and reconnect edge cases

## Roadmap

The package intentionally targets the 80-90% most commonly used `nats.go`
behavior before chasing the long tail. Further parity work should focus on:

- Core NATS: flush/drain edge cases, remaining slow-consumer edge cases, queue
  group edge cases, TLS-first failure modes, and STARTTLS edge cases.
- JetStream: richer ordered-consumer edge-case parity, key-value watch edge
  cases, advanced object-store edge cases.
- Tests: port the useful parts of `nats.go/test` and `nats.go/jetstream/test`
  into Julia integration tests using Harbor containers.
