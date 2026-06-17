# NATS.jl

JuliaServices NATS client for Julia, started as a production-oriented port of
the common `nats.go` client surface.

This package is intentionally built around:

- `Reseau.TCP` and `Reseau.TLS` for core NATS TCP/TLS transports.
- `HTTP.WebSockets` for `ws://` and `wss://` NATS transports.
- Harbor-managed NATS server containers for integration tests.
- A minimal export surface: call APIs through the `NATS` namespace.

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
