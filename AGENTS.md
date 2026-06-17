# NATS.jl

## Overview

JuliaServices NATS client, built as a focused Julia port of the common
`nats.go` surface. The package keeps a minimal export surface; call APIs through
the `NATS` namespace.

## Package Structure

```text
src/NATS.jl                  main module and dependency loading
src/protocol.jl              NATS wire parser and frame serialization
src/transport.jl             URL parsing plus Reseau TCP/TLS and HTTP WebSocket setup
src/connection.jl            connection state, reader loop, reconnect, pub/sub/request
src/jetstream/JetStream.jl   initial JetStream API helpers
test/runtests.jl             Harbor-backed integration tests
```

## Key Types

- `NATS.Connection`: active client connection with a reader task and
  subscription map.
- `NATS.Subscription`: subscription channel plus optional callback task.
- `NATS.Msg`: delivered core or JetStream message, including headers and reply.
- `NATS.TLSOptions`: Reseau TLS configuration wrapper.
- `NATS.JetStream.StreamConfig` and `NATS.JetStream.ConsumerConfig`: initial
  JetStream config helpers.

## Running Tests

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

Tests use Harbor to run `nats:2.10.18` containers on dynamically allocated host
ports. Docker must be available for the integration suite.

## Development Notes

- Preserve the minimal export surface. Prefer `NATS.function_name` over exports.
- Keep transport behavior aligned with `nats.go`: `tls://` means secure NATS
  with the normal INFO-driven TLS upgrade; use `tls_handshake_first=true` only
  for servers configured to handshake before INFO.
- Use Reseau for TCP/TLS and HTTP.jl 2.x `HTTP.WebSockets` for WebSocket
  transports. Keep `ws://` and `wss://` behavior aligned with `nats.go`; do not
  mix WebSocket and non-WebSocket URLs in one server pool.
- Add tests by porting the relevant `nats.go` behavior first, then adapting the
  Julia API shape where needed.
