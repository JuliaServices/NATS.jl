# Receive-path comparison

Run from the repository root after `Pkg.instantiate()`:

```sh
julia --startup-file=no --threads=1 --project=. benchmark/receive.jl
```

The script alternates warmed unbuffered/buffered runs in the same process. It checks message counts and payload sizes, then prints every sample and the median/range of wall time and GC time, plus allocated bytes. No NATS server or Docker is needed.

The timed boundary is local TCP transfer, background protocol parsing, subscription/channel delivery and public `NATS.next_msg`. It includes reader startup and its 16 KiB buffer. Connection/subscription setup, NATS server routing, TLS and remote-network effects are excluded. Both paths use the same package parser and message-delivery methods. The unbuffered reference loop is only a timing control; it is not a reconnect implementation.

Use a longer stream to amortize reader startup and examine natural GC:

```sh
NATS_LARGE_HMSG_ONLY=1 NATS_LARGE_COUNT=128 NATS_REPS=7 \
  julia --startup-file=no --threads=1 --project=. benchmark/receive.jl
```

This streams 128 messages with 1 MiB payloads per sample, using about 128 MiB for the wire fixture. `NATS_REPS` controls the number of alternating pairs (default 7), and `NATS_LARGE_COUNT` controls the number of large messages (default 8). The default matrix also includes small messages and 64 KiB payloads.

For a separate GC-placement diagnostic, set `NATS_GC_BEFORE_SAMPLE=1`. That runs a full collection after setup and before each timed sample; it must not be presented as natural-GC throughput. Keep both measurements, especially for short streams where one collection can dominate the timing. Natural-GC large-body results can differ by runtime and heap state, and local coalesced small-frame gains do not imply a general remote-network or large-body speedup.
