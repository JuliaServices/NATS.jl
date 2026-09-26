# Times local TCP delivery through `NATS.reader_loop` and `NATS.next_msg`, with
# the buffered reads used for TCP and TLS against byte-at-a-time reads. Runs
# alternate between the two; large-payload timings are GC-sensitive. No NATS
# server is needed:
#
#     julia --project=. benchmark/receive.jl
using NATS, Printf, Reseau, Sockets, Statistics

# Hides the TCP type from `NATS.reader_loop`, which then reads unbuffered.
struct Unbuffered{T}
    io::T
end

Base.read(u::Unbuffered, ::Type{UInt8}) = read(u.io, UInt8)
Base.read(u::Unbuffered, n::Integer) = read(u.io, n)
Base.write(u::Unbuffered, data::AbstractVector{UInt8}) = write(u.io, data)
Base.flush(u::Unbuffered) = flush(u.io)
Base.close(u::Unbuffered) = close(u.io)

function wire_frames(kind, payload_size, n)
    data = zeros(UInt8, payload_size)
    header = "NATS/1.0\r\nX-Test: one\r\nX-Test: two\r\n\r\n"
    io = IOBuffer()
    for _ in 1:n
        if kind == :msg
            write(io, "MSG subject.name 1 reply.inbox $payload_size\r\n", data, "\r\n")
        else
            write(io, "HMSG subject.name 1 reply.inbox $(sizeof(header)) $(sizeof(header) + payload_size)\r\n", header, data, "\r\n")
        end
    end
    return take!(io)
end

function sample(wrap, wire, n)
    listener = Sockets.listen(ip"127.0.0.1", 0)
    port = Sockets.getsockname(listener)[2]
    start = Channel{Nothing}(1)
    server = @async begin
        peer = Sockets.accept(listener)
        take!(start)
        write(peer, wire)
        peer
    end
    io = wrap(Reseau.TCP.connect("127.0.0.1:$port"))
    url = NATS.parse_server_url("nats://127.0.0.1:$port")
    conn = NATS.new_connection(url, [url], NATS.Options(allow_reconnect = false, write_buffer_size = 0),
        io, NATS.ServerInfo(headers = true), NATS.CONNECTED; connected_once = true)
    sub = NATS.subscribe(conn, "subject.name"; channel_size = n)
    try
        return @timed begin
            errormonitor(@async NATS.reader_loop(conn, io))
            put!(start, nothing)
            sum(_ -> length(NATS.next_msg(sub).data), 1:n)
        end
    finally
        NATS.close(conn)
        close(fetch(server))
        close(listener)
    end
end

const REPS = 7
println("Julia ", VERSION, ", Reseau ", pkgversion(Reseau), "; medians of ", REPS, " runs")
for (kind, bytes, n) in ((:msg, 0, 1000), (:msg, 64, 1000), (:hmsg, 64, 1000), (:msg, 65536, 128), (:hmsg, 1048576, 8))
    wire = wire_frames(kind, bytes, n)
    runs = Dict(Unbuffered => [], identity => [])
    for rep in 0:REPS, wrap in (isodd(rep) ? (Unbuffered, identity) : (identity, Unbuffered))
        run = sample(wrap, wire, n)
        run.value == bytes * n || error("delivered $(run.value) of $(bytes * n) payload bytes")
        rep > 0 && push!(runs[wrap], run) # rep 0 warms up
    end
    for (label, wrap) in (("unbuffered", Unbuffered), ("buffered", identity))
        @printf("%-4s %7d B x %4d  %-10s %8.3f ms (gc %6.3f ms) %10d bytes allocated\n", kind, bytes, n, label,
            1000median(r.time for r in runs[wrap]), 1000median(r.gctime for r in runs[wrap]), median(r.bytes for r in runs[wrap]))
    end
end
