using NATS, Reseau, Sockets, Printf, Test

# Unbuffered reference loop. Both paths share the package parser and delivery
# methods; this fixture has no handshake, reconnect or protocol control frames.
function raw_reader_loop(conn::NATS.Connection, _io)
    try
        while conn.status != NATS.CLOSED
            msg = NATS.read_protocol_message(conn.io)
            NATS.handle_protocol_message(conn, msg)
        end
    catch err
        conn.status == NATS.CLOSED && return nothing
        conn.status == NATS.RECONNECTING && return nothing
        if conn.options.allow_reconnect && conn.status != NATS.DRAINING
            NATS.start_reconnect!(conn, err)
        else
            NATS.close_after_terminal_error!(conn, err)
        end
    end
    return nothing
end

function wire_fixture(kind, payload_size, n)
    data = zeros(UInt8, payload_size)
    header = "NATS/1.0\r\nX-Test: one\r\nX-Test: two\r\n\r\n"
    io = IOBuffer()
    for _ in 1:n
        if kind == :msg
            write(io, "MSG subject.name 7 reply.inbox $payload_size\r\n", data, "\r\n")
        else
            write(io, "HMSG subject.name 7 reply.inbox $(sizeof(header)) $(sizeof(header) + payload_size)\r\n", header, data, "\r\n")
        end
    end
    return take!(io)
end

function consume(sub, n)
    bytes = 0
    for _ in 1:n
        bytes += length(NATS.next_msg(sub).data)
    end
    return bytes
end

function sample(loop, wire, n)
    listener = Sockets.listen(ip"127.0.0.1", 0)
    port = Sockets.getsockname(listener)[2]
    start, finish = Channel{Nothing}(1), Channel{Nothing}(1)
    server = @async begin
        peer = Sockets.accept(listener)
        try
            take!(start)
            write(peer, wire)
            take!(finish)
        finally
            close(peer)
        end
    end
    io = Reseau.TCP.connect("127.0.0.1:$port")
    url = NATS.parse_server_url("nats://127.0.0.1:$port")
    conn = NATS.new_connection(url, [url], NATS.Options(allow_reconnect = false, write_buffer_size = 0),
        io, NATS.ServerInfo(headers = true), NATS.CONNECTED; connected_once = true)
    conn.next_sid = 6
    sub = NATS.subscribe(conn, "subject.name"; channel_size = n + 1)
    reader_task = nothing
    try
        get(ENV, "NATS_GC_BEFORE_SAMPLE", "0") == "1" && GC.gc()
        result = @timed begin
            reader_task = @async loop(conn, io)
            put!(start, nothing)
            consume(sub, n)
        end
        @test NATS.stats(conn).in_msgs == n
        return result
    finally
        put!(finish, nothing)
        NATS.close(conn)
        wait(server)
        reader_task === nothing || wait(reader_task)
        close(listener)
    end
end

println("Julia ", VERSION, "; threads=", Threads.nthreads(), "; Reseau ", pkgversion(Reseau))
println("Boundary: TCP transfer + background reader + dispatch/channel + public next_msg; setup, NATS server routing and TLS excluded.")
println("Baseline is an unbuffered reference loop; candidate uses NATS.reader_loop.")
println("GC before each timed sample: ", get(ENV, "NATS_GC_BEFORE_SAMPLE", "0") == "1")
large_count = parse(Int, get(ENV, "NATS_LARGE_COUNT", "8"))
for (kind, bytes, n) in ((:msg, 0, 1000), (:msg, 64, 1000), (:hmsg, 64, 1000), (:msg, 65536, 128), (:msg, 1048576, large_count), (:hmsg, 1048576, large_count))
    get(ENV, "NATS_LARGE_HMSG_ONLY", "0") == "1" && !(kind == :hmsg && bytes == 1048576) && continue
    wire = wire_fixture(kind, bytes, n)
    for loop in (raw_reader_loop, NATS.reader_loop)
        @test sample(loop, wire, n).value == bytes * n
    end
    results = Dict(mode => NamedTuple[] for mode in (:raw, :buffered))
    for rep in 1:parse(Int, get(ENV, "NATS_REPS", "7"))
        for mode in (isodd(rep) ? (:raw, :buffered) : (:buffered, :raw))
            result = sample(mode == :raw ? raw_reader_loop : NATS.reader_loop, wire, n)
            @test result.value == bytes * n
            push!(results[mode], (; result.time, result.bytes, result.gctime))
        end
    end
    for mode in (:raw, :buffered)
        med(v) = sort(v)[cld(length(v), 2)]
        samples = results[mode]
        @printf("%s payload=%d n=%d %s ms=%.3f alloc=%d gc_ms=%.3f\n", kind, bytes, n, mode,
            1000med([s.time for s in samples]), med([s.bytes for s in samples]), 1000med([s.gctime for s in samples]))
        @printf("  range_ms=[%.3f, %.3f] gc_range_ms=[%.3f, %.3f] median_non_gc_ms=%.3f\n",
            1000minimum(s.time for s in samples), 1000maximum(s.time for s in samples),
            1000minimum(s.gctime for s in samples), 1000maximum(s.gctime for s in samples),
            1000med([s.time - s.gctime for s in samples]))
        for (i, s) in enumerate(samples)
            @printf("  sample=%d wall_ms=%.6f gc_ms=%.6f alloc=%d\n", i, 1000s.time, 1000s.gctime, s.bytes)
        end
    end
end
