mutable struct FragmentedProtocolIO
    bytes::IOBuffer
    chunk::Int
    reads::Int
end

function Base.readbytes!(io::FragmentedProtocolIO, bytes::AbstractVector{UInt8}, nb::Integer = length(bytes); all::Bool = true)
    io.reads += 1
    n = min(Int(nb), bytesavailable(io.bytes), all ? Int(nb) : io.chunk)
    for i in 1:n
        bytes[i] = read(io.bytes, UInt8)
    end
    return n
end

# `chunk` caps each short transport read; `capacity` sizes the read-ahead buffer.
function protocol_reader(bytes; chunk = typemax(Int), capacity = 16 * 1024)
    io = FragmentedProtocolIO(IOBuffer(bytes), chunk, 0)
    return NATS.ProtocolReader(io, Vector{UInt8}(undef, capacity), 1, 0)
end

function reader_connection(io; status = NATS.CONNECTED, kwargs...)
    server = NATS.parse_server_url("nats://reader.example:4222")
    options = NATS.Options(; allow_reconnect = false, write_buffer_size = 0, kwargs...)
    return NATS.new_connection(server, [server], options, io, NATS.ServerInfo(), status; connected_once = true)
end

function reader_message_bytes(data; headers = false)
    io = IOBuffer()
    if headers
        header = "NATS/1.0 201 Created\r\nX-Test: α\r\nX-Test: β\r\n\r\n"
        write(io, "HMSG test.subject 17 reply.inbox $(sizeof(header)) $(sizeof(header) + length(data))\r\n", header)
    else
        write(io, "MSG test.subject 17 reply.inbox $(length(data))\r\n")
    end
    write(io, data, "\r\n")
    return take!(io)
end

@testset "buffered protocol reads" begin
    # Ported from nats.go TestParserPing and TestParserSplitMsg: control lines
    # and payloads split across transport reads and buffer refills.
    payloads = (UInt8[], UInt8[0, 0xff, 0x0d, 0x0a], UInt8[mod(i, 251) for i in 1:32771])
    frames = [reader_message_bytes(data; headers) for headers in (false, true) for data in payloads]
    wire = vcat(codeunits("PING\r\n"), frames..., codeunits("PONG\r\n"))
    for chunk in (1, 7, typemax(Int)), capacity in (17, 16 * 1024)
        reader = protocol_reader(wire; chunk, capacity)
        @test NATS.read_protocol_message(reader) isa NATS.Ping
        messages = [NATS.read_protocol_message(reader) for _ in frames]
        @test NATS.read_protocol_message(reader) isa NATS.Pong
        @test_throws EOFError NATS.read_protocol_message(reader)
        for (i, msg) in enumerate(messages)
            @test msg.subject == "test.subject"
            @test msg.sid == 17
            @test msg.reply == "reply.inbox"
            @test msg.data == payloads[mod1(i, 3)]
            @test msg.headers == (i <= 3 ? Pair{String,String}[] : ["X-Test" => "α", "X-Test" => "β"])
            @test msg.status == (i <= 3 ? 200 : 201)
            @test msg.description == (i <= 3 ? "" : "Created")
        end
        # Each payload owns its bytes; later reads must not reuse them.
        messages[2].data[1] = 0x42
        @test messages[5].data == payloads[2]
        @test messages[3].data == payloads[3]
    end

    # Every truncated frame, including a partial payload CRLF, fails the same
    # way buffered as unbuffered.
    short_frames = vcat([collect(codeunits("PING\r\n"))], frames[[1, 2, 4, 5]])
    for frame in short_frames, stop in 0:(length(frame) - 1), chunk in (1, 7, typemax(Int))
        prefix = frame[1:stop]
        expected = try NATS.read_protocol_message(IOBuffer(prefix)) catch err; err end
        actual = try NATS.read_protocol_message(protocol_reader(prefix; chunk)) catch err; err end
        @test expected isa Exception
        @test typeof(actual) === typeof(expected)
    end

    for text in ("MSG x -1 0\r\n\r\n", "MSG x 1 nope\r\n", "MSG x 1 3\r\nabc!\n", repeat("x", 4097))
        @test_throws NATS.ProtocolError NATS.read_protocol_message(protocol_reader(text))
    end

    reader = protocol_reader(repeat("PING\r\n", 1000))
    for _ in 1:1000
        @test NATS.read_protocol_message(reader) isa NATS.Ping
    end
    @test reader.io.reads == 1

    # A large read takes the buffered prefix, then reads the rest directly.
    reader = protocol_reader(UInt8[mod(i, 251) for i in 1:100000])
    @test read(reader, UInt8) == 0x01
    @test read(reader, 99999) == UInt8[mod(i, 251) for i in 2:100000]
    @test reader.io.reads == 2
end

# Pauses the reader task after each parsed message, then optionally fails the read.
struct PausedProtocolReader{T}
    reader::T
    parsed::Channel{Nothing}
    resume::Channel{Nothing}
    failure::Union{Nothing, Exception}
end

PausedProtocolReader(text::String, failure = nothing) =
    PausedProtocolReader(protocol_reader(text), Channel{Nothing}(1), Channel{Nothing}(1), failure)

function NATS.read_protocol_message(io::PausedProtocolReader)
    msg = NATS.read_protocol_message(io.reader)
    put!(io.parsed, nothing)
    take!(io.resume)
    io.failure === nothing || throw(io.failure)
    return msg
end

@testset "retired readers cannot affect replacement transports" begin
    for (text, failure) in (("PING\r\n", nothing), ("MSG old 1 0\r\n\r\n", nothing), ("PING\r\n", EOFError()))
        old = PausedProtocolReader(text * "PING\r\n", failure)
        replacement = CountingWriteIO(Vector{UInt8}[], false)
        conn = reader_connection(old)
        task = @async NATS.reader_loop(conn, old)
        try
            wait_ready(old.parsed)
            lock(conn.lock) do
                conn.io = replacement
            end
            put!(old.resume, nothing)
            @test timedwait(() -> istaskdone(task), 2; pollint = 0.001) == :ok
            @test !istaskfailed(task)
            @test NATS.connection_status(conn) == NATS.CONNECTED
            @test !replacement.closed
            @test isempty(replacement.writes)
            @test NATS.stats(conn).in_msgs == 0
        finally
            isready(old.resume) || put!(old.resume, nothing)
            NATS.close(conn)
        end
    end

    # Reconnect can retire a read before its replacement is installed.
    old = PausedProtocolReader("MSG old 1 0\r\n\r\n")
    conn = reader_connection(old)
    task = @async NATS.reader_loop(conn, old)
    try
        wait_ready(old.parsed)
        NATS.set_connection_status!(conn, NATS.RECONNECTING)
        put!(old.resume, nothing)
        @test timedwait(() -> istaskdone(task), 2; pollint = 0.001) == :ok
        @test !istaskfailed(task)
        @test NATS.stats(conn).in_msgs == 0
    finally
        isready(old.resume) || put!(old.resume, nothing)
        NATS.close(conn)
    end
end

@testset "reader actions recheck retirement after waiting for a lock" begin
    for (text, reconnect) in (("PING\r\n", false), ("PONG\r\n", false),
            ("broken\r\n", false), ("broken\r\n", true),
            ("-ERR 'Authorization Violation'\r\n", false), ("-ERR 'Stale Connection'\r\n", true))
        old = protocol_reader(text)
        replacement = CountingWriteIO(Vector{UInt8}[], false)
        conn = reader_connection(old; allow_reconnect = reconnect, max_reconnect = 0)
        gate = startswith(text, "PONG") ? conn.lock : conn.write_lock
        lock(gate)
        task = @async NATS.reader_loop(conn, old)
        try
            # Wait until the reader is blocked on `gate`.
            @test timedwait(() -> !isempty(gate.cond_wait.waitq), 30; pollint = 0.001) == :ok
            lock(conn.lock) do
                conn.io = replacement
                conn.pings_out = 7
                append!(conn.write_buffer, UInt8[1, 2])
                push!(conn.pending_frames, UInt8[3, 4])
                conn.pending_bytes = 2
            end
        finally
            unlock(gate)
        end
        try
            @test timedwait(() -> istaskdone(task), 2; pollint = 0.001) == :ok
            @test !istaskfailed(task)
            @test NATS.connection_status(conn) == NATS.CONNECTED
            @test !replacement.closed
            @test isempty(replacement.writes)
            @test conn.pings_out == 7
            @test !isready(conn.pongs)
            @test conn.write_buffer == UInt8[1, 2]
            @test conn.pending_frames == [UInt8[3, 4]]
            @test conn.pending_bytes == 2
            @test !isready(conn.async_errors)
            @test conn.reconnect_task === nothing
        finally
            NATS.close(conn)
        end
    end
end

@testset "PING while draining gets a PONG" begin
    io = CountingWriteIO(Vector{UInt8}[], false)
    conn = reader_connection(io; status = NATS.DRAINING)
    try
        NATS.handle_protocol_message(conn, NATS.Ping(), io)
        @test io.writes == [NATS.pong_frame()]
        @test NATS.connection_status(conn) == NATS.DRAINING
    finally
        NATS.close(conn)
    end
end

function test_buffered_delivery(conn, subject)
    sub = NATS.subscribe(conn, subject)
    payloads = (UInt8[], UInt8[0, 0xff, 0x0d, 0x0a], UInt8[mod(i, 251) for i in 1:32771])
    headers = ["X-Test" => "α", "X-Test" => "β"]
    try
        for with_headers in (false, true), data in payloads
            NATS.publish(conn, subject, data; headers = with_headers ? headers : Pair{String,String}[])
        end
        NATS.flush(conn)
        messages = [NATS.next_msg(sub; timeout = 2) for _ in 1:6]
        for (i, msg) in enumerate(messages)
            @test msg.data == payloads[mod1(i, 3)]
            @test msg.headers == (i <= 3 ? Pair{String,String}[] : headers)
        end
        messages[2].data[1] = 0x42
        @test messages[5].data == payloads[2]
        @test messages[3].data == payloads[3]
    finally
        NATS.unsubscribe(conn, sub)
    end
end
