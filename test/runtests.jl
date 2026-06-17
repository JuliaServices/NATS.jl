using Test
using Base64
using NATS
using NATS.JetStream
using Harbor
using HTTP
using JSON3
using Random
using Reseau
using SHA
using Sockets

const NKEY_TEST_SEED = "SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY"
const JWT_OPERATOR_SEED = "SOAL7GTNI66CTVVNXBNQMG6V2HTDRWC3HGEP7D2OUTWNWSNYZDXWFOX4SU"
const JWT_ACCOUNT_SEED = "SAAASUPRY3ONU4GJR7J5RUVYRUFZXG56F4WEXELLLORQ65AEPSMIFTOJGE"
const JWT_USER_SEED = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY"
const JWT_ACCOUNT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJLWjZIUVRXRlY3WkRZSFo3NklRNUhPM0pINDVRNUdJS0JNMzJTSENQVUJNNk5PNkU3TUhRIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJPRDJXMkk0TVZSQTVUR1pMWjJBRzZaSEdWTDNPVEtGV1FKRklYNFROQkVSMjNFNlA0NlMzNDVZWSIsInN1YiI6IkFBUFFKUVVQS1ZYR1c1Q1pINUcySEZKVUxZU0tERUxBWlJWV0pBMjZWRFpPN1dTQlVOSVlSRk5RIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.8o35JPQgvhgFT84Bi2Z-zAeSiLrzzEZn34sgr1DIBEDTwa-EEiMhvTeos9cvXxoZVCCadqZxAWVwS6paAMj8Bg"
const JWT_USER = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBSFQzRzNXRElDS1FWQ1FUWFJUTldPRlVVUFRWNE00RFZQV0JGSFpJQUROWEZIWEpQR0FBIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJBQVBRSlFVUEtWWEdXNUNaSDVHMkhGSlVMWVNLREVMQVpSVldKQTI2VkRaTzdXU0JVTklZUkZOUSIsInN1YiI6IlVBVDZCV0NTQ1dMVUtKVDZLNk1CSkpPRU9UWFo1QUpET1lLTkVWUkZDN1ZOTzZPQTQzTjRUUk5PIiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ._8A1XM88Q2kp7XVJZ42bQuO9E3QPsNAGKtVjAkDycj8A5PtRPby9UpqBUZzBwiJQQO3TUcD5GGqSvsMm6X8hCQ"
const JWT_CHAINED_CREDENTIALS = """
-----BEGIN NATS USER JWT-----
$JWT_USER
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
$JWT_USER_SEED
------END USER NKEY SEED------
"""

@testset "connected server metadata accessors" begin
    server = NATS.parse_server_url("nats://metadata.example:4222")
    conn = NATS.new_connection(
        server,
        [server],
        NATS.Options(),
        IOBuffer(),
        NATS.ServerInfo(
            server_id = "sid",
            server_name = "sname",
            version = "2.10.18",
            cluster = "C1",
            client_id = UInt64(42),
            client_ip = "127.0.0.1",
            jetstream = true,
            api_lvl = 3,
            acc_is_sys = true,
            auth_required = true,
            tls_required = true,
            tls_available = true,
            tls_verify = true,
            max_payload = 1024,
        ),
        NATS.CONNECTED;
        connected_once = true,
    )

    @test NATS.connected_url(conn) == "nats://metadata.example:4222"
    @test NATS.connected_server_id(conn) == "sid"
    @test NATS.connected_server_name(conn) == "sname"
    @test NATS.connected_server_version(conn) == "2.10.18"
    @test NATS.connected_cluster_name(conn) == "C1"
    @test NATS.connected_client_id(conn) == UInt64(42)
    @test NATS.connected_client_ip(conn) == "127.0.0.1"
    @test NATS.connected_server_jetstream(conn) == (true, 3)
    @test NATS.is_system_account(conn)
    @test NATS.auth_required(conn)
    @test NATS.tls_required(conn)
    @test NATS.tls_available(conn)
    @test NATS.tls_verify(conn)
    @test NATS.max_payload(conn) == 1024

    NATS.set_connection_status!(conn, NATS.CLOSED)
    @test NATS.connected_url(conn) == ""
    @test NATS.connected_server_id(conn) == ""
    @test NATS.connected_server_name(conn) == ""
    @test NATS.connected_server_version(conn) == ""
    @test NATS.connected_cluster_name(conn) == ""
    @test NATS.connected_client_id(conn) === nothing
    @test NATS.connected_client_ip(conn) == ""
    @test NATS.connected_server_jetstream(conn) == (false, 0)
    @test !NATS.is_system_account(conn)
    @test !NATS.auth_required(conn)
    @test !NATS.tls_required(conn)
    @test !NATS.tls_available(conn)
    @test !NATS.tls_verify(conn)
    @test NATS.max_payload(conn) == 0
end

@testset "max payload publish guard" begin
    server = NATS.parse_server_url("nats://max.example:4222")
    io = IOBuffer()
    conn = NATS.new_connection(
        server,
        [server],
        NATS.Options(),
        io,
        NATS.ServerInfo(headers = true, max_payload = 10),
        NATS.CONNECTED;
        connected_once = true,
    )

    NATS.publish(conn, "natsjl.max", "1234567890")
    @test String(take!(io)) == "PUB natsjl.max 10\r\n1234567890\r\n"

    oversized = try
        NATS.publish(conn, "natsjl.max", "hello world")
        nothing
    catch err
        err
    end
    @test oversized isa NATS.MaxPayloadError
    @test oversized.max_payload == 10
    @test oversized.payload_size == 11
    @test isempty(take!(io))

    msg = NATS.new_msg("natsjl.max.msg", "hello world")
    @test_throws NATS.MaxPayloadError NATS.publish_msg(conn, msg)
    @test isempty(take!(io))

    headers = ["X-NATS-Test" => "1234567890"]
    header_oversized = try
        NATS.publish(conn, "natsjl.max.headers", "a"; headers)
        nothing
    catch err
        err
    end
    @test header_oversized isa NATS.MaxPayloadError
    @test header_oversized.max_payload == 10
    @test header_oversized.payload_size == 1 + length(NATS.headers_bytes(headers))
    @test isempty(take!(io))
end

@testset "inbox and publish request helpers" begin
    inbox = NATS.new_inbox()
    @test startswith(inbox, "_INBOX.")
    @test length(split(inbox, ".")) == 2

    seeded_a = NATS.new_inbox("\$BOB"; rng = MersenneTwister(1))
    seeded_b = NATS.new_inbox("\$BOB"; rng = MersenneTwister(1))
    @test seeded_a == seeded_b
    @test startswith(seeded_a, "\$BOB.")
    @test length(split(seeded_a, ".")) == 2

    for bad_prefix in ("", ".", "_INBOX.", "_INBOX.*", "_INBOX.>", "_INBOX..BAD", "bad prefix")
        @test_throws ArgumentError NATS.new_inbox(bad_prefix)
    end

    server = NATS.parse_server_url("nats://publish-request.example:4222")
    io = IOBuffer()
    conn = NATS.new_connection(
        server,
        [server],
        NATS.Options(),
        io,
        NATS.ServerInfo(headers = true, max_payload = 1024),
        NATS.CONNECTED;
        connected_once = true,
    )

    NATS.publish_request(conn, "natsjl.pubreq", "_INBOX.reply", "hello")
    @test String(take!(io)) == "PUB natsjl.pubreq _INBOX.reply 5\r\nhello\r\n"
    stats = NATS.stats(conn)
    @test stats.out_msgs == 1
    @test stats.out_bytes == UInt64(sizeof("hello"))

    @test_throws NATS.BadSubjectError NATS.publish_request(conn, "natsjl.pubreq", "bad reply", "hello")
    @test isempty(take!(io))
end

function free_port()
    server = listen(ip"127.0.0.1", 0)
    try
        return Int(last(getsockname(server)))
    finally
        close(server)
    end
end

function with_nats(f; tag = "2.10.18")
    port = free_port()
    Harbor.with_container("nats"; tag, ports = Dict(4222 => port), command = ["--jetstream"]) do _container
        sleep(1)
        f("nats://localhost:$port")
    end
end

function with_domain_nats(f; domain = "ABC", tag = "2.10.18")
    port = free_port()
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        jetstream: { domain: $domain }
        """)
        Harbor.with_container(
            "nats";
            tag,
            ports = Dict(4222 => port),
            volumes = Dict("/config" => dir),
            command = ["-c", "/config/nats.conf"],
        ) do _container
            sleep(1)
            f("nats://localhost:$port", String(domain))
        end
    end
end

function with_nats_container(f; command = ["--jetstream"], tag = "2.10.18")
    port = free_port()
    Harbor.with_container("nats"; tag, ports = Dict(4222 => port), command) do container
        sleep(1)
        f(container, "nats://localhost:$port", port)
    end
end

function with_tls_nats(f)
    port = free_port()
    certs = abspath(joinpath(@__DIR__, "certs"))
    Harbor.with_container(
        "nats";
        tag = "2.10.18",
        ports = Dict(4222 => port),
        volumes = Dict("/certs" => certs),
        command = [
            "--tls",
            "--tlscert", "/certs/server.pem",
            "--tlskey", "/certs/key.pem",
            "--jetstream",
        ],
    ) do _container
        sleep(1)
        f(certs, "tls://localhost:$port")
    end
end

function with_mtls_nats(f)
    port = free_port()
    certs = abspath(joinpath(@__DIR__, "certs"))
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        tls {
          cert_file: "/certs/server.pem"
          key_file: "/certs/key.pem"
          ca_file: "/certs/ca.pem"
          verify: true
          timeout: 2
        }
        """)
        Harbor.with_container(
            "nats";
            tag = "2.10.18",
            ports = Dict(4222 => port),
            volumes = Dict("/config" => dir, "/certs" => certs),
            command = ["-c", "/config/nats.conf", "--jetstream"],
        ) do _container
            sleep(1)
            f(certs, "tls://localhost:$port")
        end
    end
end

function with_tls_first_mock(f)
    certs = abspath(joinpath(@__DIR__, "certs"))
    server_config = Reseau.TLS.Config(
        cert_file = joinpath(certs, "server.pem"),
        key_file = joinpath(certs, "key.pem"),
        handshake_timeout_ns = 2_000_000_000,
    )
    listener = Reseau.TLS.listen("tcp", "127.0.0.1:0", server_config; backlog = 8)
    port = Int(Reseau.TLS.addr(listener).port)
    server_result = Channel{Any}(1)
    server_task = errormonitor(@async begin
        conn = nothing
        try
            conn = Reseau.TLS.accept(listener)
            Reseau.TLS.handshake!(conn)
            write(conn, "INFO {\"server_id\":\"tls-first\",\"server_name\":\"tls-first\",\"version\":\"2.10.18\",\"go\":\"go1.22\",\"host\":\"localhost\",\"port\":$port,\"headers\":true,\"proto\":1,\"auth_required\":false,\"tls_required\":true,\"tls_available\":true,\"max_payload\":1048576}\r\n")
            flush(conn)
            connect_line = NATS.readline_crlf(conn)
            ping_line = NATS.readline_crlf(conn)
            write(conn, "PONG\r\n")
            flush(conn)
            put!(server_result, (connect_line, ping_line))
        catch err
            put!(server_result, err)
        finally
            conn === nothing || try close(conn) catch end
        end
    end)
    try
        f(certs, "tls://localhost:$port", server_result)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_no_headers_mock(f)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            sock = accept(listener)
            write(sock, "INFO {\"server_id\":\"no-headers\",\"server_name\":\"no-headers\",\"version\":\"2.10.18\",\"go\":\"go1.22\",\"host\":\"127.0.0.1\",\"port\":$port,\"headers\":false,\"proto\":1,\"auth_required\":false,\"max_payload\":1048576}\r\n")
            flush(sock)
            NATS.readline_crlf(sock)
            ping_line = NATS.readline_crlf(sock)
            ping_line == "PING" && write(sock, "PONG\r\n")
            flush(sock)
            while true
                line = NATS.readline_crlf(sock)
                put!(events, line)
                if line == "PING"
                    write(sock, "PONG\r\n")
                    flush(sock)
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError)
                put!(events, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        f("nats://127.0.0.1:$port", events)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function write_mock_info(sock, port::Integer; server_id::AbstractString = "mock", nonce = nothing)
    info = Dict{String, Any}(
        "server_id" => server_id,
        "server_name" => server_id,
        "version" => "2.10.18",
        "go" => "go1.22",
        "host" => "127.0.0.1",
        "port" => port,
        "headers" => true,
        "proto" => 1,
        "auth_required" => false,
        "max_payload" => 1048576,
    )
    nonce === nothing || (info["nonce"] = nonce)
    write(sock, "INFO $(JSON3.write(info))\r\n")
    flush(sock)
    return nothing
end

function read_mock_connect_ping(sock)
    connect_line = NATS.readline_crlf(sock)
    ping_line = NATS.readline_crlf(sock)
    ping_line == "PING" && write(sock, "PONG\r\n")
    flush(sock)
    return connect_line, ping_line
end

function read_mock_connect_ping_without_pong(sock)
    connect_line = NATS.readline_crlf(sock)
    ping_line = NATS.readline_crlf(sock)
    return connect_line, ping_line
end

function with_server_error_mock(f, message::AbstractString; handshake::Bool = false)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    ready = Channel{Any}(1)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            sock = accept(listener)
            write_mock_info(sock, port; server_id = "server-error")
            connect_line = NATS.readline_crlf(sock)
            put!(events, connect_line)
            ping_line = NATS.readline_crlf(sock)
            put!(events, ping_line)
            if handshake
                write(sock, "-ERR '$message'\r\n")
                flush(sock)
                put!(ready, true)
                return
            end
            ping_line == "PING" && write(sock, "PONG\r\n")
            flush(sock)
            write(sock, "-ERR '$message'\r\n")
            flush(sock)
            put!(ready, true)
            while true
                line = NATS.readline_crlf(sock)
                put!(events, line)
                if line == "PING"
                    write(sock, "PONG\r\n")
                    flush(sock)
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        f("nats://127.0.0.1:$port", events, ready)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_repeated_auth_error_mock(f, message::AbstractString)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    trigger = Channel{Any}(1)
    server_task = errormonitor(@async begin
        attempt = 0
        try
            while true
                sock = accept(listener)
                attempt += 1
                try
                    write_mock_info(sock, port; server_id = "repeated-auth-$attempt", nonce = "nonce-$attempt")
                    if attempt == 1
                        connect_line, ping_line = read_mock_connect_ping(sock)
                        put!(events, (attempt, connect_line, ping_line))
                        take!(trigger)
                        write(sock, "-ERR '$message'\r\n")
                    else
                        connect_line, ping_line = read_mock_connect_ping_without_pong(sock)
                        put!(events, (attempt, connect_line, ping_line))
                        write(sock, "-ERR '$message'\r\n")
                    end
                    flush(sock)
                finally
                    try close(sock) catch end
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        end
    end)
    try
        f("nats://127.0.0.1:$port", port, events, trigger)
    finally
        try put!(trigger, true) catch end
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_reconnect_error_mock(f, message::AbstractString)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    trigger = Channel{Any}(1)
    server_task = errormonitor(@async begin
        try
            for attempt in 1:2
                sock = accept(listener)
                try
                    write_mock_info(sock, port; server_id = "reconnect-error-$attempt")
                    connect_line, ping_line = read_mock_connect_ping(sock)
                    put!(events, (attempt, connect_line, ping_line))
                    if attempt == 1
                        take!(trigger)
                        write(sock, "-ERR '$message'\r\n")
                        flush(sock)
                    else
                        while true
                            line = NATS.readline_crlf(sock)
                            put!(events, line)
                            if line == "PING"
                                write(sock, "PONG\r\n")
                                flush(sock)
                            end
                        end
                    end
                finally
                    try close(sock) catch end
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        end
    end)
    try
        f("nats://127.0.0.1:$port", port, events, trigger)
    finally
        try put!(trigger, true) catch end
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_auth_expired_jwt_refresh_mock(f)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    trigger = Channel{Any}(1)
    server_task = errormonitor(@async begin
        try
            for attempt in 1:2
                sock = accept(listener)
                try
                    write_mock_info(sock, port; server_id = "jwt-refresh-$attempt", nonce = "nonce-$attempt")
                    connect_line, ping_line = read_mock_connect_ping(sock)
                    put!(events, (attempt, connect_line, ping_line))
                    if attempt == 1
                        take!(trigger)
                        write(sock, "-ERR 'User Authentication Expired'\r\n")
                        flush(sock)
                    else
                        while true
                            line = NATS.readline_crlf(sock)
                            put!(events, line)
                            if line == "PING"
                                write(sock, "PONG\r\n")
                                flush(sock)
                            end
                        end
                    end
                finally
                    try close(sock) catch end
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        end
    end)
    try
        f("nats://127.0.0.1:$port", port, events, trigger)
    finally
        try put!(trigger, true) catch end
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_subscription_permission_mock(f)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            sock = accept(listener)
            write_mock_info(sock, port; server_id = "permission-mock")
            connect_line, ping_line = read_mock_connect_ping(sock)
            put!(events, (connect_line, ping_line))
            while true
                line = NATS.readline_crlf(sock)
                put!(events, line)
                if startswith(line, "SUB ")
                    parts = split(line)
                    subject = String(parts[2])
                    queue = length(parts) == 4 ? String(parts[3]) : nothing
                    detail = queue === nothing ?
                             "Permissions Violation for Subscription to \"$subject\"" :
                             "Permissions Violation for Subscription to \"$subject\" using queue \"$queue\""
                    write(sock, "-ERR '$detail'\r\n")
                    flush(sock)
                elseif line == "PING"
                    write(sock, "PONG\r\n")
                    flush(sock)
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        f("nats://127.0.0.1:$port", events)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_flush_hang_mock(f)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(16)
    ready = Channel{Any}(1)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            sock = accept(listener)
            write(sock, "INFO {\"server_id\":\"flush-hang\",\"server_name\":\"flush-hang\",\"version\":\"2.10.18\",\"go\":\"go1.22\",\"host\":\"127.0.0.1\",\"port\":$port,\"headers\":true,\"proto\":1,\"auth_required\":false,\"max_payload\":1048576}\r\n")
            flush(sock)
            put!(events, NATS.readline_crlf(sock))
            ping_line = NATS.readline_crlf(sock)
            put!(events, ping_line)
            ping_line == "PING" && write(sock, "PONG\r\n")
            flush(sock)
            put!(ready, true)
            while true
                put!(events, NATS.readline_crlf(sock))
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        f("nats://127.0.0.1:$port", events, ready)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_ping_liveness_mock(f; respond_timer_pings::Bool, max_timer_pings::Int = 3)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    events = Channel{Any}(32)
    ready = Channel{Any}(1)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            sock = accept(listener)
            write_mock_info(sock, port; server_id = "ping-liveness")
            put!(events, NATS.readline_crlf(sock))
            initial_ping = NATS.readline_crlf(sock)
            put!(events, initial_ping)
            initial_ping == "PING" && write(sock, "PONG\r\n")
            flush(sock)
            put!(ready, true)
            timer_pings = 0
            while timer_pings < max_timer_pings
                line = NATS.readline_crlf(sock)
                put!(events, line)
                if line == "PING"
                    timer_pings += 1
                    if respond_timer_pings
                        write(sock, "PONG\r\n")
                        flush(sock)
                    end
                end
            end
            put!(events, (:done, timer_pings))
            while true
                line = NATS.readline_crlf(sock)
                put!(events, line)
                if line == "PING" && respond_timer_pings
                    write(sock, "PONG\r\n")
                    flush(sock)
                end
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(events, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        f("nats://127.0.0.1:$port", events, ready)
    finally
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

mutable struct ThrowingWriteIO
    err::Any
    closed::Bool
end

Base.write(io::ThrowingWriteIO, ::AbstractVector{UInt8}) = throw(io.err)
Base.flush(::ThrowingWriteIO) = nothing
Base.close(io::ThrowingWriteIO) = (io.closed = true; nothing)

mutable struct FailingReadIO <: IO
    payload::Vector{UInt8}
    position::Int
    err::Any
end

function Base.read(io::FailingReadIO, n::Integer)
    io.position > 1 && throw(io.err)
    stop = min(io.position + Int(n) - 1, length(io.payload))
    chunk = io.payload[io.position:stop]
    io.position = stop + 1
    return chunk
end

Base.eof(io::FailingReadIO) = io.position > length(io.payload)

function write_error_connection(url::AbstractString; kwargs...)
    server = NATS.parse_server_url(url)
    options = NATS.validate_options(NATS.Options(;
        reconnect_wait = 0.0,
        reconnect_jitter = 0.0,
        reconnect_jitter_tls = 0.0,
        max_reconnect = 5,
        connect_timeout = 0.2,
        request_timeout = 1.0,
        kwargs...,
    ))
    io = ThrowingWriteIO(ErrorException("injected write failure"), false)
    conn = NATS.new_connection(
        server,
        [server],
        options,
        io,
        NATS.ServerInfo(headers = true, max_payload = 1048576),
        NATS.CONNECTED;
        connected_once = true,
    )
    return conn, io
end

function discovery_info(port::Integer, urls::Vector{String}; ldm::Bool = false)
    return "INFO $(JSON3.write(Dict(
        "server_id" => "discovery-mock",
        "server_name" => "discovery-mock",
        "version" => "2.10.18",
        "go" => "go1.22",
        "host" => "127.0.0.1",
        "port" => Int(port),
        "headers" => true,
        "proto" => 1,
        "auth_required" => false,
        "max_payload" => 1048576,
        "connect_urls" => urls,
        "ldm" => ldm,
    )))\r\n"
end

function with_discovery_mock(f; initial_urls::Vector{String} = String[], initial_ldm::Bool = false)
    listener = listen(ip"127.0.0.1", 0)
    port = Int(last(getsockname(listener)))
    commands = Channel{Any}(4)
    ready = Channel{Any}(1)
    started = Channel{Any}(1)
    server_task = errormonitor(@async begin
        sock = nothing
        try
            put!(started, true)
            sock = accept(listener)
            write(sock, discovery_info(port, initial_urls; ldm = initial_ldm))
            flush(sock)
            NATS.readline_crlf(sock)
            ping_line = NATS.readline_crlf(sock)
            ping_line == "PING" && write(sock, "PONG\r\n")
            flush(sock)
            put!(ready, true)
            while true
                item = take!(commands)
                urls, ldm = item isa Tuple ? item : (item, false)
                write(sock, discovery_info(port, urls; ldm))
                flush(sock)
            end
        catch err
            if !(err isa EOFError || err isa Base.IOError || err isa InvalidStateException)
                put!(ready, err)
            end
        finally
            sock === nothing || try close(sock) catch end
        end
    end)
    try
        wait_ready(started)
        f("nats://127.0.0.1:$port", commands, ready)
    finally
        try close(commands) catch end
        try close(listener) catch end
        timedwait(() -> istaskdone(server_task), 2; pollint = 0.001)
    end
end

function with_ws_nats(f)
    client_port = free_port()
    websocket_port = free_port()
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        websocket {
          host: "0.0.0.0"
          port: 8080
          no_tls: true
        }
        """)
        Harbor.with_container(
            "nats";
            tag = "2.10.18",
            ports = Dict(4222 => client_port, 8080 => websocket_port),
            volumes = Dict("/config" => dir),
            command = ["-c", "/config/nats.conf", "--jetstream"],
        ) do _container
            sleep(1)
            f("ws://localhost:$websocket_port")
        end
    end
end

function with_wss_nats(f)
    client_port = free_port()
    websocket_port = free_port()
    certs = abspath(joinpath(@__DIR__, "certs"))
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        websocket {
          host: "0.0.0.0"
          port: 8080
          tls {
            cert_file: "/certs/server.pem"
            key_file: "/certs/key.pem"
          }
        }
        """)
        Harbor.with_container(
            "nats";
            tag = "2.10.18",
            ports = Dict(4222 => client_port, 8080 => websocket_port),
            volumes = Dict("/config" => dir, "/certs" => certs),
            command = ["-c", "/config/nats.conf", "--jetstream"],
        ) do _container
            sleep(1)
            f(certs, "wss://localhost:$websocket_port")
        end
    end
end

function with_ws_mock(f)
    events = Channel{Any}(16)
    server = HTTP.WebSockets.listen!("127.0.0.1", 0) do ws
        try
            x_header = HTTP.header(ws.handshake_request.headers, "X-NATS-JL-WS", nothing)
            authorization = HTTP.header(ws.handshake_request.headers, "Authorization", nothing)
            put!(events, (:headers, x_header, authorization, ws.handshake_request.target))
            multi_headers = String[]
            for value in HTTP.headers(ws.handshake_request.headers, "X-Multi")
                append!(multi_headers, strip.(split(value, ",")))
            end
            isempty(multi_headers) || put!(events, (:multi_headers, multi_headers))
            HTTP.WebSockets.send(ws, "INFO {\"server_id\":\"ws-mock\",\"server_name\":\"ws-mock\",\"version\":\"2.10.18\",\"go\":\"go1.22\",\"host\":\"127.0.0.1\",\"port\":0,\"headers\":true,\"proto\":1,\"auth_required\":false,\"max_payload\":1048576}\r\n")
            while true
                data = String(HTTP.WebSockets.receive(ws))
                for line in split(data, "\r\n"; keepempty = false)
                    startswith(line, "CONNECT ") && put!(events, (:connect, line))
                    line == "PING" && HTTP.WebSockets.send(ws, "PONG\r\n")
                end
            end
        catch err
            if !(err isa HTTP.WebSockets.WebSocketError && HTTP.WebSockets.isok(err))
                put!(events, err)
            end
        end
    end
    try
        f("ws://$(HTTP.WebSockets.server_addr(server))", events)
    finally
        close(server)
    end
end

function ws_info_line(server_id::AbstractString; ws_connect_urls = nothing)
    info = Dict{String, Any}(
        "server_id" => String(server_id),
        "server_name" => String(server_id),
        "version" => "2.10.18",
        "go" => "go1.22",
        "host" => "127.0.0.1",
        "port" => 0,
        "headers" => true,
        "proto" => 1,
        "auth_required" => false,
        "max_payload" => 1048576,
    )
    ws_connect_urls === nothing || (info["ws_connect_urls"] = ws_connect_urls)
    return "INFO $(JSON3.write(info))\r\n"
end

function serve_ws_mock!(ws, events::Channel, label::Symbol, info_line::String)
    try
        x_header = HTTP.header(ws.handshake_request.headers, "X-NATS-JL-WS", nothing)
        x_header === nothing || put!(events, (:ws_header, label, x_header))
        put!(events, (:headers, label, ws.handshake_request.target))
        HTTP.WebSockets.send(ws, info_line)
        while true
            data = String(HTTP.WebSockets.receive(ws))
            for line in split(data, "\r\n"; keepempty = false)
                startswith(line, "CONNECT ") && put!(events, (:connect, label, line))
                line == "PING" && HTTP.WebSockets.send(ws, "PONG\r\n")
            end
        end
    catch err
        if !(err isa HTTP.WebSockets.WebSocketError && HTTP.WebSockets.isok(err))
            put!(events, err)
        end
    end
end

function with_discovering_ws_mocks(f)
    events = Channel{Any}(32)
    second = HTTP.WebSockets.listen!("127.0.0.1", 0) do ws
        serve_ws_mock!(ws, events, :second, ws_info_line("ws-discovered"))
    end
    second_addr = HTTP.WebSockets.server_addr(second)
    first = HTTP.WebSockets.listen!("127.0.0.1", 0) do ws
        serve_ws_mock!(ws, events, :first, ws_info_line("ws-first"; ws_connect_urls = [second_addr]))
    end
    try
        f("ws://$(HTTP.WebSockets.server_addr(first))", "ws://$second_addr", events)
    finally
        close(first)
        close(second)
    end
end

function with_auth_nats(f)
    with_nats_container(command = ["--user", "derek", "--pass", "porkchop"]) do _container, url, _port
        f(url)
    end
end

function with_token_nats(f)
    with_nats_container(command = ["--auth", "secret"]) do _container, url, _port
        f(url)
    end
end

function with_nkey_nats(f)
    port = free_port()
    public = NATS.nkey_public_from_seed(NKEY_TEST_SEED)
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        authorization {
          users = [
            { nkey: "$public" }
          ]
        }
        """)
        Harbor.with_container(
            "nats";
            tag = "2.10.18",
            ports = Dict(4222 => port),
            volumes = Dict("/config" => dir),
            command = ["-c", "/config/nats.conf", "--jetstream"],
        ) do _container
            sleep(1)
            f("nats://localhost:$port")
        end
    end
end

function with_jwt_nats(f)
    port = free_port()
    operator_public = NATS.nkey_public_from_seed(JWT_OPERATOR_SEED)
    account_public = NATS.nkey_public_from_seed(JWT_ACCOUNT_SEED)
    mktempdir() do dir
        config = joinpath(dir, "nats.conf")
        write(config, """
        port: 4222
        trusted_keys = ["$operator_public"]
        resolver = MEMORY
        resolver_preload = {
          $account_public: "$JWT_ACCOUNT"
        }
        """)
        Harbor.with_container(
            "nats";
            tag = "2.10.18",
            ports = Dict(4222 => port),
            volumes = Dict("/config" => dir),
            command = ["-c", "/config/nats.conf"],
        ) do _container
            sleep(1)
            f(dir, "nats://127.0.0.1:$port")
        end
    end
end

function wait_ready(ch::Channel, timeout::Real = 2)
    result = timedwait(() -> isready(ch), timeout; pollint = 0.001)
    result == :ok || error("timed out waiting for channel")
    return take!(ch)
end

function fake_subscription(subject::AbstractString = "test.fake"; channel_size::Int = 1)
    return NATS.Subscription(
        nothing,
        String(subject),
        nothing,
        0,
        Channel{Any}(channel_size),
        channel_size,
        ReentrantLock(),
        nothing,
        false,
        0,
        false,
        nothing,
        false,
        NATS.SUBSCRIPTION_ACTIVE,
        Dict{Channel{NATS.SubscriptionStatus}, Vector{NATS.SubscriptionStatus}}(),
        nothing,
        nothing,
        0,
        0,
        0,
        0,
        0,
        -1,
        -1,
    )
end

function fake_push_subscription(conn::NATS.Connection; heartbeat_ns::Int64 = 10_000_000)
    sub = fake_subscription("push.fake")
    info = (name = "PUSH_FAKE", config = (idle_heartbeat = heartbeat_ns,))
    return JetStream.PushSubscription(
        conn,
        "STREAM",
        "PUSH_FAKE",
        JetStream.DEFAULT_API_PREFIX,
        "push.fake",
        nothing,
        sub,
        info,
        false,
        false,
        ReentrantLock(),
        false,
    )
end

function consumer_missing_error(err)
    return err isa JetStream.ConsumerNotFoundError
end

function wait_port_closed(port::Integer, timeout::Real = 5)
    function can_connect(host)
        sock = try
            connect(host, Int(port))
        catch
            return false
        end
        close(sock)
        return true
    end
    result = timedwait(timeout; pollint = 0.01) do
        return !(can_connect("localhost") || can_connect("127.0.0.1"))
    end
    result == :ok || error("timed out waiting for port $port to close")
    return nothing
end

@testset "protocol serialization" begin
    @test String(NATS.pub_frame("foo", "bar")) == "PUB foo 3\r\nbar\r\n"
    valid_header_key = "!#\$%&'*+-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ^_`abcdefghijklmnopqrstuvwxyz|~"
    @test String(NATS.headers_bytes([valid_header_key => "value"])) == "NATS/1.0\r\n$valid_header_key: value\r\n\r\n"
    multi_header = String(NATS.headers_bytes([
        "CorrelationID" => "123",
        "X-Test" => "First",
        "X-Test" => "Second",
    ]))
    @test startswith(multi_header, "NATS/1.0\r\n")
    @test endswith(multi_header, "\r\n\r\n")
    @test occursin("CorrelationID: 123\r\n", multi_header)
    @test occursin("X-Test: First\r\n", multi_header)
    @test occursin("X-Test: Second\r\n", multi_header)
    @test occursin("X-Test: line1 line2 line3\r\n", String(NATS.headers_bytes(["X-Test" => "line1\nline2\rline3"])))
    @test occursin("key: \0→\u200b\r\n", String(NATS.headers_bytes(["key" => "\0→\u200b"])))
    for bad_header_key in ("", ":", "\r", "\n", "\t", "\0", "→", "\u200b")
        err = try
            NATS.headers_bytes([bad_header_key => "value"])
            nothing
        catch err
            err
        end
        @test err isa NATS.InvalidHeaderKeyError
        @test err.key == bad_header_key
    end
    parsed_headers, parsed_status, parsed_description = NATS.parse_header_block(Vector{UInt8}(codeunits("NATS/1.0\r\nX-Test: one\r\nX-Test: two\r\n\r\n")))
    @test parsed_headers == ["X-Test" => "one", "X-Test" => "two"]
    @test parsed_status == 200
    @test parsed_description == ""
    for (header_line, status, description) in (
        ("NATS/1.0 503\r\n\r\n", 503, ""),
        ("NATS/1.0 503 No Responders\r\n\r\n", 503, "No Responders"),
        ("NATS/1.0  404   No Messages\r\n\r\n", 404, "No Messages"),
    )
        _, parsed_status, parsed_description = NATS.parse_header_block(Vector{UInt8}(codeunits(header_line)))
        @test parsed_status == status
        @test parsed_description == description
    end
    for bad_header_block in ("", "BAD/1.0\r\n\r\n", "NATS/1.0", "NATS/1.0\r\n", "NATS/1.0\r\nk1:v1", "NATS/1.0\r\nk1:v1\r\n", "NATS/1.0\r\nnot-a-header\r\n\r\n")
        @test_throws NATS.ProtocolError NATS.parse_header_block(Vector{UInt8}(codeunits(bad_header_block)))
    end
    @test_throws NATS.InvalidHeaderKeyError NATS.parse_header_block(Vector{UInt8}(codeunits("NATS/1.0\r\nbad key: value\r\n\r\n")))
    malformed_hmsg = IOBuffer(Vector{UInt8}(codeunits("NATS/1.0\r\nk1:v1payload\r\n")))
    @test_throws NATS.ProtocolError NATS.parse_msg_line("HMSG foo 1 15 22", malformed_hmsg, true)
    @test String(NATS.sub_frame("foo", nothing, 7)) == "SUB foo 7\r\n"
    @test String(NATS.unsub_frame(7)) == "UNSUB 7\r\n"
    @test JetStream.api_subject("STREAM.INFO.FOO") == "\$JS.API.STREAM.INFO.FOO"
    @test JetStream.api_subject("STREAM.INFO.FOO"; api_prefix = "\$JS.ABC.API") == "\$JS.ABC.API.STREAM.INFO.FOO"
    @test JetStream.api_subject("STREAM.INFO.FOO"; domain = "ABC") == "\$JS.ABC.API.STREAM.INFO.FOO"
    @test_throws ArgumentError JetStream.api_subject("STREAM.INFO.FOO"; api_prefix = "")
    @test_throws ArgumentError JetStream.api_subject("STREAM.INFO.FOO"; domain = "")
    @test_throws ArgumentError JetStream.api_subject("STREAM.INFO.FOO"; api_prefix = "\$JS.ABC.API", domain = "ABC")
    @test JetStream.validate_stream_name("STREAM") == "STREAM"
    @test_throws JetStream.StreamNameRequiredError JetStream.validate_stream_name("")
    for bad_stream_name in ("bad.stream", "bad stream", "bad*stream", "bad>stream", "bad/stream", "bad\\stream", "bad\tstream", "bad\rstream", "bad\nstream")
        err = try
            JetStream.validate_stream_name(bad_stream_name)
            nothing
        catch err
            err
        end
        @test err isa JetStream.InvalidStreamNameError
        @test err.stream == bad_stream_name
    end
    @test JetStream.validate_consumer_name("CONSUMER") == "CONSUMER"
    @test_throws JetStream.ConsumerNameRequiredError JetStream.validate_consumer_name("")
    for bad_consumer_name in ("bad.consumer", "bad consumer", "bad*consumer", "bad>consumer", "bad/consumer", "bad\\consumer", "bad\tconsumer", "bad\rconsumer", "bad\nconsumer")
        err = try
            JetStream.validate_consumer_name(bad_consumer_name)
            nothing
        catch err
            err
        end
        @test err isa JetStream.InvalidConsumerNameError
        @test err.consumer == bad_consumer_name
    end
    @test JetStream.overlapping_filter_subjects(JetStream.OverlappingFilterSubjectsError())
    @test JetStream.overlapping_filter_subjects(JetStream.JetStreamError(500, 10138, "consumer subject filters cannot overlap"))
    @test !JetStream.overlapping_filter_subjects(JetStream.JetStreamError(500, 0, "other JetStream error"))
    @test startswith(sprint(showerror, JetStream.OverlappingFilterSubjectsError()), "JetStream consumer filter subjects")
    echoed_filters_info = JSON3.read("""{"config":{"filter_subjects":["FOO.A","FOO.B"]}}""")
    missing_filters_info = JSON3.read("""{"config":{}}""")
    @test JetStream.ensure_multiple_filter_subjects_supported(echoed_filters_info, ["FOO.A", "FOO.B"]) === echoed_filters_info
    @test JetStream.ensure_multiple_filter_subjects_supported(missing_filters_info, nothing) === missing_filters_info
    @test_throws JetStream.MultipleFilterSubjectsNotSupportedError JetStream.ensure_multiple_filter_subjects_supported(missing_filters_info, ["FOO.A", "FOO.B"])
    @test_throws JetStream.MultipleFilterSubjectsNotSupportedError JetStream.ensure_multiple_filter_subjects_supported(JSON3.read("{}"), ["FOO.A", "FOO.B"])
    @test startswith(sprint(showerror, JetStream.MultipleFilterSubjectsNotSupportedError()), "JetStream multiple consumer filter subjects")
    @test JetStream.consumer_name(JetStream.ConsumerConfig(name = "", durable_name = "DURABLE")) == "DURABLE"
    fallback_consumer_config = JetStream.config_dict(JetStream.ConsumerConfig(name = "", durable_name = "DURABLE"))
    @test !haskey(fallback_consumer_config, "name")
    @test fallback_consumer_config["durable_name"] == "DURABLE"
    single_filter_config = JetStream.ConsumerConfig(name = "FILTER", filter_subject = "FOO.A")
    @test JetStream.consumer_filter_subjects(single_filter_config) == ("FOO.A", nothing)
    @test JetStream.config_dict(single_filter_config)["filter_subject"] == "FOO.A"
    @test JetStream.consumer_create_subject("STREAM", "FILTER", single_filter_config) == "\$JS.API.CONSUMER.CREATE.STREAM.FILTER.FOO.A"
    empty_filter_config = JetStream.ConsumerConfig(name = "EMPTYFILTER", filter_subject = "", filter_subjects = String[])
    empty_filter_body = JetStream.config_dict(empty_filter_config)
    @test !haskey(empty_filter_body, "filter_subject")
    @test !haskey(empty_filter_body, "filter_subjects")
    @test JetStream.consumer_create_subject("STREAM", "EMPTYFILTER", empty_filter_config) == "\$JS.API.CONSUMER.CREATE.STREAM.EMPTYFILTER"
    multi_filter_config = JetStream.ConsumerConfig(name = "FILTERS", filter_subjects = ["FOO.A", "FOO.B"])
    @test JetStream.consumer_filter_subjects(multi_filter_config) == (nothing, ["FOO.A", "FOO.B"])
    @test JetStream.config_dict(multi_filter_config)["filter_subjects"] == ["FOO.A", "FOO.B"]
    @test JetStream.consumer_create_subject("STREAM", "FILTERS", multi_filter_config) == "\$JS.API.CONSUMER.CREATE.STREAM.FILTERS"
    @test_throws ArgumentError JetStream.consumer_filter_subjects(JetStream.ConsumerConfig(filter_subject = "FOO.C", filter_subjects = ["FOO.A", "FOO.B"]))
    for bad_filter in ("", ".foo", "foo.", "foo..bar", "foo bar", "foo\tbar")
        @test_throws NATS.BadSubjectError JetStream.consumer_filter_subjects(JetStream.ConsumerConfig(filter_subjects = ["FOO.A", bad_filter]))
    end
    for bad_filter in (".foo", "foo.", "foo..bar", "foo bar", "foo\tbar")
        @test_throws NATS.BadSubjectError JetStream.consumer_filter_subjects(JetStream.ConsumerConfig(filter_subject = bad_filter))
    end
    @test JetStream.validate_push_queue_controls(JetStream.ConsumerConfig(deliver_group = "workers")) === nothing
    @test JetStream.validate_push_queue_controls(JetStream.ConsumerConfig(deliver_group = "", idle_heartbeat = 1_000_000_000, flow_control = true)) === nothing
    @test_throws ArgumentError JetStream.validate_push_queue_controls(JetStream.ConsumerConfig(deliver_group = "workers", idle_heartbeat = 1_000_000_000))
    @test_throws ArgumentError JetStream.validate_push_queue_controls(JetStream.ConsumerConfig(deliver_group = "workers", flow_control = true))

    server = NATS.parse_server_url("nats://validation.example:4222")
    conn = NATS.new_connection(
        server,
        [server],
        NATS.Options(),
        IOBuffer(),
        NATS.ServerInfo(headers = true),
        NATS.CONNECTED;
        connected_once = true,
    )
    @test_throws JetStream.StreamNameRequiredError JetStream.create_stream(conn, JetStream.StreamConfig(name = "", subjects = ["natsjl.validation"]))
    @test_throws JetStream.InvalidStreamNameError JetStream.update_stream(conn, JetStream.StreamConfig(name = "bad.stream", subjects = ["natsjl.validation"]))
    @test_throws JetStream.StreamNameRequiredError JetStream.create_or_update_stream(conn, JetStream.StreamConfig(name = "", subjects = ["natsjl.validation"]))
    @test_throws JetStream.InvalidStreamNameError JetStream.stream_info(conn, "bad stream")
    @test_throws JetStream.StreamNameRequiredError JetStream.delete_stream(conn, "")
    @test_throws JetStream.InvalidStreamNameError JetStream.get_msg(conn, "bad\tstream", 1)
    @test_throws JetStream.InvalidStreamNameError JetStream.purge_stream(conn, "bad>stream")
    @test_throws JetStream.StreamNameRequiredError JetStream.create_consumer(conn, "", JetStream.ConsumerConfig(name = "C", durable_name = "C"))
    @test_throws JetStream.InvalidConsumerNameError JetStream.create_consumer(conn, "STREAM", JetStream.ConsumerConfig(name = "bad.consumer", durable_name = "bad.consumer"))
    @test_throws JetStream.InvalidStreamNameError JetStream.consumer_info(conn, "bad/stream", "C")
    @test_throws JetStream.ConsumerNameRequiredError JetStream.consumer_info(conn, "STREAM", "")
    @test_throws JetStream.InvalidConsumerNameError JetStream.consumer_info(conn, "STREAM", "bad/consumer")
    @test_throws JetStream.InvalidStreamNameError JetStream.delete_consumer(conn, "bad*stream", "C")
    @test_throws JetStream.InvalidConsumerNameError JetStream.delete_consumer(conn, "STREAM", "bad consumer")
    @test_throws JetStream.InvalidConsumerNameError JetStream.pull_request_subject("STREAM", "bad>consumer")
    @test_throws JetStream.InvalidStreamNameError JetStream.ordered_consumer(conn, "bad\\stream"; timeout = 0.001)
    @test_throws NATS.BadSubjectError JetStream.create_consumer(conn, "STREAM", JetStream.ConsumerConfig(name = "BADFILTER", filter_subject = ".foo"))
    @test_throws NATS.BadSubjectError JetStream.create_consumer(conn, "STREAM", JetStream.ConsumerConfig(name = "BADFILTERS", filter_subjects = ["FOO.A", ""]))
    @test_throws ArgumentError JetStream.create_consumer(conn, "STREAM", JetStream.ConsumerConfig(name = "DUPFILTER", filter_subject = "FOO.C", filter_subjects = ["FOO.A"]))

    servers = [NATS.parse_server_url("ws://localhost:8080")]
    info = NATS.ServerInfo(connect_urls = ["localhost:4222"], ws_connect_urls = ["localhost:8081"])
    NATS.add_discovered_servers!(servers, first(servers), info)
    @test any(server -> server.scheme == "ws" && server.port == 8081, servers)
    @test !any(server -> server.scheme == "ws" && server.port == 4222, servers)
    ws_with_query = NATS.parse_server_url("ws://localhost:8080/nats?token=abc&x=1")
    @test ws_with_query.path == "/nats"
    @test ws_with_query.query == "token=abc&x=1"
    @test NATS.websocket_url(ws_with_query) == "ws://localhost:8080/nats?token=abc&x=1"
    @test NATS.websocket_url(ws_with_query; proxy_path = "/proxy/nats") == "ws://localhost:8080/proxy/nats?token=abc&x=1"
end

@testset "discovered server policy" begin
    with_discovery_mock(initial_urls = ["127.0.0.1:4223"]) do url, commands, ready
        callbacks = Channel{Vector{String}}(4)
        conn = NATS.connect(url; discovered_servers_cb = c -> put!(callbacks, NATS.discovered_servers(c)))
        try
            wait_ready(ready)
            @test NATS.servers(conn) == [url, "nats://127.0.0.1:4223"]
            @test NATS.discovered_servers(conn) == ["nats://127.0.0.1:4223"]
            sleep(0.05)
            @test !isready(callbacks)

            put!(commands, ["127.0.0.1:4223", "me:1"])
            discovered = wait_ready(callbacks)
            @test discovered == ["nats://127.0.0.1:4223", "nats://me:1"]
            @test NATS.discovered_servers(conn) == discovered
            @test NATS.servers(conn) == [url, "nats://127.0.0.1:4223", "nats://me:1"]
        finally
            NATS.close(conn)
        end
    end

    with_discovery_mock(initial_urls = ["127.0.0.1:4223"]) do url, commands, ready
        callbacks = Channel{Vector{String}}(4)
        conn = NATS.connect(url; ignore_discovered_servers = true, discovered_servers_cb = c -> put!(callbacks, NATS.discovered_servers(c)))
        try
            wait_ready(ready)
            @test NATS.servers(conn) == [url]
            @test isempty(NATS.discovered_servers(conn))
            put!(commands, ["127.0.0.1:4223", "me:1"])
            sleep(0.05)
            @test NATS.servers(conn) == [url]
            @test isempty(NATS.discovered_servers(conn))
            @test !isready(callbacks)
        finally
            NATS.close(conn)
        end
    end

    with_discovery_mock() do url, commands, ready
        callbacks = Channel{Vector{String}}(4)
        conn = NATS.connect(url)
        try
            wait_ready(ready)
            @test NATS.discovered_servers_handler(conn) === nothing
            NATS.set_discovered_servers_handler!(conn, c -> put!(callbacks, NATS.discovered_servers(c)))
            @test NATS.discovered_servers_handler(conn) !== nothing
            put!(commands, ["later:4222"])
            @test wait_ready(callbacks) == ["nats://later:4222"]
        finally
            NATS.close(conn)
        end
    end

    with_discovery_mock(initial_ldm = true) do url, commands, ready
        ldm_events = Channel{Bool}(4)
        conn = NATS.connect(url; lame_duck_cb = _ -> put!(ldm_events, true))
        try
            wait_ready(ready)
            sleep(0.05)
            @test !isready(ldm_events)

            put!(commands, (String[], true))
            @test wait_ready(ldm_events) === true
        finally
            NATS.close(conn)
        end
    end

    with_discovery_mock() do url, commands, ready
        ldm_events = Channel{Bool}(4)
        closed_events = Channel{Bool}(4)
        errors = Channel{Any}(4)
        connected_events = Channel{Bool}(4)
        reconnect_errors = Channel{Any}(4)
        conn = NATS.connect(url)
        try
            wait_ready(ready)
            @test NATS.last_error(conn) === nothing
            @test NATS.connected_handler(conn) === nothing
            @test NATS.lame_duck_handler(conn) === nothing
            @test NATS.closed_handler(conn) === nothing
            @test NATS.error_handler(conn) === nothing
            @test NATS.reconnect_error_handler(conn) === nothing
            @test NATS.disconnected_handler(conn) === nothing
            @test NATS.reconnected_handler(conn) === nothing

            NATS.set_connected_handler!(conn, _ -> put!(connected_events, true))
            NATS.set_lame_duck_handler!(conn, _ -> put!(ldm_events, true))
            reconnect_marker = ErrorException("reconnect callback parity")
            NATS.set_closed_handler!(conn, c -> put!(closed_events, NATS.last_error(c) === reconnect_marker))
            NATS.set_error_handler!(conn, (_conn, err) -> put!(errors, err))
            NATS.set_reconnect_error_handler!(conn, (_conn, err) -> put!(reconnect_errors, err))
            NATS.set_disconnected_handler!(conn, (_conn, _err) -> nothing)
            NATS.set_reconnected_handler!(conn, _ -> nothing)
            @test NATS.connected_handler(conn) !== nothing
            @test NATS.lame_duck_handler(conn) !== nothing
            @test NATS.closed_handler(conn) !== nothing
            @test NATS.error_handler(conn) !== nothing
            @test NATS.reconnect_error_handler(conn) !== nothing
            @test NATS.disconnected_handler(conn) !== nothing
            @test NATS.reconnected_handler(conn) !== nothing

            put!(commands, (String[], true))
            @test wait_ready(ldm_events) === true

            error_marker = ErrorException("callback parity")
            NATS.notify_error!(conn, error_marker)
            @test wait_ready(errors) === error_marker
            @test NATS.last_error(conn) === error_marker
            sleep(0.05)
            @test !isready(errors)
            NATS.notify_connected!(conn)
            @test wait_ready(connected_events) === true
            NATS.notify_reconnect_error!(conn, reconnect_marker)
            @test wait_ready(reconnect_errors) === reconnect_marker
            @test NATS.last_error(conn) === reconnect_marker

            NATS.close(conn)
            @test wait_ready(closed_events) === true

            @test NATS.set_connected_handler!(conn, nothing) === conn
            @test NATS.connected_handler(conn) === nothing
            @test NATS.set_lame_duck_handler!(conn, nothing) === conn
            @test NATS.lame_duck_handler(conn) === nothing
            @test NATS.set_closed_handler!(conn, nothing) === conn
            @test NATS.closed_handler(conn) === nothing
            @test NATS.set_error_handler!(conn, nothing) === conn
            @test NATS.error_handler(conn) === nothing
            @test NATS.set_reconnect_error_handler!(conn, nothing) === conn
            @test NATS.reconnect_error_handler(conn) === nothing
            @test NATS.set_disconnected_handler!(conn, nothing) === conn
            @test NATS.disconnected_handler(conn) === nothing
            @test NATS.set_reconnected_handler!(conn, nothing) === conn
            @test NATS.reconnected_handler(conn) === nothing
        finally
            NATS.close(conn)
        end
    end
end

@testset "nkey helpers and connect payload" begin
    public = NATS.nkey_public_from_seed(NKEY_TEST_SEED)
    @test startswith(public, "U")
    @test length(NATS.nkey_sign(NKEY_TEST_SEED, "anonce")) == 64

    url = NATS.parse_server_url("nats://localhost:4222")
    info = NATS.ServerInfo(nonce = "anonce", headers = true)
    connect_json(frame) = JSON3.read(match(r"^CONNECT (.*)\r\n$", String(frame)).captures[1])

    user_info_calls = Ref(0)
    user_info_options = NATS.Options(user_info_cb = () -> ("dynamic$(user_info_calls[] += 1)", "secret$(user_info_calls[])"))
    user_info_first = connect_json(NATS.connect_payload(user_info_options, url, info, false))
    user_info_second = connect_json(NATS.connect_payload(user_info_options, url, info, false))
    @test String(user_info_first.user) == "dynamic1"
    @test String(user_info_first.pass) == "secret1"
    @test String(user_info_second.user) == "dynamic2"
    @test String(user_info_second.pass) == "secret2"
    @test user_info_calls[] == 2
    user_info_pair = connect_json(NATS.connect_payload(NATS.Options(user_info_cb = () -> "pairuser" => "pairpass"), url, info, false))
    @test String(user_info_pair.user) == "pairuser"
    @test String(user_info_pair.pass) == "pairpass"
    user_info_named = connect_json(NATS.connect_payload(NATS.Options(user_info_cb = () -> (user = "nameduser", password = "namedpass")), url, info, false))
    @test String(user_info_named.user) == "nameduser"
    @test String(user_info_named.pass) == "namedpass"
    url_info_calls = Ref(0)
    url_with_user = NATS.parse_server_url("nats://urluser:urlpass@localhost:4222")
    url_user = connect_json(NATS.connect_payload(NATS.Options(user_info_cb = () -> (url_info_calls[] += 1; ("ignored", "ignored"))), url_with_user, info, false))
    @test String(url_user.user) == "urluser"
    @test String(url_user.pass) == "urlpass"
    @test url_info_calls[] == 0
    no_echo_payload = connect_json(NATS.connect_payload(NATS.Options(no_echo = true), url, NATS.ServerInfo(headers = true, proto = 1), false))
    @test no_echo_payload.echo === false
    @test_throws NATS.NoEchoNotSupportedError NATS.connect_payload(NATS.Options(no_echo = true), url, NATS.ServerInfo(headers = true, proto = 0), false)
    connect_options_payload = connect_json(NATS.connect_payload(
        NATS.Options(name = "natsjl-client", verbose = true, pedantic = true),
        url,
        info,
        true,
    ))
    @test String(connect_options_payload.name) == "natsjl-client"
    @test connect_options_payload.verbose === true
    @test connect_options_payload.pedantic === true
    @test connect_options_payload.tls_required === true
    @test_throws NATS.UserInfoAlreadySetError NATS.validate_options(NATS.Options(user = "static", user_info_cb = () -> ("dynamic", "secret")))
    @test_throws NATS.UserInfoAlreadySetError NATS.validate_options(NATS.Options(password = "static", user_info_cb = () -> ("dynamic", "secret")))
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(user_info_cb = () -> "bad"), url, info, false)

    token_calls = Ref(0)
    token_options = NATS.Options(token_cb = () -> "dynamic-token-$(token_calls[] += 1)")
    token_first = connect_json(NATS.connect_payload(token_options, url, info, false))
    token_second = connect_json(NATS.connect_payload(token_options, url, info, false))
    @test String(token_first.auth_token) == "dynamic-token-1"
    @test String(token_second.auth_token) == "dynamic-token-2"
    @test token_calls[] == 2
    url_token_calls = Ref(0)
    url_with_token = NATS.parse_server_url("nats://urltoken@localhost:4222")
    @test_throws NATS.TokenAlreadySetError NATS.connect_payload(NATS.Options(token_cb = () -> (url_token_calls[] += 1; "ignored")), url_with_token, info, false)
    @test url_token_calls[] == 0
    @test_throws NATS.TokenAlreadySetError NATS.validate_options(NATS.Options(token = "static", token_cb = () -> "dynamic"))
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(token_cb = () -> 1), url, info, false)

    frame = String(NATS.connect_payload(NATS.Options(jwt = "user.jwt", nkey_seed = NKEY_TEST_SEED), url, info, false))
    data = connect_json(frame)
    @test data.jwt == "user.jwt"
    @test haskey(data, :sig)
    @test !haskey(data, :nkey)

    jwt_calls = Ref(0)
    jwt_options = NATS.Options(jwt_cb = () -> "user.jwt.$(jwt_calls[] += 1)", nkey_seed = NKEY_TEST_SEED)
    jwt_first_frame = String(NATS.connect_payload(jwt_options, url, info, false))
    jwt_second_info = NATS.ServerInfo(nonce = "another", headers = true)
    jwt_second_frame = String(NATS.connect_payload(jwt_options, url, jwt_second_info, false))
    jwt_first = connect_json(jwt_first_frame)
    jwt_second = connect_json(jwt_second_frame)
    @test jwt_first.jwt == "user.jwt.1"
    @test jwt_second.jwt == "user.jwt.2"
    @test jwt_calls[] == 2

    no_nonce = NATS.ServerInfo(headers = true)
    @test_throws NATS.UnsupportedTransportError NATS.connect_payload(NATS.Options(nkey_seed = NKEY_TEST_SEED), url, no_nonce, false)
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(nkey = public), url, info, false)
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(jwt = JWT_USER), url, info, false)
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(jwt = "user.jwt", nkey = public, signature_cb = _ -> zeros(UInt8, 64)), url, info, false)
    @test_throws ArgumentError NATS.connect_payload(NATS.Options(jwt_cb = () -> 1, nkey_seed = NKEY_TEST_SEED), url, info, false)
    conflicting_jwt = NATS.Options(jwt = "static.jwt", jwt_cb = () -> "dynamic.jwt", nkey_seed = NKEY_TEST_SEED)
    @test_throws ArgumentError NATS.connect_payload(conflicting_jwt, url, info, false)
end

@testset "credential parsing" begin
    @test NATS.find_user_jwt(JWT_CHAINED_CREDENTIALS) == JWT_USER
    @test NATS.find_user_nkey_seed(JWT_CHAINED_CREDENTIALS) == JWT_USER_SEED
    mktempdir() do dir
        creds = joinpath(dir, "user.creds")
        write(creds, JWT_CHAINED_CREDENTIALS)
        @test NATS.read_user_jwt(creds) == JWT_USER
        @test NATS.read_user_nkey_seed(creds) == JWT_USER_SEED
    end
    @test_throws ArgumentError NATS.find_user_jwt("not credentials")
    @test_throws ArgumentError NATS.find_user_nkey_seed("not credentials")
end

@testset "reconnect delay policy" begin
    nats_url = NATS.parse_server_url("nats://localhost:4222")
    tls_url = NATS.parse_server_url("tls://localhost:4222")

    attempts = Int[]
    opts = NATS.Options(
        reconnect_wait = 10.0,
        reconnect_jitter = 10.0,
        reconnect_jitter_tls = 10.0,
        custom_reconnect_delay_cb = n -> (push!(attempts, n); n * 0.01),
    )
    @test NATS.reconnect_delay_seconds(opts, nats_url, [nats_url], 1, MersenneTwister(1)) == 0.01
    @test NATS.reconnect_delay_seconds(opts, nats_url, [nats_url], 2, MersenneTwister(1)) == 0.02
    @test attempts == [1, 2]

    no_jitter = NATS.Options(reconnect_wait = 0.25, reconnect_jitter = 0.0, reconnect_jitter_tls = 0.0)
    @test NATS.reconnect_delay_seconds(no_jitter, nats_url, [nats_url], 1, MersenneTwister(2)) == 0.25
    @test NATS.reconnect_delay_seconds(no_jitter, tls_url, [tls_url], 1, MersenneTwister(2)) == 0.25

    tls_jitter = NATS.Options(reconnect_wait = 0.25, reconnect_jitter = 0.0, reconnect_jitter_tls = 0.5)
    delay = NATS.reconnect_delay_seconds(tls_jitter, tls_url, [tls_url], 1, MersenneTwister(3))
    @test 0.25 <= delay <= 0.75
    @test NATS.reconnect_delay_seconds(tls_jitter, nats_url, [nats_url], 1, MersenneTwister(3)) == 0.25

    mixed_pool = [nats_url, tls_url]
    delay = NATS.reconnect_delay_seconds(tls_jitter, nats_url, mixed_pool, 1, MersenneTwister(4))
    @test 0.25 <= delay <= 0.75

    @test_throws ArgumentError NATS.validate_options(NATS.Options(reconnect_wait = -0.1))
    @test_throws ArgumentError NATS.validate_options(NATS.Options(reconnect_jitter = -0.1))
    @test_throws ArgumentError NATS.validate_options(NATS.Options(reconnect_jitter_tls = -0.1))
    @test_throws ArgumentError NATS.validate_options(NATS.Options(reconnect_buffer_size = -2))
    @test_throws ArgumentError NATS.validate_options(NATS.Options(ping_interval = -0.1))
    for bad_prefix in ["\$BOB.", "\$BOB.*", "\$BOB.>", ">", ".", "", "BOB.*.X", "BOB.>.X", ".BOB", "BOB..X", "BOB X"]
        @test_throws ArgumentError NATS.validate_options(NATS.Options(inbox_prefix = bad_prefix))
    end
    @test_throws ArgumentError NATS.reconnect_delay_seconds(
        NATS.Options(custom_reconnect_delay_cb = _ -> -0.1),
        nats_url,
        [nats_url],
        1,
        MersenneTwister(5),
    )
end

with_flush_hang_mock() do url, events, ready
    @testset "flush release on close" begin
        conn = NATS.connect(url; allow_reconnect = false)
        try
            @test wait_ready(ready) === true
            @test startswith(wait_ready(events), "CONNECT ")
            @test wait_ready(events) == "PING"

            @test_throws NATS.ConnectionTimeoutError NATS.flush(conn; timeout = 0.05)
            @test wait_ready(events) == "PING"

            task = @async begin
                try
                    NATS.flush(conn; timeout = 5)
                    nothing
                catch err
                    err
                end
            end
            @test wait_ready(events) == "PING"
            sleep(0.05)
            elapsed = @elapsed NATS.close(conn)
            @test elapsed < 1
            @test timedwait(() -> istaskdone(task), 1; pollint = 0.001) == :ok
            @test fetch(task) isa NATS.ConnectionClosedError
        finally
            NATS.close(conn)
        end
    end
end

@testset "ping liveness" begin
    with_ping_liveness_mock(respond_timer_pings = false, max_timer_pings = 3) do url, events, ready
        closed = Channel{Bool}(1)
        conn = NATS.connect(
            url;
            allow_reconnect = false,
            ping_interval = 0.02,
            max_pings_out = 2,
            closed_cb = _ -> put!(closed, true),
        )
        try
            @test wait_ready(ready) === true
            result = timedwait(() -> NATS.connection_status(conn) == NATS.CLOSED, 2; pollint = 0.001)
            @test result == :ok
            @test NATS.last_error(conn) isa NATS.StaleConnectionError
            @test wait_ready(closed) === true
            observed = Any[]
            while isready(events)
                push!(observed, take!(events))
            end
            @test count(==("PING"), observed) >= 3
        finally
            NATS.close(conn)
        end
    end

    with_ping_liveness_mock(respond_timer_pings = true, max_timer_pings = 3) do url, events, ready
        conn = NATS.connect(
            url;
            allow_reconnect = false,
            ping_interval = 0.02,
            max_pings_out = 1,
        )
        try
            @test wait_ready(ready) === true
            observed = Any[]
            for _ in 1:10
                item = wait_ready(events)
                item isa Exception && throw(item)
                push!(observed, item)
                item isa Tuple && item[1] == :done && break
            end
            @test (:done, 3) in observed
            @test count(==("PING"), observed) >= 4
            @test NATS.connection_status(conn) == NATS.CONNECTED
            @test !(NATS.last_error(conn) isa NATS.StaleConnectionError)
        finally
            NATS.close(conn)
        end
    end
end

with_nats() do url
    @testset "core pub/sub request/reply" begin
        conn = NATS.connect(url)
        try
            @test NATS.status(conn) == NATS.CONNECTED
            @test NATS.is_connected(conn)
            @test !NATS.is_closed(conn)
            @test !NATS.is_reconnecting(conn)
            @test !NATS.is_draining(conn)
            @test startswith(NATS.connected_url(conn), "nats://")
            @test NATS.connected_server_id(conn) != ""
            @test NATS.connected_server_name(conn) != ""
            @test NATS.connected_server_version(conn) != ""
            @test NATS.connected_client_id(conn) !== nothing
            @test NATS.max_payload(conn) > 0
            js_enabled, js_api_level = NATS.connected_server_jetstream(conn)
            @test js_enabled
            @test js_api_level >= 0
            @test NATS.rtt(conn; timeout = 2) >= 0
            @test NATS.num_subscriptions(conn) == 0

            verbose_conn = NATS.connect(url; name = "natsjl-verbose", verbose = true, pedantic = true)
            try
                NATS.flush(verbose_conn; timeout = 2)
                @test NATS.connection_status(verbose_conn) == NATS.CONNECTED
            finally
                NATS.close(verbose_conn)
            end

            no_echo_conn = NATS.connect(url; no_echo = true)
            no_echo_sender = NATS.connect(url)
            try
                no_echo_sub = NATS.subscribe(no_echo_conn, "natsjl.noecho")
                NATS.flush(no_echo_conn; timeout = 2)
                NATS.publish(no_echo_conn, "natsjl.noecho", "self")
                NATS.flush(no_echo_conn; timeout = 2)
                @test_throws NATS.ConnectionTimeoutError NATS.next_msg(no_echo_sub; timeout = 0.2)
                NATS.publish(no_echo_sender, "natsjl.noecho", "other")
                @test NATS.payload(NATS.next_msg(no_echo_sub; timeout = 2)) == "other"
            finally
                NATS.close(no_echo_sender)
                NATS.close(no_echo_conn)
            end

            default_closed = Channel{Any}(1)
            default_close_conn = NATS.connect(url; closed_cb = conn -> put!(default_closed, NATS.connection_status(conn)))
            try
                NATS.close(default_close_conn)
                @test wait_ready(default_closed) == NATS.CLOSED
            finally
                NATS.close(default_close_conn)
            end

            suppressed_closed = Channel{Any}(2)
            suppressed_close_conn = NATS.connect(
                url;
                no_callbacks_after_client_close = true,
                closed_cb = conn -> put!(suppressed_closed, NATS.connection_status(conn)),
            )
            try
                NATS.close(suppressed_close_conn)
                sleep(0.1)
                @test !isready(suppressed_closed)
            finally
                NATS.close(suppressed_close_conn)
            end

            suppressed_drain_conn = NATS.connect(
                url;
                no_callbacks_after_client_close = true,
                closed_cb = conn -> put!(suppressed_closed, NATS.connection_status(conn)),
            )
            try
                NATS.drain(suppressed_drain_conn; timeout = 2)
                sleep(0.1)
                @test !isready(suppressed_closed)
            finally
                NATS.close(suppressed_drain_conn)
            end

            sub = NATS.subscribe(conn, "natsjl.core")
            sub_closed = Channel{String}(1)
            @test NATS.closed_handler(sub) === nothing
            @test NATS.set_closed_handler!(sub, subject -> put!(sub_closed, subject)) === sub
            @test NATS.closed_handler(sub) !== nothing
            @test NATS.num_subscriptions(conn) == 1
            baseline_stats = NATS.stats(conn)
            NATS.publish(conn, "natsjl.core", "hello")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "hello"
            after_stats = NATS.stats(conn)
            @test after_stats.out_msgs >= baseline_stats.out_msgs + 1
            @test after_stats.out_bytes >= baseline_stats.out_bytes + sizeof("hello")
            @test after_stats.in_msgs >= baseline_stats.in_msgs + 1
            @test after_stats.in_bytes >= baseline_stats.in_bytes + sizeof("hello")
            NATS.unsubscribe(sub)
            @test wait_ready(sub_closed) == "natsjl.core"
            @test NATS.num_subscriptions(conn) == 0
            closed_status = NATS.status_changed(sub)
            @test isready(closed_status)
            @test wait_ready(closed_status) == NATS.SUBSCRIPTION_CLOSED
            @test !isopen(closed_status)
            closed_slow_status = NATS.status_changed(sub, NATS.SUBSCRIPTION_SLOW_CONSUMER)
            @test !isopen(closed_slow_status)
            @test !isready(closed_slow_status)
            @test_throws NATS.BadSubscriptionError NATS.next_msg(sub; timeout = 0.1)
            @test_throws NATS.BadSubscriptionError NATS.unsubscribe(sub)

            release_conn = NATS.connect(url)
            try
                release_sub = NATS.subscribe(release_conn, "natsjl.close.release")
                release_task = @async begin
                    try
                        NATS.next_msg(release_sub; timeout = 5)
                        nothing
                    catch err
                        err
                    end
                end
                sleep(0.05)
                elapsed = @elapsed NATS.close(release_conn)
                @test elapsed < 1
                @test timedwait(() -> istaskdone(release_task), 1; pollint = 0.001) == :ok
                @test fetch(release_task) isa NATS.ConnectionClosedError
                @test_throws NATS.ConnectionClosedError NATS.unsubscribe(release_sub)
                @test_throws NATS.BadSubscriptionError NATS.auto_unsubscribe(release_sub, 1)
            finally
                NATS.close(release_conn)
            end

            status_probe = NATS.subscribe(conn, "natsjl.status.probe")
            unread_status = NATS.status_changed(status_probe)
            status_task = @async begin
                for _ in 1:64
                    NATS.set_subscription_status!(status_probe, NATS.SUBSCRIPTION_SLOW_CONSUMER)
                    NATS.set_subscription_status!(status_probe, NATS.SUBSCRIPTION_ACTIVE)
                end
            end
            status_result = timedwait(() -> istaskdone(status_task), 1; pollint = 0.001)
            status_result == :ok || try close(unread_status) catch end
            @test status_result == :ok
            status_result == :ok && fetch(status_task)
            NATS.unsubscribe(status_probe)

            auto_sub = NATS.subscribe(conn, "natsjl.auto")
            auto_closed = Channel{String}(1)
            NATS.set_closed_handler!(auto_sub, subject -> put!(auto_closed, subject))
            NATS.auto_unsubscribe(auto_sub, 3)
            for i in 1:10
                NATS.publish(conn, "natsjl.auto", "auto-$i")
            end
            NATS.flush(conn; timeout = 2)
            @test [NATS.payload(NATS.next_msg(auto_sub; timeout = 2)) for _ in 1:3] == ["auto-1", "auto-2", "auto-3"]
            @test_throws NATS.MaxMessagesError NATS.next_msg(auto_sub; timeout = 0.1)
            @test !NATS.is_valid(auto_sub)
            @test_throws NATS.BadSubscriptionError NATS.auto_unsubscribe(auto_sub, 1)
            @test wait_ready(auto_closed) == "natsjl.auto"

            parallel_sub = NATS.subscribe(conn, "natsjl.auto.parallel")
            NATS.auto_unsubscribe(parallel_sub, 9)
            NATS.flush(conn; timeout = 2)
            parallel_seen = Ref(0)
            parallel_lock = ReentrantLock()
            parallel_done = Channel{Tuple{Int, Any}}(3)
            for _ in 1:3
                @async begin
                    local_seen = 0
                    terminal_error = nothing
                    try
                        while true
                            NATS.next_msg(parallel_sub; timeout = 5)
                            local_seen += 1
                            lock(parallel_lock)
                            try
                                parallel_seen[] += 1
                            finally
                                unlock(parallel_lock)
                            end
                        end
                    catch err
                        terminal_error = err
                    end
                    put!(parallel_done, (local_seen, terminal_error))
                end
            end
            for i in 1:9
                NATS.publish(conn, "natsjl.auto.parallel", "parallel-$i")
            end
            NATS.flush(conn; timeout = 2)
            parallel_results = [wait_ready(parallel_done, 6) for _ in 1:3]
            @test sum(first, parallel_results) == 9
            @test parallel_seen[] == 9
            @test all(last(result) isa NATS.MaxMessagesError for result in parallel_results)
            @test !NATS.is_valid(parallel_sub)

            lower_subject = "natsjl.auto.callback.lower"
            lower_seen = Channel{Int}(16)
            lower_ref = Ref{Any}()
            lower_sub = NATS.subscribe(conn, lower_subject) do msg
                n = parse(Int, NATS.payload(msg))
                put!(lower_seen, n)
                if n == 5
                    NATS.auto_unsubscribe(lower_ref[], 4)
                else
                    NATS.publish(conn, lower_subject, string(n + 1))
                end
            end
            lower_ref[] = lower_sub
            NATS.auto_unsubscribe(lower_sub, 10)
            NATS.publish(conn, lower_subject, "1")
            @test [wait_ready(lower_seen) for _ in 1:5] == collect(1:5)
            @test wait_ready(NATS.status_changed(lower_sub, NATS.SUBSCRIPTION_CLOSED)) == NATS.SUBSCRIPTION_CLOSED
            sleep(0.05)
            @test !isready(lower_seen)

            higher_subject = "natsjl.auto.callback.higher"
            higher_seen = Channel{Int}(32)
            higher_ref = Ref{Any}()
            higher_sub = NATS.subscribe(conn, higher_subject) do msg
                n = parse(Int, NATS.payload(msg))
                put!(higher_seen, n)
                n == 5 && NATS.auto_unsubscribe(higher_ref[], 20)
                n < 20 && NATS.publish(conn, higher_subject, string(n + 1))
            end
            higher_ref[] = higher_sub
            NATS.auto_unsubscribe(higher_sub, 10)
            NATS.publish(conn, higher_subject, "1")
            @test [wait_ready(higher_seen) for _ in 1:20] == collect(1:20)
            @test wait_ready(NATS.status_changed(higher_sub, NATS.SUBSCRIPTION_CLOSED)) == NATS.SUBSCRIPTION_CLOSED
            sleep(0.05)
            @test !isready(higher_seen)

            responder = NATS.subscribe(conn, "natsjl.service") do req
                NATS.respond(conn, req, "reply:" * NATS.payload(req))
            end
            rep = NATS.request(conn, "natsjl.service", "ping"; timeout = 2)
            @test NATS.payload(rep) == "reply:ping"
            NATS.unsubscribe(responder)

            header_responder = NATS.subscribe(conn, "natsjl.respond.headers") do req
                NATS.respond(conn, req, "header-reply"; headers = ["X-Reply" => "yes"])
            end
            header_rep = NATS.request(conn, "natsjl.respond.headers", "ping"; timeout = 2)
            @test NATS.payload(header_rep) == "header-reply"
            @test NATS.header(header_rep, "x-reply") == "yes"
            NATS.unsubscribe(header_responder)

            request_msg_replies = Channel{Any}(1)
            request_msg_responder = NATS.subscribe(conn, "natsjl.request.msg") do req
                put!(request_msg_replies, (something(req.reply, ""), NATS.header(req, "x-request")))
                NATS.respond_msg(conn, req, NATS.new_msg("ignored", "request-msg-reply"; headers = ["X-Reply" => "request-msg"]))
            end
            request_msg = NATS.new_msg("natsjl.request.msg", "from-msg"; reply = "not.used", headers = ["X-Request" => "msg"])
            request_msg_rep = NATS.request_msg(conn, request_msg; timeout = 2)
            @test NATS.payload(request_msg_rep) == "request-msg-reply"
            @test NATS.header(request_msg_rep, "x-reply") == "request-msg"
            request_msg_reply, request_msg_header = wait_ready(request_msg_replies)
            @test request_msg_reply != "not.used"
            @test request_msg_header == "msg"
            NATS.unsubscribe(request_msg_responder)

            msg_responder = NATS.subscribe(conn, "natsjl.respond.msg") do req
                NATS.respond_msg(conn, req, NATS.new_msg("ignored", "msg-reply"; headers = ["X-Reply" => "msg"]))
            end
            msg_rep = NATS.request(conn, "natsjl.respond.msg", "ping"; timeout = 2)
            @test NATS.payload(msg_rep) == "msg-reply"
            @test NATS.header(msg_rep, "x-reply") == "msg"
            NATS.unsubscribe(msg_responder)

            mux_responder = NATS.subscribe(conn, "natsjl.mux") do req
                NATS.publish(conn, req.reply, "mux:" * NATS.payload(req))
            end
            tasks = [@async NATS.request(conn, "natsjl.mux", string(i); timeout = 2) for i in 1:5]
            replies = sort!([NATS.payload(fetch(task)) for task in tasks])
            @test replies == ["mux:1", "mux:2", "mux:3", "mux:4", "mux:5"]
            @test conn.request_sub !== nothing
            @test isempty(conn.request_map)
            NATS.unsubscribe(mux_responder)

            leak_responder = NATS.subscribe(conn, "natsjl.request.leak") do req
                NATS.respond(conn, req, "leak-response")
            end
            NATS.flush(conn; timeout = 2)
            for _ in 1:50
                leak_reply = NATS.request(conn, "natsjl.request.leak"; timeout = 2)
                @test NATS.payload(leak_reply) == "leak-response"
                @test isempty(conn.request_map)
            end
            NATS.unsubscribe(leak_responder)

            multi_reply_seen = Channel{String}(2)
            swallow_seen = Channel{String}(1)
            multi_reply_responder = NATS.subscribe(conn, "natsjl.request.multi-reply") do req
                put!(multi_reply_seen, something(req.reply, ""))
                NATS.respond(conn, req, "first")
                NATS.respond(conn, req, "second")
            end
            swallow_responder = NATS.subscribe(conn, "natsjl.request.swallow") do req
                put!(swallow_seen, something(req.reply, ""))
            end
            NATS.flush(conn; timeout = 2)
            multi_request_conn = NATS.connect(url)
            try
                no_reply_task = @async begin
                    try
                        NATS.request(multi_request_conn, "natsjl.request.swallow"; timeout = 1)
                    catch err
                        err
                    end
                end
                @test startswith(wait_ready(swallow_seen), multi_request_conn.request_prefix::String)
                multi_reply = NATS.request(multi_request_conn, "natsjl.request.multi-reply"; timeout = 2)
                @test NATS.payload(multi_reply) == "first"
                @test startswith(wait_ready(multi_reply_seen), multi_request_conn.request_prefix::String)
                @test timedwait(() -> istaskdone(no_reply_task), 2; pollint = 0.001) == :ok
                @test fetch(no_reply_task) isa NATS.ConnectionTimeoutError
                @test isempty(multi_request_conn.request_map)
            finally
                NATS.close(multi_request_conn)
                NATS.unsubscribe(swallow_responder)
                NATS.unsubscribe(multi_reply_responder)
            end

            custom_conn = NATS.connect(url; inbox_prefix = "\$BOB")
            try
                custom_inbox = NATS.new_inbox(custom_conn)
                @test startswith(custom_inbox, "\$BOB.")
                @test length(split(custom_inbox, ".")) == 2

                custom_replies = Channel{String}(4)
                custom_responder = NATS.subscribe(custom_conn, "natsjl.custom-inbox") do req
                    put!(custom_replies, req.reply)
                    NATS.respond(custom_conn, req, "ok")
                end
                NATS.flush(custom_conn; timeout = 2)

                custom_mux_reply = NATS.request(custom_conn, "natsjl.custom-inbox", "mux"; timeout = 2)
                @test NATS.payload(custom_mux_reply) == "ok"
                custom_mux_inbox = wait_ready(custom_replies)
                @test startswith(custom_mux_inbox, "\$BOB.")
                @test length(split(custom_mux_inbox, ".")) == 3

                custom_exact_reply = NATS.request(custom_conn, "natsjl.custom-inbox", "exact"; timeout = 2, mux = false)
                @test NATS.payload(custom_exact_reply) == "ok"
                custom_exact_inbox = wait_ready(custom_replies)
                @test startswith(custom_exact_inbox, "\$BOB.")
                @test length(split(custom_exact_inbox, ".")) == 2

                NATS.unsubscribe(custom_responder)
            finally
                NATS.close(custom_conn)
            end

            request_close_conn = NATS.connect(url)
            try
                request_close_sink = NATS.subscribe(request_close_conn, "natsjl.request.close")
                NATS.flush(request_close_conn; timeout = 2)
                request_close_task = @async begin
                    try
                        NATS.request(request_close_conn, "natsjl.request.close", "hang"; timeout = 5)
                        nothing
                    catch err
                        err
                    end
                end
                @test NATS.payload(NATS.next_msg(request_close_sink; timeout = 2)) == "hang"
                sleep(0.05)
                elapsed = @elapsed NATS.close(request_close_conn)
                @test elapsed < 1
                @test timedwait(() -> istaskdone(request_close_task), 1; pollint = 0.001) == :ok
                @test fetch(request_close_task) isa NATS.ConnectionClosedError
                @test isempty(request_close_conn.request_map)
            finally
                NATS.close(request_close_conn)
            end

            hsub = NATS.subscribe(conn, "natsjl.headers")
            NATS.publish(conn, "natsjl.headers", "with headers"; headers = ["X-NATS-JL" => "yes"])
            hmsg = NATS.next_msg(hsub; timeout = 2)
            @test NATS.payload(hmsg) == "with headers"
            @test NATS.header(hmsg, "x-nats-jl") == "yes"
            NATS.unsubscribe(hsub)

            publish_msg_sub = NATS.subscribe(conn, "natsjl.publish-msg")
            NATS.publish_msg(conn, NATS.new_msg("natsjl.publish-msg", "from-msg"; headers = ["X-Pub" => "msg"]))
            publish_msg = NATS.next_msg(publish_msg_sub; timeout = 2)
            @test NATS.payload(publish_msg) == "from-msg"
            @test NATS.header(publish_msg, "x-pub") == "msg"
            NATS.unsubscribe(publish_msg_sub)
            @test_throws NATS.InvalidMsgError NATS.publish_msg(conn, nothing)
            @test_throws NATS.InvalidMsgError NATS.request_msg(conn, nothing)

            no_reply_sub = NATS.subscribe(conn, "natsjl.no-reply")
            NATS.publish(conn, "natsjl.no-reply", "plain")
            no_reply_msg = NATS.next_msg(no_reply_sub; timeout = 2)
            @test_throws NATS.MsgNoReplyError NATS.respond(conn, no_reply_msg, "cannot")
            @test_throws NATS.MsgNoReplyError NATS.respond_msg(conn, no_reply_msg, NATS.new_msg("ignored", "cannot"))
            NATS.unsubscribe(no_reply_sub)

            @test_throws NATS.BadSubjectError NATS.publish(conn, "", "bad")
            @test_throws NATS.BadSubjectError NATS.publish(conn, "foo bar", "bad")
            @test_throws NATS.BadSubjectError NATS.publish(conn, "natsjl.good", "bad"; reply = "bad reply")
            @test_throws NATS.BadSubjectError NATS.request(conn, "bad subject", "bad"; timeout = 0.1)
            for bad_subj in ["", "foo bar", "foo..bar", ".foo", "bar.baz.", "baz\t.foo"]
                @test_throws NATS.BadSubjectError NATS.subscribe(conn, bad_subj)
            end
            for bad_queue in ["foo group", "group\t1", "g1\r\n2"]
                @test_throws NATS.BadQueueNameError NATS.subscribe(conn, "natsjl.good"; queue = bad_queue)
            end

            skip_conn = NATS.connect(url; skip_subject_validation = true)
            try
                NATS.publish(skip_conn, "foo bar", "skip-ok")
                NATS.flush(skip_conn; timeout = 2)
                @test_throws NATS.BadSubjectError NATS.publish(skip_conn, "", "still-bad")
            finally
                NATS.close(skip_conn)
            end

            @test_throws NATS.NoRespondersError NATS.request(conn, "natsjl.no.responders", "ping"; timeout = 2)
            inbox = NATS.new_inbox(conn)
            no_responders_sub = NATS.subscribe(conn, inbox; channel_size = 1)
            try
                NATS.flush(conn; timeout = 2)
                NATS.publish(conn, "natsjl.no.responders.next", nothing; reply = inbox)
                try
                    NATS.next_msg(no_responders_sub; timeout = 2)
                    error("expected NoRespondersError")
                catch err
                    @test err isa NATS.NoRespondersError
                    @test err.subject == inbox
                end
                @test NATS.pending(no_responders_sub) == 0
            finally
                NATS.unsubscribe(no_responders_sub)
            end

            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
        @test NATS.status(conn) == NATS.CLOSED
        @test NATS.is_closed(conn)
        @test !NATS.is_connected(conn)
        @test NATS.connected_url(conn) == ""
        @test_throws NATS.ConnectionClosedError NATS.rtt(conn; timeout = 0.1)
        @test_throws NATS.ConnectionClosedError NATS.publish(conn, "natsjl.closed", "after-close")
        @test_throws NATS.ConnectionClosedError NATS.publish_msg(conn, NATS.new_msg("natsjl.closed.msg", "after-close"))
        @test_throws NATS.ConnectionClosedError NATS.flush(conn; timeout = 0.1)
        @test_throws NATS.ConnectionClosedError NATS.subscribe(conn, "natsjl.closed")
        @test_throws NATS.ConnectionClosedError NATS.subscribe(_ -> nothing, conn, "natsjl.closed.callback")
        @test_throws NATS.ConnectionClosedError NATS.subscribe(conn, "natsjl.closed.queue"; queue = "workers")
        @test_throws NATS.ConnectionClosedError NATS.request(conn, "natsjl.closed.request", "help"; timeout = 0.1)
        @test_throws NATS.ConnectionClosedError NATS.request_msg(conn, NATS.new_msg("natsjl.closed.request-msg", "help"); timeout = 0.1)
        @test NATS.num_subscriptions(conn) == 0
        @test conn.request_sub === nothing
        @test isempty(conn.request_map)
    end

    @testset "micro services" begin
        conn = NATS.connect(url)
        services = NATS.Micro.Service[]
        try
            handler = req -> NATS.Micro.respond(req, "42")
            config = NATS.Micro.ServiceConfig(
                name = "CoolAddService",
                version = "0.1.0",
                description = "Add things together",
                metadata = Dict("basic" => "metadata"),
                endpoint = NATS.Micro.EndpointConfig(subject = "svc.add", handler = handler),
            )
            for _ in 1:3
                push!(services, NATS.Micro.add_service(conn, config))
            end

            for _ in 1:12
                rep = NATS.request(conn, "svc.add", raw"{ \"x\": 22, \"y\": 20 }"; timeout = 2)
                @test NATS.payload(rep) == "42"
            end

            local_info = NATS.Micro.info(first(services))
            @test local_info.name == "CoolAddService"
            @test local_info.version == "0.1.0"
            @test local_info.metadata == Dict("basic" => "metadata")
            @test length(local_info.endpoints) == 1
            @test first(local_info.endpoints).subject == "svc.add"
            @test first(local_info.endpoints).queue_group == NATS.Micro.DEFAULT_QUEUE_GROUP

            info_subject = NATS.Micro.control_subject(NATS.Micro.INFO, "CoolAddService")
            info_msg = NATS.request(conn, info_subject; timeout = 2)
            info_json = JSON3.read(String(info_msg.data))
            @test info_json.type == NATS.Micro.INFO_RESPONSE_TYPE
            @test info_json.name == "CoolAddService"
            @test length(info_json.endpoints) == 1

            function collect_monitor(subject, expected)
                inbox = NATS.new_inbox(conn)
                sub = NATS.subscribe(conn, inbox)
                try
                    NATS.publish(conn, subject; reply = inbox)
                    replies = Any[]
                    deadline = time() + 2
                    while length(replies) < expected && time() < deadline
                        push!(replies, JSON3.read(String(NATS.next_msg(sub; timeout = max(0.01, deadline - time())).data)))
                    end
                    return replies
                finally
                    NATS.unsubscribe(sub)
                end
            end

            ping_replies = collect_monitor(NATS.Micro.control_subject(NATS.Micro.PING, "CoolAddService"), 3)
            @test length(ping_replies) == 3
            @test all(reply -> reply.type == NATS.Micro.PING_RESPONSE_TYPE, ping_replies)
            @test length(unique(String(reply.id) for reply in ping_replies)) == 3

            stats_replies = collect_monitor(NATS.Micro.control_subject(NATS.Micro.STATS, "CoolAddService"), 3)
            @test length(stats_replies) == 3
            @test sum(sum(Int(endpoint.num_requests) for endpoint in reply.endpoints) for reply in stats_replies) == 12

            NATS.Micro.reset!(first(services))
            @test first(NATS.Micro.stats(first(services)).endpoints).num_requests == 0

            group = NATS.Micro.add_group(first(services), "numbers")
            NATS.Micro.add_endpoint!(group, "Increment") do req
                NATS.Micro.respond(req, string(parse(Int, NATS.Micro.payload(req)) + 1))
            end
            inc = NATS.request(conn, "numbers.Increment", "3"; timeout = 2)
            @test NATS.payload(inc) == "4"

            NATS.Micro.add_endpoint!(first(services), "Bad"; subject = "svc.bad") do req
                NATS.Micro.respond_error(req, "400", "bad request", "details")
            end
            bad = NATS.request(conn, "svc.bad", "oops"; timeout = 2)
            @test NATS.payload(bad) == "details"
            @test NATS.header(bad, NATS.Micro.ERROR_CODE_HEADER) == "400"
            @test NATS.header(bad, NATS.Micro.ERROR_HEADER) == "bad request"
            bad_stats = only(filter(endpoint -> endpoint.name == "Bad", NATS.Micro.stats(first(services)).endpoints))
            @test bad_stats.num_requests == 1
            @test bad_stats.num_errors == 1
            @test bad_stats.last_error == "400:bad request"

            queue_service = NATS.Micro.add_service(
                conn;
                name = "QueueService",
                version = "0.1.0",
                queue_group = "q-config",
                endpoint = NATS.Micro.EndpointConfig(
                    subject = "queue.default",
                    queue_group = "q-default",
                    handler = req -> NATS.Micro.respond(req, "default"),
                ),
            )
            push!(services, queue_service)
            NATS.Micro.add_endpoint!(queue_service, "bar"; queue_group = "q-bar") do req
                NATS.Micro.respond(req, "bar")
            end
            g1 = NATS.Micro.add_group(queue_service, "g1"; queue_group = "q-g1")
            g2 = NATS.Micro.add_group(g1, "g2")
            g3 = NATS.Micro.add_group(g2, "g3"; queue_group = "q-g3")
            g4 = NATS.Micro.add_group(g2, "g4"; queue_group_disabled = true)
            NATS.Micro.add_endpoint!(g2, "baz") do req
                NATS.Micro.respond(req, "baz")
            end
            NATS.Micro.add_endpoint!(g2, "qux"; queue_group = "q-qux") do req
                NATS.Micro.respond(req, "qux")
            end
            NATS.Micro.add_endpoint!(g3, "quux") do req
                NATS.Micro.respond(req, "quux")
            end
            NATS.Micro.add_endpoint!(g4, "disabled") do req
                NATS.Micro.respond(req, "disabled")
            end
            queue_groups = Dict(endpoint.name => endpoint.queue_group for endpoint in NATS.Micro.info(queue_service).endpoints)
            @test queue_groups == Dict(
                "default" => "q-default",
                "bar" => "q-bar",
                "baz" => "q-g1",
                "qux" => "q-qux",
                "quux" => "q-g3",
                "disabled" => "",
            )
            stats_groups = Dict(endpoint.name => endpoint.queue_group for endpoint in NATS.Micro.stats(queue_service).endpoints)
            @test stats_groups == queue_groups

            function collect_replies(subject, expected)
                inbox = NATS.new_inbox(conn)
                sub = NATS.subscribe(conn, inbox)
                try
                    NATS.publish(conn, subject, "req"; reply = inbox)
                    replies = String[]
                    deadline = time() + 2
                    while length(replies) < expected && time() < deadline
                        push!(replies, NATS.payload(NATS.next_msg(sub; timeout = max(0.01, deadline - time()))))
                    end
                    return sort!(replies)
                finally
                    NATS.unsubscribe(sub)
                end
            end

            for i in 1:4
                svc = NATS.Micro.add_service(
                    conn;
                    name = "MultiQueueService",
                    version = "0.1.0",
                    queue_group = "q-$i",
                    endpoint = NATS.Micro.EndpointConfig(
                        subject = "svc.multi.queue",
                        handler = req -> NATS.Micro.respond(req, string(i)),
                    ),
                )
                push!(services, svc)
            end
            @test collect_replies("svc.multi.queue", 4) == ["1", "2", "3", "4"]

            for i in 1:4
                svc = NATS.Micro.add_service(
                    conn;
                    name = "NoQueueService",
                    version = "0.1.0",
                    queue_group_disabled = true,
                    endpoint = NATS.Micro.EndpointConfig(
                        subject = "svc.no.queue",
                        handler = req -> NATS.Micro.respond(req, string(i)),
                    ),
                )
                push!(services, svc)
            end
            @test collect_replies("svc.no.queue", 4) == ["1", "2", "3", "4"]

            limited = NATS.Micro.add_endpoint!(first(services), "Limited"; subject = "svc.limited", pending_msg_limit = 2, pending_bytes_limit = -1) do req
                NATS.Micro.respond(req, "limited")
            end
            @test NATS.pending_limits(limited.subscription::NATS.Subscription) == (2, -1)
            config_limited = NATS.Micro.add_service(
                conn;
                name = "ConfigLimited",
                version = "0.1.0",
                endpoint = NATS.Micro.EndpointConfig(
                    subject = "svc.config-limited",
                    handler = req -> NATS.Micro.respond(req, "ok"),
                    pending_msg_limit = -1,
                    pending_bytes_limit = 4096,
                ),
            )
            push!(services, config_limited)
            @test NATS.pending_limits(only(config_limited.endpoints).subscription::NATS.Subscription) == (-1, 4096)
            @test_throws NATS.Micro.ConfigValidationError NATS.Micro.add_endpoint!(first(services), "BadLimit", req -> nothing; pending_msg_limit = 0, pending_bytes_limit = 0)

            @test_throws NATS.Micro.ConfigValidationError NATS.Micro.add_service(conn; name = "bad!", version = "0.1.0")
            @test_throws NATS.Micro.ConfigValidationError NATS.Micro.add_service(conn; name = "bad", version = "not-semver")
            @test_throws NATS.Micro.ConfigValidationError NATS.Micro.add_service(
                conn;
                name = "BadQueue",
                version = "0.1.0",
                queue_group = ">.abc",
                endpoint = NATS.Micro.EndpointConfig(subject = "bad.queue", handler = req -> nothing),
            )
            @test_throws NATS.Micro.ConfigValidationError NATS.Micro.add_service(
                conn;
                name = "BadEndpointQueue",
                version = "0.1.0",
                endpoint = NATS.Micro.EndpointConfig(subject = "bad.endpoint.queue", queue_group = ">.abc", handler = req -> nothing),
            )
            @test_throws NATS.Micro.ServiceNameRequiredError NATS.Micro.control_subject(NATS.Micro.PING, "", "id")
        finally
            for service in services
                NATS.Micro.stop(service; timeout = 2)
                @test NATS.Micro.stopped(service)
            end
            NATS.close(conn)
        end
    end

    @testset "headers unsupported" begin
        disabled_server = NATS.parse_server_url("nats://client-disabled-headers.example:4222")
        disabled_io = IOBuffer()
        disabled_conn = NATS.new_connection(
            disabled_server,
            [disabled_server],
            NATS.Options(headers = false),
            disabled_io,
            NATS.ServerInfo(headers = true, max_payload = 1024),
            NATS.CONNECTED;
            connected_once = true,
        )
        @test !NATS.headers_supported(disabled_conn)
        @test_throws NATS.HeadersNotSupportedError NATS.publish(disabled_conn, "natsjl.noheaders.client", "bad"; headers = ["X-Test" => "no"])
        @test_throws NATS.HeadersNotSupportedError NATS.publish_request(disabled_conn, "natsjl.noheaders.client", "_INBOX.reply", "bad"; headers = ["X-Test" => "no"])
        @test isempty(take!(disabled_io))

        connect_frame = String(NATS.connect_payload(
            NATS.Options(headers = false, no_responders = true),
            disabled_server,
            NATS.ServerInfo(headers = true),
            false,
        ))
        @test occursin("\"headers\":false", connect_frame)
        @test occursin("\"no_responders\":false", connect_frame)

        with_no_headers_mock() do url, events
            conn = NATS.connect(url; allow_reconnect = false)
            try
                @test !NATS.headers_supported(conn)
                @test_throws NATS.HeadersNotSupportedError NATS.publish(conn, "natsjl.noheaders", "bad"; headers = ["X-Test" => "no"])
                @test_throws NATS.HeadersNotSupportedError NATS.request(conn, "natsjl.noheaders", "bad"; headers = ["X-Test" => "no"])
                @test_throws NATS.HeadersNotSupportedError NATS.publish_msg(conn, NATS.new_msg("natsjl.noheaders", "bad"; headers = ["X-Test" => "no"]))
                @test_throws NATS.HeadersNotSupportedError NATS.request_msg(conn, NATS.new_msg("natsjl.noheaders", "bad"; headers = ["X-Test" => "no"]))
                sleep(0.05)
                @test !isready(events)

                NATS.publish(conn, "natsjl.noheaders", "plain")
                line = wait_ready(events)
                line isa Exception && throw(line)
                @test startswith(line, "PUB natsjl.noheaders")
            finally
                NATS.close(conn)
            end
        end
    end

    @testset "server error classification" begin
        @test NATS.classify_server_error("Stale Connection") isa NATS.StaleConnectionError
        @test NATS.classify_server_error("Permissions Violation for Publish to \"Foo\"") isa NATS.PermissionViolationError
        @test NATS.classify_server_error("Authorization Violation") isa NATS.AuthorizationViolationError
        @test NATS.classify_server_error("User Authentication Expired") isa NATS.AuthenticationExpiredError
        @test NATS.classify_server_error("User Authentication Revoked") isa NATS.AuthenticationRevokedError
        @test NATS.classify_server_error("Account Authentication Expired") isa NATS.AccountAuthenticationExpiredError
        @test NATS.classify_server_error("Maximum Connections Exceeded") isa NATS.MaxConnectionsExceededError
        @test NATS.classify_server_error("Maximum Account Active Connections Exceeded") isa NATS.MaxAccountConnectionsExceededError
        @test NATS.classify_server_error("Maximum Subscriptions Exceeded") isa NATS.MaxSubscriptionsExceededError
        @test NATS.classify_server_error("Mystery Server Error") isa NATS.ServerError

        with_server_error_mock("Authorization Violation"; handshake = true) do url, _events, _ready
            @test_throws NATS.AuthorizationViolationError NATS.connect(url; allow_reconnect = false)
        end

        with_server_error_mock("Permissions Violation for Publish to \"Foo\"") do url, _events, ready
            conn = NATS.connect(url; allow_reconnect = false)
            try
                callback_events = Channel{Any}(1)
                NATS.set_error_handler!(conn, (_conn, sub, err) -> put!(callback_events, (sub, err)))
                @test wait_ready(ready) === true
                err = wait_ready(conn.async_errors)
                @test err isa NATS.PermissionViolationError
                @test occursin("Foo", err.message)
                callback_sub, callback_err = wait_ready(callback_events)
                @test callback_sub === nothing
                @test callback_err isa NATS.PermissionViolationError
                @test NATS.last_error(conn) isa NATS.PermissionViolationError
                @test NATS.connection_status(conn) == NATS.CONNECTED
                NATS.flush(conn; timeout = 2)
                @test NATS.connection_status(conn) == NATS.CONNECTED
            finally
                NATS.close(conn)
            end
        end

        with_subscription_permission_mock() do url, events
            conn = NATS.connect(url; permission_err_on_subscribe = true, allow_reconnect = false)
            try
                @test wait_ready(events)[2] == "PING"
                sub = NATS.subscribe(conn, "natsjl.denied"; queue = "workers")
                sub_status = NATS.status_changed(sub, NATS.SUBSCRIPTION_CLOSED)
                @test startswith(wait_ready(events), "SUB natsjl.denied workers ")
                err = try
                    NATS.next_msg(sub; timeout = 2)
                    nothing
                catch err
                    err
                end
                @test err isa NATS.PermissionViolationError
                @test occursin("natsjl.denied", err.message)
                @test occursin("workers", err.message)
                @test wait_ready(sub_status) == NATS.SUBSCRIPTION_CLOSED
                @test !NATS.is_valid(sub)
                @test_throws NATS.PermissionViolationError NATS.next_msg(sub; timeout = 0.01)
                async_err = wait_ready(conn.async_errors)
                @test async_err isa NATS.PermissionViolationError
                @test NATS.last_error(conn) isa NATS.PermissionViolationError
                @test NATS.connection_status(conn) == NATS.CONNECTED
            finally
                NATS.close(conn)
            end
        end

        with_subscription_permission_mock() do url, events
            conn = NATS.connect(url; permission_err_on_subscribe = false, allow_reconnect = false)
            try
                @test wait_ready(events)[2] == "PING"
                sub = NATS.subscribe(conn, "natsjl.denied")
                @test startswith(wait_ready(events), "SUB natsjl.denied ")
                async_err = wait_ready(conn.async_errors)
                @test async_err isa NATS.PermissionViolationError
                @test NATS.last_error(conn) isa NATS.PermissionViolationError
                @test NATS.connection_status(conn) == NATS.CONNECTED
                @test NATS.is_valid(sub)
                @test_throws NATS.ConnectionTimeoutError NATS.next_msg(sub; timeout = 0.05)
                NATS.unsubscribe(sub)
            finally
                NATS.close(conn)
            end
        end

        with_server_error_mock("Mystery Server Error") do url, _events, ready
            closed = Channel{Any}(1)
            conn = NATS.connect(url; allow_reconnect = false, closed_cb = conn -> put!(closed, NATS.connection_status(conn)))
            try
                @test wait_ready(ready) === true
                err = wait_ready(conn.async_errors)
                @test err isa NATS.ServerError
                @test err.message == "Mystery Server Error"
                @test wait_ready(closed) == NATS.CLOSED
                @test NATS.connection_status(conn) == NATS.CLOSED
            finally
                NATS.close(conn)
            end
        end

        with_server_error_mock("Terminal Server Error") do url, _events, ready
            closed = Channel{Any}(1)
            conn = NATS.connect(
                url;
                allow_reconnect = false,
                no_callbacks_after_client_close = true,
                closed_cb = conn -> put!(closed, NATS.connection_status(conn)),
            )
            try
                @test wait_ready(ready) === true
                err = wait_ready(conn.async_errors)
                @test err isa NATS.ServerError
                @test err.message == "Terminal Server Error"
                @test wait_ready(closed) == NATS.CLOSED
                @test NATS.connection_status(conn) == NATS.CLOSED
            finally
                NATS.close(conn)
            end
        end

        for (message, error_type) in (
            ("Stale Connection", NATS.StaleConnectionError),
            ("User Authentication Expired", NATS.AuthenticationExpiredError),
        )
            with_reconnect_error_mock(message) do url, port, events, trigger
                disconnected = Channel{Any}(1)
                reconnected = Channel{Any}(1)
                conn = NATS.connect(
                    url;
                    reconnect_wait = 0.01,
                    reconnect_jitter = 0.0,
                    max_reconnect = 10,
                    disconnected_cb = (_conn, err) -> put!(disconnected, err),
                    reconnected_cb = conn -> put!(reconnected, conn.url.port),
                )
                try
                    first = wait_ready(events)
                    @test first[1] == 1
                    @test first[3] == "PING"

                    status_ch = NATS.status_changed(conn, NATS.RECONNECTING, NATS.CONNECTED)
                    put!(trigger, true)
                    @test wait_ready(status_ch, 2) == NATS.RECONNECTING
                    @test isa(wait_ready(disconnected, 2), error_type)
                    second = wait_ready(events, 2)
                    @test second[1] == 2
                    @test second[3] == "PING"
                    @test wait_ready(status_ch, 2) == NATS.CONNECTED
                    @test wait_ready(reconnected, 2) == port
                    NATS.flush(conn; timeout = 2)
                    @test NATS.connection_status(conn) == NATS.CONNECTED
                finally
                    NATS.close(conn)
                end
            end
        end

        with_auth_expired_jwt_refresh_mock() do url, port, events, trigger
            jwt_calls = Ref(0)
            disconnected = Channel{Any}(1)
            reconnected = Channel{Any}(1)
            conn = NATS.connect(
                url;
                jwt_cb = () -> "jwt-$(jwt_calls[] += 1)",
                nkey_seed = NKEY_TEST_SEED,
                reconnect_wait = 0.01,
                reconnect_jitter = 0.0,
                max_reconnect = 10,
                disconnected_cb = (_conn, err) -> put!(disconnected, err),
                reconnected_cb = conn -> put!(reconnected, conn.url.port),
            )
            try
                first = wait_ready(events)
                @test first[1] == 1
                first_data = JSON3.read(match(r"^CONNECT (.*)$", first[2]).captures[1])
                @test first_data.jwt == "jwt-1"
                @test first[3] == "PING"

                status_ch = NATS.status_changed(conn, NATS.RECONNECTING, NATS.CONNECTED)
                put!(trigger, true)
                @test wait_ready(status_ch, 2) == NATS.RECONNECTING
                @test wait_ready(disconnected, 2) isa NATS.AuthenticationExpiredError
                second = wait_ready(events, 2)
                @test second[1] == 2
                second_data = JSON3.read(match(r"^CONNECT (.*)$", second[2]).captures[1])
                @test second_data.jwt == "jwt-2"
                @test second[3] == "PING"
                @test jwt_calls[] == 2
                @test wait_ready(status_ch, 2) == NATS.CONNECTED
                @test wait_ready(reconnected, 2) == port
            finally
                NATS.close(conn)
            end
        end

        with_repeated_auth_error_mock("User Authentication Expired") do url, _port, events, trigger
            disconnected = Channel{Any}(1)
            reconnect_errors = Channel{Any}(4)
            closed = Channel{Any}(1)
            conn = NATS.connect(
                url;
                jwt_cb = () -> "unchanged.jwt",
                nkey_seed = NKEY_TEST_SEED,
                reconnect_wait = 0.01,
                reconnect_jitter = 0.0,
                max_reconnect = 10,
                disconnected_cb = (_conn, err) -> put!(disconnected, err),
                reconnect_error_cb = (_conn, err) -> put!(reconnect_errors, err),
                closed_cb = conn -> put!(closed, NATS.last_error(conn)),
            )
            try
                first = wait_ready(events)
                @test first[1] == 1
                @test first[3] == "PING"

                status_ch = NATS.status_changed(conn, NATS.RECONNECTING, NATS.CLOSED)
                put!(trigger, true)
                @test wait_ready(status_ch, 2) == NATS.RECONNECTING
                @test wait_ready(disconnected, 2) isa NATS.AuthenticationExpiredError
                second = wait_ready(events, 2)
                @test second[1] == 2
                @test second[3] == "PING"
                @test wait_ready(reconnect_errors, 2) isa NATS.AuthenticationExpiredError
                @test wait_ready(status_ch, 2) == NATS.CLOSED
                @test wait_ready(closed, 2) isa NATS.AuthenticationExpiredError
                sleep(0.05)
                @test !isready(events)
            finally
                NATS.close(conn)
            end
        end

        with_repeated_auth_error_mock("User Authentication Expired") do url, _port, events, trigger
            reconnect_errors = Channel{Any}(4)
            closed = Channel{Any}(1)
            conn = NATS.connect(
                url;
                jwt_cb = () -> "unchanged.jwt",
                nkey_seed = NKEY_TEST_SEED,
                reconnect_wait = 0.01,
                reconnect_jitter = 0.0,
                max_reconnect = 2,
                ignore_auth_error_abort = true,
                reconnect_error_cb = (_conn, err) -> put!(reconnect_errors, err),
                closed_cb = conn -> put!(closed, NATS.last_error(conn)),
            )
            try
                @test wait_ready(events)[1] == 1
                put!(trigger, true)
                @test wait_ready(events, 2)[1] == 2
                @test wait_ready(events, 2)[1] == 3
                @test wait_ready(reconnect_errors, 2) isa NATS.AuthenticationExpiredError
                @test wait_ready(reconnect_errors, 2) isa NATS.AuthenticationExpiredError
                failed = wait_ready(closed, 2)
                @test failed isa NATS.ReconnectFailedError
                @test failed.last_error isa NATS.AuthenticationExpiredError
                @test NATS.connection_status(conn) == NATS.CLOSED
            finally
                NATS.close(conn)
            end
        end
    end

    @testset "slow consumer backpressure" begin
        conn = NATS.connect(url)
        try
            error_events = Channel{Any}(8)
            NATS.set_error_handler!(conn, (_conn, sub, err) -> put!(error_events, (sub, err)))
            slow = NATS.subscribe(conn, "natsjl.slow"; channel_size = 1)
            slow_status = NATS.status_changed(slow)
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_ACTIVE
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_ACTIVE
            @test NATS.is_valid(slow)
            @test !NATS.is_draining(slow)
            fast = NATS.subscribe(conn, "natsjl.fast")
            NATS.flush(conn; timeout = 2)

            for i in 1:4
                NATS.publish(conn, "natsjl.slow", "slow-$i")
            end
            NATS.publish(conn, "natsjl.fast", "reader-still-moving")
            NATS.flush(conn; timeout = 2)

            err = wait_ready(conn.async_errors)
            cb_sub, cb_err = wait_ready(error_events)
            @test err isa NATS.SlowConsumerError
            @test cb_sub === slow
            @test cb_err isa NATS.SlowConsumerError
            @test NATS.last_error(conn) isa NATS.SlowConsumerError
            @test NATS.last_error(conn).subject == "natsjl.slow"
            @test err.subject == "natsjl.slow"
            @test err.sid == slow.sid
            @test cb_err.sid == slow.sid
            @test err.capacity == 1
            @test NATS.pending(slow) == 1
            @test NATS.max_pending(slow) == 1
            @test NATS.dropped(slow) >= 1
            @test err.dropped <= NATS.dropped(slow)
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_SLOW_CONSUMER
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_SLOW_CONSUMER
            sleep(0.05)
            @test !isready(conn.async_errors)
            @test !isready(error_events)
            @test NATS.delivered(slow) == 0
            slow_next = try
                NATS.next_msg(slow; timeout = 2)
                nothing
            catch err
                err
            end
            @test slow_next isa NATS.SlowConsumerError
            @test slow_next.subject == "natsjl.slow"
            @test slow_next.sid == slow.sid
            @test NATS.pending(slow) == 1
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_ACTIVE
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_ACTIVE
            @test startswith(NATS.payload(NATS.next_msg(slow; timeout = 2)), "slow-")
            @test NATS.delivered(slow) == 1
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_ACTIVE

            NATS.publish(conn, "natsjl.slow", "slow-again-1")
            NATS.publish(conn, "natsjl.slow", "slow-again-2")
            NATS.flush(conn; timeout = 2)
            again_err = wait_ready(conn.async_errors)
            again_cb_sub, again_cb_err = wait_ready(error_events)
            @test again_err isa NATS.SlowConsumerError
            @test again_cb_sub === slow
            @test again_cb_err isa NATS.SlowConsumerError
            @test again_err.sid == slow.sid
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_SLOW_CONSUMER
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_SLOW_CONSUMER
            sleep(0.05)
            @test !isready(conn.async_errors)
            @test !isready(error_events)
            @test_throws NATS.SlowConsumerError NATS.next_msg(slow; timeout = 2)
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_ACTIVE
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_ACTIVE
            @test startswith(NATS.payload(NATS.next_msg(slow; timeout = 2)), "slow-again-")
            @test NATS.subscription_status(slow) == NATS.SUBSCRIPTION_ACTIVE
            @test NATS.payload(NATS.next_msg(fast; timeout = 2)) == "reader-still-moving"
            @test startswith(sprint(showerror, err), "NATS slow consumer")
            NATS.unsubscribe(slow)
            @test wait_ready(slow_status) == NATS.SUBSCRIPTION_CLOSED
            @test !NATS.is_valid(slow)
            @test_throws NATS.BadSubscriptionError NATS.pending(slow)
            @test_throws NATS.BadSubscriptionError NATS.pending_bytes(slow)
            @test_throws NATS.BadSubscriptionError NATS.pending_limits(slow)
            @test_throws NATS.BadSubscriptionError NATS.max_pending(slow)
            @test_throws NATS.BadSubscriptionError NATS.max_pending_bytes(slow)
            @test_throws NATS.BadSubscriptionError NATS.delivered(slow)
            @test_throws NATS.BadSubscriptionError NATS.dropped(slow)
        finally
            NATS.close(conn)
        end

        byte_conn = NATS.connect(url)
        try
            payload = "hello"
            byte_sub = NATS.subscribe(byte_conn, "natsjl.slow-bytes"; channel_size = 10)
            @test NATS.pending_limits(byte_sub) == (10, NATS.DEFAULT_SUB_PENDING_BYTES_LIMIT)
            @test_throws ArgumentError NATS.set_pending_limits(byte_sub, 0, 1)
            @test_throws ArgumentError NATS.set_pending_limits(byte_sub, 1, 0)

            NATS.set_pending_limits(byte_sub, -1, 2 * sizeof(payload))
            @test NATS.pending_limits(byte_sub) == (-1, 2 * sizeof(payload))
            NATS.flush(byte_conn; timeout = 2)

            for _ in 1:5
                NATS.publish(byte_conn, "natsjl.slow-bytes", payload)
            end
            NATS.flush(byte_conn; timeout = 2)

            byte_err = wait_ready(byte_conn.async_errors)
            @test byte_err isa NATS.SlowConsumerError
            @test byte_err.subject == "natsjl.slow-bytes"
            @test byte_err.pending_bytes > byte_err.bytes_limit
            @test NATS.pending(byte_sub) == 2
            @test NATS.pending_bytes(byte_sub) == 2 * sizeof(payload)
            @test NATS.max_pending(byte_sub) == 2
            @test NATS.max_pending_bytes(byte_sub) == 2 * sizeof(payload)

            @test_throws NATS.SlowConsumerError NATS.next_msg(byte_sub; timeout = 2)
            @test NATS.pending(byte_sub) == 2
            @test NATS.pending_bytes(byte_sub) == 2 * sizeof(payload)
            @test NATS.delivered(byte_sub) == 0
            @test NATS.payload(NATS.next_msg(byte_sub; timeout = 2)) == payload
            @test NATS.pending(byte_sub) == 1
            @test NATS.pending_bytes(byte_sub) == sizeof(payload)
            @test NATS.delivered(byte_sub) == 1

            NATS.clear_max_pending!(byte_sub)
            @test NATS.max_pending(byte_sub) == 0
            @test NATS.max_pending_bytes(byte_sub) == 0

            NATS.set_pending_limits(byte_sub, 2, -1)
            @test NATS.pending_limits(byte_sub) == (2, -1)
            NATS.unsubscribe(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.pending(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.pending_bytes(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.max_pending(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.max_pending_bytes(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.pending_limits(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.set_pending_limits(byte_sub, 1, 1)
            @test_throws NATS.BadSubscriptionError NATS.clear_max_pending!(byte_sub)
            @test_throws NATS.BadSubscriptionError NATS.dropped(byte_sub)
        finally
            NATS.close(byte_conn)
        end

        callback_slow_conn = NATS.connect(url)
        try
            callback_errors = Channel{Any}(4)
            NATS.set_error_handler!(callback_slow_conn, (_conn, err) -> put!(callback_errors, err))
            release_callback = Channel{Nothing}(1)
            callback_seen = Channel{String}(8)
            callback_sub = NATS.subscribe(callback_slow_conn, "natsjl.slow-callback"; channel_size = 10) do msg
                payload = NATS.payload(msg)
                put!(callback_seen, payload)
                payload == "callback-1" && take!(release_callback)
            end
            callback_status = NATS.status_changed(callback_sub)
            @test wait_ready(callback_status) == NATS.SUBSCRIPTION_ACTIVE
            NATS.set_pending_limits(callback_sub, 3, 1024)
            NATS.flush(callback_slow_conn; timeout = 2)

            for i in 1:8
                NATS.publish(callback_slow_conn, "natsjl.slow-callback", "callback-$i")
            end
            NATS.flush(callback_slow_conn; timeout = 2)

            @test wait_ready(callback_seen) == "callback-1"
            callback_err = wait_ready(callback_slow_conn.async_errors)
            callback_cb_err = wait_ready(callback_errors)
            @test callback_err isa NATS.SlowConsumerError
            @test callback_cb_err isa NATS.SlowConsumerError
            @test callback_err.subject == "natsjl.slow-callback"
            @test callback_err.sid == callback_sub.sid
            @test NATS.pending(callback_sub) == 3
            @test NATS.pending_bytes(callback_sub) == 3 * sizeof("callback-1")
            @test NATS.max_pending(callback_sub) == 3
            @test NATS.delivered(callback_sub) == 1
            @test NATS.subscription_status(callback_sub) == NATS.SUBSCRIPTION_SLOW_CONSUMER
            @test wait_ready(callback_status) == NATS.SUBSCRIPTION_SLOW_CONSUMER

            put!(release_callback, nothing)
            @test wait_ready(callback_seen) == "callback-2"
            @test wait_ready(callback_seen) == "callback-3"
            @test timedwait(() -> NATS.pending(callback_sub) == 0, 2; pollint = 0.001) == :ok
            @test NATS.delivered(callback_sub) == 3

            NATS.publish(callback_slow_conn, "natsjl.slow-callback", "callback-recovered")
            NATS.flush(callback_slow_conn; timeout = 2)
            @test wait_ready(callback_status) == NATS.SUBSCRIPTION_ACTIVE
            @test wait_ready(callback_seen) == "callback-recovered"
            @test NATS.pending(callback_sub) == 0
            @test NATS.delivered(callback_sub) == 4
            NATS.unsubscribe(callback_sub)
        finally
            NATS.close(callback_slow_conn)
        end
    end

    @testset "barrier async callbacks" begin
        conn = NATS.connect(url)
        try
            barrier_results = Channel{Bool}(8)
            pub_counts = Ref(0)
            pub_lock = ReentrantLock()
            pub_sub = NATS.subscribe(conn, "natsjl.barrier.pub") do _msg
                sleep(0.05)
                lock(pub_lock)
                try
                    pub_counts[] += 1
                finally
                    unlock(pub_lock)
                end
            end
            close_sub = NATS.subscribe(conn, "natsjl.barrier.close") do _msg
                NATS.barrier(conn) do
                    count = begin
                        lock(pub_lock)
                        try
                            pub_counts[]
                        finally
                            unlock(pub_lock)
                        end
                    end
                    put!(barrier_results, count == 2)
                end
            end
            NATS.flush(conn; timeout = 2)
            NATS.publish(conn, "natsjl.barrier.pub", "one")
            NATS.publish(conn, "natsjl.barrier.pub", "two")
            NATS.publish(conn, "natsjl.barrier.close", "done")
            NATS.flush(conn; timeout = 2)
            @test wait_ready(barrier_results, 3) === true
            NATS.unsubscribe(pub_sub)
            NATS.unsubscribe(close_sub)

            NATS.barrier(conn) do
                put!(barrier_results, true)
            end
            @test wait_ready(barrier_results) === true

            async_count = Ref(0)
            async_sub = NATS.subscribe(conn, "natsjl.barrier.onlyasync") do _msg
                async_count[] += 1
                NATS.barrier(conn) do
                    put!(barrier_results, async_count[] == 1)
                end
            end
            sync_sub = NATS.subscribe(conn, "natsjl.barrier.onlyasync")
            NATS.flush(conn; timeout = 2)
            NATS.publish(conn, "natsjl.barrier.onlyasync", "hello")
            NATS.flush(conn; timeout = 2)
            @test wait_ready(barrier_results, 3) === true
            @test NATS.payload(NATS.next_msg(sync_sub; timeout = 2)) == "hello"
            NATS.unsubscribe(async_sub)
            NATS.unsubscribe(sync_sub)

            republished = Channel{Bool}(1)
            repub_sub = NATS.subscribe(conn, "natsjl.barrier.republish") do msg
                payload = NATS.payload(msg)
                if payload == "first"
                    NATS.barrier(conn) do
                        NATS.publish(conn, "natsjl.barrier.republish", "second")
                        NATS.flush(conn; timeout = 2)
                    end
                elseif payload == "second"
                    put!(republished, true)
                end
            end
            NATS.flush(conn; timeout = 2)
            NATS.publish(conn, "natsjl.barrier.republish", "first")
            @test wait_ready(republished, 3) === true
            NATS.unsubscribe(repub_sub)
        finally
            NATS.close(conn)
        end
        @test_throws NATS.ConnectionClosedError NATS.barrier(conn, () -> put!(barrier_results, false))
    end

    @testset "queue groups and drain" begin
        conn = NATS.connect(url)
        try
            sub1 = NATS.subscribe(conn, "natsjl.queue", queue = "workers")
            sub2 = NATS.subscribe(conn, "natsjl.queue", queue = "workers")
            NATS.publish(conn, "natsjl.queue", "one")
            NATS.flush(conn; timeout = 2)
            received = 0
            for sub in (sub1, sub2)
                try
                    msg = NATS.next_msg(sub; timeout = 0.5)
                    @test NATS.payload(msg) == "one"
                    received += 1
                catch err
                    err isa NATS.ConnectionTimeoutError || rethrow()
                end
            end
            @test received == 1

            bulk_sub1 = NATS.subscribe(conn, "natsjl.queue.bulk"; queue = "workers")
            bulk_sub2 = NATS.subscribe(conn, "natsjl.queue.bulk"; queue = "workers")
            NATS.flush(conn; timeout = 2)
            bulk_total = 200
            for i in 1:bulk_total
                NATS.publish(conn, "natsjl.queue.bulk", "bulk-$i")
            end
            NATS.flush(conn; timeout = 2)
            bulk1 = NATS.pending(bulk_sub1)
            bulk2 = NATS.pending(bulk_sub2)
            @test bulk1 + bulk2 == bulk_total
            @test bulk1 > 0
            @test bulk2 > 0
            for _ in 1:bulk1
                @test startswith(NATS.payload(NATS.next_msg(bulk_sub1; timeout = 2)), "bulk-")
            end
            for _ in 1:bulk2
                @test startswith(NATS.payload(NATS.next_msg(bulk_sub2; timeout = 2)), "bulk-")
            end
            NATS.unsubscribe(bulk_sub1)
            NATS.unsubscribe(bulk_sub2)

            queue_drain_conn = NATS.connect(url)
            try
                drain_subject = "natsjl.queue.drain.replace"
                expected = 256
                received_count = Ref(0)
                done_sent = Ref(false)
                received_lock = ReentrantLock()
                done = Channel{Bool}(1)
                drain_errors = Channel{Any}(8)

                function note_queue_drain_message!()
                    lock(received_lock)
                    try
                        received_count[] += 1
                        count = received_count[]
                        if count >= expected && !done_sent[]
                            done_sent[] = true
                            put!(done, true)
                        end
                        return count
                    finally
                        unlock(received_lock)
                    end
                end

                function create_replacement_queue_sub!()
                    NATS.subscribe(queue_drain_conn, drain_subject; queue = "workers") do _msg
                        note_queue_drain_message!()
                    end
                end

                function create_draining_queue_sub!()
                    sub_ref = Ref{Union{Nothing, NATS.Subscription}}(nothing)
                    sub = NATS.subscribe(queue_drain_conn, drain_subject; queue = "workers") do _msg
                        count = note_queue_drain_message!()
                        if count % 3 == 0
                            sub = sub_ref[]
                            if sub !== nothing && !sub.closed
                                try
                                    NATS.drain(sub; timeout = 2)
                                    create_replacement_queue_sub!()
                                catch err
                                    put!(drain_errors, err)
                                end
                            end
                        end
                    end
                    sub_ref[] = sub
                    return sub
                end

                for _ in 1:8
                    create_draining_queue_sub!()
                end
                NATS.flush(queue_drain_conn; timeout = 2)
                for _ in 1:expected
                    NATS.publish(queue_drain_conn, drain_subject, "work")
                end
                NATS.flush(queue_drain_conn; timeout = 2)
                @test wait_ready(done, 5) === true
                sleep(0.05)
                @test !isready(drain_errors)
                @test received_count[] == expected
            finally
                NATS.close(queue_drain_conn)
            end

            callback_conn = NATS.connect(url)
            try
                callback_seen = Channel{String}(2)
                callback_sub = NATS.subscribe(callback_conn, "natsjl.callback-errors") do msg
                    payload = NATS.payload(msg)
                    if payload == "bad"
                        error("callback boom")
                    elseif payload == "good"
                        put!(callback_seen, payload)
                    end
                end
                NATS.flush(callback_conn; timeout = 2)
                @test_throws NATS.SyncSubscriptionRequiredError NATS.next_msg(callback_sub; timeout = 0.1)
                @test sprint(showerror, NATS.SyncSubscriptionRequiredError()) == "NATS illegal call on an async subscription"
                NATS.publish(callback_conn, "natsjl.callback-errors", "bad")
                NATS.publish(callback_conn, "natsjl.callback-errors", "good")
                NATS.flush(callback_conn; timeout = 2)
                @test wait_ready(callback_conn.async_errors) isa ErrorException
                @test wait_ready(callback_seen) == "good"
                NATS.unsubscribe(callback_sub)

                callback_flush_done = Channel{Bool}(1)
                callback_flush_sub = NATS.subscribe(callback_conn, "natsjl.callback-flush") do msg
                    NATS.flush(callback_conn; timeout = 2)
                    put!(callback_flush_done, NATS.payload(msg) == "flush")
                end
                NATS.flush(callback_conn; timeout = 2)
                NATS.publish(callback_conn, "natsjl.callback-flush", "flush")
                @test wait_ready(callback_flush_done, 3) === true
                NATS.unsubscribe(callback_flush_sub)
            finally
                NATS.close(callback_conn)
            end

            sub_timeout_conn = NATS.connect(url)
            try
                sub_timeout_done = Channel{Bool}(1)
                sub_timeout = NATS.subscribe(sub_timeout_conn, "natsjl.subdrain.timeout") do _msg
                    sleep(0.15)
                    put!(sub_timeout_done, true)
                end
                NATS.flush(sub_timeout_conn; timeout = 2)
                NATS.publish(sub_timeout_conn, "natsjl.subdrain.timeout", "slow")
                NATS.flush(sub_timeout_conn; timeout = 2)
                @test_throws NATS.DrainTimeoutError NATS.drain(sub_timeout; timeout = 0.01)
                @test NATS.last_error(sub_timeout_conn) isa NATS.DrainTimeoutError
                @test NATS.connection_status(sub_timeout_conn) == NATS.CONNECTED
                @test wait_ready(sub_timeout_done, 1) === true
            finally
                NATS.close(sub_timeout_conn)
            end

            conn_timeout_closed = Channel{Any}(1)
            conn_timeout_errors = Channel{Any}(1)
            conn_timeout_done = Channel{Bool}(1)
            conn_timeout = NATS.connect(
                url;
                error_cb = (_conn, err) -> put!(conn_timeout_errors, err),
                closed_cb = conn -> put!(conn_timeout_closed, NATS.last_error(conn)),
            )
            try
                NATS.subscribe(conn_timeout, "natsjl.drain.timeout") do _msg
                    sleep(0.15)
                    put!(conn_timeout_done, true)
                end
                NATS.flush(conn_timeout; timeout = 2)
                NATS.publish(conn_timeout, "natsjl.drain.timeout", "slow")
                NATS.flush(conn_timeout; timeout = 2)
                @test_throws NATS.DrainTimeoutError NATS.drain(conn_timeout; timeout = 0.01)
                @test wait_ready(conn_timeout_errors) isa NATS.DrainTimeoutError
                @test wait_ready(conn_timeout_closed) isa NATS.DrainTimeoutError
                @test NATS.last_error(conn_timeout) isa NATS.DrainTimeoutError
                @test NATS.connection_status(conn_timeout) == NATS.CLOSED
                @test wait_ready(conn_timeout_done, 1) === true
            finally
                NATS.close(conn_timeout)
            end

            drained_messages = String[]
            sub_drain_closed = Channel{String}(1)
            sub_drain = NATS.subscribe(conn, "natsjl.subdrain") do msg
                push!(drained_messages, NATS.payload(msg))
            end
            NATS.set_closed_handler!(sub_drain, subject -> put!(sub_drain_closed, subject))
            sub_drain_status = NATS.status_changed(sub_drain)
            @test wait_ready(sub_drain_status) == NATS.SUBSCRIPTION_ACTIVE
            NATS.flush(conn; timeout = 2)
            for i in 1:5
                NATS.publish(conn, "natsjl.subdrain", "drain-$i")
            end
            NATS.drain(sub_drain; timeout = 2)
            @test wait_ready(sub_drain_closed) == "natsjl.subdrain"
            @test wait_ready(sub_drain_status) == NATS.SUBSCRIPTION_DRAINING
            @test wait_ready(sub_drain_status) == NATS.SUBSCRIPTION_CLOSED
            @test sort(drained_messages) == ["drain-1", "drain-2", "drain-3", "drain-4", "drain-5"]
            @test sub_drain.closed
            @test !haskey(conn.subscriptions, sub_drain.sid)
            @test sub_drain.task !== nothing && istaskdone(sub_drain.task)
            @test_throws NATS.BadSubscriptionError NATS.drain(sub_drain; timeout = 0.1)
            @test_throws NATS.BadSubscriptionError NATS.unsubscribe(sub_drain)

            drain_reply_conn = NATS.connect(url)
            drain_client_conn = NATS.connect(url)
            try
                responses = Channel{String}(8)
                release_callbacks = Channel{Bool}(1)
                NATS.subscribe(drain_client_conn, "natsjl.conn-drain.responses") do msg
                    put!(responses, NATS.payload(msg))
                end
                NATS.flush(drain_client_conn; timeout = 2)

                NATS.subscribe(drain_reply_conn, "natsjl.conn-drain.requests") do msg
                    while !isready(release_callbacks)
                        sleep(0.001)
                    end
                    NATS.publish(drain_reply_conn, msg.reply, "reply-$(NATS.payload(msg))")
                end
                NATS.flush(drain_reply_conn; timeout = 2)

                for i in 1:5
                    NATS.publish(
                        drain_client_conn,
                        "natsjl.conn-drain.requests",
                        string(i);
                        reply = "natsjl.conn-drain.responses",
                    )
                end
                NATS.flush(drain_client_conn; timeout = 2)
                NATS.flush(drain_reply_conn; timeout = 2)

                drain_reply_status = NATS.status_changed(drain_reply_conn, NATS.DRAINING, NATS.CLOSED)
                drain_reply_task = @async NATS.drain(drain_reply_conn; timeout = 2)
                @test wait_ready(drain_reply_status) == NATS.DRAINING
                @test_throws NATS.ConnectionDrainingError NATS.subscribe(drain_reply_conn, "natsjl.conn-drain.new-sub")
                @test NATS.publish(drain_reply_conn, "natsjl.conn-drain.side-publish", "allowed") === nothing
                put!(release_callbacks, true)

                @test timedwait(() -> istaskdone(drain_reply_task), 2; pollint = 0.001) == :ok
                fetch(drain_reply_task)
                @test wait_ready(drain_reply_status) == NATS.CLOSED
                @test sort([wait_ready(responses) for _ in 1:5]) == ["reply-1", "reply-2", "reply-3", "reply-4", "reply-5"]
                @test !isready(drain_reply_conn.async_errors)
            finally
                NATS.close(drain_reply_conn)
                NATS.close(drain_client_conn)
            end

            drain_request_conn = NATS.connect(url)
            drain_upstream_conn = NATS.connect(url)
            drain_request_client = NATS.connect(url)
            try
                upstream = NATS.subscribe(drain_upstream_conn, "natsjl.conn-drain.upstream") do req
                    NATS.respond(drain_upstream_conn, req, "upstream-$(NATS.payload(req))")
                end
                NATS.flush(drain_upstream_conn; timeout = 2)
                warm = NATS.request(drain_request_conn, "natsjl.conn-drain.upstream", "warm"; timeout = 2)
                @test NATS.payload(warm) == "upstream-warm"
                @test drain_request_conn.request_sub !== nothing

                request_errors = Channel{Any}(1)
                request_release = Channel{Bool}(1)
                NATS.subscribe(drain_request_conn, "natsjl.conn-drain.final-request") do req
                    while !isready(request_release)
                        sleep(0.001)
                    end
                    try
                        upstream_reply = NATS.request(
                            drain_request_conn,
                            "natsjl.conn-drain.upstream",
                            NATS.payload(req);
                            timeout = 2,
                        )
                        NATS.respond(drain_request_conn, req, "final-$(NATS.payload(upstream_reply))")
                    catch err
                        put!(request_errors, err)
                    end
                end
                responses = Channel{String}(1)
                NATS.subscribe(drain_request_client, "natsjl.conn-drain.final-responses") do msg
                    put!(responses, NATS.payload(msg))
                end
                NATS.flush(drain_request_conn; timeout = 2)
                NATS.flush(drain_request_client; timeout = 2)

                NATS.publish(
                    drain_request_client,
                    "natsjl.conn-drain.final-request",
                    "work";
                    reply = "natsjl.conn-drain.final-responses",
                )
                NATS.flush(drain_request_client; timeout = 2)
                NATS.flush(drain_request_conn; timeout = 2)

                request_drain_status = NATS.status_changed(drain_request_conn, NATS.DRAINING, NATS.CLOSED)
                request_drain_task = @async NATS.drain(drain_request_conn; timeout = 2)
                @test wait_ready(request_drain_status) == NATS.DRAINING
                put!(request_release, true)

                @test timedwait(() -> istaskdone(request_drain_task), 2; pollint = 0.001) == :ok
                fetch(request_drain_task)
                @test wait_ready(request_drain_status) == NATS.CLOSED
                @test wait_ready(responses) == "final-upstream-work"
                @test !isready(request_errors)
                @test !isready(drain_request_conn.async_errors)
                @test upstream isa NATS.Subscription
            finally
                NATS.close(drain_request_conn)
                NATS.close(drain_upstream_conn)
                NATS.close(drain_request_client)
            end

            drain_sub = NATS.subscribe(conn, "natsjl.drain")
            drain_sub_closed = Channel{String}(1)
            NATS.set_closed_handler!(drain_sub, subject -> put!(drain_sub_closed, subject))
            NATS.publish(conn, "natsjl.drain", "before-close")
            @test NATS.payload(NATS.next_msg(drain_sub; timeout = 2)) == "before-close"
            conn_status = NATS.status_changed(conn, NATS.DRAINING, NATS.CLOSED)
            NATS.drain(conn; timeout = 2)
            @test wait_ready(drain_sub_closed) == "natsjl.drain"
            @test wait_ready(conn_status) == NATS.DRAINING
            @test wait_ready(conn_status) == NATS.CLOSED
            @test NATS.connection_status(conn) == NATS.CLOSED
            @test_throws NATS.ConnectionClosedError NATS.publish(conn, "natsjl.drain", "after-close")
            @test_throws NATS.ConnectionClosedError NATS.subscribe(conn, "natsjl.closed")
            @test isempty(conn.subscriptions)
        finally
            NATS.close(conn)
        end
    end

    @testset "JetStream publish and ack variants" begin
        conn = NATS.connect(url)
        stream = "NATSJL_TEST"
        ack_stream = "NATSJL_ACKS"
        config_stream = "NATSJL_CONFIG"
        ordered_stream = "NATSJL_ORDERED"
        ordered_concurrent_stream = "NATSJL_ORDERED_CONCURRENT"
        ordered_mode_stream = "NATSJL_ORDERED_MODE"
        ordered_gap_stream = "NATSJL_ORDERED_GAP"
        ordered_consume_stream = "NATSJL_ORDERED_CONSUME"
        push_stream = "NATSJL_PUSH"
        bytes_stream = "NATSJL_BYTES"
        async_noack_stream = "NATSJL_ASYNC_NOACK"
        async_retry_stream = "NATSJL_ASYNC_RETRY"
        async_handler_stream = "NATSJL_ASYNC_HANDLER"
        sync_retry_stream = "NATSJL_SYNC_RETRY"
        origin_stream = "NATSJL_ORIGIN"
        mirror_stream = "NATSJL_MIRROR"
        sourced_stream = "NATSJL_SOURCED"
        drain_cleanup_stream = "NATSJL_DRAIN_CLEANUP"
        manage_stream = "NATSJL_MGMT"
        purge_stream_name = "NATSJL_PURGE"
        create_or_update_stream_name = "NATSJL_UPSERT"
        bucket = "NATSJLKV"
        create_bucket = "NATSJLKVCREATE"
        update_bucket = "NATSJLKVUPDATE"
        create_or_update_bucket = "NATSJLKVCREATEORUPDATE"
        repair_bucket = "NATSJLKVREPAIR"
        republish_bucket = "NATSJLKVPUB"
        mirror_bucket = "NATSJLKVMIRROR"
        mirror_source_bucket = "NATSJLKVMIRRORSRC"
        sourced_bucket = "NATSJLKVSOURCED"
            source_bucket_one = "NATSJLKVSRC1"
            source_bucket_two = "NATSJLKVSRC2"
            filter_bucket = "NATSJLKVMATCH"
            purge_deletes_bucket = "NATSJLKVPURGEDELETES"
            stop_bucket = "NATSJLKVSTOP"
            object_bucket = "NATSJLOBJ"
        object_bucket_link = "NATSJLOBJLINK"
        object_cleanup_bucket = "NATSJLOBJCLEANUP"
        object_empty_bucket = "NATSJLOBJEMPTY"
        object_bad_meta_bucket = "NATSJLOBJBADMETA"
        object_prefix_decoy_stream = "OBJ_NATSJL_DECOY"
        object_subject_decoy_stream = "NATSJL_OBJ_DECOY"
        try
            account_before = JetStream.account_info(conn; timeout = 2)
            @test Int(Base.get(account_before.api, :total, 0)) >= 0

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = config_stream,
                subjects = ["$config_stream.*"],
                description = "config parity stream",
                storage = "file",
                max_consumers = 5,
                max_msgs = 100,
                max_bytes = 1_000_000,
                discard = "new",
                max_age = 60_000_000_000,
                max_msgs_per_subject = 10,
                max_msg_size = 1024,
                duplicate_window = 30_000_000_000,
                allow_rollup_hdrs = true,
                compression = "s2",
                subject_transform = JetStream.SubjectTransformConfig(source = ">", destination = "$config_stream.transformed.>"),
                republish = JetStream.RePublish(source = ">", destination = "$config_stream.rep.>", headers_only = true),
                consumer_limits = JetStream.StreamConsumerLimits(inactive_threshold = 120_000_000_000, max_ack_pending = 50),
                metadata = Dict("purpose" => "config-parity"),
            ))
            sinfo = JetStream.stream_info(conn, config_stream; timeout = 2)
            @test String(sinfo.config.description) == "config parity stream"
            @test Int(sinfo.config.max_consumers) == 5
            @test Int(sinfo.config.max_msgs) == 100
            @test Int(sinfo.config.max_bytes) == 1_000_000
            @test String(sinfo.config.discard) == "new"
            @test Int(sinfo.config.max_age) == 60_000_000_000
            @test Int(sinfo.config.max_msgs_per_subject) == 10
            @test Int(sinfo.config.max_msg_size) == 1024
            @test Int(sinfo.config.duplicate_window) == 30_000_000_000
            @test Bool(sinfo.config.allow_rollup_hdrs)
            @test String(sinfo.config.compression) == "s2"
            @test String(sinfo.config.subject_transform.src) == ">"
            @test String(sinfo.config.subject_transform.dest) == "$config_stream.transformed.>"
            @test String(sinfo.config.republish.src) == ">"
            @test String(sinfo.config.republish.dest) == "$config_stream.rep.>"
            @test Bool(sinfo.config.republish.headers_only)
            @test Int(sinfo.config.consumer_limits.inactive_threshold) == 120_000_000_000
            @test Int(sinfo.config.consumer_limits.max_ack_pending) == 50
            @test String(sinfo.config.metadata.purpose) == "config-parity"
            @test JetStream.stream_name_by_subject(conn, "$config_stream.one"; timeout = 2) == config_stream
            @test JetStream.stream_name_by_subject(conn, "$config_stream.*"; timeout = 2) == config_stream
            @test_throws JetStream.NoMatchingStreamError JetStream.stream_name_by_subject(conn, "NATSJL_MISSING.subject"; timeout = 2)
            stream_infos = JetStream.streams(conn; timeout = 2)
            @test any(info -> String(info.config.name) == config_stream, stream_infos)
            filtered_streams = JetStream.streams(conn; subject_filter = "$config_stream.one", timeout = 2)
            @test length(filtered_streams) == 1
            @test String(only(filtered_streams).config.name) == config_stream
            @test isempty(JetStream.streams(conn; subject_filter = "NATSJL_MISSING.subject", timeout = 2))
            stream_name_in_use_err = try
                JetStream.create_stream(conn, JetStream.StreamConfig(
                    name = config_stream,
                    subjects = ["$config_stream.conflict"],
                    description = "conflicting create-only stream",
                    storage = "memory",
                ); timeout = 2)
                nothing
            catch err
                err
            end
            @test stream_name_in_use_err isa JetStream.StreamNameAlreadyInUseError
            @test stream_name_in_use_err.stream == config_stream
            missing_stream = "NATSJL_MISSING_STREAM"
            missing_stream_err = try
                JetStream.stream_info(conn, missing_stream; timeout = 2)
                nothing
            catch err
                err
            end
            @test missing_stream_err isa JetStream.StreamNotFoundError
            @test missing_stream_err.stream == missing_stream
            @test_throws JetStream.StreamNotFoundError JetStream.update_stream(conn, JetStream.StreamConfig(
                name = missing_stream,
                subjects = ["$missing_stream.*"],
                storage = "memory",
            ); timeout = 2)
            @test_throws JetStream.StreamNotFoundError JetStream.delete_stream(conn, missing_stream; timeout = 2)
            @test_throws JetStream.StreamNotFoundError JetStream.create_consumer(conn, missing_stream, JetStream.ConsumerConfig(
                name = "MISSING_STREAM_CONSUMER",
                durable_name = "MISSING_STREAM_CONSUMER",
            ); timeout = 2)
            @test_throws JetStream.StreamNotFoundError JetStream.consumer_names(conn, missing_stream; timeout = 2)
            @test_throws JetStream.StreamNotFoundError JetStream.consumers(conn, missing_stream; timeout = 2)
            upsert_stream = JetStream.create_or_update_stream(conn, JetStream.StreamConfig(
                name = create_or_update_stream_name,
                subjects = ["$create_or_update_stream_name.*"],
                description = "created by create_or_update_stream",
                storage = "memory",
            ); timeout = 2)
            @test String(upsert_stream.config.description) == "created by create_or_update_stream"
            upsert_stream = JetStream.create_or_update_stream(conn, JetStream.StreamConfig(
                name = create_or_update_stream_name,
                subjects = ["$create_or_update_stream_name.*"],
                description = "updated by create_or_update_stream",
                storage = "memory",
                max_msgs = 10,
            ); timeout = 2)
            @test String(upsert_stream.config.description) == "updated by create_or_update_stream"
            @test Int(upsert_stream.config.max_msgs) == 10

            JetStream.create_stream(conn, JetStream.StreamConfig(name = origin_stream, subjects = ["$origin_stream.*"], storage = "memory", allow_direct = true))
            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = mirror_stream,
                mirror = JetStream.StreamSource(
                    name = origin_stream,
                    opt_start_seq = UInt64(1),
                    subject_transforms = [JetStream.SubjectTransformConfig(source = ">", destination = "$mirror_stream.>")],
                ),
                mirror_direct = true,
            ))
            minfo = JetStream.stream_info(conn, mirror_stream; timeout = 2)
            @test String(minfo.config.mirror.name) == origin_stream
            @test UInt64(minfo.config.mirror.opt_start_seq) == 1
            @test String(minfo.config.mirror.subject_transforms[1].src) == ">"
            @test String(minfo.config.mirror.subject_transforms[1].dest) == "$mirror_stream.>"
            @test Bool(minfo.config.mirror_direct)

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = sourced_stream,
                subjects = ["$sourced_stream.*"],
                sources = [JetStream.StreamSource(
                    name = origin_stream,
                    subject_transforms = [JetStream.SubjectTransformConfig(source = ">", destination = "$sourced_stream.>")],
                )],
            ))
            source_info = JetStream.stream_info(conn, sourced_stream; timeout = 2)
            @test String(source_info.config.sources[1].name) == origin_stream
            @test String(source_info.config.sources[1].subject_transforms[1].src) == ">"
            @test String(source_info.config.sources[1].subject_transforms[1].dest) == "$sourced_stream.>"

            JetStream.create_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFG",
                durable_name = "CFG",
                description = "config parity consumer",
                deliver_policy = "all",
                replay_policy = "instant",
                ack_wait = 5_000_000_000,
                max_deliver = 3,
                backoff = [1_000_000_000, 2_000_000_000],
                max_waiting = 32,
                max_ack_pending = 20,
                headers_only = true,
                max_batch = 16,
                max_expires = 1_000_000_000,
                max_bytes = 100,
                inactive_threshold = 60_000_000_000,
                replicas = 1,
                memory_storage = true,
                metadata = Dict("purpose" => "config-parity"),
            ))
            cfg_cinfo = JetStream.consumer_info(conn, config_stream, "CFG"; timeout = 2)
            @test String(cfg_cinfo.config.description) == "config parity consumer"
            @test String(cfg_cinfo.config.deliver_policy) == "all"
            @test String(cfg_cinfo.config.replay_policy) == "instant"
            @test Int(cfg_cinfo.config.ack_wait) == 1_000_000_000
            @test Int(cfg_cinfo.config.max_deliver) == 3
            @test Int.(cfg_cinfo.config.backoff) == [1_000_000_000, 2_000_000_000]
            @test Int(cfg_cinfo.config.max_waiting) == 32
            @test Int(cfg_cinfo.config.max_ack_pending) == 20
            @test Bool(cfg_cinfo.config.headers_only)
            @test Int(cfg_cinfo.config.max_batch) == 16
            @test Int(cfg_cinfo.config.max_expires) == 1_000_000_000
            @test Int(cfg_cinfo.config.max_bytes) == 100
            @test Int(cfg_cinfo.config.inactive_threshold) == 60_000_000_000
            @test Int(cfg_cinfo.config.num_replicas) == 1
            @test Bool(cfg_cinfo.config.mem_storage)
            @test String(cfg_cinfo.config.metadata.purpose) == "config-parity"
            @test_throws JetStream.ConsumerExistsError JetStream.create_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFG",
                durable_name = "CFG",
                description = "conflicting create-only consumer",
            ); timeout = 2)
            @test_throws JetStream.ConsumerDoesNotExistError JetStream.update_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFGMISSING",
                durable_name = "CFGMISSING",
                description = "missing update-only consumer",
            ); timeout = 2)
            overlap_err = try
                JetStream.create_consumer(conn, config_stream, JetStream.ConsumerConfig(
                    name = "CFGOVERLAP",
                    durable_name = "CFGOVERLAP",
                    filter_subjects = ["$config_stream.*", "$config_stream.owned"],
                ); timeout = 2)
                nothing
            catch err
                err
            end
            @test overlap_err isa JetStream.OverlappingFilterSubjectsError
            cfg_filters = JetStream.create_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFGFILTERS",
                durable_name = "CFGFILTERS",
                filter_subjects = ["$config_stream.one", "$config_stream.two"],
            ); timeout = 2)
            @test String.(cfg_filters.config.filter_subjects) == ["$config_stream.one", "$config_stream.two"]
            cfg_upsert = JetStream.create_or_update_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFGUPSERT",
                durable_name = "CFGUPSERT",
                description = "created by create_or_update_consumer",
            ); timeout = 2)
            @test String(cfg_upsert.config.description) == "created by create_or_update_consumer"
            cfg_upsert = JetStream.create_or_update_consumer(conn, config_stream, JetStream.ConsumerConfig(
                name = "CFGUPSERT",
                durable_name = "CFGUPSERT",
                description = "updated by create_or_update_consumer",
                max_ack_pending = 12,
            ); timeout = 2)
            @test String(cfg_upsert.config.description) == "updated by create_or_update_consumer"
            @test Int(cfg_upsert.config.max_ack_pending) == 12
            cfg_pull = JetStream.pull_subscribe(conn, config_stream, "CFG"; timeout = 2)
            try
                @test !cfg_pull.delete_on_close
                @test String(JetStream.cached_consumer_info(cfg_pull).name) == "CFG"
                @test String(JetStream.consumer_info(cfg_pull; timeout = 2).name) == "CFG"
                @test Int(JetStream.cached_consumer_info(cfg_pull).config.max_batch) == 16
                batch_err = try
                    JetStream.fetch(cfg_pull; batch = 17, no_wait = true, timeout = 2)
                    nothing
                catch err
                    err
                end
                @test batch_err isa ArgumentError
                @test occursin("MaxRequestBatch of 16", sprint(showerror, batch_err))
                bytes_err = try
                    JetStream.fetch_bytes(cfg_pull, 101; batch = 1, expires_ns = 1_000_000_000, timeout = 2)
                    nothing
                catch err
                    err
                end
                @test bytes_err isa ArgumentError
                @test occursin("MaxRequestMaxBytes of 100", sprint(showerror, bytes_err))
                expires_err = try
                    JetStream.fetch(cfg_pull; batch = 1, expires_ns = 1_000_000_001, timeout = 2)
                    nothing
                catch err
                    err
                end
                @test expires_err isa ArgumentError
                @test occursin("MaxRequestExpires of 1000000000ns", sprint(showerror, expires_err))
                @test isempty(JetStream.fetch(cfg_pull; batch = 1, no_wait = true, timeout = 2))
            finally
                close(cfg_pull)
            end
            @test_throws NATS.ConnectionClosedError JetStream.consumer_info(cfg_pull; timeout = 2)
            @test String(JetStream.consumer_info(conn, config_stream, "CFG"; timeout = 2).name) == "CFG"

            owned_pull = JetStream.pull_subscribe(
                conn,
                config_stream,
                JetStream.ConsumerConfig(
                    name = "CFGOWNED",
                    ack_policy = "explicit",
                    filter_subject = "$config_stream.owned",
                );
                timeout = 2,
            )
            try
                @test owned_pull.delete_on_close
                @test owned_pull.consumer == "CFGOWNED"
                @test String(JetStream.cached_consumer_info(owned_pull).name) == "CFGOWNED"
                @test String(JetStream.consumer_info(conn, config_stream, "CFGOWNED"; timeout = 2).name) == "CFGOWNED"
            finally
                close(owned_pull)
            end
            owned_pull_err = try
                JetStream.consumer_info(conn, config_stream, "CFGOWNED"; timeout = 2)
                nothing
            catch err
                err
            end
            @test consumer_missing_error(owned_pull_err)
            @test owned_pull_err.stream == config_stream
            @test owned_pull_err.consumer == "CFGOWNED"
            account_after = JetStream.account_info(conn; timeout = 2)
            @test Int(account_after.streams) >= Int(account_before.streams) + 1
            @test Int(account_after.consumers) >= Int(account_before.consumers) + 1

            JetStream.create_stream(conn, JetStream.StreamConfig(name = drain_cleanup_stream, subjects = ["$drain_cleanup_stream.*"], storage = "memory"))
            JetStream.create_consumer(conn, drain_cleanup_stream, JetStream.ConsumerConfig(
                name = "DRAINBOUND",
                durable_name = "DRAINBOUND",
                filter_subject = "$drain_cleanup_stream.bound",
            ); timeout = 2)
            drain_conn = NATS.connect(url)
            local drain_pull
            local drain_push
            local drain_bound
            try
                drain_pull = JetStream.pull_subscribe(
                    drain_conn,
                    drain_cleanup_stream,
                    JetStream.ConsumerConfig(name = "DRAINPULL", filter_subject = "$drain_cleanup_stream.pull");
                    timeout = 2,
                )
                drain_push = JetStream.push_subscribe(
                    drain_conn,
                    drain_cleanup_stream,
                    JetStream.ConsumerConfig(name = "DRAINPUSH", filter_subject = "$drain_cleanup_stream.push");
                    timeout = 2,
                )
                drain_bound = JetStream.pull_subscribe(drain_conn, drain_cleanup_stream, "DRAINBOUND"; timeout = 2)
                @test drain_pull.delete_on_close
                @test drain_push.delete_on_close
                @test !drain_bound.delete_on_close
                @test String(JetStream.consumer_info(conn, drain_cleanup_stream, "DRAINPULL"; timeout = 2).name) == "DRAINPULL"
                @test String(JetStream.consumer_info(conn, drain_cleanup_stream, "DRAINPUSH"; timeout = 2).name) == "DRAINPUSH"
                NATS.drain(drain_conn; timeout = 5)
                @test drain_pull.subscription.closed
                @test drain_push.subscription.closed
                @test drain_bound.subscription.closed
            finally
                NATS.close(drain_conn)
            end
            drain_pull_err = try
                JetStream.consumer_info(conn, drain_cleanup_stream, "DRAINPULL"; timeout = 2)
                nothing
            catch err
                err
            end
            drain_push_err = try
                JetStream.consumer_info(conn, drain_cleanup_stream, "DRAINPUSH"; timeout = 2)
                nothing
            catch err
                err
            end
            @test consumer_missing_error(drain_pull_err)
            @test consumer_missing_error(drain_push_err)
            @test String(JetStream.consumer_info(conn, drain_cleanup_stream, "DRAINBOUND"; timeout = 2).name) == "DRAINBOUND"
            JetStream.delete_consumer(conn, drain_cleanup_stream, "DRAINBOUND"; timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(name = manage_stream, subjects = ["$manage_stream.*"], storage = "memory", allow_direct = true))
            mgmt1 = JetStream.publish(conn, "$manage_stream.items", "manage-1"; timeout = 2)
            mgmt2 = JetStream.publish(conn, "$manage_stream.items", "manage-2"; timeout = 2)
            mgmt_other = JetStream.publish(conn, "$manage_stream.other", "keep-me"; headers = ["MyHeader" => "MyValue"], timeout = 2)
            stored = JetStream.get_msg(conn, manage_stream, mgmt1.seq; timeout = 2)
            @test String(stored.subject) == "$manage_stream.items"
            @test UInt64(stored.seq) == mgmt1.seq
            @test String(stored.data) == "manage-1"

            last = JetStream.get_last_msg(conn, manage_stream, "$manage_stream.items"; timeout = 2)
            @test UInt64(last.seq) == mgmt2.seq
            @test String(last.data) == "manage-2"

            direct_other = JetStream.get_msg(conn, manage_stream, mgmt_other.seq; direct = true, timeout = 2)
            @test direct_other.stream == manage_stream
            @test direct_other.subject == "$manage_stream.other"
            @test direct_other.seq == mgmt_other.seq
            @test String(direct_other.data) == "keep-me"
            @test Dict(direct_other.headers)["MyHeader"] == "MyValue"
            @test Dict(direct_other.headers)["Nats-Subject"] == "$manage_stream.other"

            direct_next = JetStream.get_msg(conn, manage_stream, 0; direct = true, next_subject = "$manage_stream.other", timeout = 2)
            @test direct_next.seq == mgmt_other.seq
            @test direct_next.subject == "$manage_stream.other"

            direct_last = JetStream.get_last_msg(conn, manage_stream, "$manage_stream.items"; direct = true, timeout = 2)
            @test direct_last.seq == mgmt2.seq
            @test String(direct_last.data) == "manage-2"
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, manage_stream, 100; direct = true, timeout = 2)

            deleted_msg = JetStream.delete_msg(conn, manage_stream, mgmt1.seq; timeout = 2)
            @test Bool(Base.get(deleted_msg, :success, false))
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, manage_stream, mgmt1.seq; timeout = 2)
            default_deleted = JetStream.stream_info(conn, manage_stream; deleted_details = false, timeout = 2)
            @test String(default_deleted.config.name) == manage_stream
            @test !haskey(default_deleted.state, :deleted) || isempty(default_deleted.state.deleted)
            deleted_details = JetStream.stream_info(conn, manage_stream; deleted_details = true, timeout = 2)
            @test String(deleted_details.config.name) == manage_stream

            purged = JetStream.purge_stream(conn, manage_stream; subject_filter = "$manage_stream.items", timeout = 2)
            @test Bool(Base.get(purged, :success, false))
            @test Int(purged.purged) >= 1
            @test_throws JetStream.JetStreamError JetStream.get_last_msg(conn, manage_stream, "$manage_stream.items"; timeout = 2)
            kept = JetStream.get_msg(conn, manage_stream, mgmt_other.seq; timeout = 2)
            @test String(kept.data) == "keep-me"
            @test_throws ArgumentError JetStream.get_msg(conn, manage_stream, 0; timeout = 2)
            @test_throws ArgumentError JetStream.delete_msg(conn, manage_stream, 0; timeout = 2)
            @test_throws ArgumentError JetStream.purge_stream(conn, manage_stream; seq = 0, timeout = 2)
            @test_throws ArgumentError JetStream.purge_stream(conn, manage_stream; keep = -1, timeout = 2)
            @test_throws ArgumentError JetStream.purge_stream(conn, manage_stream; seq = 2, keep = 1, timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(name = purge_stream_name, subjects = ["$purge_stream_name.*"], storage = "memory"))
            for i in 1:5
                subject = isodd(i) ? "$purge_stream_name.A" : "$purge_stream_name.B"
                JetStream.publish(conn, subject, "purge-$i"; timeout = 2)
            end
            seq_purge = JetStream.purge_stream(conn, purge_stream_name; seq = 3, timeout = 2)
            @test Bool(Base.get(seq_purge, :success, false))
            @test Int(seq_purge.purged) == 2
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 1; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 2; timeout = 2)
            @test String(JetStream.get_msg(conn, purge_stream_name, 3; timeout = 2).data) == "purge-3"

            keep_purge = JetStream.purge_stream(conn, purge_stream_name; keep = 1, timeout = 2)
            @test Bool(Base.get(keep_purge, :success, false))
            @test Int(keep_purge.purged) == 2
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 3; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 4; timeout = 2)
            @test String(JetStream.get_msg(conn, purge_stream_name, 5; timeout = 2).data) == "purge-5"

            for i in 6:9
                subject = isodd(i) ? "$purge_stream_name.A" : "$purge_stream_name.B"
                JetStream.publish(conn, subject, "purge-$i"; timeout = 2)
            end
            filter_keep_purge = JetStream.purge_stream(conn, purge_stream_name; subject_filter = "$purge_stream_name.A", keep = 1, timeout = 2)
            @test Bool(Base.get(filter_keep_purge, :success, false))
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 5; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.get_msg(conn, purge_stream_name, 7; timeout = 2)
            @test String(JetStream.get_msg(conn, purge_stream_name, 6; timeout = 2).data) == "purge-6"
            @test String(JetStream.get_msg(conn, purge_stream_name, 8; timeout = 2).data) == "purge-8"
            @test String(JetStream.get_msg(conn, purge_stream_name, 9; timeout = 2).data) == "purge-9"

            JetStream.create_stream(conn, JetStream.StreamConfig(name = stream, subjects = ["$stream.*"], storage = "memory"))
            ack = JetStream.publish(conn, "$stream.one", "hello"; timeout = 2)
            @test ack isa JetStream.PubAck
            @test ack.stream == stream
            @test ack.seq == 1
            @test_throws ArgumentError JetStream.publish(conn, "$stream.one", "bad"; retry_attempts = -1)
            @test_throws ArgumentError JetStream.publish(conn, "$stream.one", "bad"; retry_wait = 0)

            @test_throws JetStream.NoStreamResponseError JetStream.publish(
                conn,
                "NATSJL_SYNC_MISSING.items",
                "missing";
                timeout = 1,
                retry_attempts = 1,
                retry_wait = 0.01,
            )
            sync_retry_creator = errormonitor(@async begin
                sleep(0.15)
                JetStream.create_stream(
                    conn,
                    JetStream.StreamConfig(name = sync_retry_stream, subjects = ["$sync_retry_stream.*"], storage = "memory");
                    timeout = 2,
                )
            end)
            sync_retry_ack = JetStream.publish(
                conn,
                "$sync_retry_stream.items",
                "sync-retry";
                timeout = 2,
                retry_attempts = 10,
                retry_wait = 0.05,
            )
            wait(sync_retry_creator)
            @test sync_retry_ack.stream == sync_retry_stream

            JetStream.create_consumer(conn, stream, JetStream.ConsumerConfig(name = "C", durable_name = "C"))
            msg = JetStream.next_msg(conn, stream, "C"; no_wait = true, timeout = 2)
            @test NATS.payload(msg) == "hello"
            meta = JetStream.metadata(msg)
            @test meta.stream == stream
            @test meta.consumer == "C"
            @test meta.num_delivered == 1
            @test meta.sequence.stream == 1
            @test meta.sequence.consumer == 1
            JetStream.in_progress(conn, msg)
            JetStream.ack_sync(conn, msg; timeout = 2)

            cinfo = JetStream.consumer_info(conn, stream, "C"; timeout = 2)
            @test String(cinfo.name) == "C"
            @test String(cinfo.stream_name) == stream
            @test String(cinfo.config.durable_name) == "C"

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ack_stream, subjects = ["$ack_stream.*"], storage = "memory"))
            JetStream.create_consumer(conn, ack_stream, JetStream.ConsumerConfig(name = "ACKS", durable_name = "ACKS"))
            for i in 1:5
                JetStream.publish(conn, "$ack_stream.acks", "ack-$i"; timeout = 2)
            end
            ack_msgs = JetStream.fetch(conn, ack_stream, "ACKS"; batch = 5, no_wait = true, timeout = 2)
            @test length(ack_msgs) == 5
            capture_ack_payload(msg, f) = begin
                tap = NATS.subscribe(conn, msg.reply)
                try
                    f()
                    NATS.payload(NATS.next_msg(tap; timeout = 2))
                finally
                    try NATS.unsubscribe(conn, tap) catch end
                end
            end
            @test capture_ack_payload(ack_msgs[1], () -> JetStream.double_ack(conn, ack_msgs[1]; timeout = 2)) == "+ACK"
            @test capture_ack_payload(ack_msgs[2], () -> JetStream.nak(conn, ack_msgs[2])) == "-NAK"
            @test capture_ack_payload(ack_msgs[3], () -> JetStream.nak_with_delay(conn, ack_msgs[3], 123)) == "-NAK {\"delay\": 123}"
            @test capture_ack_payload(ack_msgs[4], () -> JetStream.term_with_reason(conn, ack_msgs[4], "with reason")) == "+TERM with reason"
            @test capture_ack_payload(ack_msgs[5], () -> JetStream.in_progress(conn, ack_msgs[5])) == "+WPI"
            JetStream.ack(conn, ack_msgs[5])
            @test_throws ArgumentError JetStream.nak_with_delay(conn, ack_msgs[5], -1)

            JetStream.create_consumer(conn, ack_stream, JetStream.ConsumerConfig(
                name = "NOACK_PULL",
                durable_name = "NOACK_PULL",
                ack_policy = "none",
                filter_subject = "$ack_stream.noackpull",
            ); timeout = 2)
            noack_pull = JetStream.pull_subscribe(conn, ack_stream, "NOACK_PULL"; timeout = 2)
            try
                @test noack_pull.ack_none
                JetStream.publish(conn, "$ack_stream.noackpull", "noack-pull"; timeout = 2)
                noack_pull_msg = only(JetStream.fetch(noack_pull; batch = 1, no_wait = true, timeout = 2))
                @test NATS.payload(noack_pull_msg) == "noack-pull"
                @test JetStream.metadata(noack_pull_msg).consumer == "NOACK_PULL"
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack(conn, noack_pull_msg)
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack_sync(conn, noack_pull_msg; timeout = 0.2)
            finally
                close(noack_pull)
            end

            JetStream.create_consumer(conn, stream, JetStream.ConsumerConfig(name = "D", durable_name = "D", description = "second consumer"))
            @test JetStream.consumer_names(conn, stream; timeout = 2) == ["C", "D"]
            consumer_list = JetStream.consumers(conn, stream; timeout = 2)
            @test sort([String(info.name) for info in consumer_list]) == ["C", "D"]

            updated = JetStream.update_consumer(
                conn,
                stream,
                JetStream.ConsumerConfig(name = "C", durable_name = "C", description = "updated consumer", max_ack_pending = 10);
                timeout = 2,
            )
            @test String(updated.config.description) == "updated consumer"
            @test Int(updated.config.max_ack_pending) == 10
            deleted = JetStream.delete_consumer(conn, stream, "D"; timeout = 2)
            @test Bool(Base.get(deleted, :success, false))
            @test JetStream.consumer_names(conn, stream; timeout = 2) == ["C"]
            @test_throws JetStream.ConsumerNotFoundError JetStream.consumer_info(conn, stream, "D"; timeout = 2)
            @test_throws JetStream.ConsumerNotFoundError JetStream.delete_consumer(conn, stream, "D"; timeout = 2)

            for i in 1:3
                JetStream.publish(conn, "$stream.batch", "batch-$i"; timeout = 2)
            end
            batch = JetStream.fetch(conn, stream, "C"; batch = 2, no_wait = true, timeout = 2)
            @test NATS.payload.(batch) == ["batch-1", "batch-2"]
            foreach(msg -> JetStream.ack(conn, msg), batch)

            rest = JetStream.fetch(conn, stream, "C"; batch = 5, no_wait = true, timeout = 2)
            @test NATS.payload.(rest) == ["batch-3"]
            foreach(msg -> JetStream.ack(conn, msg), rest)

            @test isempty(JetStream.fetch(conn, stream, "C"; batch = 2, no_wait = true, timeout = 2))
            @test_throws ArgumentError JetStream.fetch(conn, stream, "C"; batch = 0)

            for i in 1:3
                JetStream.publish(conn, "$stream.pullsub", "pullsub-$i"; timeout = 2)
            end
            pull = JetStream.pull_subscribe(conn, stream, "C")
            try
                first_pull = JetStream.fetch(pull; batch = 2, no_wait = true, timeout = 2)
                @test NATS.payload.(first_pull) == ["pullsub-1", "pullsub-2"]
                foreach(msg -> JetStream.ack(conn, msg), first_pull)

                single = JetStream.next_msg(pull; no_wait = true, timeout = 2)
                @test NATS.payload(single) == "pullsub-3"
                JetStream.ack(conn, single)

                @test isempty(JetStream.fetch(pull; batch = 2, no_wait = true, timeout = 2))
                @test_throws JetStream.JetStreamError JetStream.next_msg(pull; no_wait = true, timeout = 2)
                @test_throws ArgumentError JetStream.fetch(pull; batch = 0)

                empty_with_heartbeat = JetStream.fetch(
                    pull;
                    batch = 1,
                    expires_ns = 300_000_000,
                    heartbeat_ns = 100_000_000,
                    timeout = 2,
                )
                @test isempty(empty_with_heartbeat)
            finally
                close(pull)
            end
            @test pull.closed
            @test pull.subscription.closed
            @test_throws NATS.ConnectionClosedError JetStream.fetch(pull; no_wait = true, timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(name = bytes_stream, subjects = ["$bytes_stream.*"], storage = "memory"))
            JetStream.create_consumer(conn, bytes_stream, JetStream.ConsumerConfig(name = "BYTES", durable_name = "BYTES"))
            for _ in 1:5
                JetStream.publish(conn, "$bytes_stream.items", "0123456789"; timeout = 2)
            end
            @test_throws ArgumentError JetStream.fetch_bytes(conn, bytes_stream, "BYTES", 0)
            too_small_bytes = JetStream.fetch_bytes(conn, bytes_stream, "BYTES", 1; no_wait = true, timeout = 2)
            @test isempty(too_small_bytes)
            limited_bytes = JetStream.fetch_bytes(conn, bytes_stream, "BYTES", 200; no_wait = true, timeout = 2)
            @test 1 <= length(limited_bytes) < 5
            foreach(msg -> JetStream.ack(conn, msg), limited_bytes)
            pull_bytes = JetStream.pull_subscribe(conn, bytes_stream, "BYTES")
            try
                remaining_bytes = JetStream.fetch_bytes(pull_bytes, 10_000; batch = 10, no_wait = true, timeout = 2)
                @test length(remaining_bytes) == 5 - length(limited_bytes)
                foreach(msg -> JetStream.ack(conn, msg), remaining_bytes)
            finally
                close(pull_bytes)
            end

            for i in 1:3
                JetStream.publish(conn, "$stream.consume", "consume-$i"; timeout = 2)
            end
            consumer_ctx = JetStream.consume(
                conn,
                stream,
                "C";
                batch = 2,
                expires_ns = 500_000_000,
                timeout = 2,
                channel_size = 4,
            )
            try
                consumed = [JetStream.next_msg(consumer_ctx; timeout = 2) for _ in 1:3]
                @test NATS.payload.(consumed) == ["consume-1", "consume-2", "consume-3"]
                foreach(msg -> JetStream.ack(conn, msg), consumed)
                @test !isready(JetStream.errors(consumer_ctx))
            finally
                close(consumer_ctx)
            end
            @test consumer_ctx.closed
            @test_throws NATS.ConnectionClosedError JetStream.next_msg(consumer_ctx; timeout = 0.1)
            @test_throws ArgumentError JetStream.consume(conn, stream, "C"; batch = 0)

            JetStream.create_stream(conn, JetStream.StreamConfig(name = push_stream, subjects = ["$push_stream.*"], storage = "memory"))
            push = JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(name = "PUSH", durable_name = "PUSH", ack_policy = "explicit");
                timeout = 2,
                channel_size = 8,
            )
            try
                @test push.delete_on_close
                @test push.consumer == "PUSH"
                @test startswith(push.deliver_subject, "_INBOX.")
                @test push.deliver_group === nothing
                @test String(JetStream.cached_consumer_info(push).config.deliver_subject) == push.deliver_subject
                @test String(JetStream.consumer_info(push; timeout = 2).name) == "PUSH"
                @test String(JetStream.cached_consumer_info(push).name) == "PUSH"
                for i in 1:3
                    JetStream.publish(conn, "$push_stream.items", "push-$i"; timeout = 2)
                end
                pushed = [JetStream.next_msg(push; timeout = 2) for _ in 1:3]
                @test NATS.payload.(pushed) == ["push-1", "push-2", "push-3"]
                @test [JetStream.metadata(msg).consumer for msg in pushed] == ["PUSH", "PUSH", "PUSH"]
                foreach(msg -> JetStream.ack(conn, msg), pushed)
            finally
                close(push)
            end
            @test push.closed
            @test push.subscription.closed
            @test_throws NATS.ConnectionClosedError JetStream.next_msg(push; timeout = 0.1)
            @test_throws NATS.ConnectionClosedError JetStream.consumer_info(push; timeout = 2)
            push_err = try
                JetStream.consumer_info(conn, push_stream, "PUSH"; timeout = 2)
                nothing
            catch err
                err
            end
            @test consumer_missing_error(push_err)
            @test_throws ArgumentError JetStream.push_subscribe(conn, stream, "C"; timeout = 2)

            push_noack = JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "PUSHNOACK",
                    ack_policy = "none",
                    filter_subject = "$push_stream.noack",
                );
                timeout = 2,
                channel_size = 4,
            )
            try
                @test push_noack.delete_on_close
                @test push_noack.ack_none
                JetStream.publish(conn, "$push_stream.noack", "push-noack"; timeout = 2)
                pushed_noack = JetStream.next_msg(push_noack; timeout = 2)
                @test NATS.payload(pushed_noack) == "push-noack"
                @test JetStream.metadata(pushed_noack).consumer == "PUSHNOACK"
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack(conn, pushed_noack)
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack_sync(conn, pushed_noack; timeout = 0.2)
            finally
                close(push_noack)
            end
            push_noack_err = try
                JetStream.consumer_info(conn, push_stream, "PUSHNOACK"; timeout = 2)
                nothing
            catch err
                err
            end
            @test consumer_missing_error(push_noack_err)

            push_queue_deliver = NATS.new_inbox(conn)
            JetStream.create_consumer(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "PUSHQ",
                    durable_name = "PUSHQ",
                    deliver_subject = push_queue_deliver,
                    deliver_group = "workers",
                    ack_policy = "explicit",
                    filter_subject = "$push_stream.queue",
                );
                timeout = 2,
            )
            push_queue = JetStream.push_subscribe(conn, push_stream, "PUSHQ"; timeout = 2, channel_size = 4)
            try
                @test !push_queue.delete_on_close
                @test push_queue.deliver_subject == push_queue_deliver
                @test push_queue.deliver_group == "workers"
                @test push_queue.subscription.queue == "workers"
                @test String(JetStream.consumer_info(push_queue; timeout = 2).name) == "PUSHQ"
                JetStream.publish(conn, "$push_stream.queue", "queue-1"; timeout = 2)
                queued = JetStream.next_msg(push_queue; timeout = 2)
                @test NATS.payload(queued) == "queue-1"
                @test JetStream.metadata(queued).consumer == "PUSHQ"
                JetStream.ack(conn, queued)
            finally
                close(push_queue)
            end
            @test String(JetStream.consumer_info(conn, push_stream, "PUSHQ"; timeout = 2).name) == "PUSHQ"
            @test_throws ArgumentError JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "BADPUSHQHB",
                    durable_name = "BADPUSHQHB",
                    deliver_group = "workers",
                    idle_heartbeat = 1_000_000_000,
                    filter_subject = "$push_stream.bad-hb",
                );
                timeout = 2,
            )
            @test_throws ArgumentError JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "BADPUSHQFC",
                    durable_name = "BADPUSHQFC",
                    deliver_group = "workers",
                    flow_control = true,
                    filter_subject = "$push_stream.bad-fc",
                );
                timeout = 2,
            )
            wrong_pull_err = try
                JetStream.pull_subscribe(conn, push_stream, "PUSHQ"; timeout = 2)
                nothing
            catch err
                err
            end
            @test wrong_pull_err isa ArgumentError
            @test occursin("pull consumer", sprint(showerror, wrong_pull_err))

            push_ctx_sub = JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "PUSHCTX",
                    durable_name = "PUSHCTX",
                    ack_policy = "explicit",
                    filter_subject = "$push_stream.ctx",
                );
                timeout = 2,
                channel_size = 4,
            )
            push_ctx = JetStream.consume(push_ctx_sub; channel_size = 4)
            try
                for i in 1:3
                    JetStream.publish(conn, "$push_stream.ctx", "pushctx-$i"; timeout = 2)
                end
                pushed_ctx = [JetStream.next_msg(push_ctx; timeout = 2) for _ in 1:3]
                @test NATS.payload.(pushed_ctx) == ["pushctx-1", "pushctx-2", "pushctx-3"]
                @test [JetStream.metadata(msg).consumer for msg in pushed_ctx] == ["PUSHCTX", "PUSHCTX", "PUSHCTX"]
                foreach(msg -> JetStream.ack(conn, msg), pushed_ctx)
                @test !isready(JetStream.errors(push_ctx))
            finally
                close(push_ctx)
            end
            @test push_ctx.closed
            @test push_ctx_sub.closed
            @test push_ctx_sub.subscription.closed
            @test_throws NATS.ConnectionClosedError JetStream.next_msg(push_ctx; timeout = 0.1)
            @test_throws ArgumentError JetStream.consume(push_ctx_sub; channel_size = 0)
            @test_throws ArgumentError JetStream.consume(push_ctx_sub; poll_interval = 0)

            push_keep = JetStream.push_subscribe(
                conn,
                push_stream,
                JetStream.ConsumerConfig(
                    name = "PUSHKEEP",
                    durable_name = "PUSHKEEP",
                    ack_policy = "explicit",
                    filter_subject = "$push_stream.keep",
                );
                timeout = 2,
                channel_size = 2,
            )
            push_keep_ctx = JetStream.consume(push_keep; channel_size = 1, poll_interval = 0.05, close_push = false)
            try
                close(push_keep_ctx)
                @test timedwait(() -> istaskdone(push_keep_ctx.task), 2; pollint = 0.01) == :ok
                @test push_keep_ctx.closed
                @test !push_keep.closed
                @test !push_keep.subscription.closed
            finally
                close(push_keep)
            end
            @test push_keep.closed
            @test push_keep.subscription.closed

            fake_push = fake_push_subscription(conn; heartbeat_ns = 10_000_000)
            try
                @test_throws JetStream.NoHeartbeatError JetStream.next_msg(fake_push; timeout = 1)
            finally
                close(fake_push.subscription.channel)
            end

            fake_push_ctx_sub = fake_push_subscription(conn; heartbeat_ns = 10_000_000)
            fake_push_ctx = JetStream.consume(fake_push_ctx_sub; channel_size = 1, poll_interval = 0.005, close_push = false)
            try
                err = wait_ready(JetStream.errors(fake_push_ctx), 1)
                @test err isa JetStream.NoHeartbeatError
                @test timedwait(() -> fake_push_ctx.closed, 1; pollint = 0.001) == :ok
                @test !fake_push_ctx_sub.closed
            finally
                close(fake_push_ctx_sub.subscription.channel)
            end

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ordered_stream, subjects = ["$ordered_stream.*"], storage = "memory"))
            for i in 1:8
                JetStream.publish(conn, "$ordered_stream.items", "ordered-$i"; timeout = 2)
            end
            ordered = JetStream.ordered_consumer(conn, ordered_stream; timeout = 2, name_prefix = "ORDTEST")
            try
                ordered_info = JetStream.consumer_info(ordered; timeout = 2)
                @test startswith(String(ordered_info.name), "ORDTEST_")
                @test String(ordered_info.config.ack_policy) == "none"
                @test String(ordered_info.config.deliver_policy) == "all"
                @test Int(ordered_info.config.max_deliver) == -1
                @test Int(ordered_info.config.max_waiting) == 512
                @test Bool(ordered_info.config.mem_storage)

                ordered_batch = JetStream.fetch(ordered; batch = 3, no_wait = true, timeout = 2)
                @test NATS.payload.(ordered_batch) == ["ordered-1", "ordered-2", "ordered-3"]
                @test [JetStream.metadata(msg).sequence.stream for msg in ordered_batch] == UInt64[1, 2, 3]
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack(conn, first(ordered_batch))
                @test_throws JetStream.MsgNoAckReplyError JetStream.ack_sync(conn, first(ordered_batch); timeout = 0.2)

                deleted_consumer = String(JetStream.consumer_info(ordered; timeout = 2).name)
                JetStream.delete_consumer(conn, ordered_stream, deleted_consumer; timeout = 2)
                ordered_rest = JetStream.fetch(ordered; batch = 5, no_wait = true, timeout = 2)
                @test NATS.payload.(ordered_rest) == ["ordered-4", "ordered-5", "ordered-6", "ordered-7", "ordered-8"]
                @test [JetStream.metadata(msg).sequence.stream for msg in ordered_rest] == UInt64[4, 5, 6, 7, 8]
                @test isempty(JetStream.fetch(ordered; batch = 1, no_wait = true, timeout = 2))
                @test_throws JetStream.JetStreamError JetStream.next_msg(ordered; no_wait = true, timeout = 2)
                @test_throws ArgumentError JetStream.fetch(ordered; batch = 0)
            finally
                close(ordered)
            end

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ordered_concurrent_stream, subjects = ["$ordered_concurrent_stream.*"], storage = "memory"))
            JetStream.publish(conn, "$ordered_concurrent_stream.items", "ordered-concurrent-1"; timeout = 2)
            concurrent_ordered = JetStream.ordered_consumer(conn, ordered_concurrent_stream; timeout = 2)
            try
                fetch_result = Channel{Any}(1)
                @async begin
                    try
                        put!(fetch_result, JetStream.fetch(concurrent_ordered; batch = 2, expires_ns = 800_000_000, timeout = 2))
                    catch err
                        put!(fetch_result, err)
                    end
                end
                @test timedwait(() -> concurrent_ordered.operation_running, 1; pollint = 0.001) == :ok
                @test_throws JetStream.OrderedConsumerConcurrentRequestsError JetStream.fetch(concurrent_ordered; batch = 1, no_wait = true, timeout = 2)
                fetched = wait_ready(fetch_result, 3)
                @test fetched isa Vector{NATS.Msg}
                @test NATS.payload.(fetched) == ["ordered-concurrent-1"]
            finally
                close(concurrent_ordered)
            end

            concurrent_bytes = JetStream.ordered_consumer(conn, ordered_concurrent_stream; timeout = 2)
            try
                fetch_bytes_result = Channel{Any}(1)
                @async begin
                    try
                        put!(fetch_bytes_result, JetStream.fetch_bytes(concurrent_bytes, 128; batch = 100, expires_ns = 800_000_000, timeout = 2))
                    catch err
                        put!(fetch_bytes_result, err)
                    end
                end
                @test timedwait(() -> concurrent_bytes.operation_running, 1; pollint = 0.001) == :ok
                @test_throws JetStream.OrderedConsumerConcurrentRequestsError JetStream.fetch_bytes(concurrent_bytes, 128; batch = 1, no_wait = true, timeout = 2)
                fetched_bytes = wait_ready(fetch_bytes_result, 3)
                @test fetched_bytes isa Vector{NATS.Msg}
            finally
                close(concurrent_bytes)
            end

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ordered_mode_stream, subjects = ["$ordered_mode_stream.*"], storage = "memory"))
            JetStream.publish(conn, "$ordered_mode_stream.items", "ordered-mode-1"; timeout = 2)
            fetch_mode_ordered = JetStream.ordered_consumer(conn, ordered_mode_stream; timeout = 2)
            try
                @test NATS.payload.(JetStream.fetch(fetch_mode_ordered; batch = 1, no_wait = true, timeout = 2)) == ["ordered-mode-1"]
                @test_throws JetStream.OrderedConsumerUsedAsFetchError JetStream.consume(fetch_mode_ordered; batch = 1, timeout = 2)
            finally
                close(fetch_mode_ordered)
            end

            consume_mode_ordered = JetStream.ordered_consumer(conn, ordered_mode_stream; timeout = 2)
            consume_mode_ctx = JetStream.consume(
                consume_mode_ordered;
                batch = 1,
                expires_ns = 100_000_000,
                timeout = 1,
                channel_size = 1,
                close_ordered = false,
            )
            try
                @test timedwait(() -> consume_mode_ordered.operation_running, 1; pollint = 0.001) == :ok
                @test_throws JetStream.OrderedConsumerUsedAsConsumeError JetStream.fetch(consume_mode_ordered; batch = 1, no_wait = true, timeout = 2)
                @test_throws JetStream.OrderedConsumerUsedAsConsumeError JetStream.fetch_bytes(consume_mode_ordered, 128; batch = 1, no_wait = true, timeout = 2)
                @test_throws JetStream.OrderedConsumerConcurrentRequestsError JetStream.consume(consume_mode_ordered; batch = 1, timeout = 2, close_ordered = false)
            finally
                close(consume_mode_ctx)
                @test timedwait(() -> !consume_mode_ordered.operation_running, 2; pollint = 0.001) == :ok
                close(consume_mode_ordered)
            end

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ordered_gap_stream, subjects = ["$ordered_gap_stream.*"], storage = "memory"))
            for i in 1:3
                JetStream.publish(conn, "$ordered_gap_stream.items", "gap-$i"; timeout = 2)
            end
            JetStream.delete_msg(conn, ordered_gap_stream, 2; timeout = 2)
            gap_ordered = JetStream.ordered_consumer(conn, ordered_gap_stream; timeout = 2)
            try
                gap_batch = JetStream.fetch(gap_ordered; batch = 3, no_wait = true, timeout = 2)
                @test NATS.payload.(gap_batch) == ["gap-1", "gap-3"]
                @test [JetStream.metadata(msg).sequence.stream for msg in gap_batch] == UInt64[1, 3]
            finally
                close(gap_ordered)
            end

            JetStream.create_stream(conn, JetStream.StreamConfig(name = ordered_consume_stream, subjects = ["$ordered_consume_stream.*"], storage = "memory"))
            ordered_ctx_consumer = JetStream.ordered_consumer(
                conn,
                ordered_consume_stream,
                JetStream.OrderedConsumerConfig(
                    deliver_policy = "by_start_sequence",
                    opt_start_seq = UInt64(1),
                    filter_subject = "$ordered_consume_stream.items",
                    inactive_threshold = 60_000_000_000,
                    headers_only = true,
                    metadata = Dict("purpose" => "ordered-test"),
                    name_prefix = "ORDCTX",
                );
                timeout = 2,
            )
            try
                ctx_info = JetStream.consumer_info(ordered_ctx_consumer; timeout = 2)
                @test startswith(String(ctx_info.name), "ORDCTX_")
                @test String(ctx_info.config.filter_subject) == "$ordered_consume_stream.items"
                @test Bool(ctx_info.config.headers_only)
                @test String(ctx_info.config.metadata.purpose) == "ordered-test"

                ordered_ctx = JetStream.consume(
                    ordered_ctx_consumer;
                    batch = 2,
                    expires_ns = 500_000_000,
                    timeout = 2,
                    channel_size = 4,
                )
                try
                    for i in 1:3
                        JetStream.publish(conn, "$ordered_consume_stream.items", "consume-ordered-$i"; timeout = 2)
                    end
                    consumed_ordered = [JetStream.next_msg(ordered_ctx; timeout = 2) for _ in 1:3]
                    @test isempty.(NATS.payload.(consumed_ordered)) == [true, true, true]
                    @test [JetStream.metadata(msg).sequence.stream for msg in consumed_ordered] == UInt64[1, 2, 3]
                    @test !isready(JetStream.errors(ordered_ctx))
                finally
                    close(ordered_ctx)
                end
            finally
                close(ordered_ctx_consumer)
            end

            dup1 = JetStream.publish(conn, "$stream.dedupe", "once"; timeout = 2, msg_id = "dedupe-1")
            dup2 = JetStream.publish(conn, "$stream.dedupe", "again"; timeout = 2, msg_id = "dedupe-1")
            @test dup2.duplicate === true
            @test dup2.seq == dup1.seq

            futures = [
                JetStream.publish_async(conn, "$stream.async", "async-$i"; timeout = 2, msg_id = "async-$i")
                for i in 1:3
            ]
            @test JetStream.publish_async_pending(conn) <= 3
            JetStream.publish_async_complete(conn; timeout = 2)
            async_acks = [JetStream.wait_ack(future; timeout = 2) for future in futures]
            @test all(ack -> ack isa JetStream.PubAck, async_acks)
            @test all(ack -> ack.stream == stream, async_acks)
            @test issorted([ack.seq for ack in async_acks])
            @test JetStream.publish_async_pending(conn) == 0
            @test isempty(conn.request_map)
            @test_throws ArgumentError JetStream.publish_async(conn, "$stream.async", "bad"; max_pending = 0)

            JetStream.create_stream(conn, JetStream.StreamConfig(name = async_noack_stream, subjects = ["$async_noack_stream.*"], storage = "memory", no_ack = true))
            stalled = [
                JetStream.publish_async(conn, "$async_noack_stream.items", "stall-$i"; timeout = 0.2, max_pending = 3, stall_wait = 0.05)
                for i in 1:3
            ]
            @test JetStream.publish_async_pending(conn) == 3
            @test_throws NATS.TooManyStalledMsgsError JetStream.publish_async(
                conn,
                "$async_noack_stream.items",
                "stall-4";
                timeout = 0.2,
                max_pending = 3,
                stall_wait = 0.01,
            )
            @test_throws NATS.ConnectionTimeoutError JetStream.publish_async_complete(conn; timeout = 0.01)
            for future in stalled
                @test_throws NATS.ConnectionTimeoutError JetStream.wait_ack(future; timeout = 1)
            end
            JetStream.publish_async_complete(conn; timeout = 1)
            @test JetStream.publish_async_pending(conn) == 0

            retry_future = JetStream.publish_async(
                conn,
                "$async_retry_stream.items",
                "retry-until-ready";
                timeout = 1,
                retry_attempts = 10,
                retry_wait = 0.05,
            )
            sleep(0.05)
            @test JetStream.publish_async_pending(conn) == 1
            retry_creator = errormonitor(@async begin
                sleep(0.15)
                JetStream.create_stream(
                    conn,
                    JetStream.StreamConfig(name = async_retry_stream, subjects = ["$async_retry_stream.*"], storage = "memory");
                    timeout = 2,
                )
            end)
            retry_ack = JetStream.wait_ack(retry_future; timeout = 3)
            wait(retry_creator)
            @test retry_ack.stream == async_retry_stream
            @test JetStream.publish_async_pending(conn) == 0

            async_errors = Channel{Any}(4)
            failed_future = JetStream.publish_async(
                conn,
                "NATSJL_MISSING.items",
                "missing";
                timeout = 1,
                retry_attempts = 1,
                retry_wait = 0.01,
                error_cb = (c, msg, err) -> put!(async_errors, (msg, err)),
            )
            @test_throws JetStream.NoStreamResponseError JetStream.wait_ack(failed_future; timeout = 2)
            failed_msg, failed_err = wait_ready(async_errors)
            @test failed_msg isa JetStream.AsyncPublishMessage
            @test failed_msg.subject == "NATSJL_MISSING.items"
            @test String(failed_msg.data) == "missing"
            @test failed_err isa JetStream.NoStreamResponseError
            @test JetStream.publish_async_pending(conn) == 0
            @test_throws ArgumentError JetStream.publish_async(conn, "$stream.async", "bad"; retry_attempts = -1)
            @test_throws ArgumentError JetStream.publish_async(conn, "$stream.async", "bad"; retry_wait = 0)

            cleanup_async_errors = Channel{Any}(8)
            cleanup_futures = [
                JetStream.publish_async(
                    conn,
                    "$async_noack_stream.items",
                    "cleanup-$i";
                    timeout = 10,
                    error_cb = (_conn, msg, err) -> put!(cleanup_async_errors, (msg, err)),
                )
                for i in 1:3
            ]
            @test JetStream.publish_async_pending(conn) == 3
            @test JetStream.cleanup_publisher(conn) === nothing
            @test JetStream.publish_async_pending(conn) == 0
            @test JetStream.publish_async_complete(conn; timeout = 1) === nothing
            for future in cleanup_futures
                @test_throws JetStream.PublisherClosedError JetStream.wait_ack(future; timeout = 1)
            end
            cleanup_events = [wait_ready(cleanup_async_errors) for _ in 1:3]
            @test sort([String(event[1].data) for event in cleanup_events]) == ["cleanup-1", "cleanup-2", "cleanup-3"]
            @test all(event -> event[1] isa JetStream.AsyncPublishMessage, cleanup_events)
            @test all(event -> event[2] isa JetStream.PublisherClosedError, cleanup_events)
            after_cleanup = JetStream.publish_async(conn, "$stream.async", "after-cleanup"; timeout = 2, msg_id = "after-cleanup")
            @test JetStream.wait_ack(after_cleanup; timeout = 2).stream == stream

            reconnect_async_errors = Channel{Any}(2)
            reconnect_future = JetStream.publish_async(
                conn,
                "$async_noack_stream.items",
                "reset-on-reconnect";
                timeout = 10,
                max_pending = 10,
                error_cb = (_conn, msg, err) -> put!(reconnect_async_errors, (msg, err)),
            )
            @test JetStream.publish_async_pending(conn) == 1
            NATS.force_reconnect(conn; timeout = 3)
            @test JetStream.publish_async_pending(conn) == 0
            @test_throws NATS.ConnectionReconnectingError JetStream.wait_ack(reconnect_future; timeout = 1)
            reconnect_msg, reconnect_err = wait_ready(reconnect_async_errors)
            @test reconnect_msg isa JetStream.AsyncPublishMessage
            @test reconnect_msg.subject == "$async_noack_stream.items"
            @test String(reconnect_msg.data) == "reset-on-reconnect"
            @test reconnect_err isa NATS.ConnectionReconnectingError

            handler_errors = Channel{Any}(8)
            handler_gate = Channel{Nothing}(8)
            handler_republished = Channel{JetStream.PubAckFuture}(8)
            handler_cb = function (c, msg, err)
                put!(handler_errors, (msg, err))
                take!(handler_gate)
                put!(handler_republished, JetStream.publish_async(c, msg; timeout = 2))
            end
            original_futures = [
                JetStream.publish_async(
                    conn,
                    "$async_handler_stream.items",
                    "handler-$i";
                    timeout = 1,
                    retry_attempts = 0,
                    error_cb = handler_cb,
                )
                for i in 1:3
            ]
            handler_seen = [wait_ready(handler_errors) for _ in 1:3]
            @test all(event -> event[2] isa JetStream.NoStreamResponseError, handler_seen)
            @test String(handler_seen[1][1].data) == "handler-1"
            JetStream.create_stream(conn, JetStream.StreamConfig(name = async_handler_stream, subjects = ["$async_handler_stream.*"], storage = "memory"); timeout = 2)
            for _ in 1:3
                put!(handler_gate, nothing)
            end
            for future in original_futures
                @test_throws JetStream.NoStreamResponseError JetStream.wait_ack(future; timeout = 2)
            end
            republished = [wait_ready(handler_republished) for _ in 1:3]
            republished_acks = [JetStream.wait_ack(future; timeout = 2) for future in republished]
            @test all(ack -> ack.stream == async_handler_stream, republished_acks)
            JetStream.publish_async_complete(conn; timeout = 2)
            handler_info = JetStream.stream_info(conn, async_handler_stream; timeout = 2)
            @test Int(handler_info.state.messages) == 3

            kv = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = bucket, storage = "memory", history = 3))
            @test_throws JetStream.NoKeysFoundError JetStream.keys(kv; timeout = 2)
            @test_throws JetStream.KeyNotFoundError JetStream.get(kv, "missing"; timeout = 2)
            @test_throws JetStream.KeyNotFoundError JetStream.get_revision(kv, "missing", 1; timeout = 2)
            empty_key_lister = JetStream.list_keys(kv; timeout = 2)
            try
                @test JetStream.next_key(empty_key_lister; timeout = 2) === nothing
            finally
                close(empty_key_lister)
            end
            rev1 = JetStream.put(kv, "foo", "one"; timeout = 2)
            json_get_tap = NATS.subscribe(conn, "\$JS.API.STREAM.MSG.GET.$(kv.stream)")
            try
                NATS.flush(conn; timeout = 2)
                entry = JetStream.get(kv, "foo"; timeout = 2)
                @test JetStream.value_string(entry) == "one"
                @test entry.revision == rev1
                @test entry.operation == :put
                @test_throws NATS.ConnectionTimeoutError NATS.next_msg(json_get_tap; timeout = 0.2)
            finally
                try NATS.unsubscribe(conn, json_get_tap) catch end
            end

            @test_throws JetStream.JetStreamError JetStream.create(kv, "foo", "again"; timeout = 2)
            kv_create = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = create_bucket, storage = "memory", history = 5))
            recreate_rev1 = JetStream.create(kv_create, "again", "one"; timeout = 2)
            recreate_del = JetStream.delete(kv_create, "again"; timeout = 2)
            recreate_rev2 = JetStream.create(kv_create, "again", "two"; timeout = 2)
            @test recreate_rev2 > recreate_del > recreate_rev1
            recreated_entry = JetStream.get(kv_create, "again"; timeout = 2)
            @test recreated_entry.revision == recreate_rev2
            @test JetStream.value_string(recreated_entry) == "two"
            recreate_purge = JetStream.purge(kv_create, "again"; timeout = 2)
            recreate_rev3 = JetStream.create(kv_create, "again", "three"; timeout = 2)
            @test recreate_rev3 > recreate_purge
            @test JetStream.value_string(JetStream.get(kv_create, "again"; timeout = 2)) == "three"
            @test_throws JetStream.JetStreamError JetStream.create(kv_create, "again", "exists"; timeout = 2)
            special_key = "path/to_key=1-2"
            special_rev = JetStream.put(kv_create, special_key, "special"; timeout = 2)
            special_entry = JetStream.get(kv_create, special_key; timeout = 2)
            @test special_entry.revision == special_rev
            @test JetStream.value_string(special_entry) == "special"
            rev2 = JetStream.update(kv, "foo", "two", rev1; timeout = 2)
            @test rev2 > rev1
            @test JetStream.value_string(JetStream.get(kv, "foo"; timeout = 2)) == "two"
            json_revision_tap = NATS.subscribe(conn, "\$JS.API.STREAM.MSG.GET.$(kv.stream)")
            try
                NATS.flush(conn; timeout = 2)
                @test JetStream.value_string(JetStream.get_revision(kv, "foo", rev1; timeout = 2)) == "one"
                @test_throws NATS.ConnectionTimeoutError NATS.next_msg(json_revision_tap; timeout = 0.2)
            finally
                try NATS.unsubscribe(conn, json_revision_tap) catch end
            end
            @test_throws JetStream.JetStreamError JetStream.update(kv, "foo", "bad", rev1; timeout = 2)

            bar_rev1 = JetStream.put(kv, "bar", "three"; timeout = 2)
            @test JetStream.keys(kv; timeout = 2) == ["bar", "foo"]
            foo_history = JetStream.history(kv, "foo"; timeout = 2)
            @test JetStream.value_string.(foo_history) == ["one", "two"]
            @test [entry.revision for entry in foo_history] == [rev1, rev2]

            kv_status = JetStream.status(kv; timeout = 2)
            @test kv_status.bucket == bucket
            @test kv_status.history == 3
            @test kv_status.storage == "memory"
            @test kv_status.values >= 3
            @test bucket in JetStream.key_value_store_names(conn; timeout = 2)
            @test any(s -> s.bucket == bucket && s.history == 3, JetStream.key_value_stores(conn; timeout = 2))
            @test_throws JetStream.BucketNotFoundError JetStream.key_value(conn, "NATSJLKVMISSING"; timeout = 2)
            @test_throws JetStream.BucketNotFoundError JetStream.delete_key_value(conn, "NATSJLKVMISSING"; timeout = 2)

            @test_throws JetStream.BucketNotFoundError JetStream.update_key_value(conn, JetStream.KeyValueConfig(
                bucket = update_bucket,
                description = "missing bucket",
                storage = "memory",
            ); timeout = 2)
            kv_update = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                bucket = update_bucket,
                description = "old bucket",
                storage = "memory",
                history = 2,
            ); timeout = 2)
            @test JetStream.status(kv_update; timeout = 2).config.description == "old bucket"
            kv_updated = JetStream.update_key_value(conn, JetStream.KeyValueConfig(
                bucket = update_bucket,
                description = "updated bucket",
                storage = "memory",
                history = 2,
            ); timeout = 2)
            @test JetStream.status(kv_updated; timeout = 2).config.description == "updated bucket"

            @test_throws ArgumentError JetStream.create_or_update_key_value(conn, JetStream.KeyValueConfig(bucket = "bad."); timeout = 2)
            kv_created_or_updated = JetStream.create_or_update_key_value(conn, JetStream.KeyValueConfig(
                bucket = create_or_update_bucket,
                description = "created bucket",
                storage = "memory",
                history = 2,
            ); timeout = 2)
            @test JetStream.status(kv_created_or_updated; timeout = 2).config.description == "created bucket"
            kv_created_or_updated = JetStream.create_or_update_key_value(conn, JetStream.KeyValueConfig(
                bucket = create_or_update_bucket,
                description = "reconciled bucket",
                storage = "memory",
                history = 2,
            ); timeout = 2)
            @test JetStream.status(kv_created_or_updated; timeout = 2).config.description == "reconciled bucket"

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = JetStream.kv_stream(repair_bucket),
                subjects = [JetStream.kv_subject(repair_bucket, ">")],
                retention = "limits",
                storage = "memory",
                replicas = 1,
                allow_direct = false,
                max_consumers = -1,
                max_msgs = -1,
                max_bytes = -1,
                max_msgs_per_subject = 1,
                max_msg_size = -1,
                duplicate_window = 120_000_000_000,
                deny_delete = true,
                allow_rollup_hdrs = true,
                discard = "old",
            ); timeout = 2)
            repaired_kv = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = repair_bucket, storage = "memory"); timeout = 2)
            repaired_info = JetStream.stream_info(conn, repaired_kv.stream; timeout = 2)
            @test String(repaired_info.config.discard) == "new"
            @test Bool(repaired_info.config.allow_direct)
            @test_throws JetStream.JetStreamError JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                bucket = repair_bucket,
                description = "different bucket",
                storage = "memory",
            ); timeout = 2)

            republish_sub = NATS.subscribe(conn, "natsjl.kv.republish.>")
            try
                NATS.flush(conn; timeout = 2)
                kv_republish = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                    bucket = republish_bucket,
                    storage = "memory",
                    republish = JetStream.RePublish(source = ">", destination = "natsjl.kv.republish.>"),
                ); timeout = 2)
                republish_status = JetStream.status(kv_republish; timeout = 2)
                @test republish_status.config.republish !== nothing
                @test republish_status.config.republish.source == ">"
                @test republish_status.config.republish.destination == "natsjl.kv.republish.>"
                republish_rev = JetStream.put(kv_republish, "republished", "value"; timeout = 2)
                republished_msg = NATS.next_msg(republish_sub; timeout = 2)
                @test NATS.payload(republished_msg) == "value"
                @test Dict(republished_msg.headers)["Nats-Subject"] == "\$KV.$republish_bucket.republished"
                @test republish_rev >= 1
            finally
                try NATS.unsubscribe(conn, republish_sub) catch end
            end

            kv_source = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = mirror_source_bucket, storage = "memory"); timeout = 2)
            kv_mirror = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                bucket = mirror_bucket,
                storage = "memory",
                mirror = JetStream.StreamSource(name = mirror_source_bucket),
            ); timeout = 2)
            mirror_status = JetStream.status(kv_mirror; timeout = 2)
            @test mirror_status.config.mirror !== nothing
            @test mirror_status.config.mirror.name == JetStream.kv_stream(mirror_source_bucket)
            @test Bool(mirror_status.stream_info.config.mirror_direct)
            mirror_rev = JetStream.put(kv_mirror, "mirrored", "source-value"; timeout = 2)
            @test mirror_rev >= 1
            @test JetStream.value_string(JetStream.get(kv_source, "mirrored"; timeout = 2)) == "source-value"

            kv_source_one = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = source_bucket_one, storage = "memory", history = 5); timeout = 2)
            kv_source_two = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = source_bucket_two, storage = "memory", history = 5); timeout = 2)
            caller_source = JetStream.StreamSource(name = source_bucket_one)
            kv_sourced = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                bucket = sourced_bucket,
                storage = "memory",
                history = 5,
                sources = [
                    caller_source,
                    JetStream.StreamSource(name = JetStream.kv_stream(source_bucket_two)),
                ],
            ); timeout = 2)
            @test caller_source.name == source_bucket_one
            sourced_status = JetStream.status(kv_sourced; timeout = 2)
            @test sourced_status.config.sources !== nothing
            @test length(sourced_status.config.sources) == 2
            @test sort([source.name for source in sourced_status.config.sources]) == sort([JetStream.kv_stream(source_bucket_one), JetStream.kv_stream(source_bucket_two)])
            @test all(source -> length(source.subject_transforms) == 1, sourced_status.config.sources)
            @test sort([only(source.subject_transforms).source for source in sourced_status.config.sources]) == sort([JetStream.kv_subject(source_bucket_one, ">"), JetStream.kv_subject(source_bucket_two, ">")])
            @test all(source -> only(source.subject_transforms).destination == JetStream.kv_subject(sourced_bucket, ">"), sourced_status.config.sources)
            JetStream.put(kv_source_one, "one", "value-one"; timeout = 2)
            JetStream.put(kv_source_two, "two", "value-two"; timeout = 2)
            @test timedwait(2; pollint = 0.01) do
                try
                    JetStream.value_string(JetStream.get(kv_sourced, "one"; timeout = 0.2)) == "value-one"
                catch
                    false
                end
            end == :ok
            @test timedwait(2; pollint = 0.01) do
                try
                    JetStream.value_string(JetStream.get(kv_sourced, "two"; timeout = 0.2)) == "value-two"
                catch
                    false
                end
            end == :ok

            history_watcher = JetStream.watch(kv, "foo"; include_history = true, timeout = 2)
            try
                watched_history = JetStream.KeyValueEntry[]
                while true
                    update = JetStream.next_update(history_watcher; timeout = 2)
                    update === nothing && break
                    push!(watched_history, update)
                end
                @test JetStream.value_string.(watched_history) == ["one", "two"]
                @test [entry.revision for entry in watched_history] == [rev1, rev2]
            finally
                close(history_watcher)
            end

            resume_watcher = JetStream.watch_all(kv; resume_from_revision = rev2, timeout = 2)
            try
                resumed = JetStream.KeyValueEntry[]
                while true
                    update = JetStream.next_update(resume_watcher; timeout = 2)
                    update === nothing && break
                    push!(resumed, update)
                end
                @test [entry.revision for entry in resumed] == [rev2, bar_rev1]
                @test [entry.key for entry in resumed] == ["foo", "bar"]
                @test JetStream.value_string.(resumed) == ["two", "three"]
            finally
                close(resume_watcher)
            end

            watcher = JetStream.watch_all(kv; timeout = 2)
            try
                initial = JetStream.KeyValueEntry[]
                while true
                    update = JetStream.next_update(watcher; timeout = 2)
                    update === nothing && break
                    push!(initial, update)
                end
                @test sort(["$(entry.key):$(JetStream.value_string(entry))" for entry in initial]) == ["bar:three", "foo:two"]

                bar_rev2 = JetStream.put(kv, "bar", "four"; timeout = 2)
                bar_update = JetStream.next_update(watcher; timeout = 2)
                @test bar_update.key == "bar"
                @test JetStream.value_string(bar_update) == "four"
                @test bar_update.revision == bar_rev2

                del_rev = JetStream.delete(kv, "foo"; timeout = 2)
                @test del_rev > rev2
                delete_update = JetStream.next_update(watcher; timeout = 2)
                @test delete_update.key == "foo"
                @test delete_update.operation == :delete
                @test delete_update.revision == del_rev
                @test_throws JetStream.KeyNotFoundError JetStream.get_revision(kv, "foo", del_rev; timeout = 2)
            finally
                close(watcher)
            end
            @test watcher.closed
            @test_throws NATS.ConnectionClosedError JetStream.next_update(watcher; timeout = 0.1)

            @test_throws ArgumentError JetStream.watch_all(kv; include_history = true, updates_only = true)
            @test_throws ArgumentError JetStream.watch(kv, "foo*"; timeout = 2)
            @test_throws ArgumentError JetStream.watch(kv, ""; timeout = 2)
            @test_throws ArgumentError JetStream.watch(kv, "a.>.b"; timeout = 2)
            @test_throws ArgumentError JetStream.watch(kv, "foo."; timeout = 2)
            @test_throws ArgumentError JetStream.watch(kv, "foo..bar"; timeout = 2)
            @test_throws ArgumentError JetStream.watch(kv, "a..b.c"; timeout = 2)
            @test_throws ArgumentError JetStream.watch_all(kv; resume_from_revision = -1, timeout = 2)
            updates_only = JetStream.watch(kv, "bar"; updates_only = true, timeout = 2)
            try
                @test_throws NATS.ConnectionTimeoutError JetStream.next_update(updates_only; timeout = 0.2)
                bar_rev3 = JetStream.put(kv, "bar", "five"; timeout = 2)
                live_only = JetStream.next_update(updates_only; timeout = 2)
                @test live_only.key == "bar"
                @test live_only.revision == bar_rev3
                @test JetStream.value_string(live_only) == "five"
            finally
                close(updates_only)
            end

            ignore_delete_watcher = JetStream.watch(kv, "bar"; ignore_deletes = true, timeout = 2)
            try
                while JetStream.next_update(ignore_delete_watcher; timeout = 2) !== nothing
                end
                JetStream.delete(kv, "bar"; timeout = 2)
                @test_throws NATS.ConnectionTimeoutError JetStream.next_update(ignore_delete_watcher; timeout = 0.2)
            finally
                close(ignore_delete_watcher)
            end

            @test_throws JetStream.KeyNotFoundError JetStream.get(kv, "foo"; timeout = 2)
            for bad_key in ("", ".bad", "bad.", "bad..key", "bad key", "bad*", "bad>")
                @test_throws ArgumentError JetStream.put(kv, bad_key, "nope"; timeout = 2)
                @test_throws ArgumentError JetStream.get(kv, bad_key; timeout = 2)
                @test_throws ArgumentError JetStream.update(kv, bad_key, "nope", rev1; timeout = 2)
                @test_throws ArgumentError JetStream.delete(kv, bad_key; timeout = 2)
                @test_throws ArgumentError JetStream.purge(kv, bad_key; timeout = 2)
                @test_throws ArgumentError JetStream.history(kv, bad_key; timeout = 2)
            end
            @test_throws ArgumentError JetStream.put(kv, "..bad", "nope"; timeout = 2)
            foo_history_after_delete = JetStream.history(kv, "foo"; timeout = 2)
            @test [entry.operation for entry in foo_history_after_delete] == [:put, :put, :delete]
            JetStream.purge(kv, "bar"; timeout = 2)
            @test_throws JetStream.KeyNotFoundError JetStream.get(kv, "bar"; timeout = 2)
            @test_throws JetStream.NoKeysFoundError JetStream.keys(kv; timeout = 2)

            kv_filter = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = filter_bucket, storage = "memory", history = 2))
            JetStream.put(kv_filter, "alpha.one", "a1"; timeout = 2)
            JetStream.put(kv_filter, "alpha.two", "a2"; timeout = 2)
            JetStream.put(kv_filter, "beta.one", "b1"; timeout = 2)
            JetStream.put(kv_filter, "gone", "later"; timeout = 2)
            JetStream.delete(kv_filter, "gone"; timeout = 2)
            @test JetStream.keys(kv_filter; timeout = 2) == ["alpha.one", "alpha.two", "beta.one"]
            @test JetStream.keys(kv_filter, "alpha.*"; timeout = 2) == ["alpha.one", "alpha.two"]
            @test JetStream.keys(kv_filter, ["alpha.one", "beta.*"]; timeout = 2) == ["alpha.one", "beta.one"]
            @test_throws JetStream.NoKeysFoundError JetStream.keys(kv_filter, "missing.*"; timeout = 2)
            @test_throws ArgumentError JetStream.keys(kv_filter, "bad*"; timeout = 2)

            key_lister = JetStream.list_keys(kv_filter; timeout = 2)
            try
                streamed_keys = String[]
                while true
                    key = JetStream.next_key(key_lister; timeout = 2)
                    key === nothing && break
                    push!(streamed_keys, key)
                end
                @test sort!(streamed_keys) == ["alpha.one", "alpha.two", "beta.one"]
                @test JetStream.next_key(key_lister; timeout = 0.1) === nothing
            finally
                close(key_lister)
            end

            filtered_lister = JetStream.list_keys(kv_filter, "alpha.*"; timeout = 2)
            try
                streamed_filtered = String[]
                while true
                    key = JetStream.next_key(filtered_lister; timeout = 2)
                    key === nothing && break
                    push!(streamed_filtered, key)
                end
                @test sort!(streamed_filtered) == ["alpha.one", "alpha.two"]
            finally
                close(filtered_lister)
            end

            early_closed_lister = JetStream.list_keys(kv_filter; timeout = 2)
            close(early_closed_lister)
            @test_throws NATS.ConnectionClosedError JetStream.next_key(early_closed_lister; timeout = 0.1)
            @test_throws ArgumentError JetStream.list_keys(kv_filter, "bad*"; timeout = 2)
            @test_throws ArgumentError JetStream.list_keys(kv_filter; channel_size = 0, timeout = 2)

            kv_purge_deletes = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = purge_deletes_bucket, storage = "memory", history = 5))
            JetStream.put(kv_purge_deletes, "keep-marker", "one"; timeout = 2)
            keep_delete = JetStream.delete(kv_purge_deletes, "keep-marker"; timeout = 2)
            @test JetStream.purge_deletes(kv_purge_deletes; timeout = 2) === nothing
            keep_history = JetStream.history(kv_purge_deletes, "keep-marker"; timeout = 2)
            @test length(keep_history) == 1
            @test only(keep_history).operation == :delete
            @test only(keep_history).revision == keep_delete
            @test_throws JetStream.KeyNotFoundError JetStream.get(kv_purge_deletes, "keep-marker"; timeout = 2)

            JetStream.put(kv_purge_deletes, "drop-marker", "one"; timeout = 2)
            JetStream.purge(kv_purge_deletes, "drop-marker"; timeout = 2)
            @test JetStream.purge_deletes(kv_purge_deletes; delete_markers_older_than_ns = -1, timeout = 2) === nothing
            @test_throws JetStream.KeyNotFoundError JetStream.history(kv_purge_deletes, "keep-marker"; timeout = 2)
            @test_throws JetStream.KeyNotFoundError JetStream.history(kv_purge_deletes, "drop-marker"; timeout = 2)
            @test_throws JetStream.NoKeysFoundError JetStream.keys(kv_purge_deletes; timeout = 2)

            kv_stop = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = stop_bucket, storage = "memory", history = 1))
            for i in 1:100
                JetStream.put(kv_stop, "key-$i", "value-$i"; timeout = 2)
            end
            stop_kv_watcher = JetStream.watch_all(kv_stop; timeout = 2, channel_size = 8)
            elapsed = @elapsed close(stop_kv_watcher)
            @test elapsed < 1
            @test stop_kv_watcher.closed
            @test !isopen(JetStream.updates(stop_kv_watcher))
            @test !isopen(JetStream.errors(stop_kv_watcher))
            @test_throws NATS.ConnectionClosedError JetStream.next_update(stop_kv_watcher; timeout = 0.1)

            @test_throws ArgumentError JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = "bad!"); timeout = 2)
            objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(
                bucket = object_bucket,
                description = "object store test",
                storage = "memory",
                max_bytes = 2_000_000,
                metadata = Dict("purpose" => "object-test"),
            ); timeout = 2)
            object_status = JetStream.status(objects; timeout = 2)
            @test object_status.bucket == object_bucket
            @test object_status.description == "object store test"
            @test object_status.storage == "memory"
            @test object_status.metadata["purpose"] == "object-test"
            @test_throws JetStream.ObjectNotFoundError JetStream.get_info(objects, "missing.bin"; timeout = 2)
            @test_throws JetStream.ObjectNotFoundError JetStream.get_bytes(objects, "missing.bin"; timeout = 2)
            @test_throws JetStream.ObjectNotFoundError JetStream.delete(objects, "missing.bin"; timeout = 2)

            empty_objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = object_empty_bucket, storage = "memory"); timeout = 2)
            @test_throws JetStream.NoObjectsFoundError JetStream.list(empty_objects; timeout = 2)
            empty_lister = JetStream.list_objects(empty_objects; timeout = 2)
            try
                @test JetStream.next_object(empty_lister; timeout = 2) === nothing
            finally
                close(empty_lister)
            end
            JetStream.put_string(empty_objects, "gone.txt", "gone"; timeout = 2)
            JetStream.delete(empty_objects, "gone.txt"; timeout = 2)
            @test_throws JetStream.NoObjectsFoundError JetStream.list(empty_objects; timeout = 2)
            deleted_only = JetStream.list(empty_objects; show_deleted = true, timeout = 2)
            @test [info.name for info in deleted_only] == ["gone.txt"]
            @test only(deleted_only).deleted

            bad_meta_objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = object_bad_meta_bucket, storage = "memory"); timeout = 2)
            JetStream.publish_object_info(bad_meta_objects, JetStream.ObjectInfo(
                name = "corrupt.bin",
                bucket = object_bad_meta_bucket,
                nuid = "",
                size = UInt64(0),
                chunks = UInt32(0),
            ); timeout = 2)
            @test isempty(JetStream.get_info(bad_meta_objects, "corrupt.bin"; timeout = 2).nuid)
            @test_throws JetStream.BadObjectMetaError JetStream.get_bytes(bad_meta_objects, "corrupt.bin"; timeout = 2)
            @test_throws JetStream.BadObjectMetaError JetStream.get_string(bad_meta_objects, "corrupt.bin"; timeout = 2)
            @test_throws JetStream.BadObjectMetaError JetStream.delete(bad_meta_objects, "corrupt.bin"; timeout = 2)

            payload = repeat(Vector{UInt8}(codeunits("object-data-")), 20_000)
            object_info = JetStream.put(objects, JetStream.ObjectMeta(
                name = "blob.bin",
                description = "binary blob",
                headers = ["Content-Type" => "application/octet-stream"],
                metadata = Dict("kind" => "blob"),
                chunk_size = 65_536,
            ), payload; timeout = 2)
            @test object_info.size == length(payload)
            @test object_info.chunks == cld(length(payload), 65_536)
            @test object_info.digest == JetStream.object_digest(payload)
            @test JetStream.decode_object_digest(object_info.digest) == sha256(payload)
            @test !isempty(object_info.nuid)
            for bad_digest in ("not-a-digest", "MD5=AAAA", "SHA-256=%%%")
                err = try
                    JetStream.decode_object_digest(bad_digest)
                    nothing
                catch err
                    err
                end
                @test err isa JetStream.JetStreamError
                @test occursin("invalid format", err.description)
            end

            stored_info = JetStream.get_info(objects, "blob.bin"; timeout = 2)
            @test stored_info.name == "blob.bin"
            @test stored_info.description == "binary blob"
            @test stored_info.metadata["kind"] == "blob"
            @test stored_info.headers == ["Content-Type" => "application/octet-stream"]
            @test stored_info.digest == object_info.digest
            @test stored_info.mtime !== nothing
            @test JetStream.get_bytes(objects, "blob.bin"; timeout = 2) == payload

            text_info = JetStream.put_string(objects, "note.txt", "hello object store"; timeout = 2)
            @test text_info.size == sizeof("hello object store")
            @test JetStream.get_string(objects, "note.txt"; timeout = 2) == "hello object store"
            @test sort([info.name for info in JetStream.list(objects; timeout = 2)]) == ["blob.bin", "note.txt"]

            object_lister = JetStream.list_objects(objects; timeout = 2)
            try
                streamed_objects = JetStream.ObjectInfo[]
                while true
                    info = JetStream.next_object(object_lister; timeout = 2)
                    info === nothing && break
                    push!(streamed_objects, info)
                end
                @test sort([info.name for info in streamed_objects]) == ["blob.bin", "note.txt"]
                @test JetStream.next_object(object_lister; timeout = 0.1) === nothing
            finally
                close(object_lister)
            end

            early_closed_object_lister = JetStream.list_objects(objects; timeout = 2)
            close(early_closed_object_lister)
            @test_throws NATS.ConnectionClosedError JetStream.next_object(early_closed_object_lister; timeout = 0.1)
            @test_throws ArgumentError JetStream.list_objects(objects; channel_size = 0, timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = object_prefix_decoy_stream,
                subjects = ["$object_prefix_decoy_stream.items"],
                storage = "memory",
            ); timeout = 2)
            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = object_subject_decoy_stream,
                subjects = ["\$O.NATSJLDECOY.C.>"],
                storage = "memory",
            ); timeout = 2)
            object_store_names = JetStream.object_store_names(conn; timeout = 2)
            @test object_bucket in object_store_names
            @test !("NATSJL_DECOY" in object_store_names)
            @test !("NATSJLDECOY" in object_store_names)
            object_store_statuses = JetStream.object_stores(conn; timeout = 2)
            @test any(status -> status.bucket == object_bucket, object_store_statuses)
            @test all(status -> status.bucket != "NATSJL_DECOY" && status.bucket != "NATSJLDECOY", object_store_statuses)
            reopened_objects = JetStream.object_store(conn, object_bucket; timeout = 2)
            @test JetStream.get_string(reopened_objects, "note.txt"; timeout = 2) == "hello object store"
            @test_throws JetStream.BucketNotFoundError JetStream.object_store(conn, "NATSJLOBJMISSING"; timeout = 2)
            @test_throws JetStream.BucketNotFoundError JetStream.update_object_store(conn, JetStream.ObjectStoreConfig(
                bucket = "NATSJLOBJMISSING",
                storage = "memory",
            ); timeout = 2)
            @test_throws JetStream.BucketNotFoundError JetStream.delete_object_store(conn, "NATSJLOBJMISSING"; timeout = 2)

            object_watcher = JetStream.watch(objects; timeout = 2)
            try
                initial_objects = JetStream.ObjectInfo[]
                while true
                    update = JetStream.next_update(object_watcher; timeout = 2)
                    update === nothing && break
                    push!(initial_objects, update)
                end
                @test sort([info.name for info in initial_objects]) == ["blob.bin", "note.txt"]

                JetStream.update_meta(objects, "note.txt", JetStream.ObjectMeta(
                    name = "renamed.txt",
                    description = "renamed note",
                    metadata = Dict("kind" => "note-renamed"),
                ); timeout = 2)
                renamed_update = JetStream.next_update(object_watcher; timeout = 2)
                @test renamed_update.name == "renamed.txt"
                @test renamed_update.description == "renamed note"
            finally
                close(object_watcher)
            end
            @test_throws JetStream.ObjectNotFoundError JetStream.get_info(objects, "note.txt"; timeout = 2)
            renamed_info = JetStream.get_info(objects, "renamed.txt"; timeout = 2)
            @test renamed_info.metadata["kind"] == "note-renamed"
            @test JetStream.get_string(objects, "renamed.txt"; timeout = 2) == "hello object store"

            updates_only_objects = JetStream.watch(objects; updates_only = true, timeout = 2)
            try
                @test_throws NATS.ConnectionTimeoutError JetStream.next_update(updates_only_objects; timeout = 0.2)
                JetStream.put_string(objects, "watch-live.txt", "live"; timeout = 2)
                live_update = JetStream.next_update(updates_only_objects; timeout = 2)
                @test live_update.name == "watch-live.txt"
                @test !live_update.deleted
            finally
                close(updates_only_objects)
            end

            ignore_delete_objects = JetStream.watch(objects; updates_only = true, ignore_deletes = true, timeout = 2)
            try
                JetStream.delete(objects, "watch-live.txt"; timeout = 2)
                @test_throws NATS.ConnectionTimeoutError JetStream.next_update(ignore_delete_objects; timeout = 0.2)
                JetStream.put_string(objects, "watch-ignore-live.txt", "live"; timeout = 2)
                ignore_live = JetStream.next_update(ignore_delete_objects; timeout = 2)
                @test ignore_live.name == "watch-ignore-live.txt"
            finally
                close(ignore_delete_objects)
            end
            JetStream.delete(objects, "watch-ignore-live.txt"; timeout = 2)

            stop_watcher = JetStream.watch(objects; timeout = 2)
            try
                while JetStream.next_update(stop_watcher; timeout = 2) !== nothing
                end
                elapsed = @elapsed close(stop_watcher)
                @test elapsed < 1
                @test_throws NATS.ConnectionClosedError JetStream.next_update(stop_watcher; timeout = 0.1)
            finally
                close(stop_watcher)
            end

            filename_like_names = ["BLOB.txt", "foo bar", ".*<>:\"/\\|?&"]
            for name in filename_like_names
                special_info = JetStream.put_string(objects, name, "named:$name"; timeout = 2)
                @test special_info.name == name
                @test JetStream.get_info(objects, name; timeout = 2).name == name
                @test JetStream.get_string(objects, name; timeout = 2) == "named:$name"
            end
            listed_names = [info.name for info in JetStream.list(objects; timeout = 2)]
            @test all(name -> name in listed_names, filename_like_names)
            @test_throws ArgumentError JetStream.put_string(objects, "", "bad"; timeout = 2)
            for name in filename_like_names
                JetStream.delete(objects, name; timeout = 2)
            end

            JetStream.put_string(objects, "conflict.txt", "existing object"; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.update_meta(objects, "renamed.txt", JetStream.ObjectMeta(
                name = "conflict.txt",
            ); timeout = 2)
            JetStream.delete(objects, "conflict.txt"; timeout = 2)
            @test_throws JetStream.UpdateMetaDeletedError JetStream.update_meta(objects, "conflict.txt", JetStream.ObjectMeta(
                name = "conflict.txt",
            ); timeout = 2)
            @test_throws JetStream.ObjectNotFoundError JetStream.update_meta(objects, "missing-meta.txt", JetStream.ObjectMeta(
                name = "missing-meta.txt",
            ); timeout = 2)
            @test_throws ArgumentError JetStream.update_meta(objects, "renamed.txt", JetStream.ObjectMeta(
                name = "renamed.txt",
                link = JetStream.ObjectLink(bucket = object_bucket, name = "blob.bin"),
            ); timeout = 2)
            @test JetStream.get_info(objects, "renamed.txt"; timeout = 2).link === nothing

            link_info = JetStream.add_link(objects, "note-link", renamed_info; timeout = 2)
            @test link_info.link.bucket == object_bucket
            @test link_info.link.name == "renamed.txt"
            @test JetStream.get_string(objects, "note-link"; timeout = 2) == "hello object store"
            @test_throws ArgumentError JetStream.add_link(objects, "", renamed_info; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.add_link(objects, "renamed.txt", renamed_info; timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.add_link(objects, "bad-link", link_info; timeout = 2)

            stale_target = JetStream.put_string(objects, "stale-target.txt", "gone"; timeout = 2)
            JetStream.delete(objects, "stale-target.txt"; timeout = 2)
            stale_link_error = try
                JetStream.add_link(objects, "stale-link", stale_target; timeout = 2)
                nothing
            catch err
                err
            end
            @test stale_link_error isa JetStream.JetStreamError
            @test occursin("deleted", lowercase(stale_link_error.description))
            fresh_deleted = JetStream.get_info(objects, "stale-target.txt"; show_deleted = true, timeout = 2)
            @test_throws JetStream.JetStreamError JetStream.add_link(objects, "fresh-deleted-link", fresh_deleted; timeout = 2)

            linked_objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = object_bucket_link, storage = "memory"); timeout = 2)
            JetStream.put_string(linked_objects, "elsewhere.txt", "from another bucket"; timeout = 2)
            bucket_link = JetStream.add_bucket_link(objects, "linked-bucket", linked_objects; timeout = 2)
            @test bucket_link.link.bucket == object_bucket_link
            @test bucket_link.link.name === nothing
            @test_throws JetStream.JetStreamError JetStream.get_bytes(objects, "linked-bucket"; timeout = 2)

            mktempdir() do dir
                source_file = joinpath(dir, "source.txt")
                dest_file = joinpath(dir, "dest.txt")
                write(source_file, "file object")
                file_info = JetStream.put_file(objects, source_file; name = "file.txt", timeout = 2)
                @test file_info.name == "file.txt"
                JetStream.get_file(objects, "file.txt", dest_file; timeout = 2)
                @test read(dest_file, String) == "file object"

                binary_source = joinpath(dir, "source.bin")
                binary_dest = joinpath(dir, "dest.bin")
                binary_payload = rand(MersenneTwister(0x4e415453), UInt8, 256 * 1024 + 33)
                binary_payload[1:4] = UInt8[0x00, 0xff, 0x0d, 0x0a]
                write(binary_source, binary_payload)
                binary_info = JetStream.put_file(objects, binary_source; name = "binary.bin", timeout = 2)
                @test binary_info.name == "binary.bin"
                @test binary_info.size == length(binary_payload)
                @test binary_info.chunks == cld(length(binary_payload), JetStream.OBJ_DEFAULT_CHUNK_SIZE)
                @test binary_info.digest == JetStream.object_digest(binary_payload)
                JetStream.get_file(objects, "binary.bin", binary_dest; timeout = 2)
                @test read(binary_dest) == binary_payload
            end

            stream_payload = rand(MersenneTwister(0x5354524d), UInt8, 96 * 1024 + 7)
            stream_info = JetStream.put(objects, JetStream.ObjectMeta(
                name = "stream.bin",
                chunk_size = 32 * 1024,
            ), IOBuffer(stream_payload); timeout = 2)
            @test stream_info.size == length(stream_payload)
            @test stream_info.chunks == cld(length(stream_payload), 32 * 1024)
            @test stream_info.digest == JetStream.object_digest(stream_payload)
            @test JetStream.get_bytes(objects, "stream.bin"; timeout = 2) == stream_payload

            before_failed_put = Int(JetStream.stream_info(conn, objects.stream; timeout = 2).state.messages)
            failing_reader = FailingReadIO(fill(UInt8(0x42), 96), 1, ErrorException("injected object read failure"))
            @test_throws ErrorException JetStream.put(objects, JetStream.ObjectMeta(
                name = "partial.bin",
                chunk_size = 32,
            ), failing_reader; timeout = 2)
            after_failed_put = Int(JetStream.stream_info(conn, objects.stream; timeout = 2).state.messages)
            @test after_failed_put == before_failed_put
            @test_throws JetStream.ObjectNotFoundError JetStream.get_info(objects, "partial.bin"; timeout = 2)

            digest_payload = Vector{UInt8}(codeunits("digest me"))
            digest_info = JetStream.put(objects, "digest.bin", digest_payload; timeout = 2)
            bad_digest_info = JetStream.ObjectInfo(
                name = digest_info.name,
                bucket = digest_info.bucket,
                nuid = digest_info.nuid,
                size = digest_info.size,
                mtime = digest_info.mtime,
                chunks = digest_info.chunks,
                digest = JetStream.object_digest(UInt8[0x00]),
                deleted = digest_info.deleted,
                description = digest_info.description,
                headers = digest_info.headers,
                metadata = digest_info.metadata,
                link = digest_info.link,
                chunk_size = digest_info.chunk_size,
            )
            JetStream.publish_object_info(objects, bad_digest_info; timeout = 2)
            digest_mismatch = try
                JetStream.get_bytes(objects, "digest.bin"; timeout = 2)
                nothing
            catch err
                err
            end
            @test digest_mismatch isa JetStream.JetStreamError
            @test occursin("digest mismatch", digest_mismatch.description)
            invalid_digest_info = JetStream.ObjectInfo(
                name = digest_info.name,
                bucket = digest_info.bucket,
                nuid = digest_info.nuid,
                size = digest_info.size,
                mtime = digest_info.mtime,
                chunks = digest_info.chunks,
                digest = "not-a-digest",
                deleted = digest_info.deleted,
                description = digest_info.description,
                headers = digest_info.headers,
                metadata = digest_info.metadata,
                link = digest_info.link,
                chunk_size = digest_info.chunk_size,
            )
            JetStream.publish_object_info(objects, invalid_digest_info; timeout = 2)
            invalid_digest = try
                JetStream.get_bytes(objects, "digest.bin"; timeout = 2)
                nothing
            catch err
                err
            end
            @test invalid_digest isa JetStream.JetStreamError
            @test occursin("invalid format", invalid_digest.description)
            JetStream.delete(objects, "digest.bin"; timeout = 2)

            cleanup_objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = object_cleanup_bucket, storage = "memory"); timeout = 2)
            cleanup_first = JetStream.put(cleanup_objects, JetStream.ObjectMeta(name = "replace.bin", chunk_size = 64), fill(UInt8(0x41), 32); timeout = 2)
            @test cleanup_first.chunks == 1
            @test Int(JetStream.stream_info(conn, cleanup_objects.stream; timeout = 2).state.messages) == 2
            cleanup_second = JetStream.put(cleanup_objects, JetStream.ObjectMeta(name = "replace.bin", chunk_size = 64), fill(UInt8(0x42), 96); timeout = 2)
            @test cleanup_second.chunks == 2
            @test JetStream.get_bytes(cleanup_objects, "replace.bin"; timeout = 2) == fill(UInt8(0x42), 96)
            @test Int(JetStream.stream_info(conn, cleanup_objects.stream; timeout = 2).state.messages) == 3

            delete_watcher = JetStream.watch(objects; updates_only = true, timeout = 2)
            try
                JetStream.delete(objects, "blob.bin"; timeout = 2)
                deleted_update = JetStream.next_update(delete_watcher; timeout = 2)
                @test deleted_update.name == "blob.bin"
                @test deleted_update.deleted
            finally
                close(delete_watcher)
            end

            JetStream.delete(objects, "blob.bin"; timeout = 2)
            @test_throws JetStream.ObjectNotFoundError JetStream.get_info(objects, "blob.bin"; timeout = 2)
            deleted_info = JetStream.get_info(objects, "blob.bin"; show_deleted = true, timeout = 2)
            @test deleted_info.deleted
            @test isempty(JetStream.get_bytes(objects, "blob.bin"; show_deleted = true, timeout = 2))
            @test sort([info.name for info in JetStream.list(objects; timeout = 2)]) == ["binary.bin", "file.txt", "linked-bucket", "note-link", "renamed.txt", "stream.bin"]
            @test "blob.bin" in [info.name for info in JetStream.list(objects; show_deleted = true, timeout = 2)]

            show_deleted_lister = JetStream.list_objects(objects; show_deleted = true, timeout = 2)
            try
                listed_with_deleted = String[]
                while true
                    info = JetStream.next_object(show_deleted_lister; timeout = 2)
                    info === nothing && break
                    push!(listed_with_deleted, info.name)
                end
                @test "blob.bin" in listed_with_deleted
            finally
                close(show_deleted_lister)
            end

            JetStream.seal(objects; timeout = 2)
            @test JetStream.status(objects; timeout = 2).sealed
        finally
            try JetStream.delete_object_store(conn, object_bad_meta_bucket; timeout = 2) catch end
            try JetStream.delete_object_store(conn, object_empty_bucket; timeout = 2) catch end
            try JetStream.delete_object_store(conn, object_cleanup_bucket; timeout = 2) catch end
            try JetStream.delete_object_store(conn, object_bucket_link; timeout = 2) catch end
            try JetStream.delete_object_store(conn, object_bucket; timeout = 2) catch end
            try JetStream.delete_stream(conn, object_subject_decoy_stream; timeout = 2) catch end
            try JetStream.delete_stream(conn, object_prefix_decoy_stream; timeout = 2) catch end
            try JetStream.delete_key_value(conn, filter_bucket) catch end
            try JetStream.delete_key_value(conn, purge_deletes_bucket) catch end
            try JetStream.delete_key_value(conn, stop_bucket) catch end
            try JetStream.delete_key_value(conn, source_bucket_two) catch end
            try JetStream.delete_key_value(conn, source_bucket_one) catch end
            try JetStream.delete_key_value(conn, sourced_bucket) catch end
            try JetStream.delete_key_value(conn, mirror_bucket) catch end
            try JetStream.delete_key_value(conn, mirror_source_bucket) catch end
            try JetStream.delete_key_value(conn, republish_bucket) catch end
            try JetStream.delete_key_value(conn, repair_bucket) catch end
            try JetStream.delete_key_value(conn, create_or_update_bucket) catch end
            try JetStream.delete_key_value(conn, update_bucket) catch end
            try JetStream.delete_key_value(conn, create_bucket) catch end
            try JetStream.delete_key_value(conn, bucket) catch end
            try JetStream.delete_stream(conn, push_stream) catch end
            try JetStream.delete_stream(conn, ordered_consume_stream) catch end
            try JetStream.delete_stream(conn, ordered_gap_stream) catch end
            try JetStream.delete_stream(conn, ordered_stream) catch end
            try JetStream.delete_stream(conn, async_noack_stream) catch end
            try JetStream.delete_stream(conn, async_retry_stream) catch end
            try JetStream.delete_stream(conn, async_handler_stream) catch end
            try JetStream.delete_stream(conn, sync_retry_stream) catch end
            try JetStream.delete_stream(conn, stream) catch end
            try JetStream.delete_stream(conn, ack_stream) catch end
            try JetStream.delete_stream(conn, sourced_stream) catch end
            try JetStream.delete_stream(conn, mirror_stream) catch end
            try JetStream.delete_stream(conn, origin_stream) catch end
            try JetStream.delete_stream(conn, purge_stream_name) catch end
            try JetStream.delete_stream(conn, manage_stream) catch end
            try JetStream.delete_stream(conn, create_or_update_stream_name) catch end
            try JetStream.delete_stream(conn, config_stream) catch end
            NATS.close(conn)
        end
    end
end

with_domain_nats() do url, domain
    @testset "JetStream API domain and prefix" begin
        conn = NATS.connect(url)
        stream = "NATSJL_DOMAIN_PREFIX"
        consumer = "DOMAIN_DURABLE"
        bucket = "DOMAINKV"
        object_bucket = "DOMAINOBJ"
        api_prefix = "\$JS.$domain.API"
        normalized_prefix = "$api_prefix."
        try
            account = JetStream.account_info(conn; domain, timeout = 2)
            @test String(account.domain) == domain

            JetStream.create_stream(
                conn,
                JetStream.StreamConfig(name = stream, subjects = ["$stream.*"], storage = "memory");
                domain,
                timeout = 2,
            )
            sinfo = JetStream.stream_info(conn, stream; api_prefix, timeout = 2)
            @test String(sinfo.config.name) == stream
            @test stream in JetStream.stream_names(conn; domain, timeout = 2)
            @test any(info -> String(info.config.name) == stream, JetStream.streams(conn; domain, timeout = 2))
            @test any(info -> String(info.config.name) == stream, JetStream.streams(conn; subject_filter = "$stream.items", api_prefix, timeout = 2))
            @test JetStream.stream_name_by_subject(conn, "$stream.items"; domain, timeout = 2) == stream

            ack = JetStream.publish(conn, "$stream.items", "one"; timeout = 2)
            @test ack.domain == domain

            JetStream.create_consumer(
                conn,
                stream,
                JetStream.ConsumerConfig(durable_name = consumer);
                api_prefix,
                timeout = 2,
            )
            @test String(JetStream.consumer_info(conn, stream, consumer; domain, timeout = 2).name) == consumer
            @test consumer in JetStream.consumer_names(conn, stream; api_prefix, timeout = 2)
            @test any(info -> String(info.name) == consumer, JetStream.consumers(conn, stream; domain, timeout = 2))

            pull = JetStream.pull_subscribe(conn, stream, consumer; domain)
            try
                messages = JetStream.fetch(pull; batch = 1, no_wait = true, timeout = 2)
                @test NATS.payload(only(messages)) == "one"
                JetStream.ack(conn, only(messages))
            finally
                close(pull)
            end

            kv = JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = bucket, storage = "memory"); domain, timeout = 2)
            @test kv.api_prefix == normalized_prefix
            @test JetStream.put(kv, "foo", "bar"; timeout = 2) == 1
            reopened_kv = JetStream.key_value(conn, bucket; api_prefix, timeout = 2)
            @test reopened_kv.api_prefix == normalized_prefix
            @test JetStream.value_string(JetStream.get(reopened_kv, "foo"; timeout = 2)) == "bar"
            @test JetStream.keys(reopened_kv; timeout = 2) == ["foo"]
            @test JetStream.status(reopened_kv; timeout = 2).bucket == bucket
            @test bucket in JetStream.key_value_store_names(conn; domain, timeout = 2)

            objects = JetStream.create_object_store(
                conn,
                JetStream.ObjectStoreConfig(bucket = object_bucket, storage = "memory");
                domain,
                timeout = 2,
            )
            @test objects.api_prefix == normalized_prefix
            JetStream.put_string(objects, "note.txt", "domain object"; timeout = 2)
            reopened_objects = JetStream.object_store(conn, object_bucket; api_prefix, timeout = 2)
            @test reopened_objects.api_prefix == normalized_prefix
            @test JetStream.get_string(reopened_objects, "note.txt"; timeout = 2) == "domain object"
            @test [info.name for info in JetStream.list(reopened_objects; timeout = 2)] == ["note.txt"]
            @test JetStream.status(reopened_objects; timeout = 2).bucket == object_bucket
            @test object_bucket in JetStream.object_store_names(conn; domain, timeout = 2)
        finally
            try JetStream.delete_object_store(conn, object_bucket; domain, timeout = 2) catch end
            try JetStream.delete_key_value(conn, bucket; api_prefix, timeout = 2) catch end
            try JetStream.delete_consumer(conn, stream, consumer; domain, timeout = 2) catch end
            try JetStream.delete_stream(conn, stream; domain, timeout = 2) catch end
            NATS.close(conn)
        end
    end
end

with_nats(tag = "2.14.2") do url
    @testset "JetStream latest config flags" begin
        conn = NATS.connect(url)
        stream = "NATSJL_LATEST_CFG"
        atomic_stream = "NATSJL_LATEST_ATOMIC"
        priority_stream = "NATSJL_PULL_PRIORITY"
        reset_stream = "NATSJL_CONSUMER_RESET"
        kv_config_bucket = "NATSJLKVCONFIG"
        object_config_bucket = "NATSJLOBJCONFIG"
        object_mirror_bucket = "NATSJLOBJMIRROR"
        try
            all_cfg = JetStream.config_dict(JetStream.StreamConfig(
                name = "ALL_FIELDS",
                allow_msg_ttl = true,
                subject_delete_marker_ttl = 50_000_000_000,
                allow_msg_counter = true,
                allow_atomic_publish = true,
                allow_msg_schedules = true,
                persist_mode = "async",
                allow_batch_publish = true,
            ))
            @test all_cfg["allow_msg_ttl"] === true
            @test all_cfg["subject_delete_marker_ttl"] == 50_000_000_000
            @test all_cfg["allow_msg_counter"] === true
            @test all_cfg["allow_atomic"] === true
            @test all_cfg["allow_msg_schedules"] === true
            @test all_cfg["persist_mode"] == "async"
            @test all_cfg["allow_batched"] === true

            c_cfg = JetStream.config_dict(JetStream.ConsumerConfig(
                name = "PRI",
                durable_name = "PRI",
                pause_until = "2099-01-01T00:00:00Z",
                priority_policy = "pinned_client",
                priority_timeout = 5_000_000_000,
                priority_groups = ["A"],
            ))
            @test c_cfg["pause_until"] == "2099-01-01T00:00:00Z"
            @test c_cfg["priority_policy"] == "pinned_client"
            @test c_cfg["priority_timeout"] == 5_000_000_000
            @test c_cfg["priority_groups"] == ["A"]

            pull_body = JSON3.read(JetStream.next_request_body(
                batch = 2,
                expires_ns = 1_000_000_000,
                max_bytes = 1024,
                min_pending = 10,
                min_ack_pending = 3,
                priority_group = "A",
                priority = 0,
                heartbeat_ns = 250_000_000,
            ))
            @test Int(pull_body.batch) == 2
            @test Int(pull_body.expires) == 1_000_000_000
            @test Int(pull_body.max_bytes) == 1024
            @test Int(pull_body.min_pending) == 10
            @test Int(pull_body.min_ack_pending) == 3
            @test String(pull_body.group) == "A"
            @test Int(pull_body.priority) == 0
            @test Int(pull_body.idle_heartbeat) == 250_000_000
            @test_throws ArgumentError JetStream.next_request_body(min_pending = 0)
            @test_throws ArgumentError JetStream.next_request_body(min_ack_pending = 0)
            @test_throws ArgumentError JetStream.next_request_body(max_bytes = 0)
            @test_throws ArgumentError JetStream.next_request_body(priority = 10)
            @test_throws ArgumentError JetStream.next_request_body(heartbeat_ns = -1)
            @test_throws ArgumentError JetStream.next_request_body(expires_ns = 100_000_000, heartbeat_ns = 75_000_000)
            @test_throws ArgumentError JetStream.next_request_body(no_wait = true, heartbeat_ns = 1)
            hb_sub = fake_subscription("heartbeat.test")
            try
                @test_throws JetStream.NoHeartbeatError JetStream.collect_pull_messages(hb_sub, 1, 1; heartbeat_ns = 10_000_000)
            finally
                close(hb_sub.channel)
            end

            batch_completed_sub = fake_subscription("batch-completed.test"; channel_size = 2)
            try
                put!(batch_completed_sub.channel, NATS.Msg("batch-completed.test", 0, nothing, Pair{String,String}[], NATS.bytes_payload("data"), 200, ""))
                put!(batch_completed_sub.channel, NATS.Msg("batch-completed.test", 0, nothing, Pair{String,String}[], UInt8[], 409, "batch completed"))
                messages, terminal = JetStream.collect_pull_messages(batch_completed_sub, 10, 1; return_terminal = true)
                @test NATS.payload.(messages) == ["data"]
                @test terminal.status == 409
            finally
                close(batch_completed_sub.channel)
            end

            max_bytes_sub = fake_subscription("max-bytes.test"; channel_size = 1)
            try
                put!(max_bytes_sub.channel, NATS.Msg("max-bytes.test", 0, nothing, Pair{String,String}[], UInt8[], 409, "message size exceeds maxbytes"))
                @test isempty(JetStream.collect_pull_messages(max_bytes_sub, 10, 1))
            finally
                close(max_bytes_sub.channel)
            end

            kv_cfg = JetStream.kv_config_dict(JetStream.KeyValueConfig(
                bucket = "KV_ALL_FIELDS",
                compression = true,
                limit_marker_ttl_ns = 45_000_000_000,
                metadata = Dict("purpose" => "config-test"),
            ))
            @test kv_cfg["compression"] == "s2"
            @test kv_cfg["allow_msg_ttl"] === true
            @test kv_cfg["subject_delete_marker_ttl"] == 45_000_000_000
            @test kv_cfg["metadata"] == Dict("purpose" => "config-test")
            kv_mirror_cfg = JetStream.kv_config_dict(JetStream.KeyValueConfig(
                bucket = "KV_MIRROR_CFG",
                mirror = JetStream.StreamSource(name = "SOURCE_CFG"),
            ))
            @test !haskey(kv_mirror_cfg, "subjects")
            @test kv_mirror_cfg["mirror"]["name"] == "KV_SOURCE_CFG"
            @test kv_mirror_cfg["mirror_direct"] === true
            kv_sources_cfg = JetStream.kv_config_dict(JetStream.KeyValueConfig(
                bucket = "KV_SOURCED_CFG",
                sources = [
                    JetStream.StreamSource(name = "SRC1"),
                    JetStream.StreamSource(name = "KV_SRC2"),
                    JetStream.StreamSource(
                        name = "STREAM_SRC",
                        subject_transforms = [JetStream.SubjectTransformConfig(source = "stream.>", destination = "\$KV.KV_SOURCED_CFG.>")],
                    ),
                ],
            ))
            @test kv_sources_cfg["subjects"] == ["\$KV.KV_SOURCED_CFG.>"]
            @test kv_sources_cfg["sources"][1]["name"] == "KV_SRC1"
            @test kv_sources_cfg["sources"][1]["subject_transforms"][1]["src"] == "\$KV.SRC1.>"
            @test kv_sources_cfg["sources"][1]["subject_transforms"][1]["dest"] == "\$KV.KV_SOURCED_CFG.>"
            @test kv_sources_cfg["sources"][2]["name"] == "KV_SRC2"
            @test kv_sources_cfg["sources"][2]["subject_transforms"][1]["src"] == "\$KV.SRC2.>"
            @test kv_sources_cfg["sources"][3]["name"] == "STREAM_SRC"
            @test kv_sources_cfg["sources"][3]["subject_transforms"][1]["src"] == "stream.>"
            @test_throws ArgumentError JetStream.kv_config_dict(JetStream.KeyValueConfig(bucket = "BAD_KV", limit_marker_ttl_ns = -1))

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = stream,
                subjects = ["$stream.>"],
                storage = "file",
                allow_msg_ttl = true,
                subject_delete_marker_ttl = 50_000_000_000,
                allow_msg_schedules = true,
                persist_mode = "async",
                allow_batch_publish = true,
            ); timeout = 2)
            sinfo = JetStream.stream_info(conn, stream; timeout = 2)
            @test Bool(sinfo.config.allow_msg_ttl)
            @test Int(sinfo.config.subject_delete_marker_ttl) == 50_000_000_000
            @test Bool(sinfo.config.allow_msg_schedules)
            @test String(sinfo.config.persist_mode) == "async"
            @test Bool(sinfo.config.allow_batched)

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = atomic_stream,
                subjects = ["$atomic_stream.>"],
                storage = "memory",
                allow_atomic_publish = true,
            ); timeout = 2)
            atomic_info = JetStream.stream_info(conn, atomic_stream; timeout = 2)
            @test Bool(atomic_info.config.allow_atomic)

            ack = JetStream.publish(
                conn,
                "$stream.schedule",
                "scheduled";
                timeout = 2,
                msg_ttl = "60s",
                schedule = "0 30 * * * *",
                schedule_target = "$stream.target",
                schedule_ttl = "never",
                schedule_timezone = "UTC",
            )
            stored = JetStream.get_msg(conn, stream, ack.seq; timeout = 2)
            header_map = Dict(stored.headers)
            @test header_map["Nats-TTL"] == "60s"
            @test header_map["Nats-Schedule"] == "0 30 * * * *"
            @test header_map["Nats-Schedule-Target"] == "$stream.target"
            @test header_map["Nats-Schedule-TTL"] == "never"
            @test header_map["Nats-Schedule-Time-Zone"] == "UTC"

            deleted_seqs = UInt64[
                JetStream.publish(conn, "$stream.deleted", "deleted-$i"; timeout = 2).seq
                for i in 1:4
            ]
            JetStream.delete_msg(conn, stream, deleted_seqs[2]; timeout = 2)
            JetStream.delete_msg(conn, stream, deleted_seqs[4]; timeout = 2)
            latest_default_deleted = JetStream.stream_info(conn, stream; deleted_details = false, timeout = 2)
            @test !haskey(latest_default_deleted.state, :deleted) || isempty(latest_default_deleted.state.deleted)
            latest_deleted_details = JetStream.stream_info(conn, stream; deleted_details = true, timeout = 2)
            @test deleted_seqs[2] in UInt64.(latest_deleted_details.state.deleted)
            @test deleted_seqs[4] in UInt64.(latest_deleted_details.state.deleted)

            kv_config = JetStream.create_key_value(conn, JetStream.KeyValueConfig(
                bucket = kv_config_bucket,
                storage = "file",
                compression = true,
                limit_marker_ttl_ns = 45_000_000_000,
                metadata = Dict("purpose" => "kv-config"),
            ); timeout = 2)
            kv_status = JetStream.status(kv_config; timeout = 2)
            @test kv_status.compressed
            @test kv_status.config.compression
            @test kv_status.limit_marker_ttl_ns == 45_000_000_000
            @test kv_status.config.limit_marker_ttl_ns == 45_000_000_000
            @test kv_status.metadata["purpose"] == "kv-config"
            @test kv_status.config.metadata["purpose"] == "kv-config"
            ttl_create_rev = JetStream.create(kv_config, "ttl-create", "value"; msg_ttl = "60s", timeout = 2)
            ttl_create_entry = JetStream.get_revision(kv_config, "ttl-create", ttl_create_rev; timeout = 2)
            @test Dict(ttl_create_entry.headers)["Nats-TTL"] == "60s"
            @test_throws ArgumentError JetStream.delete(kv_config, "ttl-create"; msg_ttl = "60s", timeout = 2)
            JetStream.purge(kv_config, "ttl-create"; msg_ttl = "90s", timeout = 2)
            ttl_purge_entry = only(JetStream.history(kv_config, "ttl-create"; timeout = 2))
            @test ttl_purge_entry.operation == :purge
            @test Dict(ttl_purge_entry.headers)["Nats-TTL"] == "90s"

            compressed_objects = JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(
                bucket = object_config_bucket,
                max_bytes = 1024,
                compression = true,
                metadata = Dict("purpose" => "object-config"),
            ); timeout = 2)
            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = JetStream.object_stream(object_mirror_bucket),
                mirror = JetStream.StreamSource(
                    name = compressed_objects.stream,
                    subject_transforms = [JetStream.SubjectTransformConfig(
                        source = "\$O.$(object_config_bucket).>",
                        destination = "\$O.$(object_mirror_bucket).>",
                    )],
                ),
                storage = "memory",
                allow_rollup_hdrs = true,
            ); timeout = 2)
            JetStream.put_string(compressed_objects, "compressed.txt", "compressed object"; timeout = 2)
            object_config_status = JetStream.status(compressed_objects; timeout = 2)
            @test object_config_status.compressed
            @test Int(object_config_status.stream_info.config.max_bytes) == 1024
            @test String(object_config_status.stream_info.config.compression) == "s2"
            @test object_config_status.metadata["purpose"] == "object-config"
            @test JetStream.get_string(compressed_objects, "compressed.txt"; timeout = 2) == "compressed object"
            mirrored_objects = JetStream.object_store(conn, object_mirror_bucket; timeout = 2)
            mirror_sync = timedwait(
                () -> try
                    JetStream.get_string(mirrored_objects, "compressed.txt"; timeout = 0.2) == "compressed object"
                catch
                    false
                end,
                5;
                pollint = 0.05,
            )
            @test mirror_sync == :ok
            mirror_watcher = JetStream.watch(mirrored_objects; timeout = 2)
            try
                mirrored_update = JetStream.next_update(mirror_watcher; timeout = 2)
                @test mirrored_update !== nothing
                @test mirrored_update.name == "compressed.txt"
                @test JetStream.next_update(mirror_watcher; timeout = 2) === nothing
            finally
                close(mirror_watcher)
            end

            JetStream.create_consumer(conn, stream, JetStream.ConsumerConfig(
                name = "PRI",
                durable_name = "PRI",
                priority_policy = "pinned_client",
                priority_timeout = 5_000_000_000,
                priority_groups = ["A"],
            ); timeout = 2)
            pinfo = JetStream.consumer_info(conn, stream, "PRI"; timeout = 2)
            @test String(pinfo.config.priority_policy) == "pinned_client"
            @test Int(pinfo.config.priority_timeout) == 5_000_000_000
            @test String.(pinfo.config.priority_groups) == ["A"]

            paused = JetStream.pause_consumer(conn, stream, "PRI", "2099-01-01T00:00:00Z"; timeout = 2)
            @test Bool(paused.paused)
            resumed = JetStream.resume_consumer(conn, stream, "PRI"; timeout = 2)
            @test !Bool(resumed.paused)
            pinned_msg = only(JetStream.fetch(conn, stream, "PRI"; batch = 1, no_wait = true, priority_group = "A", timeout = 2))
            @test haskey(Dict(pinned_msg.headers), "Nats-Pin-Id")
            JetStream.unpin_consumer(conn, stream, "PRI", "A"; timeout = 2)
            JetStream.ack(conn, pinned_msg)
            @test_throws JetStream.ConsumerNotFoundError JetStream.pause_consumer(conn, stream, "MISSING", "2099-01-01T00:00:00Z"; timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = reset_stream,
                subjects = ["$reset_stream.*"],
                storage = "memory",
            ); timeout = 2)
            for i in 1:10
                JetStream.publish(conn, "$reset_stream.items", "reset-$i"; timeout = 2)
            end
            JetStream.create_consumer(conn, reset_stream, JetStream.ConsumerConfig(
                name = "RST",
                durable_name = "RST",
            ); timeout = 2)
            drained = JetStream.fetch(conn, reset_stream, "RST"; batch = 5, no_wait = true, timeout = 2)
            @test length(drained) == 5
            reset = JetStream.reset_consumer(conn, reset_stream, "RST"; timeout = 2)
            @test UInt64(reset.reset_seq) == 1
            @test Int(reset.num_pending) == 10
            reset_first = only(JetStream.fetch(conn, reset_stream, "RST"; batch = 1, no_wait = true, timeout = 2))
            @test JetStream.metadata(reset_first).sequence.stream == 1

            reset_to_seq = JetStream.reset_consumer_to_sequence(conn, reset_stream, "RST", 7; timeout = 2)
            @test UInt64(reset_to_seq.reset_seq) == 7
            @test Int(reset_to_seq.num_pending) == 4
            reset_seven = only(JetStream.fetch(conn, reset_stream, "RST"; batch = 1, no_wait = true, timeout = 2))
            @test JetStream.metadata(reset_seven).sequence.stream == 7
            @test_throws ArgumentError JetStream.reset_consumer_to_sequence(conn, reset_stream, "RST", 0; timeout = 2)
            @test_throws JetStream.ConsumerNotFoundError JetStream.reset_consumer(conn, reset_stream, "MISSING"; timeout = 2)
            @test_throws JetStream.ConsumerNotFoundError JetStream.reset_consumer_to_sequence(conn, reset_stream, "MISSING", 1; timeout = 2)
            @test_throws JetStream.ConsumerNotFoundError JetStream.unpin_consumer(conn, reset_stream, "MISSING", "A"; timeout = 2)

            JetStream.create_stream(conn, JetStream.StreamConfig(
                name = priority_stream,
                subjects = ["$priority_stream.*"],
                storage = "memory",
            ); timeout = 2)
            JetStream.create_consumer(conn, priority_stream, JetStream.ConsumerConfig(
                name = "OVF",
                durable_name = "OVF",
                priority_policy = "overflow",
                priority_groups = ["A"],
            ); timeout = 2)
            for i in 1:100
                JetStream.publish(conn, "$priority_stream.items", "priority-$i"; timeout = 2)
            end
            too_few = JetStream.fetch(
                conn,
                priority_stream,
                "OVF";
                batch = 10,
                expires_ns = 500_000_000,
                min_pending = 110,
                priority_group = "A",
                timeout = 1,
            )
            @test isempty(too_few)
            for i in 101:120
                JetStream.publish(conn, "$priority_stream.items", "priority-$i"; timeout = 2)
            end
            priority_msgs = JetStream.fetch(
                conn,
                priority_stream,
                "OVF";
                batch = 10,
                min_pending = 110,
                priority_group = "A",
                timeout = 2,
            )
            @test length(priority_msgs) == 10
            @test NATS.payload(first(priority_msgs)) == "priority-1"
            foreach(msg -> JetStream.ack(conn, msg), priority_msgs)
        finally
            try JetStream.delete_object_store(conn, object_mirror_bucket; timeout = 2) catch end
            try JetStream.delete_object_store(conn, object_config_bucket; timeout = 2) catch end
            try JetStream.delete_key_value(conn, kv_config_bucket; timeout = 2) catch end
            try JetStream.delete_stream(conn, priority_stream; timeout = 2) catch end
            try JetStream.delete_stream(conn, reset_stream; timeout = 2) catch end
            try JetStream.delete_stream(conn, atomic_stream; timeout = 2) catch end
            try JetStream.delete_stream(conn, stream; timeout = 2) catch end
            NATS.close(conn)
        end
    end
end

with_tls_nats() do certs, url
    @testset "TLS upgrade" begin
        tls = NATS.TLSOptions(ca_file = joinpath(certs, "ca.pem"))
        conn = NATS.connect(url; tls)
        try
            sub = NATS.subscribe(conn, "natsjl.tls")
            NATS.publish(conn, "natsjl.tls", "secure")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "secure"
            @test NATS.tls_required(conn) || NATS.tls_available(conn)
        finally
            NATS.close(conn)
        end
    end
end

with_mtls_nats() do certs, url
    @testset "mutual TLS client certificate" begin
        tls_without_client_cert = NATS.TLSOptions(ca_file = joinpath(certs, "ca.pem"), handshake_timeout = 1)
        @test_throws Exception NATS.connect(url; tls = tls_without_client_cert, allow_reconnect = false, connect_timeout = 1)

        tls = NATS.TLSOptions(
            ca_file = joinpath(certs, "ca.pem"),
            cert_file = joinpath(certs, "client-cert.pem"),
            key_file = joinpath(certs, "client-key.pem"),
            max_version = Reseau.TLS.TLS1_2_VERSION,
        )
        conn = NATS.connect(url; tls, allow_reconnect = false)
        try
            sub = NATS.subscribe(conn, "natsjl.mtls")
            NATS.publish(conn, "natsjl.mtls", "client-cert")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "client-cert"
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
    end
end

with_tls_first_mock() do certs, url, server_result
    @testset "TLS handshake first" begin
        tls = NATS.TLSOptions(ca_file = joinpath(certs, "ca.pem"))
        conn = NATS.connect(url; tls, tls_handshake_first = true, allow_reconnect = false)
        try
            @test NATS.tls_required(conn)
        finally
            NATS.close(conn)
        end
        result = wait_ready(server_result)
        result isa Exception && throw(result)
        connect_line, ping_line = result
        @test startswith(connect_line, "CONNECT ")
        @test ping_line == "PING"
    end
end

with_ws_nats() do url
    @testset "WebSocket transport" begin
        conn = NATS.connect(url)
        try
            sub = NATS.subscribe(conn, "natsjl.ws")
            payload = repeat("w", 70_000)
            NATS.publish(conn, "natsjl.ws", payload)
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == payload
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
    end
end

@testset "WebSocket connection headers" begin
    @test_throws NATS.WebSocketHeadersAlreadySetError NATS.connect(
        "ws://127.0.0.1:1";
        websocket_headers = ["X-NATS-JL-WS" => "static"],
        websocket_headers_cb = () -> ["X-NATS-JL-WS" => "callback"],
    )
    @test_throws ArgumentError NATS.connect("ws://127.0.0.1:1"; proxy_path = "nats")

    with_ws_mock() do url, events
        conn = NATS.connect(
            url;
            websocket_headers = [
                "X-NATS-JL-WS" => "static",
                "Authorization" => "Bearer static-token",
                "X-Multi" => "static-1",
                "X-Multi" => "static-2",
            ],
        )
        try
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
        @test wait_ready(events) == (:headers, "static", "Bearer static-token", "/")
        @test wait_ready(events) == (:multi_headers, ["static-1", "static-2"])
        @test first(wait_ready(events)) == :connect
    end

    callback_calls = Ref(0)
    with_ws_mock() do url, events
        conn = NATS.connect(
            url;
            websocket_headers_cb = () -> begin
                callback_calls[] += 1
                [
                    ("X-NATS-JL-WS", "callback"),
                    ("X-Multi", "callback-1"),
                    ("X-Multi", "callback-2"),
                ]
            end,
        )
        try
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
        @test callback_calls[] == 1
        @test wait_ready(events) == (:headers, "callback", nothing, "/")
        @test wait_ready(events) == (:multi_headers, ["callback-1", "callback-2"])
        @test first(wait_ready(events)) == :connect
    end

    with_ws_mock() do url, events
        conn = NATS.connect("$url/from-url"; proxy_path = "/proxy/nats")
        try
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
        @test wait_ready(events) == (:headers, nothing, nothing, "/proxy/nats")
        @test first(wait_ready(events)) == :connect
    end

    with_ws_mock() do url, events
        conn = NATS.connect("$url/from-url?token=abc&x=1"; proxy_path = "/proxy/nats")
        try
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
        @test wait_ready(events) == (:headers, nothing, nothing, "/proxy/nats?token=abc&x=1")
        @test first(wait_ready(events)) == :connect
    end

    with_discovering_ws_mocks() do first_url, second_url, events
        conn = NATS.connect(
            first_url;
            proxy_path = "/proxy/nats",
            reconnect_wait = 0.01,
            reconnect_jitter = 0.0,
            max_reconnect = 3,
        )
        try
            @test wait_ready(events) == (:headers, :first, "/proxy/nats")
            @test wait_ready(events)[1:2] == (:connect, :first)
            @test NATS.discovered_servers(conn) == [second_url]
            @test second_url in NATS.servers(conn)

            NATS.force_reconnect(conn; timeout = 2)
            @test NATS.connected_url(conn) == second_url
            @test wait_ready(events) == (:headers, :second, "/proxy/nats")
            @test wait_ready(events)[1:2] == (:connect, :second)
        finally
            NATS.close(conn)
        end
    end

    reconnect_header_calls = Ref(0)
    with_discovering_ws_mocks() do first_url, second_url, events
        conn = NATS.connect(
            first_url;
            proxy_path = "/proxy/nats",
            reconnect_wait = 0.01,
            reconnect_jitter = 0.0,
            max_reconnect = 3,
            websocket_headers_cb = () -> begin
                reconnect_header_calls[] += 1
                [("X-NATS-JL-WS", "reconnect-$(reconnect_header_calls[])")]
            end,
        )
        try
            @test reconnect_header_calls[] == 1
            @test wait_ready(events) == (:ws_header, :first, "reconnect-1")
            @test wait_ready(events) == (:headers, :first, "/proxy/nats")
            @test wait_ready(events)[1:2] == (:connect, :first)
            @test NATS.discovered_servers(conn) == [second_url]

            NATS.force_reconnect(conn; timeout = 2)
            @test reconnect_header_calls[] == 2
            @test NATS.connected_url(conn) == second_url
            @test wait_ready(events) == (:ws_header, :second, "reconnect-2")
            @test wait_ready(events) == (:headers, :second, "/proxy/nats")
            @test wait_ready(events)[1:2] == (:connect, :second)
        finally
            NATS.close(conn)
        end
    end
end

with_wss_nats() do certs, url
    @testset "secure WebSocket transport" begin
        tls = NATS.TLSOptions(ca_file = joinpath(certs, "ca.pem"))
        conn = NATS.connect(url; tls)
        try
            sub = NATS.subscribe(conn, "natsjl.wss")
            NATS.publish(conn, "natsjl.wss", "secure-ws")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "secure-ws"
            NATS.flush(conn; timeout = 2)
        finally
            NATS.close(conn)
        end
    end
end

with_auth_nats() do url
    @testset "user password auth" begin
        conn = NATS.connect(url; user = "derek", password = "porkchop")
        try
            sub = NATS.subscribe(conn, "natsjl.auth")
            NATS.publish(conn, "natsjl.auth", "ok")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "ok"
        finally
            NATS.close(conn)
        end

        cb_calls = Ref(0)
        conn = NATS.connect(url; user_info_cb = () -> (cb_calls[] += 1; ("derek", "porkchop")))
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
            @test cb_calls[] == 1
        finally
            NATS.close(conn)
        end

        auth_url = replace(url, "nats://" => "nats://derek:porkchop@")
        conn = NATS.connect(auth_url)
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end
    end
end

with_token_nats() do url
    @testset "token auth" begin
        conn = NATS.connect(url; token = "secret")
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end

        cb_calls = Ref(0)
        conn = NATS.connect(url; token_cb = () -> (cb_calls[] += 1; "secret"))
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
            @test cb_calls[] == 1
        finally
            NATS.close(conn)
        end

        token_url = replace(url, "nats://" => "nats://secret@")
        conn = NATS.connect(token_url)
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end
    end
end

with_nkey_nats() do url
    @testset "nkey auth" begin
        public = NATS.nkey_public_from_seed(NKEY_TEST_SEED)

        @test_throws Exception NATS.connect(url; nkey = public, signature_cb = _ -> zeros(UInt8, 64), allow_reconnect = false, connect_timeout = 1)

        conn = NATS.connect(url; nkey_seed = NKEY_TEST_SEED)
        try
            sub = NATS.subscribe(conn, "natsjl.nkey")
            NATS.publish(conn, "natsjl.nkey", "seed")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "seed"
        finally
            NATS.close(conn)
        end

        conn = NATS.connect(url; nkey = public, signature_cb = nonce -> NATS.nkey_sign(NKEY_TEST_SEED, nonce))
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end
    end
end

with_jwt_nats() do dir, url
    @testset "jwt auth" begin
        conn = NATS.connect(url; jwt = JWT_USER, nkey_seed = JWT_USER_SEED)
        try
            sub = NATS.subscribe(conn, "natsjl.jwt")
            NATS.publish(conn, "natsjl.jwt", "direct")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "direct"
        finally
            NATS.close(conn)
        end

        jwt_file = joinpath(dir, "user.jwt")
        seed_file = joinpath(dir, "user.nk")
        creds_file = joinpath(dir, "user.creds")
        write(jwt_file, JWT_USER)
        write(seed_file, JWT_USER_SEED)
        write(creds_file, JWT_CHAINED_CREDENTIALS)

        conn = NATS.connect(url; jwt_file, nkey_seed_file = seed_file)
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end

        conn = NATS.connect(url; credentials = creds_file)
        try
            NATS.flush(conn; timeout = 2)
            @test NATS.connection_status(conn) == NATS.CONNECTED
        finally
            NATS.close(conn)
        end
    end
end

with_nats_container() do _container, url, port
    @testset "force reconnect resubscribes" begin
        disconnected = Channel{Any}(1)
        reconnected = Channel{Any}(1)
        conn = NATS.connect(
            url;
            allow_reconnect = false,
            disconnected_cb = (_conn, err) -> put!(disconnected, err),
            reconnected_cb = conn -> put!(reconnected, conn.url.port),
        )
        try
            status_ch = NATS.status_changed(conn)
            removed_status_ch = NATS.status_changed(conn, NATS.RECONNECTING)
            @test NATS.remove_status_listener!(conn, removed_status_ch) === conn
            @test !isopen(removed_status_ch)

            sub = NATS.subscribe(conn, "natsjl.force-reconnect")
            NATS.publish(conn, "natsjl.force-reconnect", "before")
            @test NATS.payload(NATS.next_msg(sub; timeout = 2)) == "before"

            NATS.force_reconnect(conn; timeout = 2)
            @test wait_ready(status_ch) == NATS.RECONNECTING
            @test wait_ready(status_ch) == NATS.CONNECTED
            @test wait_ready(disconnected) isa Exception
            @test wait_ready(reconnected) == port
            @test NATS.connection_status(conn) == NATS.CONNECTED

            NATS.publish(conn, "natsjl.force-reconnect", "after")
            @test NATS.payload(NATS.next_msg(sub; timeout = 2)) == "after"
            NATS.close(conn)
            @test wait_ready(status_ch) == NATS.CLOSED
            @test wait_ready(NATS.status_changed(conn)) == NATS.CLOSED
        finally
            NATS.close(conn)
        end
    end
end

with_nats_container() do _container, url, port
    @testset "write-error reconnect policy" begin
        default_error_conn, default_io = write_error_connection(url)
        try
            err = try
                NATS.publish(default_error_conn, "natsjl.write-error.default", "data")
                nothing
            catch err
                err
            end
            @test err === default_io.err
            @test wait_ready(default_error_conn.async_errors) === default_io.err
            @test NATS.last_error(default_error_conn) === default_io.err
            @test NATS.connection_status(default_error_conn) == NATS.CONNECTED
            @test !default_io.closed
        finally
            NATS.close(default_error_conn)
        end

        closed = Channel{Any}(1)
        no_reconnect_conn, no_reconnect_io = write_error_connection(
            url;
            reconnect_on_flusher_error = true,
            allow_reconnect = false,
            closed_cb = conn -> put!(closed, NATS.connection_status(conn)),
        )
        try
            err = try
                NATS.publish(no_reconnect_conn, "natsjl.write-error.no-reconnect", "data")
                nothing
            catch err
                err
            end
            @test err === no_reconnect_io.err
            @test wait_ready(no_reconnect_conn.async_errors) === no_reconnect_io.err
            @test NATS.last_error(no_reconnect_conn) === no_reconnect_io.err
            @test wait_ready(closed) == NATS.CLOSED
            @test NATS.connection_status(no_reconnect_conn) == NATS.CLOSED
            @test no_reconnect_io.closed
        finally
            NATS.close(no_reconnect_conn)
        end

        receiver = NATS.connect(url)
        reconnected = Channel{Any}(1)
        reconnect_conn, reconnect_io = write_error_connection(
            url;
            reconnect_on_flusher_error = true,
            reconnected_cb = conn -> put!(reconnected, conn.url.port),
        )
        try
            sub = NATS.subscribe(receiver, "natsjl.write-error.reconnect")
            NATS.flush(receiver; timeout = 2)

            @test NATS.publish(reconnect_conn, "natsjl.write-error.reconnect", "replayed") === nothing
            @test reconnect_io.closed
            @test wait_ready(reconnected, 5) == port
            NATS.flush(reconnect_conn; timeout = 5)
            msg = NATS.next_msg(sub; timeout = 5)
            @test NATS.payload(msg) == "replayed"
            @test NATS.connection_status(reconnect_conn) == NATS.CONNECTED
        finally
            NATS.close(reconnect_conn)
            NATS.close(receiver)
        end
    end
end

@testset "retry on failed connect" begin
    port = free_port()
    url = "nats://localhost:$port"
    @test_throws Exception NATS.connect(url; connect_timeout = 0.05)

    connected = Channel{Any}(2)
    reconnected = Channel{Any}(2)
    errors = Channel{Any}(32)
    reconnect_errors = Channel{Any}(32)
    conn = NATS.connect(
        url;
        retry_on_failed_connect = true,
        max_reconnect = -1,
        reconnect_wait = 0.02,
        reconnect_jitter = 0.0,
        connect_timeout = 0.05,
        connected_cb = conn -> put!(connected, NATS.connection_status(conn)),
        reconnected_cb = conn -> put!(reconnected, conn.url.port),
        error_cb = (_conn, err) -> put!(errors, err),
        reconnect_error_cb = (_conn, err) -> put!(reconnect_errors, err),
    )
    try
        @test NATS.connection_status(conn) == NATS.RECONNECTING
        @test wait_ready(reconnect_errors) isa Exception

        sub = NATS.subscribe(conn, "natsjl.retry-startup")
        NATS.publish(conn, "natsjl.retry-startup", "during-startup")
        @test conn.pending_bytes > 0

        Harbor.with_container("nats"; tag = "2.10.18", ports = Dict(4222 => port), command = ["--jetstream"]) do _container
            @test wait_ready(connected, 10) == NATS.CONNECTED
            sleep(0.05)
            @test !isready(reconnected)
            NATS.flush(conn; timeout = 5)
            @test conn.pending_bytes == 0
            msg = NATS.next_msg(sub; timeout = 5)
            @test NATS.payload(msg) == "during-startup"
            @test NATS.stats(conn).reconnects == 0
        end
    finally
        NATS.close(conn)
    end

    closed = Channel{Any}(2)
    exhausted = NATS.connect(
        "nats://localhost:$(free_port())";
        retry_on_failed_connect = true,
        max_reconnect = 0,
        reconnect_wait = 0.01,
        reconnect_jitter = 0.0,
        connect_timeout = 0.05,
        closed_cb = conn -> put!(closed, NATS.connection_status(conn)),
    )
    try
        @test wait_ready(closed) == NATS.CLOSED
        @test NATS.connection_status(exhausted) == NATS.CLOSED
    finally
        NATS.close(exhausted)
    end

    custom_attempts = Channel{Int}(8)
    custom_closed = Channel{Any}(2)
    started = time()
    custom = NATS.connect(
        "nats://localhost:$(free_port())";
        retry_on_failed_connect = true,
        max_reconnect = 4,
        reconnect_wait = 5.0,
        reconnect_jitter = 0.0,
        connect_timeout = 0.02,
        custom_reconnect_delay_cb = n -> (put!(custom_attempts, n); 0.01),
        closed_cb = conn -> put!(custom_closed, NATS.connection_status(conn)),
    )
    try
        @test wait_ready(custom_closed, 2) == NATS.CLOSED
        @test time() - started < 2
        @test wait_ready(custom_attempts) == 1
        @test isready(custom_attempts)
        @test take!(custom_attempts) == 2
    finally
        NATS.close(custom)
    end
end

with_nats_container() do _first_container, first_url, first_port
    @testset "reconnect to server callback" begin
        second_port = free_port()
        second_url = "nats://localhost:$second_port"
        Harbor.with_container("nats"; tag = "2.10.18", ports = Dict(4222 => second_port), command = ["--jetstream"]) do _second_container
            selected_snapshots = Channel{Vector{String}}(4)
            reconnected = Channel{Int}(4)
            conn = NATS.connect(
                first_url;
                servers = [second_url],
                no_randomize = true,
                reconnect_wait = 5.0,
                reconnect_jitter = 0.0,
                max_reconnect = 10,
                reconnect_to_server_cb = (servers, _info) -> (put!(selected_snapshots, servers); (first_url, 0.1)),
                reconnected_cb = conn -> put!(reconnected, conn.url.port),
            )
            try
                @test conn.url.port == first_port
                elapsed = @elapsed NATS.force_reconnect(conn; timeout = 3)
                @test elapsed >= 0.08
                @test conn.url.port == first_port
                @test wait_ready(reconnected) == first_port
                @test wait_ready(selected_snapshots) == [second_url, first_url]
            finally
                NATS.close(conn)
            end

            reconnect_errors = Channel{Any}(4)
            fallback = NATS.connect(
                first_url;
                servers = [second_url],
                no_randomize = true,
                reconnect_wait = 0.05,
                reconnect_jitter = 0.0,
                max_reconnect = 10,
                reconnect_to_server_cb = (_servers, _info) -> ("nats://127.0.0.1:9", 0.0),
                reconnect_error_cb = (_conn, err) -> put!(reconnect_errors, err),
            )
            try
                @test fallback.url.port == first_port
                NATS.force_reconnect(fallback; timeout = 3)
                @test fallback.url.port == second_port
                @test wait_ready(reconnect_errors) isa NATS.ServerNotInPoolError
            finally
                NATS.close(fallback)
            end

            nil_fallback = NATS.connect(
                first_url;
                servers = [second_url],
                no_randomize = true,
                reconnect_wait = 0.05,
                reconnect_jitter = 0.0,
                max_reconnect = 10,
                reconnect_to_server_cb = (_servers, _info) -> nothing,
            )
            try
                @test nil_fallback.url.port == first_port
                NATS.force_reconnect(nil_fallback; timeout = 3)
                @test nil_fallback.url.port == second_port
            finally
                NATS.close(nil_fallback)
            end
        end
    end
end

with_nats_container() do first_container, first_url, first_port
    @testset "reconnect resubscribes and buffers publishes" begin
        second_port = free_port()
        second_url = "nats://localhost:$second_port"
        errors = Channel{Any}(16)
        disconnected = Channel{Any}(1)
        reconnected = Channel{Any}(1)
        closed = Channel{Any}(1)
        no_buffer_disconnected = Channel{Any}(1)
        no_buffer_closed = Channel{Any}(1)
        small_buffer_disconnected = Channel{Any}(1)
        small_frame = NATS.pub_frame("natsjl.reconnect.cap", "food")
        conn = NATS.connect(
            first_url;
            servers = [second_url],
            no_randomize = true,
            reconnect_wait = 0.05,
            reconnect_jitter = 0.0,
            max_reconnect = 60,
            error_cb = (_conn, err) -> put!(errors, err),
            disconnected_cb = (_conn, err) -> put!(disconnected, err),
            reconnected_cb = conn -> put!(reconnected, conn.url.port),
            closed_cb = conn -> put!(closed, NATS.connection_status(conn)),
        )
        no_buffer_conn = NATS.connect(
            first_url;
            servers = [second_url],
            no_randomize = true,
            reconnect_wait = 0.05,
            reconnect_jitter = 0.0,
            max_reconnect = 60,
            reconnect_buffer_size = -1,
            disconnected_cb = (_conn, err) -> put!(no_buffer_disconnected, err),
            closed_cb = conn -> put!(no_buffer_closed, NATS.connection_status(conn)),
        )
        small_buffer_conn = NATS.connect(
            first_url;
            servers = [second_url],
            no_randomize = true,
            reconnect_wait = 0.05,
            reconnect_jitter = 0.0,
            max_reconnect = 60,
            reconnect_buffer_size = 2 * length(small_frame),
            disconnected_cb = (_conn, err) -> put!(small_buffer_disconnected, err),
        )
        try
            @test conn.url.port == first_port
            for c in (conn, no_buffer_conn, small_buffer_conn)
                lock(c.lock)
                try
                    c.servers = [NATS.parse_server_url(second_url)]
                finally
                    unlock(c.lock)
                end
            end
            sub = NATS.subscribe(conn, "natsjl.reconnect")
            auto_sub = NATS.subscribe(conn, "natsjl.auto.reconnect")
            auto_status = NATS.status_changed(auto_sub, NATS.SUBSCRIPTION_CLOSED)
            NATS.auto_unsubscribe(auto_sub, 4)
            NATS.flush(conn; timeout = 2)
            for i in 1:2
                NATS.publish(conn, "natsjl.auto.reconnect", "auto-$i")
            end
            NATS.flush(conn; timeout = 2)
            @test [NATS.payload(NATS.next_msg(auto_sub; timeout = 2)) for _ in 1:2] == ["auto-1", "auto-2"]

            Harbor.stop!(first_container; timeout = 0)
            wait_port_closed(first_port)
            @test wait_ready(disconnected) isa Exception
            @test wait_ready(no_buffer_disconnected) isa Exception
            @test wait_ready(small_buffer_disconnected) isa Exception
            @test NATS.connection_status(conn) == NATS.RECONNECTING
            @test NATS.connection_status(no_buffer_conn) == NATS.RECONNECTING
            @test NATS.connection_status(small_buffer_conn) == NATS.RECONNECTING
            NATS.publish(conn, "natsjl.reconnect", "buffered")
            @test NATS.buffered(conn) > 0
            @test_throws NATS.ReconnectBufferExceededError NATS.publish(no_buffer_conn, "natsjl.reconnect.no-buffer", "food")
            @test NATS.buffered(no_buffer_conn) == 0
            @test_throws NATS.ConnectionReconnectingError NATS.drain(no_buffer_conn; timeout = 0.2)
            @test wait_ready(no_buffer_closed) == NATS.CLOSED
            @test NATS.connection_status(no_buffer_conn) == NATS.CLOSED
            NATS.publish(small_buffer_conn, "natsjl.reconnect.cap", "food")
            NATS.publish(small_buffer_conn, "natsjl.reconnect.cap", "food")
            @test NATS.buffered(small_buffer_conn) == 2 * length(small_frame)
            @test_throws NATS.ReconnectBufferExceededError NATS.publish(small_buffer_conn, "natsjl.reconnect.cap", "food")
            while isready(reconnected)
                take!(reconnected)
            end

            Harbor.with_container("nats"; tag = "2.10.18", ports = Dict(4222 => second_port), command = ["--jetstream"]) do _second_container
                sleep(1)
                NATS.flush(conn; timeout = 5)
                @test conn.url.port == second_port
                @test NATS.connection_status(conn) == NATS.CONNECTED
                @test NATS.buffered(conn) == 0
                @test wait_ready(reconnected) == second_port
                @test wait_ready(errors) isa Exception

                msg = NATS.next_msg(sub; timeout = 2)
                @test NATS.payload(msg) == "buffered"

                for i in 3:10
                    NATS.publish(conn, "natsjl.auto.reconnect", "auto-$i")
                end
                NATS.flush(conn; timeout = 2)
                @test [NATS.payload(NATS.next_msg(auto_sub; timeout = 2)) for _ in 1:2] == ["auto-3", "auto-4"]
                @test wait_ready(auto_status) == NATS.SUBSCRIPTION_CLOSED
                @test_throws NATS.MaxMessagesError NATS.next_msg(auto_sub; timeout = 0.1)
            end

            NATS.publish(conn, "natsjl.reconnect", "after")
            msg = NATS.next_msg(sub; timeout = 2)
            @test NATS.payload(msg) == "after"
            NATS.close(conn)
            @test wait_ready(closed) == NATS.CLOSED
        finally
            NATS.close(conn)
            NATS.close(no_buffer_conn)
            NATS.close(small_buffer_conn)
        end
    end
end
