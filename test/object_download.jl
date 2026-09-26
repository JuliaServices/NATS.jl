module ObjectDownloadTests

using Test, NATS, JSON, SHA, Random, Sockets, Base64
using NATS.JetStream

mutable struct Output{F} <: IO
    buffer::IOBuffer
    action::F
    calls::Int
    open::Bool
end
Output(f) = Output(IOBuffer(), f, 0, true)
Base.isopen(io::Output) = io.open
Base.close(io::Output) = (io.open = false; nothing)
function Base.write(io::Output, bytes::StridedVector{UInt8})
    io.calls += 1
    return io.action(io, bytes)
end

capture(f) = try
    f()
    nothing
catch err
    err
end

function replace_info(store, info; kwargs...)
    fields = JetStream.object_info_dict(info)
    for (key, value) in kwargs
        fields[String(key)] = value
    end
    JetStream.publish(store.connection, JetStream.object_meta_subject(store.bucket, info.name),
        JSON.json(fields); headers=["Nats-Rollup" => "sub"], timeout=2)
end

function runtests(url)
    protocol_tests()
    conn = NATS.connect(url; allow_reconnect=false, request_timeout=2)
    bucket = "DOWNLOAD_" * randstring(8)
    other_bucket = bucket * "_OTHER"
    store = JetStream.create_object_store(conn,
        JetStream.ObjectStoreConfig(bucket=bucket, storage="memory"); timeout=2)
    other = JetStream.create_object_store(conn,
        JetStream.ObjectStoreConfig(bucket=other_bucket, storage="memory"); timeout=2)
    try
        # Ported from nats.go TestObjectBasics, TestObjectMulti, TestObjectLinks,
        # TestObjectDeleteMarkers and TestGetObjectDigestMismatch. The sink and
        # batching checks cover the Julia get_to interface.
        @testset "streaming object downloads" begin
            data = UInt8[mod(i, 251) for i in 0:132]
            data[1:4] = UInt8[0, 0xff, 0x0d, 0x0a]
            info = JetStream.put(store, JetStream.ObjectMeta(name="binary", chunk_size=17), data; timeout=2)
            JetStream.create_consumer(conn, store.stream,
                JetStream.ConsumerConfig(durable_name="unrelated", ack_policy="explicit"); timeout=2)
            names() = Set(JetStream.consumer_names(conn, store.stream; timeout=2))
            @test names() == Set(["unrelated"])
            subscriptions = NATS.num_subscriptions(conn)
            requests = NATS.subscribe(conn, "\$JS.API.CONSUMER.MSG.NEXT.$(store.stream).>"; channel_size=128)
            try
                for batch in (1, 2, 32)
                    output = IOBuffer()
                    result = JetStream.get_to(store, "binary", output; batch_size=batch, timeout=2)
                    @test result.nuid == info.nuid
                    @test result.size == length(data)
                    @test result.chunks == 8
                    @test take!(output) == data
                    @test isopen(output)
                    @test names() == Set(["unrelated"])
                    NATS.flush(conn; timeout=2)
                    bodies = [JSON.parse(NATS.next_msg(requests; timeout=2).data) for _ in 1:cld(8, batch)]
                    @test [body.batch for body in bodies] == [min(batch, 8 - first + 1) for first in 1:batch:8]
                    @test all(body -> body.no_wait, bodies)
                    @test !isready(requests.channel)
                end
            finally
                NATS.unsubscribe(conn, requests)
            end
            @test NATS.num_subscriptions(conn) == subscriptions
            @test JetStream.get_bytes(store, "binary"; timeout=2) == data

            short = Output((io, bytes) -> write(io.buffer, @view bytes[1:min(3, length(bytes))]))
            @test JetStream.get_to(store, "binary", short; batch_size=2, timeout=2).nuid == info.nuid
            @test short.calls > info.chunks
            @test take!(short.buffer) == data
            @test isopen(short)

            for invalid in (0, -1, 1.5, nothing, typemax(Int))
                output = Output((_, _) -> invalid)
                @test capture(() -> JetStream.get_to(store, "binary", output; timeout=2)) isa Base.IOError
                @test isopen(output)
                @test names() == Set(["unrelated"])
            end
            failure = ErrorException("output failed")
            output = Output() do io, bytes
                io.calls == 2 && throw(failure)
                write(io.buffer, bytes)
            end
            @test capture(() -> JetStream.get_to(store, "binary", output; batch_size=1, timeout=2)) === failure
            @test take!(output.buffer) == data[1:17]
            @test isopen(output)
            @test names() == Set(["unrelated"])

            # If deletion itself fails, preserve a prior output failure. A
            # verified download still reports its own cleanup failure.
            one = JetStream.put(store, "one", "abc"; timeout=2)
            for fail_body in (false, true)
                output = Output() do io, bytes
                    owned = only(setdiff(names(), Set(["unrelated"])))
                    JetStream.delete_consumer(conn, store.stream, owned; timeout=2)
                    fail_body && throw(failure)
                    write(io.buffer, bytes)
                end
                err = capture(() -> JetStream.get_to(store, "one", output; timeout=2))
                @test fail_body ? err === failure : err isa JetStream.ConsumerNotFoundError
                @test isopen(output)
                @test names() == Set(["unrelated"])
            end

            @testset "empty, deleted, missing and invalid input" begin
                empty = JetStream.put(store, "empty", UInt8[]; timeout=2)
                output = IOBuffer()
                @test JetStream.get_to(store, "empty", output; timeout=2).nuid == empty.nuid
                @test position(output) == 0
                created = NATS.subscribe(conn, "\$JS.API.CONSUMER.CREATE.$(store.stream).>"; channel_size=8)
                try
                    JetStream.get_to(store, "empty", output; timeout=2)
                    NATS.flush(conn; timeout=2)
                    @test !isready(created.channel)
                finally
                    NATS.unsubscribe(conn, created)
                end
                JetStream.delete(store, "one"; timeout=2)
                @test_throws JetStream.ObjectNotFoundError JetStream.get_to(store, "one", output; timeout=2)
                @test JetStream.get_to(store, "one", output; show_deleted=true, timeout=2).deleted
                @test_throws JetStream.ObjectNotFoundError JetStream.get_to(store, "missing", output; timeout=2)
                @test_throws ArgumentError JetStream.get_to(store, "", output; timeout=2)
                for batch in (0, -1)
                    @test_throws ArgumentError JetStream.get_to(store, "binary", output; batch_size=batch, timeout=2)
                end
                for timeout in (0, -1, Inf, NaN)
                    @test_throws ArgumentError JetStream.get_to(store, "binary", output; timeout)
                end
                for fields in ((size=1,), (digest=JetStream.object_digest(UInt8[1]),))
                    replace_info(store, empty; fields...)
                    @test_throws JetStream.JetStreamError JetStream.get_to(store, "empty", output; timeout=2)
                end
                @test position(output) == 0
                @test names() == Set(["unrelated"])
            end

            @testset "links and cycle rejection" begin
                JetStream.add_link(store, "same", info; timeout=2)
                JetStream.add_link(other, "cross", info; timeout=2)
                for (source, name) in ((store, "same"), (other, "cross"))
                    output = IOBuffer()
                    @test JetStream.get_to(source, name, output; timeout=2).nuid == info.nuid
                    @test take!(output) == data
                    @test JetStream.get_bytes(source, name; timeout=2) == data
                end
                JetStream.add_bucket_link(store, "bucket", other; timeout=2)
                for download in ((s, n) -> JetStream.get_to(s, n, IOBuffer(); timeout=2),
                                 (s, n) -> JetStream.get_bytes(s, n; timeout=2))
                    @test_throws JetStream.JetStreamError download(store, "bucket")
                end
                for (source, name, target, target_name) in (
                    (store, "self", store, "self"),
                    (store, "cycle", other, "cycle"), (other, "cycle", store, "cycle"),
                )
                    JetStream.publish_object_info(source, JetStream.ObjectInfo(name=name, bucket=source.bucket,
                        nuid=randstring(22), size=0, chunks=0,
                        link=JetStream.ObjectLink(bucket=target.bucket, name=target_name)); timeout=2)
                end
                for name in ("self", "cycle")
                    @test_throws JetStream.BadObjectMetaError JetStream.get_to(store, name, IOBuffer(); timeout=2)
                    @test_throws JetStream.BadObjectMetaError JetStream.get_bytes(store, name; timeout=2)
                end
            end

            @testset "corrupt object integrity" begin
                for fields in ((size=info.size-1,), (size=info.size+1,),
                               (chunks=info.chunks-1,), (chunks=info.chunks+1,),
                               (digest=JetStream.object_digest(UInt8[1]),),
                               (digest="SHA-256=%%%",), (digest="MD5=AAAA",),
                               (nuid="",))
                    replace_info(store, info; fields...)
                    output = IOBuffer()
                    @test capture(() -> JetStream.get_to(store, "binary", output; batch_size=2, timeout=2)) isa
                        (haskey(fields, :nuid) ? JetStream.BadObjectMetaError : JetStream.JetStreamError)
                    @test isopen(output)
                    @test names() == Set(["unrelated"])
                end
                replace_info(store, info; digest="")
                output = IOBuffer()
                @test JetStream.get_to(store, "binary", output; timeout=2).digest == ""
                @test take!(output) == data
                JetStream.publish_object_info(store, info; timeout=2)
                # nats.go appends an extra chunk to the published NUID: even a
                # full final batch must not succeed with the original prefix.
                JetStream.publish(conn, JetStream.object_chunk_subject(store.bucket, info.nuid), "extra"; timeout=2)
                for batch in (1, 2, 32)
                    @test_throws JetStream.JetStreamError JetStream.get_to(store, "binary", IOBuffer(); batch_size=batch, timeout=2)
                end
                JetStream.purge_stream(conn, store.stream; subject_filter=JetStream.object_chunk_subject(store.bucket, info.nuid), timeout=2)
                @test_throws JetStream.JetStreamError JetStream.get_to(store, "binary", IOBuffer(); timeout=2)
                @test names() == Set(["unrelated"])

                mktempdir() do dir
                    path = joinpath(dir, "existing")
                    write(path, "keep this file")
                    @test_throws JetStream.JetStreamError JetStream.get_file(store, "binary", path; timeout=2)
                    @test read(path, String) == "keep this file"
                end

                changing = JetStream.put(store, JetStream.ObjectMeta(name="changing", chunk_size=17), data; timeout=2)
                output = Output() do io, bytes
                    io.calls == 1 && JetStream.put(store, "changing", "replacement"; timeout=2)
                    write(io.buffer, bytes)
                end
                @test_throws JetStream.JetStreamError JetStream.get_to(store, "changing", output; batch_size=1, timeout=2)
                @test take!(output.buffer) == data[1:17]
                @test JetStream.get_string(store, "changing"; timeout=2) == "replacement"
                @test names() == Set(["unrelated"])
            end

            @testset "real TCP destination" begin
                payload = rand(MersenneTwister(0x4e415453), UInt8, 256 * 1024 + 13)
                tcpinfo = JetStream.put(store, JetStream.ObjectMeta(name="tcp", chunk_size=32*1024), payload; timeout=2)
                listener = listen(ip"127.0.0.1", 0)
                received = @async begin
                    socket = accept(listener)
                    try
                        read(socket, length(payload))
                    finally
                        close(socket)
                    end
                end
                output = connect(ip"127.0.0.1", last(getsockname(listener)))
                try
                    @test JetStream.get_to(store, "tcp", output; batch_size=2, timeout=2).nuid == tcpinfo.nuid
                    @test isopen(output)
                    @test fetch(received) == payload
                finally
                    close(output)
                    close(listener)
                end
                @test names() == Set(["unrelated"])
                @test NATS.num_subscriptions(conn) == subscriptions
            end

            @testset "disconnected cleanup fallback" begin
                broken = NATS.connect(url; allow_reconnect=false, request_timeout=2)
                broken_store = JetStream.object_store(broken, bucket; timeout=2)
                owned = Ref("")
                output = Output() do io, bytes
                    owned[] = only(setdiff(names(), Set(["unrelated"])))
                    write(io.buffer, bytes)
                    NATS.close(broken)
                    throw(failure)
                end
                try
                    @test capture(() -> JetStream.get_to(broken_store, "tcp", output; batch_size=1, timeout=2)) === failure
                    @test position(output.buffer) == 32 * 1024
                    @test isopen(output)
                    @test NATS.num_subscriptions(broken) == 0
                    @test names() == Set(["unrelated", owned[]])
                    pending = JetStream.consumer_info(conn, store.stream, owned[]; timeout=2)
                    @test pending.config.inactive_threshold == 300_000_000_000
                finally
                    NATS.close(broken)
                    isempty(owned[]) || JetStream.delete_consumer(conn, store.stream, owned[]; timeout=2)
                end
                @test names() == Set(["unrelated"])
            end
        end
    finally
        for name in (bucket, other_bucket)
            JetStream.delete_object_store(conn, name; timeout=2)
        end
        NATS.close(conn)
    end
end

function protocol_tests()
    @testset "object download protocol failures" begin
        for fault in (:none, :cycle, :create, :info, :stream, :consumer, :subject,
                      :sequence, :reversed, :pending, :reply, :disconnect)
            listener = listen(ip"127.0.0.1", 0)
            port = last(getsockname(listener))
            events = Symbol[]
            deleted = String[]
            peer = @async begin
                socket = accept(listener)
                subscriptions = Dict{String,String}()
                try
                    write(socket, "INFO ", JSON.json(Dict("server_id"=>"object-peer", "server_name"=>"object-peer",
                        "version"=>"2.10.18", "go"=>"go1.22", "host"=>"127.0.0.1", "port"=>port,
                        "proto"=>1, "headers"=>true, "auth_required"=>false, "max_payload"=>1048576)), "\r\n")
                    flush(socket)
                    while !eof(socket)
                        fields = split(readline(socket))
                        isempty(fields) && continue
                        if fields[1] == "PING"
                            write(socket, "PONG\r\n")
                            flush(socket)
                        elseif fields[1] == "SUB"
                            subscriptions[String(fields[2])] = String(fields[end])
                        elseif fields[1] == "PUB"
                            subject, reply = String(fields[2]), String(fields[3])
                            read(socket, parse(Int, fields[end]) + 2)
                            key = only(filter(k -> k == reply || (endswith(k, ".*") && startswith(reply, k[1:end-1])), keys(subscriptions)))
                            sid = subscriptions[key]
                            body = if occursin("STREAM.MSG.GET", subject)
                                push!(events, :metadata)
                                info = Dict("name"=>"object", "bucket"=>"PEER", "nuid"=>"id", "size"=>4, "chunks"=>2)
                                fault == :cycle && (info["options"] = Dict("link"=>Dict("bucket"=>"PEER", "name"=>"object")))
                                count(==(:metadata), events) > 4 ? "{\"error\":{\"code\":500,\"description\":\"repeated metadata request\"}}" :
                                    JSON.json(Dict("message"=>Dict("data"=>base64encode(JSON.json(info)))))
                            elseif occursin("CONSUMER.CREATE", subject)
                                push!(events, :create)
                                fault == :create ? "{\"error\":{\"code\":500,\"description\":\"create failed\"}}" :
                                    "{\"name\":\"owned\",\"config\":{\"ack_policy\":\"none\"}}"
                            elseif occursin("CONSUMER.INFO", subject)
                                push!(events, :info)
                                fault == :info ? "{\"error\":{\"code\":500,\"description\":\"info failed\"}}" :
                                    "{\"name\":\"owned\",\"config\":{\"ack_policy\":\"none\"}}"
                            elseif occursin("CONSUMER.DELETE", subject)
                                push!(deleted, last(split(subject, '.')))
                                push!(events, :delete)
                                "{\"success\":true}"
                            elseif occursin("CONSUMER.MSG.NEXT", subject)
                                push!(events, :pull)
                                fault == :disconnect && break
                                for i in 1:2
                                    stream = fault == :stream ? "OTHER" : "OBJ_PEER"
                                    consumer = fault == :consumer ? "other" : "owned"
                                    sequence = fault == :sequence ? i+1 : i
                                    stream_sequence = fault == :reversed ? 3-i : i
                                    pending = fault == :pending ? 1 : 2-i
                                    msg_subject = fault == :subject ? "other" : "\$O.PEER.C.id"
                                    ack = fault == :reply ? "invalid" : "\$JS.ACK.$stream.$consumer.1.$stream_sequence.$sequence.1.$pending"
                                    write(socket, "MSG $msg_subject $sid $ack 2\r\n", UInt8[2i-1, 2i], "\r\n")
                                end
                                flush(socket)
                                continue
                            else
                                error("unexpected object request: $subject")
                            end
                            write(socket, "MSG $reply $sid $(sizeof(body))\r\n", body, "\r\n")
                            flush(socket)
                        end
                    end
                finally
                    close(socket)
                end
            end
            conn = NATS.connect("nats://127.0.0.1:$port"; allow_reconnect=false, request_timeout=2, ping_interval=0)
            try
                store = JetStream.ObjectStore(conn, "PEER", "OBJ_PEER", "\$O.PEER.C.", "\$O.PEER.M.", JetStream.DEFAULT_API_PREFIX)
                output = IOBuffer()
                err = capture(() -> JetStream.get_to(store, "object", output; timeout=2))
                @test fault == :none ? err === nothing : err isa Exception
                @test isopen(output)
                fault == :none && @test take!(output) == UInt8[1,2,3,4]
                fault == :cycle && @test err isa JetStream.BadObjectMetaError
            finally
                NATS.close(conn)
                close(listener)
            end
            fetch(peer)
            if fault == :cycle
                @test events == [:metadata, :metadata]
                continue
            end
            @test events[1:2] == [:metadata, :create]
            if fault == :create
                @test events == [:metadata, :create]
            elseif fault == :disconnect
                @test events == [:metadata, :create, :info, :pull]
            else
                @test last(events) == :delete
                @test count(==(:delete), events) == 1
                @test deleted == ["owned"]
            end
        end
    end
end

end # module
