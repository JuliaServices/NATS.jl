@testset "watcher terminal cleanup" begin
    with_nats() do url
        conn = NATS.connect(url; allow_reconnect = false, request_timeout = 2)
        deletes = NATS.subscribe(conn, "\$JS.API.CONSUMER.DELETE.>"; channel_size = 16)
        try
            for kind in (:key_value, :object)
                bucket = "WATCH_CLEANUP_" * randstring(8)
                owner = if kind === :key_value
                    JetStream.create_key_value(conn, JetStream.KeyValueConfig(bucket = bucket, storage = "memory"); timeout = 2)
                else
                    JetStream.create_object_store(conn, JetStream.ObjectStoreConfig(bucket = bucket, storage = "memory"); timeout = 2)
                end
                for mode in (:normal, :slow_consumer, :concurrent_close)
                    @testset "$kind $mode" begin
                        subscriptions = NATS.num_subscriptions(conn)
                        watcher = kind === :key_value ?
                            JetStream.watch_all(owner; updates_only = true, channel_size = 1, timeout = 2) :
                            JetStream.watch(owner; updates_only = true, channel_size = 1, timeout = 2)
                        try
                            JetStream.put(owner, "first", "value"; timeout = 2)
                            @test timedwait(() -> isready(JetStream.updates(watcher)), 2) == :ok
                            if mode !== :normal
                                JetStream.put(owner, "second", "value"; timeout = 2)
                                @test timedwait(() -> NATS.delivered(watcher.subscription) == 2, 2) == :ok
                                for i in 3:16
                                    JetStream.put(owner, "entry-$i", "value"; timeout = 2)
                                end
                                NATS.flush(conn; timeout = 2)
                                @test NATS.dropped(watcher.subscription) > 0
                            end

                            if mode === :slow_consumer
                                drainer = @async collect(JetStream.updates(watcher))
                                @test timedwait(() -> istaskdone(watcher.task), 2) == :ok
                                buffered = fetch(drainer)
                            else
                                # Closing a watcher closes the update channel and
                                # retains already buffered updates.
                                @sync for _ in 1:(mode === :normal ? 1 : 2)
                                    @async close(watcher)
                                end
                                buffered = collect(JetStream.updates(watcher))
                            end
                            @test timedwait(() -> istaskdone(watcher.task), 2) == :ok
                            wait(watcher.task)
                            @test watcher.closed
                            @test !isopen(JetStream.updates(watcher))
                            @test !isopen(JetStream.errors(watcher))
                            @test length(buffered) == (mode === :slow_consumer ? 2 : 1)
                            @test (kind === :key_value ? first(buffered).key : first(buffered).name) == "first"
                            failures = collect(JetStream.errors(watcher))
                            if mode === :slow_consumer
                                @test length(failures) == 1
                                @test only(failures) isa NATS.SlowConsumerError
                            else
                                @test isempty(failures)
                            end

                            @test !NATS.is_valid(watcher.subscription)
                            @test NATS.num_subscriptions(conn) == subscriptions
                            @test_throws JetStream.ConsumerNotFoundError JetStream.consumer_info(conn, owner.stream, watcher.consumer; timeout = 2)
                            close(watcher)
                            NATS.flush(conn; timeout = 2)
                            requests = NATS.Msg[]
                            while isready(deletes.channel)
                                push!(requests, NATS.next_msg(deletes; timeout = 2))
                            end
                            @test length(requests) == 1
                            @test all(msg -> msg.subject == "\$JS.API.CONSUMER.DELETE.$(owner.stream).$(watcher.consumer)", requests)
                        finally
                            close(watcher)
                        end
                    end
                end
            end
        finally
            close(conn)
        end
    end
end
