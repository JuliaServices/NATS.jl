# JuliaC --trim=safe workload. The executable connects to the nats-server
# whose URL is its first argument and runs a JetStream work queue end to end:
# stream and durable pull consumer provisioning, deduplicated publishes,
# fetch, ack/nak/term, a forced reconnect, connection callbacks, keepalive
# pings, drain, and close.
using NATS
using NATS.JetStream

const STREAM = "NATSJL_TRIM"
const SUBJECT = "natsjl.trim.events"
const CONSUMER = "NATSJL_TRIM_WORKERS"
const TIMEOUT = 10

function check(condition::Bool, message::String)
    condition || error(message)
    return nothing
end

# Connection callbacks report here.
const EVENTS = Channel{String}(32)

function expect_events(names::Vector{String})
    pending = copy(names)
    deadline = time() + TIMEOUT
    while !isempty(pending) && time() < deadline
        if isready(EVENTS)
            filter!(!=(take!(EVENTS)), pending)
        else
            sleep(0.01)
        end
    end
    check(isempty(pending), "missing callbacks: $(join(pending, ", "))")
    return nothing
end

function provision(conn::NATS.Connection)
    JetStream.create_or_update_stream(
        conn,
        JetStream.StreamConfig(
            name = STREAM,
            subjects = [SUBJECT],
            description = "NATS.jl trim workload",
            storage = "file",
            max_msgs = 10_000,
            max_age = Int64(60) * 1_000_000_000,
            duplicate_window = Int64(60) * 1_000_000_000,
        );
        timeout = TIMEOUT,
    )
    JetStream.create_or_update_consumer(
        conn,
        STREAM,
        JetStream.ConsumerConfig(
            name = CONSUMER,
            durable_name = CONSUMER,
            description = "NATS.jl trim workers",
            filter_subject = SUBJECT,
            ack_policy = "explicit",
            ack_wait = 30_000_000_000,
            max_deliver = 5,
            max_ack_pending = 64,
        );
        timeout = TIMEOUT,
    )
    return JetStream.pull_subscribe(conn, STREAM, CONSUMER; timeout = TIMEOUT)
end

function publish_event(conn::NATS.Connection, id::String)
    return JetStream.publish(conn, SUBJECT, "event $id"; msg_id = id, expected_stream = STREAM, timeout = TIMEOUT)
end

function fetch_events(sub::JetStream.PullSubscription, expected::Int)
    messages = NATS.Msg[]
    deadline = time() + TIMEOUT
    while length(messages) < expected && time() < deadline
        append!(messages, JetStream.fetch(sub; batch = 16, expires_ns = 500_000_000, timeout = 2))
    end
    check(length(messages) == expected, "fetched $(length(messages)) events, expected $expected")
    return messages
end

function run_work_queue(conn::NATS.Connection)
    sub = provision(conn)
    first = publish_event(conn, "a")
    check(first.seq > 0 && !first.duplicate, "first publish acknowledged")
    check(publish_event(conn, "a").duplicate, "duplicate publish detected")
    publish_event(conn, "b")
    publish_event(conn, "c")

    for msg in fetch_events(sub, 3)
        body = NATS.payload(msg)
        if body == "event a"
            JetStream.ack_sync(conn, msg; timeout = TIMEOUT)
        elseif body == "event b"
            JetStream.nak(conn, msg; delay_ns = 100_000_000)
        elseif body == "event c"
            JetStream.term(conn, msg; reason = "trim workload")
        else
            error("unexpected event payload: $body")
        end
    end
    redelivered = only(fetch_events(sub, 1))
    check(NATS.payload(redelivered) == "event b", "nak redelivers the event")
    JetStream.ack_sync(conn, redelivered; timeout = TIMEOUT)
    close(sub)
    return nothing
end

function run_core(conn::NATS.Connection)
    sub = NATS.subscribe(conn, "natsjl.trim.core")
    NATS.publish(conn, "natsjl.trim.core", "ping")
    check(NATS.payload(NATS.next_msg(sub; timeout = TIMEOUT)) == "ping", "core publish delivered")
    NATS.force_reconnect(conn; timeout = TIMEOUT)
    check(NATS.stats(conn).reconnects == 1, "forced reconnect completed")
    expect_events(["disconnected", "reconnected"])
    NATS.publish(conn, "natsjl.trim.core", "after reconnect")
    check(NATS.payload(NATS.next_msg(sub; timeout = TIMEOUT)) == "after reconnect", "subscription resent after reconnect")
    NATS.unsubscribe(conn, sub)
    return nothing
end

function run_trim_workload(url::String)
    conn = NATS.connect(
        url;
        name = "natsjl-trim",
        error_cb = (_, _, @nospecialize(err)) -> put!(EVENTS, "error"),
        disconnected_cb = (_, @nospecialize(err)) -> put!(EVENTS, "disconnected"),
        reconnected_cb = _ -> put!(EVENTS, "reconnected"),
        closed_cb = _ -> put!(EVENTS, "closed"),
    )
    run_core(conn)
    run_work_queue(conn)
    NATS.drain(conn; timeout = TIMEOUT)
    check(NATS.is_closed(conn), "drain closes the connection")
    expect_events(["closed"])

    pinger = NATS.connect(url; name = "natsjl-trim-ping", ping_interval = 0.05)
    sleep(0.5)
    check(NATS.is_connected(pinger), "keepalive pings answered")
    NATS.close(pinger)
    check(NATS.is_closed(pinger), "close closes the connection")
    return nothing
end

function @main(args::Vector{String})::Cint
    run_trim_workload(args[1])
    println(Core.stdout, "NATS trim workload passed")
    return 0
end
