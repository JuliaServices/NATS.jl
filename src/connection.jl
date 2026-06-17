@enum ConnectionStatus CONNECTING CONNECTED RECONNECTING DRAINING CLOSED
@enum SubscriptionStatus SUBSCRIPTION_ACTIVE SUBSCRIPTION_DRAINING SUBSCRIPTION_CLOSED SUBSCRIPTION_SLOW_CONSUMER

const DEFAULT_SUB_PENDING_BYTES_LIMIT = 64 * 1024 * 1024
const CONNECTION_STATUS_CHANNEL_SIZE = 64
const SUBSCRIPTION_STATUS_CHANNEL_SIZE = 16
const DEFAULT_PING_INTERVAL = 120.0
const DEFAULT_MAX_PINGS_OUT = 2

Base.@kwdef mutable struct Options
    servers::Vector{String} = String[]
    no_randomize::Bool = false
    name::Union{Nothing, String} = nothing
    verbose::Bool = false
    pedantic::Bool = false
    no_echo::Bool = false
    no_responders::Bool = true
    headers::Bool = true
    user::Union{Nothing, String} = nothing
    password::Union{Nothing, String} = nothing
    user_info_cb::Union{Nothing, Function} = nothing
    token::Union{Nothing, String} = nothing
    token_cb::Union{Nothing, Function} = nothing
    jwt::Union{Nothing, String} = nothing
    jwt_cb::Union{Nothing, Function} = nothing
    nkey::Union{Nothing, String} = nothing
    nkey_seed::Union{Nothing, String} = nothing
    credentials::Union{Nothing, String} = nothing
    jwt_file::Union{Nothing, String} = nothing
    nkey_seed_file::Union{Nothing, String} = nothing
    signature_cb::Union{Nothing, Function} = nothing
    tls_required::Bool = false
    tls_handshake_first::Bool = false
    tls::TLSOptions = TLSOptions()
    connect_timeout::Float64 = 2.0
    request_timeout::Float64 = 5.0
    drain_timeout::Float64 = 30.0
    ping_interval::Float64 = DEFAULT_PING_INTERVAL
    max_pings_out::Int = DEFAULT_MAX_PINGS_OUT
    allow_reconnect::Bool = true
    max_reconnect::Int = 60
    reconnect_wait::Float64 = 2.0
    reconnect_jitter::Float64 = 0.1
    reconnect_jitter_tls::Float64 = 1.0
    custom_reconnect_delay_cb::Union{Nothing, Function} = nothing
    reconnect_to_server_cb::Union{Nothing, Function} = nothing
    reconnect_buffer_size::Int = 8 * 1024 * 1024
    reconnect_on_flusher_error::Bool = false
    ignore_auth_error_abort::Bool = false
    retry_on_failed_connect::Bool = false
    no_callbacks_after_client_close::Bool = false
    subscription_channel_size::Int = 64 * 1024
    subscription_pending_bytes_limit::Int = DEFAULT_SUB_PENDING_BYTES_LIMIT
    permission_err_on_subscribe::Bool = false
    inbox_prefix::String = "_INBOX"
    skip_subject_validation::Bool = false
    ignore_discovered_servers::Bool = false
    websocket_headers::Vector{Pair{String,String}} = Pair{String,String}[]
    websocket_headers_cb::Union{Nothing, Function} = nothing
    proxy_path::Union{Nothing, String} = nothing
    connected_cb::Union{Nothing, Function} = nothing
    error_cb::Union{Nothing, Function} = nothing
    reconnect_error_cb::Union{Nothing, Function} = nothing
    discovered_servers_cb::Union{Nothing, Function} = nothing
    lame_duck_cb::Union{Nothing, Function} = nothing
    disconnected_cb::Union{Nothing, Function} = nothing
    reconnected_cb::Union{Nothing, Function} = nothing
    closed_cb::Union{Nothing, Function} = nothing
end

mutable struct Subscription
    connection::Any
    subject::String
    queue::Union{Nothing, String}
    sid::Int
    channel::Channel{Any}
    capacity::Int
    lock::ReentrantLock
    task::Union{Nothing, Task}
    callback_subscription::Bool
    processing::Int
    closed::Bool
    close_error::Union{Nothing, Exception}
    server_sent::Bool
    status::SubscriptionStatus
    status_listeners::Dict{Channel{SubscriptionStatus}, Vector{SubscriptionStatus}}
    closed_cb::Union{Nothing, Function}
    max_msgs::Union{Nothing, Int}
    delivered::Int
    dropped::Int
    max_pending::Int
    pending_bytes::Int
    max_pending_bytes::Int
    pending_msg_limit::Int
    pending_bytes_limit::Int
end

mutable struct BarrierAction
    remaining::Int
    callback::Union{Nothing, Function}
    lock::ReentrantLock
end

Base.@kwdef struct Statistics
    in_msgs::UInt64 = 0
    out_msgs::UInt64 = 0
    in_bytes::UInt64 = 0
    out_bytes::UInt64 = 0
    reconnects::UInt64 = 0
end

mutable struct Connection
    url::ServerURL
    servers::Vector{ServerURL}
    explicit_server_keys::Set{Tuple{String,String,Int}}
    discovered_server_keys::Set{Tuple{String,String,Int}}
    options::Options
    io::Any
    info::ServerInfo
    in_msgs::UInt64
    out_msgs::UInt64
    in_bytes::UInt64
    out_bytes::UInt64
    reconnects::UInt64
    status::ConnectionStatus
    status_listeners::Dict{Channel{ConnectionStatus}, Vector{ConnectionStatus}}
    lock::ReentrantLock
    write_lock::ReentrantLock
    flush_lock::ReentrantLock
    subscriptions::Dict{Int, Subscription}
    next_sid::Int
    rng::AbstractRNG
    reader_task::Union{Nothing, Task}
    reconnect_task::Union{Nothing, Task}
    ping_task::Union{Nothing, Task}
    pongs::Channel{Nothing}
    pings_out::Int
    async_errors::Channel{Any}
    last_error::Any
    last_auth_errors::Dict{Tuple{String,String,Int}, DataType}
    pending_frames::Vector{Vector{UInt8}}
    pending_bytes::Int
    request_sub::Union{Nothing, Subscription}
    request_prefix::Union{Nothing, String}
    request_map::Dict{String, Channel}
    pub_ack_tokens::Set{String}
    subscription_cleanups::Dict{Int, Function}
    connected_once::Bool
end

function Base.show(io::IO, conn::Connection)
    print(io, "NATS.Connection(", conn.url.raw, ", ", conn.status, ", ", length(conn.subscriptions), " subscriptions)")
end

function safe_callback(conn::Connection, cb::Union{Nothing, Function}, args...)
    cb === nothing && return nothing
    @async begin
        try
            cb(args...)
        catch err
            put!(conn.async_errors, err)
        end
    end
    return nothing
end

function safe_error_callback(conn::Connection, cb::Union{Nothing, Function}, sub, err)
    cb === nothing && return nothing
    @async begin
        try
            if applicable(cb, conn, sub, err)
                cb(conn, sub, err)
            else
                cb(conn, err)
            end
        catch callback_err
            put!(conn.async_errors, callback_err)
        end
    end
    return nothing
end

function notify_error!(conn::Connection, err, sub = nothing)
    set_last_error!(conn, err)
    put!(conn.async_errors, err)
    safe_error_callback(conn, conn.options.error_cb, sub, err)
    return nothing
end

function notify_connected!(conn::Connection)
    safe_callback(conn, conn.options.connected_cb, conn)
    return nothing
end

function notify_reconnect_error!(conn::Connection, err)
    set_last_error!(conn, err)
    safe_callback(conn, conn.options.reconnect_error_cb, conn, err)
    return nothing
end

function notify_discovered_servers!(conn::Connection)
    safe_callback(conn, conn.options.discovered_servers_cb, conn)
    return nothing
end

function notify_lame_duck!(conn::Connection)
    safe_callback(conn, conn.options.lame_duck_cb, conn)
    return nothing
end

function notify_disconnected!(conn::Connection, err)
    set_last_error!(conn, err)
    safe_callback(conn, conn.options.disconnected_cb, conn, err)
    return nothing
end

function notify_reconnected!(conn::Connection)
    safe_callback(conn, conn.options.reconnected_cb, conn)
    return nothing
end

function notify_closed!(conn::Connection)
    safe_callback(conn, conn.options.closed_cb, conn)
    return nothing
end

function safe_subscription_callback(sub::Subscription, cb::Union{Nothing, Function}, args...)
    cb === nothing && return nothing
    @async begin
        try
            cb(args...)
        catch err
            put!(sub.connection.async_errors, err)
        end
    end
    return nothing
end

function notify_subscription_closed!(sub::Subscription)
    safe_subscription_callback(sub, sub.closed_cb, sub.subject)
    return nothing
end

function set_subscription_cleanup!(conn::Connection, sub::Subscription, cb::Union{Nothing, Function})
    lock(conn.lock)
    try
        if cb === nothing
            delete!(conn.subscription_cleanups, sub.sid)
        else
            conn.subscription_cleanups[sub.sid] = cb
        end
    finally
        unlock(conn.lock)
    end
    return sub
end

function take_subscription_cleanup!(conn::Connection, sub::Subscription)
    lock(conn.lock)
    try
        return pop!(conn.subscription_cleanups, sub.sid, nothing)
    finally
        unlock(conn.lock)
    end
end

function has_subscription_cleanups(conn::Connection)
    lock(conn.lock)
    try
        return !isempty(conn.subscription_cleanups)
    finally
        unlock(conn.lock)
    end
end

function run_subscription_cleanup(conn::Connection, cb, timeout::Real; throw_errors::Bool)
    cb === nothing && return nothing
    try
        cb(timeout)
    catch err
        notify_error!(conn, err)
        throw_errors && rethrow()
    end
    return nothing
end

function run_subscription_cleanup!(conn::Connection, sub::Subscription, timeout::Real; throw_errors::Bool)
    cb = take_subscription_cleanup!(conn, sub)
    return run_subscription_cleanup(conn, cb, timeout; throw_errors)
end

function run_subscription_cleanups!(conn::Connection, subs::Vector{Subscription}, deadline::Real, operation::AbstractString)
    for sub in subs
        remaining = remaining_until(deadline)
        remaining <= 0 && throw(ConnectionTimeoutError(String(operation), 0.0))
        run_subscription_cleanup!(conn, sub, remaining; throw_errors = false)
    end
    return nothing
end

function enqueue_reconnect_frame!(conn::Connection, frame::Vector{UInt8})
    lock(conn.lock)
    try
        conn.status == RECONNECTING || return false
        limit = conn.options.reconnect_buffer_size
        attempted_size = length(frame)
        limit < 0 && throw(ReconnectBufferExceededError(limit, attempted_size))
        if limit >= 0 && conn.pending_bytes + attempted_size > limit
            throw(ReconnectBufferExceededError(limit, attempted_size))
        end
        push!(conn.pending_frames, copy(frame))
        conn.pending_bytes += attempted_size
        return true
    finally
        unlock(conn.lock)
    end
end

function take_pending_frames!(conn::Connection)
    lock(conn.lock)
    try
        frames = conn.pending_frames
        conn.pending_frames = Vector{UInt8}[]
        conn.pending_bytes = 0
        return frames
    finally
        unlock(conn.lock)
    end
end

function abort_pub_ack_futures_locked!(conn::Connection, err)
    for token in collect(conn.pub_ack_tokens)
        ch = get(conn.request_map, token, nothing)
        if ch !== nothing
            if isopen(ch)
                try
                    put!(ch, err)
                catch
                end
            end
            try close(ch) catch end
        end
        delete!(conn.request_map, token)
    end
    empty!(conn.pub_ack_tokens)
    return nothing
end

function abort_pub_ack_futures!(conn::Connection, err)
    lock(conn.lock)
    try
        abort_pub_ack_futures_locked!(conn, err)
    finally
        unlock(conn.lock)
    end
    return nothing
end

function buffered(conn::Connection)
    lock(conn.lock)
    try
        return conn.pending_bytes
    finally
        unlock(conn.lock)
    end
end

function set_last_error!(conn::Connection, err)
    lock(conn.lock)
    try
        conn.last_error = err
    finally
        unlock(conn.lock)
    end
    return nothing
end

function last_error(conn::Connection)
    lock(conn.lock)
    try
        return conn.last_error
    finally
        unlock(conn.lock)
    end
end

function normalize_websocket_headers(headers)
    out = Pair{String,String}[]
    for item in headers
        if item isa Pair
            push!(out, String(item.first) => String(item.second))
        elseif item isa Tuple && length(item) == 2
            push!(out, String(item[1]) => String(item[2]))
        else
            throw(ArgumentError("websocket headers must be pairs or 2-tuples"))
        end
    end
    return out
end

function validate_options(options::Options)
    if !isempty(options.websocket_headers) && options.websocket_headers_cb !== nothing
        throw(WebSocketHeadersAlreadySetError())
    end
    options.proxy_path !== nothing && (options.proxy_path = normalize_websocket_proxy_path(options.proxy_path))
    if options.user_info_cb !== nothing && (options.user !== nothing || options.password !== nothing)
        throw(UserInfoAlreadySetError())
    end
    if options.token_cb !== nothing && options.token !== nothing
        throw(TokenAlreadySetError())
    end
    options.reconnect_wait < 0 && throw(ArgumentError("reconnect_wait must be nonnegative"))
    options.reconnect_jitter < 0 && throw(ArgumentError("reconnect_jitter must be nonnegative"))
    options.reconnect_jitter_tls < 0 && throw(ArgumentError("reconnect_jitter_tls must be nonnegative"))
    options.reconnect_buffer_size < -1 && throw(ArgumentError("reconnect_buffer_size must be -1 or nonnegative"))
    options.ping_interval < 0 && throw(ArgumentError("ping_interval must be nonnegative"))
    validate_inbox_prefix(options.inbox_prefix)
    return options
end

has_subject_whitespace(s::AbstractString) =
    occursin(' ', s) || occursin('\t', s) || occursin('\r', s) || occursin('\n', s)

function validate_publish_subject(subject::AbstractString; skip::Bool = false)
    subject_s = String(subject)
    isempty(subject_s) && throw(BadSubjectError(subject_s))
    if !skip && has_subject_whitespace(subject_s)
        throw(BadSubjectError(subject_s))
    end
    return subject_s
end

function validate_inbox_prefix(prefix::AbstractString)
    prefix_s = String(prefix)
    if isempty(prefix_s) || has_subject_whitespace(prefix_s) ||
            startswith(prefix_s, ".") || endswith(prefix_s, ".") ||
            occursin("..", prefix_s) || occursin("*", prefix_s) || occursin(">", prefix_s)
        throw(ArgumentError("inbox_prefix must be a nonempty literal subject prefix without wildcards or trailing dots"))
    end
    return prefix_s
end

function validate_subscribe_subject(subject::AbstractString)
    subject_s = String(subject)
    if isempty(subject_s) || has_subject_whitespace(subject_s) ||
            startswith(subject_s, ".") || endswith(subject_s, ".") || occursin("..", subject_s)
        throw(BadSubjectError(subject_s))
    end
    return subject_s
end

function validate_queue_name(queue::Union{Nothing, AbstractString})
    queue === nothing && return nothing
    queue_s = String(queue)
    has_subject_whitespace(queue_s) && throw(BadQueueNameError(queue_s))
    return queue_s
end

function websocket_connection_headers(options::Options)
    options.websocket_headers_cb === nothing && return copy(options.websocket_headers)
    headers = options.websocket_headers_cb()
    headers === nothing && return Pair{String,String}[]
    return normalize_websocket_headers(headers)
end

function user_info_callback_value(cb)
    value = cb()
    if value isa Pair
        return String(value.first), String(value.second)
    elseif value isa Tuple && length(value) == 2
        return String(value[1]), String(value[2])
    elseif value isa NamedTuple && haskey(value, :user) && haskey(value, :password)
        return String(value.user), String(value.password)
    end
    throw(ArgumentError("user_info_cb must return a (user, password) tuple, pair, or named tuple"))
end

function token_callback_value(cb)
    value = cb()
    value isa AbstractString || throw(ArgumentError("token_cb must return an AbstractString"))
    return String(value)
end

server_key(url::ServerURL) = (url.scheme, url.host, url.port)

server_error_is_auth_error(err) =
    err isa AuthorizationViolationError ||
    err isa AuthenticationExpiredError ||
    err isa AuthenticationRevokedError ||
    err isa AccountAuthenticationExpiredError

function record_auth_error!(conn::Connection, server::ServerURL, err)
    (conn.options.ignore_auth_error_abort || !server_error_is_auth_error(err)) && return false
    key = server_key(server)
    marker = typeof(err)
    lock(conn.lock)
    try
        repeated = get(conn.last_auth_errors, key, nothing) === marker
        conn.last_auth_errors[key] = marker
        return repeated
    finally
        unlock(conn.lock)
    end
end

function clear_auth_error!(conn::Connection, server::ServerURL)
    conn.options.ignore_auth_error_abort && return nothing
    lock(conn.lock)
    try
        delete!(conn.last_auth_errors, server_key(server))
    finally
        unlock(conn.lock)
    end
    return nothing
end

function server_pool(url::ServerURL, options::Options)
    servers = ServerURL[url]
    append!(servers, parse_server_url.(options.servers))
    deduped = ServerURL[]
    seen = Set{Tuple{String,String,Int}}()
    for server in servers
        key = server_key(server)
        key in seen && continue
        push!(seen, key)
        push!(deduped, server)
    end
    if any(is_websocket_scheme(server) != is_websocket_scheme(first(deduped)) for server in deduped)
        throw(UnsupportedTransportError("servers", "mixing WebSocket and non-WebSocket NATS URLs is not supported"))
    end
    if !options.no_randomize && length(deduped) > 1
        first_server = first(deduped)
        rest = deduped[2:end]
        shuffle!(rest)
        deduped = vcat(first_server, rest)
    end
    return deduped
end

function connect_payload(options::Options, url::ServerURL, info::ServerInfo, secure::Bool)
    token = options.token
    user = options.user === nothing ? url.user : options.user
    password = options.password === nothing ? url.password : options.password
    if token === nothing && options.user === nothing && options.password === nothing && url.user !== nothing && url.password === nothing
        token = url.user
        user = nothing
    end
    if options.token_cb !== nothing
        token === nothing || throw(TokenAlreadySetError())
        token = token_callback_value(options.token_cb)
    end
    if options.user_info_cb !== nothing && url.user === nothing && url.password === nothing
        if user !== nothing || password !== nothing
            throw(UserInfoAlreadySetError())
        end
        user, password = user_info_callback_value(options.user_info_cb)
    end
    options.no_echo && info.proto < 1 && throw(NoEchoNotSupportedError())
    server_headers_supported = info.headers === true
    headers_supported = options.headers && server_headers_supported
    data = Dict{String, Any}(
        "verbose" => options.verbose,
        "pedantic" => options.pedantic,
        "tls_required" => secure,
        "lang" => CLIENT_LANG,
        "version" => CLIENT_VERSION,
        "protocol" => 1,
        "echo" => !options.no_echo,
        "no_responders" => options.no_responders && headers_supported,
        "headers" => headers_supported,
    )
    options.name === nothing || (data["name"] = options.name)
    user === nothing || (data["user"] = user)
    password === nothing || (data["pass"] = password)
    token === nothing || (data["auth_token"] = token)
    add_auth_fields!(data, options, info)
    return Vector{UInt8}(codeunits("CONNECT $(JSON3.write(data))\r\n"))
end

function connection_status(conn::Connection)
    lock(conn.lock)
    try
        return conn.status
    finally
        unlock(conn.lock)
    end
end

status(conn::Connection) = connection_status(conn)

function send_connection_status_locked!(conn::Connection, status::ConnectionStatus)
    for (ch, statuses) in collect(conn.status_listeners)
        if status in statuses && isopen(ch) && Base.n_avail(ch) < CONNECTION_STATUS_CHANNEL_SIZE
            try
                put!(ch, status)
            catch
            end
        end
        if status == CLOSED || !isopen(ch)
            try close(ch) catch end
            delete!(conn.status_listeners, ch)
        end
    end
    return nothing
end

function set_connection_status_locked!(conn::Connection, status::ConnectionStatus)
    conn.status == status && return nothing
    conn.status = status
    send_connection_status_locked!(conn, status)
    return nothing
end

function set_connection_status!(conn::Connection, status::ConnectionStatus)
    lock(conn.lock)
    try
        set_connection_status_locked!(conn, status)
    finally
        unlock(conn.lock)
    end
    return nothing
end

function status_changed(conn::Connection, statuses::ConnectionStatus...)
    wanted = isempty(statuses) ? [CONNECTED, RECONNECTING, DRAINING, CLOSED] : collect(statuses)
    ch = Channel{ConnectionStatus}(CONNECTION_STATUS_CHANNEL_SIZE)
    lock(conn.lock)
    try
        if conn.status == CLOSED
            if CLOSED in wanted
                put!(ch, CLOSED)
            end
            close(ch)
        else
            conn.status_listeners[ch] = wanted
        end
    finally
        unlock(conn.lock)
    end
    return ch
end

function remove_status_listener!(conn::Connection, ch::Channel{ConnectionStatus})
    lock(conn.lock)
    try
        delete!(conn.status_listeners, ch)
        try close(ch) catch end
    finally
        unlock(conn.lock)
    end
    return conn
end

is_closed(conn::Connection) = connection_status(conn) == CLOSED
is_reconnecting(conn::Connection) = connection_status(conn) == RECONNECTING
is_draining(conn::Connection) = connection_status(conn) == DRAINING
is_connected(conn::Connection) = begin
    s = connection_status(conn)
    s == CONNECTED || s == DRAINING
end

function stats(conn::Connection)
    lock(conn.lock)
    try
        return Statistics(
            in_msgs = conn.in_msgs,
            out_msgs = conn.out_msgs,
            in_bytes = conn.in_bytes,
            out_bytes = conn.out_bytes,
            reconnects = conn.reconnects,
        )
    finally
        unlock(conn.lock)
    end
end

function num_subscriptions(conn::Connection)
    lock(conn.lock)
    try
        return count(sub -> !sub.closed, values(conn.subscriptions))
    finally
        unlock(conn.lock)
    end
end

function servers(conn::Connection)
    lock(conn.lock)
    try
        return [server.raw for server in conn.servers]
    finally
        unlock(conn.lock)
    end
end

function discovered_servers(conn::Connection)
    lock(conn.lock)
    try
        return [server.raw for server in conn.servers if server_key(server) in conn.discovered_server_keys]
    finally
        unlock(conn.lock)
    end
end

function set_discovered_servers_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.discovered_servers_cb = cb
    return conn
end

discovered_servers_handler(conn::Connection) = conn.options.discovered_servers_cb

function set_connected_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.connected_cb = cb
    return conn
end

connected_handler(conn::Connection) = conn.options.connected_cb

function set_error_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.error_cb = cb
    return conn
end

error_handler(conn::Connection) = conn.options.error_cb

function set_reconnect_error_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.reconnect_error_cb = cb
    return conn
end

reconnect_error_handler(conn::Connection) = conn.options.reconnect_error_cb

function set_lame_duck_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.lame_duck_cb = cb
    return conn
end

lame_duck_handler(conn::Connection) = conn.options.lame_duck_cb

function set_disconnected_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.disconnected_cb = cb
    return conn
end

disconnected_handler(conn::Connection) = conn.options.disconnected_cb

function set_reconnected_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.reconnected_cb = cb
    return conn
end

reconnected_handler(conn::Connection) = conn.options.reconnected_cb

function set_closed_handler!(conn::Connection, cb::Union{Nothing, Function})
    conn.options.closed_cb = cb
    return conn
end

closed_handler(conn::Connection) = conn.options.closed_cb

function connected_info(conn::Connection)
    lock(conn.lock)
    try
        conn.status == CONNECTED || return nothing
        return conn.url, conn.info
    finally
        unlock(conn.lock)
    end
end

connected_url(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : info[1].raw
end

connected_server_id(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : info[2].server_id
end

connected_server_name(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : info[2].server_name
end

connected_server_version(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : info[2].version
end

connected_cluster_name(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : something(info[2].cluster, "")
end

connected_client_id(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? nothing : info[2].client_id
end

connected_client_ip(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? "" : something(info[2].client_ip, "")
end

connected_server_jetstream(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? (false, 0) : (info[2].jetstream === true, something(info[2].api_lvl, 0))
end

is_system_account(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? false : info[2].acc_is_sys === true
end

auth_required(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? false : info[2].auth_required === true
end

tls_required(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? false : info[2].tls_required === true
end

tls_available(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? false : info[2].tls_available === true
end

tls_verify(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? false : info[2].tls_verify === true
end

max_payload(conn::Connection) = begin
    info = connected_info(conn)
    info === nothing ? 0 : info[2].max_payload
end

function rtt(conn::Connection; timeout::Real = conn.options.request_timeout)
    connection_status(conn) == CONNECTED || throw(ConnectionClosedError("connection is not connected"))
    started = time()
    flush(conn; timeout)
    return time() - started
end

function wait_connected(conn::Connection; timeout::Union{Nothing, Real} = nothing, operation::AbstractString = "connect", allow_draining::Bool = false)
    isready_status() = begin
        s = connection_status(conn)
        s == CONNECTED || s == CLOSED || s == DRAINING
    end
    if timeout === nothing
        while !isready_status()
            sleep(0.01)
        end
    else
        result = timedwait(isready_status, timeout; pollint = 0.01)
        result == :ok || throw(ConnectionTimeoutError(String(operation), Float64(timeout)))
    end
    status = connection_status(conn)
    status == CONNECTED && return nothing
    allow_draining && status == DRAINING && return nothing
    status == DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionClosedError("connection is closed"))
end

function write_frame(io, frame::Vector{UInt8})
    write(io, frame)
    try
        flush(io)
    catch
    end
    return nothing
end

function close_after_terminal_error!(conn::Connection, err)
    notify_error!(conn, err)
    lock(conn.lock)
    try
        set_connection_status_locked!(conn, CLOSED)
        empty!(conn.pending_frames)
        conn.pending_bytes = 0
    finally
        unlock(conn.lock)
    end
    close_subscriptions!(conn; err)
    try close(conn.io) catch end
    notify_closed!(conn)
    return nothing
end

close_after_write_error!(conn::Connection, err) = close_after_terminal_error!(conn, err)

function send_frame(conn::Connection, frame::Vector{UInt8}; timeout::Union{Nothing, Real} = nothing, operation::AbstractString = "write", allow_draining::Bool = false, buffer_on_reconnect::Bool = false)
    while true
        if buffer_on_reconnect && enqueue_reconnect_frame!(conn, frame)
            return nothing
        end
        wait_connected(conn; timeout, operation, allow_draining)
        local write_error = nothing
        lock(conn.write_lock)
        try
            status = connection_status(conn)
            (status == CONNECTED || (allow_draining && status == DRAINING)) ||
                throw(ConnectionClosedError("cannot write while connection is not connected"))
            write_frame(conn.io, frame)
            return nothing
        catch err
            write_error = err
        finally
            unlock(conn.write_lock)
        end
        if connection_status(conn) == CONNECTED
            if conn.options.reconnect_on_flusher_error
                if conn.options.allow_reconnect
                    start_reconnect!(conn, write_error)
                    continue
                else
                    close_after_write_error!(conn, write_error)
                end
            else
                notify_error!(conn, write_error)
            end
        end
        throw(write_error)
    end
end

function classify_server_error(message::AbstractString)
    original = String(message)
    normalized = lowercase(strip(original))
    normalized == "stale connection" && return StaleConnectionError(original)
    normalized == "maximum connections exceeded" && return MaxConnectionsExceededError(original)
    normalized == "maximum account active connections exceeded" && return MaxAccountConnectionsExceededError(original)
    startswith(normalized, "permissions violation") && return PermissionViolationError(original)
    startswith(normalized, "maximum subscriptions exceeded") && return MaxSubscriptionsExceededError(original)
    startswith(normalized, "authorization violation") && return AuthorizationViolationError(original)
    startswith(normalized, "user authentication expired") && return AuthenticationExpiredError(original)
    startswith(normalized, "user authentication revoked") && return AuthenticationRevokedError(original)
    startswith(normalized, "account authentication expired") && return AccountAuthenticationExpiredError(original)
    return ServerError(original)
end

server_error_is_transient(err) =
    err isa PermissionViolationError || err isa MaxSubscriptionsExceededError

server_error_should_reconnect(err) =
    err isa StaleConnectionError ||
    err isa MaxConnectionsExceededError ||
    err isa MaxAccountConnectionsExceededError ||
    err isa AuthenticationExpiredError ||
    err isa AccountAuthenticationExpiredError

function handle_terminal_server_error!(conn::Connection, err)
    if server_error_should_reconnect(err) && conn.options.allow_reconnect && connection_status(conn) == CONNECTED
        start_reconnect!(conn, err)
    else
        close_after_terminal_error!(conn, err)
    end
    return nothing
end

function permission_subscription_target(err::PermissionViolationError)
    subject_match = match(r"Subscription to \"([^\"]+)\"", err.message)
    subject_match === nothing && return nothing
    queue_match = match(r"using queue \"([^\"]+)\"", err.message)
    subject = String(subject_match.captures[1])
    queue = queue_match === nothing ? nothing : String(queue_match.captures[1])
    return subject, queue
end

function route_subscription_permission_error!(conn::Connection, err::PermissionViolationError)
    conn.options.permission_err_on_subscribe || return false
    target = permission_subscription_target(err)
    target === nothing && return false
    subject, queue = target
    subs = lock(conn.lock) do
        [sub for sub in values(conn.subscriptions) if sub.subject == subject && sub.queue == queue && !sub.closed]
    end
    for sub in subs
        close_subscription_local!(conn, sub; err)
    end
    return !isempty(subs)
end

function establish_connection(url::ServerURL, options::Options)
    io = is_websocket_scheme(url) ?
         open_websocket_transport(
             url,
             options.tls;
             connect_timeout = options.connect_timeout,
             headers = websocket_connection_headers(options),
             proxy_path = options.proxy_path,
         ) :
         options.tls_handshake_first ?
         open_tls_transport(url, options.tls; connect_timeout = options.connect_timeout) :
         open_tcp_transport(url; connect_timeout = options.connect_timeout)
    try
        first = read_protocol_message(io)
        first isa ServerInfo || throw(ProtocolError("expected INFO as first server message"))
        info = first::ServerInfo
        want_tls = !is_websocket_scheme(url) && (options.tls_required || is_tls_scheme(url) || options.tls_handshake_first)
        server_requires_tls = info.tls_required === true
        server_tls_available = server_requires_tls || info.tls_available === true
        secure = options.tls_handshake_first || url.scheme == "wss"
        if !secure && (want_tls || server_requires_tls)
            server_tls_available || throw(UnsupportedTransportError(url.scheme, "client requires TLS but the server did not advertise TLS support"))
            io = upgrade_to_tls(io, url, options.tls)
            secure = true
        end
        write_frame(io, connect_payload(options, url, info, secure))
        write_frame(io, ping_frame())
        while true
            msg = read_protocol_message(io)
            msg isa Ok && continue
            msg isa Ping && (write_frame(io, pong_frame()); continue)
            msg isa Err && throw(classify_server_error((msg::Err).message))
            msg isa Pong && break
            throw(ProtocolError("unexpected handshake message $(typeof(msg))"))
        end
        return io, info
    catch
        try close(io) catch end
        rethrow()
    end
end

function advertised_server_urls(base::ServerURL, info::ServerInfo)
    raw_urls = if is_websocket_scheme(base) && info.ws_connect_urls !== nothing
        info.ws_connect_urls
    else
        info.connect_urls
    end
    raw_urls === nothing && return nothing
    return [parse_server_url(occursin("://", raw) ? raw : "$(base.scheme)://$raw") for raw in raw_urls]
end

function add_discovered_servers!(servers::Vector{ServerURL}, base::ServerURL, info::ServerInfo)
    discovered_urls = advertised_server_urls(base, info)
    discovered_urls === nothing && return servers
    seen = Set(server_key.(servers))
    for discovered in discovered_urls
        key = server_key(discovered)
        key in seen && continue
        push!(seen, key)
        push!(servers, discovered)
    end
    return servers
end

function apply_discovered_servers!(conn::Connection, base::ServerURL, info::ServerInfo; notify::Bool = false)
    conn.options.ignore_discovered_servers && return false
    advertised = advertised_server_urls(base, info)
    (advertised === nothing || isempty(advertised)) && return false

    has_new = false
    lock(conn.lock)
    try
        advertised_keys = Set(server_key.(advertised))
        current_key = server_key(conn.url)

        filter!(conn.servers) do server
            key = server_key(server)
            key in conn.explicit_server_keys && return true
            key == current_key && return true
            key in conn.discovered_server_keys || return true
            return key in advertised_keys
        end
        for key in copy(conn.discovered_server_keys)
            if !(key in advertised_keys || key == current_key)
                delete!(conn.discovered_server_keys, key)
            end
        end

        seen = Set(server_key.(conn.servers))
        for server in advertised
            key = server_key(server)
            if !(key in conn.explicit_server_keys) && !(key in conn.discovered_server_keys)
                has_new = true
            end
            if !(key in seen)
                push!(conn.servers, server)
                push!(seen, key)
            end
            if !(key in conn.explicit_server_keys)
                push!(conn.discovered_server_keys, key)
            end
        end
    finally
        unlock(conn.lock)
    end
    has_new && notify && notify_discovered_servers!(conn)
    return has_new
end

function first_successful_connection(servers::Vector{ServerURL}, options::Options)
    last_error = nothing
    for server in servers
        try
            io, info = establish_connection(server, options)
            return server, io, info
        catch err
            last_error = err
        end
    end
    last_error === nothing || throw(last_error)
    throw(ConnectionClosedError("no NATS servers configured"))
end

function new_connection(
        server::ServerURL,
        servers::Vector{ServerURL},
        options::Options,
        io,
        info::ServerInfo,
        status::ConnectionStatus;
        connected_once::Bool,
)
    explicit_keys = Set(server_key.(servers))
    return Connection(
        server,
        servers,
        explicit_keys,
        Set{Tuple{String,String,Int}}(),
        options,
        io,
        info,
        UInt64(0),
        UInt64(0),
        UInt64(0),
        UInt64(0),
        UInt64(0),
        status,
        Dict{Channel{ConnectionStatus}, Vector{ConnectionStatus}}(),
        ReentrantLock(),
        ReentrantLock(),
        ReentrantLock(),
        Dict{Int, Subscription}(),
        0,
        Random.default_rng(),
        nothing,
        nothing,
        nothing,
        Channel{Nothing}(Inf),
        0,
        Channel{Any}(Inf),
        nothing,
        Dict{Tuple{String,String,Int}, DataType}(),
        Vector{UInt8}[],
        0,
        nothing,
        nothing,
        Dict{String, Channel}(),
        Set{String}(),
        Dict{Int, Function}(),
        connected_once,
    )
end

function reset_pings_out!(conn::Connection)
    lock(conn.lock)
    try
        conn.pings_out = 0
    finally
        unlock(conn.lock)
    end
    return nothing
end

function increment_pings_out!(conn::Connection)
    lock(conn.lock)
    try
        conn.pings_out += 1
        return conn.pings_out
    finally
        unlock(conn.lock)
    end
end

function start_ping_loop!(conn::Connection)
    conn.options.ping_interval > 0 || return nothing
    lock(conn.lock)
    try
        if conn.ping_task !== nothing && !istaskdone(conn.ping_task)
            return nothing
        end
        conn.ping_task = errormonitor(@async ping_loop(conn))
    finally
        unlock(conn.lock)
    end
    return nothing
end

function handle_stale_connection!(conn::Connection)
    err = StaleConnectionError("stale connection")
    if conn.options.allow_reconnect && connection_status(conn) == CONNECTED
        start_reconnect!(conn, err)
    else
        close_after_terminal_error!(conn, err)
    end
    return nothing
end

function ping_loop(conn::Connection)
    interval = conn.options.ping_interval
    while true
        sleep(interval)
        status = connection_status(conn)
        status == CLOSED && return nothing
        status == CONNECTED || continue
        if increment_pings_out!(conn) > conn.options.max_pings_out
            handle_stale_connection!(conn)
            continue
        end
        try
            send_frame(conn, ping_frame(); operation = "ping")
        catch err
            connection_status(conn) == CLOSED && return nothing
            if err isa ConnectionClosedError || err isa ConnectionReconnectingError || err isa ConnectionDrainingError
                continue
            end
        end
    end
end

function init_retrying_connection(servers::Vector{ServerURL}, options::Options, initial_error)
    conn = new_connection(
        first(servers),
        servers,
        options,
        nothing,
        ServerInfo(headers = true),
        RECONNECTING;
        connected_once = false,
    )
    notify_error!(conn, initial_error)
    notify_reconnect_error!(conn, initial_error)
    start_ping_loop!(conn)
    conn.reconnect_task = errormonitor(@async reconnect_loop(conn, initial_error))
    return conn
end

function init_connection(url::ServerURL, options::Options)
    servers = server_pool(url, options)
    server, io, info = try
        first_successful_connection(servers, options)
    catch err
        options.retry_on_failed_connect || rethrow()
        return init_retrying_connection(servers, options, err)
    end
    try
        conn = new_connection(server, servers, options, io, info, CONNECTING; connected_once = true)
        apply_discovered_servers!(conn, server, info; notify = false)
        set_connection_status!(conn, CONNECTED)
        conn.reader_task = errormonitor(@async reader_loop(conn))
        start_ping_loop!(conn)
        notify_connected!(conn)
        return conn
    catch
        try close(io) catch end
        rethrow()
    end
end

function connect(url::AbstractString = DEFAULT_URL; kwargs...)
    options = validate_options(Options(; kwargs...))
    return init_connection(parse_server_url(url), options)
end

function close(conn::Connection)
    should_notify = false
    subs = Subscription[]
    err = ConnectionClosedError("connection is closed")
    lock(conn.lock)
    try
        conn.status == CLOSED && return nothing
        set_connection_status_locked!(conn, CLOSED)
        should_notify = !conn.options.no_callbacks_after_client_close
        subs = collect(values(conn.subscriptions))
        empty!(conn.subscriptions)
        empty!(conn.pending_frames)
        conn.pending_bytes = 0
        empty!(conn.subscription_cleanups)
        abort_pub_ack_futures_locked!(conn, err)
        for ch in values(conn.request_map)
            close(ch)
        end
        empty!(conn.request_map)
        conn.request_sub = nothing
        conn.request_prefix = nothing
    finally
        unlock(conn.lock)
    end
    for sub in subs
        close_subscription_local!(conn, sub; err)
    end
    try close(conn.io) catch end
    should_notify && notify_closed!(conn)
    return nothing
end

function close_subscriptions!(conn::Connection; err::Exception = ConnectionClosedError("connection is closed"))
    subs = Subscription[]
    lock(conn.lock)
    try
        subs = collect(values(conn.subscriptions))
        empty!(conn.subscriptions)
        empty!(conn.subscription_cleanups)
        abort_pub_ack_futures_locked!(conn, err)
        for ch in values(conn.request_map)
            close(ch)
        end
        empty!(conn.request_map)
        conn.request_sub = nothing
        conn.request_prefix = nothing
    finally
        unlock(conn.lock)
    end
    for sub in subs
        close_subscription_local!(conn, sub; err)
    end
    return nothing
end

function reader_loop(conn::Connection)
    try
        while conn.status != CLOSED
            msg = read_protocol_message(conn.io)
            handle_protocol_message(conn, msg)
        end
    catch err
        conn.status == CLOSED && return nothing
        conn.status == RECONNECTING && return nothing
        if conn.options.allow_reconnect && conn.status != DRAINING
            start_reconnect!(conn, err)
        else
            notify_error!(conn, err)
            lock(conn.lock)
            try
                set_connection_status_locked!(conn, CLOSED)
                empty!(conn.pending_frames)
                conn.pending_bytes = 0
            finally
                unlock(conn.lock)
            end
            close_subscriptions!(conn; err)
            notify_closed!(conn)
        end
    end
    return nothing
end

function force_reconnect(conn::Connection; timeout::Union{Nothing, Real} = conn.options.request_timeout)
    status = connection_status(conn)
    status == CLOSED && throw(ConnectionClosedError("connection is closed"))
    status == DRAINING && throw(ConnectionDrainingError())
    status == RECONNECTING || start_reconnect!(conn, ErrorException("force reconnect requested"))
    wait_connected(conn; timeout, operation = "force_reconnect")
    return nothing
end

function start_reconnect!(conn::Connection, err)
    should_start = false
    lock(conn.lock)
    try
        if conn.status == CONNECTED || conn.status == CONNECTING
            set_connection_status_locked!(conn, RECONNECTING)
            for sub in values(conn.subscriptions)
                lock(sub.lock)
                try
                    sub.server_sent = false
                finally
                    unlock(sub.lock)
                end
            end
            abort_pub_ack_futures_locked!(conn, ConnectionReconnectingError())
            should_start = true
        end
    finally
        unlock(conn.lock)
    end
    notify_error!(conn, err)
    should_start && notify_disconnected!(conn, err)
    should_start && record_auth_error!(conn, conn.url, err)
    while isready(conn.pongs)
        take!(conn.pongs)
    end
    reset_pings_out!(conn)
    try close(conn.io) catch end
    if should_start
        conn.reconnect_task = errormonitor(@async reconnect_loop(conn, err))
    end
    return nothing
end

function reconnect_servers(conn::Connection)
    lock(conn.lock)
    try
        current = conn.url
        rest = [server for server in conn.servers if server_key(server) != server_key(current)]
        current_in_pool = any(server -> server_key(server) == server_key(current), conn.servers)
        current_in_pool || return rest
        return isempty(rest) ? copy(conn.servers) : vcat(rest, current)
    finally
        unlock(conn.lock)
    end
end

function reconnect_uses_tls_policy(options::Options, current::ServerURL, servers::Vector{ServerURL})
    options.tls_required && return true
    options.tls_handshake_first && return true
    is_tls_scheme(current) && return true
    return any(is_tls_scheme, servers)
end

function reconnect_delay_seconds(options::Options, current::ServerURL, servers::Vector{ServerURL}, whole_list_attempts::Integer, rng::AbstractRNG)
    if options.custom_reconnect_delay_cb !== nothing
        delay = options.custom_reconnect_delay_cb(whole_list_attempts)
        delay === nothing && return 0.0
        delay_s = Float64(delay)
        delay_s < 0 && throw(ArgumentError("custom reconnect delay must be nonnegative"))
        return delay_s
    end
    wait = options.reconnect_wait
    jitter = reconnect_uses_tls_policy(options, current, servers) ? options.reconnect_jitter_tls : options.reconnect_jitter
    jitter > 0 && (wait += rand(rng) * jitter)
    return wait
end

function reconnect_delay_seconds(conn::Connection, whole_list_attempts::Integer)
    lock(conn.lock)
    try
        return reconnect_delay_seconds(conn.options, conn.url, copy(conn.servers), whole_list_attempts, conn.rng)
    finally
        unlock(conn.lock)
    end
end

function normalize_reconnect_to_server_result(result)
    result === nothing && return nothing, 0.0
    selected = result
    delay = 0.0
    if result isa Tuple
        length(result) == 2 || throw(ArgumentError("reconnect_to_server_cb must return a server, nothing, or (server, delay_seconds)"))
        selected, delay = result
    elseif result isa Pair
        selected = result.first
        delay = result.second
    end
    delay === nothing && (delay = 0.0)
    delay_s = Float64(delay)
    delay_s < 0 && throw(ArgumentError("reconnect_to_server_cb delay must be nonnegative"))
    return selected, delay_s
end

function reconnect_selection_url(selection)
    selection === nothing && return nothing
    selection isa ServerURL && return selection
    selection isa AbstractString && return parse_server_url(selection)
    throw(ArgumentError("reconnect_to_server_cb server must be a NATS server URL string, ServerURL, or nothing"))
end

function matching_pool_server(selection::ServerURL, servers::Vector{ServerURL})
    key = server_key(selection)
    for server in servers
        server_key(server) == key && return server
    end
    return nothing
end

function reconnect_to_server_selection(conn::Connection, servers::Vector{ServerURL})
    cb = conn.options.reconnect_to_server_cb
    cb === nothing && return nothing, 0.0, false
    isempty(servers) && return nothing, 0.0, false

    info = lock(conn.lock) do
        conn.info
    end
    snapshot = [server.raw for server in servers]
    try
        selected, delay = normalize_reconnect_to_server_result(cb(snapshot, info))
        selected_url = reconnect_selection_url(selected)
        selected_url === nothing && return nothing, 0.0, false
        server = matching_pool_server(selected_url, servers)
        if server === nothing
            notify_reconnect_error!(conn, ServerNotInPoolError(selected_url.raw))
            return nothing, 0.0, false
        end
        return server, delay, true
    catch err
        notify_error!(conn, err)
        notify_reconnect_error!(conn, err)
        return nothing, 0.0, false
    end
end

function active_subscriptions(conn::Connection)
    lock(conn.lock)
    try
        return [sub for sub in values(conn.subscriptions) if !sub.closed]
    finally
        unlock(conn.lock)
    end
end

function send_subscription_status_locked!(sub::Subscription, status::SubscriptionStatus)
    for (ch, statuses) in collect(sub.status_listeners)
        if status in statuses && isopen(ch) && Base.n_avail(ch) < SUBSCRIPTION_STATUS_CHANNEL_SIZE
            try
                put!(ch, status)
            catch
            end
        end
        if status == SUBSCRIPTION_CLOSED || !isopen(ch)
            try close(ch) catch end
            delete!(sub.status_listeners, ch)
        end
    end
    return nothing
end

function set_subscription_status_locked!(sub::Subscription, status::SubscriptionStatus)
    sub.status == status && return nothing
    sub.status = status
    send_subscription_status_locked!(sub, status)
    return nothing
end

function set_subscription_status!(sub::Subscription, status::SubscriptionStatus)
    lock(sub.lock)
    try
        set_subscription_status_locked!(sub, status)
    finally
        unlock(sub.lock)
    end
    return nothing
end

function mark_subscription_sent!(sub::Subscription)
    lock(sub.lock)
    try
        sub.closed && return false
        sub.server_sent = true
        return true
    finally
        unlock(sub.lock)
    end
end

function subscription_sent(sub::Subscription)
    lock(sub.lock)
    try
        return sub.server_sent
    finally
        unlock(sub.lock)
    end
end

function reserve_subscription_send!(sub::Subscription)
    lock(sub.lock)
    try
        (sub.closed || sub.server_sent) && return false
        sub.server_sent = true
        return true
    finally
        unlock(sub.lock)
    end
end

function mark_subscription_unsent!(sub::Subscription)
    lock(sub.lock)
    try
        sub.closed || (sub.server_sent = false)
        return nothing
    finally
        unlock(sub.lock)
    end
end

function write_subscription_frames(conn::Connection, sub::Subscription; timeout::Union{Nothing, Real} = nothing)
    send_frame(conn, sub_frame(sub.subject, sub.queue, sub.sid); timeout, operation = "subscribe")
    if sub.max_msgs !== nothing
        remaining = sub.max_msgs::Int - sub.delivered
        remaining > 0 && send_frame(conn, unsub_frame(sub.sid; max_msgs = remaining); timeout, operation = "subscribe")
    end
    return nothing
end

function send_subscription_if_needed!(conn::Connection, sub::Subscription)
    reserve_subscription_send!(sub) || return nothing
    try
        write_subscription_frames(conn, sub; timeout = conn.options.request_timeout)
    catch
        mark_subscription_unsent!(sub)
        rethrow()
    end
    return nothing
end

function schedule_subscription_send!(conn::Connection, sub::Subscription)
    errormonitor(@async begin
        try
            wait_connected(conn; operation = "subscribe")
            send_subscription_if_needed!(conn, sub)
        catch err
            connection_status(conn) == CLOSED || notify_error!(conn, err)
        end
    end)
    return nothing
end

function resend_subscriptions(io, subs::Vector{Subscription})
    for sub in subs
        sub.closed && continue
        subscription_sent(sub) && continue
        write_frame(io, sub_frame(sub.subject, sub.queue, sub.sid))
        if sub.max_msgs !== nothing
            remaining = sub.max_msgs::Int - sub.delivered
            remaining > 0 && write_frame(io, unsub_frame(sub.sid; max_msgs = remaining))
        end
        mark_subscription_sent!(sub)
    end
    return nothing
end

function mark_reconnect_failed!(conn::Connection, attempts::Int, last_error)
    err = ReconnectFailedError(attempts, last_error)
    notify_error!(conn, err)
    lock(conn.lock)
    try
        set_connection_status_locked!(conn, CLOSED)
        conn.reconnect_task = nothing
        empty!(conn.pending_frames)
        conn.pending_bytes = 0
    finally
        unlock(conn.lock)
    end
    close_subscriptions!(conn; err)
    try close(conn.io) catch end
    notify_closed!(conn)
    return nothing
end

function abort_reconnect_after_auth_error!(conn::Connection, err)
    notify_error!(conn, err)
    lock(conn.lock)
    try
        set_connection_status_locked!(conn, CLOSED)
        conn.reconnect_task = nothing
        empty!(conn.pending_frames)
        conn.pending_bytes = 0
    finally
        unlock(conn.lock)
    end
    close_subscriptions!(conn; err)
    try close(conn.io) catch end
    notify_closed!(conn)
    return nothing
end

function reconnect_attempt!(conn::Connection, server::ServerURL)
    io = nothing
    try
        io, info = establish_connection(server, conn.options)
        subs = active_subscriptions(conn)
        first_connect = false
        lock(conn.write_lock)
        try
            lock(conn.lock)
            try
                conn.url = server
                conn.io = io
                conn.info = info
                conn.pings_out = 0
            finally
                unlock(conn.lock)
            end
            apply_discovered_servers!(conn, server, info; notify = true)
            resend_subscriptions(io, subs)
            while true
                pending = take_pending_frames!(conn)
                for frame in pending
                    write_frame(io, frame)
                end
                lock(conn.lock)
                try
                    if isempty(conn.pending_frames)
                        first_connect = !conn.connected_once
                        first_connect || (conn.reconnects += UInt64(1))
                        conn.connected_once = true
                        set_connection_status_locked!(conn, CONNECTED)
                        conn.reconnect_task = nothing
                        conn.reader_task = errormonitor(@async reader_loop(conn))
                        break
                    end
                finally
                    unlock(conn.lock)
                end
            end
        finally
            unlock(conn.write_lock)
        end
        clear_auth_error!(conn, server)
        first_connect ? notify_connected!(conn) : notify_reconnected!(conn)
        return true, nothing
    catch err
        io === nothing || try close(io) catch end
        return false, err
    end
end

function record_reconnect_error!(conn::Connection, err)
    notify_error!(conn, err)
    notify_reconnect_error!(conn, err)
    return nothing
end

function reconnect_loop(conn::Connection, initial_error = nothing)
    attempts = 0
    whole_list_attempts = 0
    last_error = initial_error
    while true
        connection_status(conn) == CLOSED && return nothing
        if conn.options.max_reconnect >= 0 && attempts >= conn.options.max_reconnect
            mark_reconnect_failed!(conn, attempts, last_error)
            return nothing
        end
        servers = reconnect_servers(conn)
        selected, selected_delay, selected_by_callback = reconnect_to_server_selection(conn, servers)
        if selected_by_callback
            connection_status(conn) == CLOSED && return nothing
            if conn.options.max_reconnect >= 0 && attempts >= conn.options.max_reconnect
                mark_reconnect_failed!(conn, attempts, last_error)
                return nothing
            end
            selected_delay > 0 ? sleep(selected_delay) : yield()
            attempts += 1
            ok, err = reconnect_attempt!(conn, selected)
            ok && return nothing
            last_error = err
            abort_auth = record_auth_error!(conn, selected, err)
            if abort_auth
                notify_reconnect_error!(conn, err)
                abort_reconnect_after_auth_error!(conn, err)
                return nothing
            end
            record_reconnect_error!(conn, err)
            continue
        end

        for server in servers
            connection_status(conn) == CLOSED && return nothing
            if conn.options.max_reconnect >= 0 && attempts >= conn.options.max_reconnect
                break
            end
            attempts += 1
            ok, err = reconnect_attempt!(conn, server)
            if ok
                return nothing
            else
                last_error = err
                abort_auth = record_auth_error!(conn, server, err)
                if abort_auth
                    notify_reconnect_error!(conn, err)
                    abort_reconnect_after_auth_error!(conn, err)
                    return nothing
                end
                record_reconnect_error!(conn, err)
            end
        end
        if conn.options.max_reconnect >= 0 && attempts >= conn.options.max_reconnect
            mark_reconnect_failed!(conn, attempts, last_error)
            return nothing
        end
        whole_list_attempts += 1
        wait = try
            reconnect_delay_seconds(conn, whole_list_attempts)
        catch err
            last_error = err
            notify_error!(conn, err)
            notify_reconnect_error!(conn, err)
            conn.options.reconnect_wait
        end
        sleep(wait)
    end
end

function handle_protocol_message(conn::Connection, ::Ok)
    return nothing
end

function handle_protocol_message(conn::Connection, ::Ping)
    send_frame(conn, pong_frame())
    return nothing
end

function handle_protocol_message(conn::Connection, ::Pong)
    reset_pings_out!(conn)
    put!(conn.pongs, nothing)
    return nothing
end

message_pending_bytes(msg::Msg) = length(msg.data)
message_header_bytes(msg::Msg) = isempty(msg.headers) && msg.status == 200 ? 0 : length(headers_bytes(msg.headers))
message_wire_bytes(msg::Msg) = length(msg.data) + message_header_bytes(msg)

function account_received_message!(conn::Connection, msg::Msg)
    lock(conn.lock)
    try
        conn.in_msgs += UInt64(1)
        conn.in_bytes += UInt64(message_wire_bytes(msg))
    finally
        unlock(conn.lock)
    end
    return nothing
end

function account_published_message!(conn::Connection, payload_bytes::Integer, header_bytes::Integer)
    lock(conn.lock)
    try
        conn.out_msgs += UInt64(1)
        conn.out_bytes += UInt64(payload_bytes + header_bytes)
    finally
        unlock(conn.lock)
    end
    return nothing
end

struct SlowConsumerDrop end
const SLOW_CONSUMER_DROP = SlowConsumerDrop()

function slow_consumer_error(sub::Subscription, pending_msgs::Int, pending_bytes::Int)
    msg_limit = sub.pending_msg_limit
    effective_msg_limit = msg_limit > 0 ? min(sub.capacity, msg_limit) : sub.capacity
    return SlowConsumerError(
        sub.subject,
        sub.sid,
        pending_msgs,
        effective_msg_limit,
        sub.dropped,
        pending_bytes,
        sub.pending_bytes_limit,
    )
end

pending_count_locked(sub::Subscription) = Base.n_avail(sub.channel) + sub.processing

function account_message_enqueue!(sub::Subscription, msg::Msg)
    msg_bytes = message_pending_bytes(msg)
    lock(sub.lock)
    try
        sub.closed && return nothing
        pending_msgs = pending_count_locked(sub)
        pending_after = pending_msgs + 1
        pending_bytes_after = sub.pending_bytes + msg_bytes
        msg_limit = sub.pending_msg_limit
        bytes_limit = sub.pending_bytes_limit
        count_limited = pending_msgs >= sub.capacity || (msg_limit > 0 && pending_after > msg_limit)
        bytes_limited = bytes_limit > 0 && pending_bytes_after > bytes_limit
        if count_limited || bytes_limited
            already_slow = sub.status == SUBSCRIPTION_SLOW_CONSUMER
            sub.dropped += 1
            set_subscription_status_locked!(sub, SUBSCRIPTION_SLOW_CONSUMER)
            return already_slow ? SLOW_CONSUMER_DROP : slow_consumer_error(sub, pending_after, pending_bytes_after)
        end
        sub.pending_bytes = pending_bytes_after
        sub.max_pending = max(sub.max_pending, pending_after)
        sub.max_pending_bytes = max(sub.max_pending_bytes, pending_bytes_after)
        sub.status == SUBSCRIPTION_SLOW_CONSUMER && set_subscription_status_locked!(sub, SUBSCRIPTION_ACTIVE)
        return nothing
    finally
        unlock(sub.lock)
    end
end

function unaccount_message_enqueue!(sub::Subscription, msg::Msg)
    msg_bytes = message_pending_bytes(msg)
    lock(sub.lock)
    try
        sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
    finally
        unlock(sub.lock)
    end
    return nothing
end

function maybe_close_after_max_delivery!(sub::Subscription, max_msgs::Union{Nothing, Int})
    max_msgs === nothing && return nothing
    close_subscription_local!(sub.connection, sub; err = MaxMessagesError(max_msgs))
    return nothing
end

function mark_message_taken!(sub::Subscription, msg::Msg; close_on_max::Bool = true)
    msg_bytes = message_pending_bytes(msg)
    max_to_close = nothing
    over_max = nothing
    lock(sub.lock)
    try
        sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
        sub.delivered += 1
        if sub.max_msgs !== nothing
            max_msgs_value = sub.max_msgs::Int
            if sub.delivered > max_msgs_value
                over_max = max_msgs_value
            elseif sub.delivered == max_msgs_value && !sub.closed
                max_to_close = max_msgs_value
            end
        end
        sub.status == SUBSCRIPTION_SLOW_CONSUMER && set_subscription_status_locked!(sub, SUBSCRIPTION_ACTIVE)
    finally
        unlock(sub.lock)
    end
    close_on_max && maybe_close_after_max_delivery!(sub, max_to_close)
    over_max === nothing || throw(MaxMessagesError(over_max))
    return msg
end

function mark_callback_message_started!(sub::Subscription, msg::Msg)
    max_err = nothing
    lock(sub.lock)
    try
        if sub.max_msgs !== nothing && sub.delivered >= sub.max_msgs::Int
            max_err = sub.max_msgs::Int
        else
            sub.processing += 1
            sub.delivered += 1
        end
    finally
        unlock(sub.lock)
    end
    max_err === nothing || throw(MaxMessagesError(max_err))
    return msg
end

function mark_callback_message_finished!(sub::Subscription, msg::Msg)
    msg_bytes = message_pending_bytes(msg)
    max_to_close = nothing
    lock(sub.lock)
    try
        sub.processing = max(0, sub.processing - 1)
        sub.pending_bytes = max(0, sub.pending_bytes - msg_bytes)
        if sub.max_msgs !== nothing && sub.delivered >= sub.max_msgs::Int && !sub.closed
            max_to_close = sub.max_msgs::Int
        end
    finally
        unlock(sub.lock)
    end
    maybe_close_after_max_delivery!(sub, max_to_close)
    return nothing
end

function take_slow_consumer_error!(sub::Subscription)
    lock(sub.lock)
    try
        sub.status == SUBSCRIPTION_SLOW_CONSUMER || return nothing
        pending_msgs = pending_count_locked(sub)
        pending_bytes = sub.pending_bytes
        err = slow_consumer_error(sub, pending_msgs, pending_bytes)
        set_subscription_status_locked!(sub, SUBSCRIPTION_ACTIVE)
        return err
    finally
        unlock(sub.lock)
    end
end

function handle_protocol_message(conn::Connection, err::Err)
    classified = classify_server_error(err.message)
    if server_error_is_transient(classified)
        classified isa PermissionViolationError && route_subscription_permission_error!(conn, classified)
        notify_error!(conn, classified)
    else
        handle_terminal_server_error!(conn, classified)
    end
    return nothing
end

function handle_protocol_message(conn::Connection, info::ServerInfo)
    lock(conn.lock)
    try
        conn.info = info
    finally
        unlock(conn.lock)
    end
    apply_discovered_servers!(conn, conn.url, info; notify = true)
    info.ldm === true && notify_lame_duck!(conn)
    return nothing
end

function handle_protocol_message(conn::Connection, msg::Msg)
    account_received_message!(conn, msg)
    sub = lock(conn.lock) do
        get(conn.subscriptions, msg.sid, nothing)
    end
    sub === nothing && return nothing
    sub.closed && return nothing
    slow_err = account_message_enqueue!(sub, msg)
    if slow_err !== nothing
        slow_err isa SlowConsumerError && notify_error!(conn, slow_err, sub)
        return nothing
    end
    try
        put!(sub.channel, msg)
    catch err
        unaccount_message_enqueue!(sub, msg)
        sub.closed || !isopen(sub.channel) || rethrow()
        return nothing
    end
    return nothing
end

function new_sid(conn::Connection)
    lock(conn.lock)
    try
        conn.next_sid += 1
        return conn.next_sid
    finally
        unlock(conn.lock)
    end
end

function new_inbox(prefix::AbstractString = "_INBOX"; rng::Random.AbstractRNG = Random.default_rng())
    prefix_s = validate_inbox_prefix(prefix)
    return "$(prefix_s).$(randstring(rng, 22))"
end

function new_inbox(conn::Connection)
    suffix = lock(conn.lock) do
        randstring(conn.rng, 22)
    end
    return "$(conn.options.inbox_prefix).$(suffix)"
end

function request_token(conn::Connection)
    return lock(conn.lock) do
        randstring(conn.rng, 22)
    end
end

function request_reply_token(subject::AbstractString, prefix::AbstractString)
    prefix_dot = "$(prefix)."
    startswith(subject, prefix_dot) || return nothing
    return String(subject[nextind(subject, lastindex(prefix_dot)):end])
end

function dispatch_request_reply(conn::Connection, prefix::String, msg::Msg)
    token = request_reply_token(msg.subject, prefix)
    token === nothing && return nothing
    ch = lock(conn.lock) do
        get(conn.request_map, token, nothing)
    end
    ch === nothing && return nothing
    isopen(ch) || return nothing
    isready(ch) && return nothing
    try
        put!(ch, msg)
    catch err
        isopen(ch) && rethrow()
    end
    return nothing
end

function finish_barrier!(barrier::BarrierAction)
    cb = nothing
    lock(barrier.lock)
    try
        barrier.remaining -= 1
        if barrier.remaining == 0
            cb = barrier.callback
            barrier.callback = nothing
        end
    finally
        unlock(barrier.lock)
    end
    cb === nothing || cb()
    return nothing
end

function enqueue_barrier!(sub::Subscription, barrier::BarrierAction)
    @async begin
        try
            put!(sub.channel, barrier)
        catch
            finish_barrier!(barrier)
        end
    end
    return nothing
end

function barrier(conn::Connection, f::Function)
    subs = Subscription[]
    lock(conn.lock)
    try
        conn.status == CLOSED && throw(ConnectionClosedError("connection is closed"))
        subs = [
            sub for sub in values(conn.subscriptions)
            if sub.callback_subscription && !sub.closed
        ]
    finally
        unlock(conn.lock)
    end
    if isempty(subs)
        f()
        return nothing
    end
    action = BarrierAction(length(subs), f, ReentrantLock())
    for sub in subs
        enqueue_barrier!(sub, action)
    end
    return nothing
end

barrier(f::Function, conn::Connection) = barrier(conn, f)

function ensure_request_mux(conn::Connection)
    existing = lock(conn.lock) do
        conn.request_sub !== nothing && !conn.request_sub.closed ? conn.request_prefix::String : nothing
    end
    existing !== nothing && return existing

    prefix = new_inbox(conn)
    sub = subscribe(conn, "$prefix.*")
    sub.task = errormonitor(@async begin
        for item in sub.channel
            item isa Msg || continue
            msg = item::Msg
            mark_message_taken!(sub, msg)
            dispatch_request_reply(conn, prefix, msg)
        end
    end)
    stored = false
    selected_prefix = prefix
    lock(conn.lock)
    try
        if conn.request_sub === nothing || conn.request_sub.closed
            conn.request_sub = sub
            conn.request_prefix = prefix
            stored = true
        else
            selected_prefix = conn.request_prefix::String
        end
    finally
        unlock(conn.lock)
    end
    if !stored
        try unsubscribe(conn, sub) catch end
    end
    return selected_prefix
end

function subscribe(conn::Connection, subject::AbstractString; queue::Union{Nothing, AbstractString} = nothing, channel_size::Int = conn.options.subscription_channel_size)
    subject_s = validate_subscribe_subject(subject)
    queue_s = validate_queue_name(queue)
    channel_size < 0 && throw(ArgumentError("channel_size must be nonnegative"))
    status = connection_status(conn)
    status == CLOSED && throw(ConnectionClosedError("connection is closed"))
    status == DRAINING && throw(ConnectionDrainingError())
    sid = new_sid(conn)
    sub = Subscription(
        conn,
        subject_s,
        queue_s,
        sid,
        Channel{Any}(channel_size),
        channel_size,
        ReentrantLock(),
        nothing,
        false,
        0,
        false,
        nothing,
        false,
        SUBSCRIPTION_ACTIVE,
        Dict{Channel{SubscriptionStatus}, Vector{SubscriptionStatus}}(),
        nothing,
        nothing,
        0,
        0,
        0,
        0,
        0,
        channel_size,
        conn.options.subscription_pending_bytes_limit,
    )
    lock(conn.lock)
    try
        conn.subscriptions[sid] = sub
    finally
        unlock(conn.lock)
    end
    if connection_status(conn) == RECONNECTING
        schedule_subscription_send!(conn, sub)
        return sub
    end
    try
        send_subscription_if_needed!(conn, sub)
    catch
        lock(conn.lock)
        try
            delete!(conn.subscriptions, sid)
        finally
            unlock(conn.lock)
        end
        sub.closed = true
        close(sub.channel)
        rethrow()
    end
    return sub
end

function subscribe(callback::Function, conn::Connection, subject::AbstractString; kwargs...)
    sub = subscribe(conn, subject; kwargs...)
    lock(sub.lock)
    try
        sub.callback_subscription = true
    finally
        unlock(sub.lock)
    end
    sub.task = errormonitor(@async begin
        for item in sub.channel
            if item isa BarrierAction
                finish_barrier!(item)
                continue
            end
            item isa Msg || continue
            msg = try
                mark_callback_message_started!(sub, item::Msg)
            catch err
                err isa MaxMessagesError || notify_error!(conn, err, sub)
                continue
            end
            try
                callback(msg)
            catch err
                notify_error!(conn, err, sub)
            finally
                mark_callback_message_finished!(sub, msg)
            end
        end
    end)
    return sub
end

function ensure_subscription_open_locked(sub::Subscription)
    sub.closed && throw(BadSubscriptionError())
    return nothing
end

function pending(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return pending_count_locked(sub)
    finally
        unlock(sub.lock)
    end
end

function subscription_status(sub::Subscription)
    lock(sub.lock)
    try
        return sub.status
    finally
        unlock(sub.lock)
    end
end

is_valid(sub::Subscription) = subscription_status(sub) != SUBSCRIPTION_CLOSED
is_draining(sub::Subscription) = subscription_status(sub) == SUBSCRIPTION_DRAINING

function status_changed(sub::Subscription, statuses::SubscriptionStatus...)
    wanted = isempty(statuses) ?
             [SUBSCRIPTION_ACTIVE, SUBSCRIPTION_DRAINING, SUBSCRIPTION_CLOSED, SUBSCRIPTION_SLOW_CONSUMER] :
             collect(statuses)
    ch = Channel{SubscriptionStatus}(SUBSCRIPTION_STATUS_CHANNEL_SIZE)
    lock(sub.lock)
    try
        if sub.status in wanted
            put!(ch, sub.status)
        end
        if sub.status == SUBSCRIPTION_CLOSED
            close(ch)
        else
            sub.status_listeners[ch] = wanted
        end
    finally
        unlock(sub.lock)
    end
    return ch
end

function set_closed_handler!(sub::Subscription, cb::Union{Nothing, Function})
    lock(sub.lock)
    try
        sub.closed_cb = cb
    finally
        unlock(sub.lock)
    end
    return sub
end

function closed_handler(sub::Subscription)
    lock(sub.lock)
    try
        return sub.closed_cb
    finally
        unlock(sub.lock)
    end
end

function delivered(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.delivered
    finally
        unlock(sub.lock)
    end
end

function max_pending(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.max_pending
    finally
        unlock(sub.lock)
    end
end

function pending_bytes(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.pending_bytes
    finally
        unlock(sub.lock)
    end
end

function max_pending_bytes(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.max_pending_bytes
    finally
        unlock(sub.lock)
    end
end

function pending_limits(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.pending_msg_limit, sub.pending_bytes_limit
    finally
        unlock(sub.lock)
    end
end

function set_pending_limits(sub::Subscription, msg_limit::Integer, bytes_limit::Integer)
    msg_limit == 0 && throw(ArgumentError("message pending limit must be nonzero"))
    bytes_limit == 0 && throw(ArgumentError("byte pending limit must be nonzero"))
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        sub.pending_msg_limit = Int(msg_limit)
        sub.pending_bytes_limit = Int(bytes_limit)
    finally
        unlock(sub.lock)
    end
    return nothing
end

function clear_max_pending!(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        sub.max_pending = 0
        sub.max_pending_bytes = 0
    finally
        unlock(sub.lock)
    end
    return nothing
end

function dropped(sub::Subscription)
    lock(sub.lock)
    try
        ensure_subscription_open_locked(sub)
        return sub.dropped
    finally
        unlock(sub.lock)
    end
end

function close_subscription_local!(conn::Connection, sub::Subscription; err::Union{Nothing, Exception} = nothing)
    should_notify = false
    lock(conn.lock)
    try
        if get(conn.subscriptions, sub.sid, nothing) === sub
            delete!(conn.subscriptions, sub.sid)
        end
    finally
        unlock(conn.lock)
    end
    lock(sub.lock)
    try
        if !sub.closed
            sub.closed = true
            sub.close_error = err
            set_subscription_status_locked!(sub, SUBSCRIPTION_CLOSED)
            should_notify = true
        end
    finally
        unlock(sub.lock)
    end
    isopen(sub.channel) && close(sub.channel)
    should_notify && notify_subscription_closed!(sub)
    return nothing
end

function subscription_closed_error(sub::Subscription)
    lock(sub.lock)
    try
        sub.close_error === nothing && return BadSubscriptionError()
        return sub.close_error::Exception
    finally
        unlock(sub.lock)
    end
end

function wait_subscription_task(sub::Subscription, timeout::Real, operation::AbstractString)
    task = sub.task
    task === nothing && return nothing
    task === Base.current_task() && return nothing
    if timeout <= 0
        istaskdone(task) || throw(ConnectionTimeoutError(String(operation), 0.0))
    else
        result = timedwait(() -> istaskdone(task), timeout; pollint = 0.001)
        result == :ok || throw(ConnectionTimeoutError(String(operation), Float64(timeout)))
    end
    return nothing
end

remaining_until(deadline::Real) = max(0.0, deadline - time())

function unsubscribe(conn::Connection, sub::Subscription; max_msgs::Union{Nothing, Integer} = nothing)
    max = max_msgs === nothing ? nothing : Int(max_msgs)
    max !== nothing && max < 0 && throw(ArgumentError("max_msgs must be nonnegative"))
    is_closed(conn) && throw(ConnectionClosedError("connection is closed"))
    immediate = max === nothing || max == 0
    sub_was_closed = false
    lock(sub.lock)
    try
        if sub.closed
            sub_was_closed = true
        elseif max !== nothing && max > 0
            if sub.delivered >= max
                immediate = true
            else
                sub.max_msgs = max
            end
        end
    finally
        unlock(sub.lock)
    end
    if sub_was_closed
        is_closed(conn) && throw(ConnectionClosedError("connection is closed"))
        throw(BadSubscriptionError())
    end
    frame = immediate ? unsub_frame(sub.sid) : unsub_frame(sub.sid; max_msgs = max)
    send_frame(conn, frame, timeout = conn.options.request_timeout, operation = "unsubscribe")
    if immediate
        close_err = max !== nothing && max > 0 ? MaxMessagesError(max) : nothing
        close_subscription_local!(conn, sub; err = close_err)
        run_subscription_cleanup!(conn, sub, conn.options.request_timeout; throw_errors = true)
    end
    return nothing
end

unsubscribe(sub::Subscription; kwargs...) = unsubscribe(sub.connection, sub; kwargs...)

function auto_unsubscribe(conn::Connection, sub::Subscription, max_msgs::Integer)
    lock(sub.lock)
    try
        sub.closed && throw(BadSubscriptionError())
    finally
        unlock(sub.lock)
    end
    unsubscribe(conn, sub; max_msgs)
    return nothing
end

auto_unsubscribe(sub::Subscription, max_msgs::Integer) = auto_unsubscribe(sub.connection, sub, max_msgs)

function drain(conn::Connection, sub::Subscription; timeout::Real = conn.options.drain_timeout)
    try
        sub.closed && throw(BadSubscriptionError())
        deadline = time() + Float64(timeout)
        set_subscription_status!(sub, SUBSCRIPTION_DRAINING)
        send_frame(conn, unsub_frame(sub.sid), timeout = timeout, operation = "drain subscription")
        flush(conn; timeout = remaining_until(deadline))
        close_subscription_local!(conn, sub)
        wait_subscription_task(sub, remaining_until(deadline), "drain subscription")
        run_subscription_cleanup!(conn, sub, remaining_until(deadline); throw_errors = false)
    catch err
        if err isa ConnectionTimeoutError
            drain_err = drain_timeout_error(timeout)
            notify_error!(conn, drain_err)
            throw(drain_err)
        end
        rethrow()
    end
    return nothing
end

drain(sub::Subscription; kwargs...) = drain(sub.connection, sub; kwargs...)

function drain_timeout_error(timeout::Real)
    return DrainTimeoutError(Float64(timeout))
end

function drain_subscription_group!(conn::Connection, subs::Vector{Subscription}, deadline::Real, operation::AbstractString)
    active = [sub for sub in subs if !sub.closed]
    isempty(active) && return nothing
    for sub in active
        send_frame(conn, unsub_frame(sub.sid), timeout = remaining_until(deadline), operation = operation, allow_draining = true)
    end
    flush(conn; timeout = remaining_until(deadline), allow_draining = true)
    for sub in active
        close_subscription_local!(conn, sub)
    end
    for sub in active
        wait_subscription_task(sub, remaining_until(deadline), operation)
    end
    return nothing
end

function handle_drain_timeout!(conn::Connection, timeout::Real)
    err = drain_timeout_error(timeout)
    notify_error!(conn, err)
    close(conn)
    return err
end

is_no_responders_msg(msg::Msg) = isempty(msg.data) && msg.status == 503

function check_next_msg_response(sub::Subscription, msg::Msg)
    taken = mark_message_taken!(sub, msg)
    is_no_responders_msg(taken) && throw(NoRespondersError(taken.subject))
    return taken
end

function validate_next_msg_subscription(sub::Subscription)
    lock(sub.lock)
    try
        !sub.closed && sub.task !== nothing && throw(SyncSubscriptionRequiredError())
    finally
        unlock(sub.lock)
    end
    return nothing
end

function next_msg(sub::Subscription; timeout::Union{Nothing, Real} = nothing)
    validate_next_msg_subscription(sub)
    slow_err = take_slow_consumer_error!(sub)
    slow_err === nothing || throw(slow_err)
    if timeout === nothing
        try
            return check_next_msg_response(sub, take!(sub.channel))
        catch err
            if !isopen(sub.channel) && !isready(sub.channel)
                throw(subscription_closed_error(sub))
            end
            rethrow()
        end
    end
    result = timedwait(
        () -> isready(sub.channel) || !isopen(sub.channel) || subscription_status(sub) == SUBSCRIPTION_SLOW_CONSUMER,
        timeout;
        pollint = 0.001,
    )
    result == :ok || throw(ConnectionTimeoutError("next_msg", Float64(timeout)))
    slow_err = take_slow_consumer_error!(sub)
    slow_err === nothing || throw(slow_err)
    isready(sub.channel) || throw(subscription_closed_error(sub))
    return check_next_msg_response(sub, take!(sub.channel))
end

headers_supported(conn::Connection) = conn.options.headers && conn.info.headers === true

function ensure_headers_supported(conn::Connection, headers::Vector{Pair{String,String}})
    !isempty(headers) && !headers_supported(conn) && throw(HeadersNotSupportedError())
    return nothing
end

function publish(conn::Connection, subject::AbstractString, data = nothing; reply::Union{Nothing, AbstractString} = nothing, headers::Vector{Pair{String,String}} = Pair{String,String}[])
    subject_s = validate_publish_subject(subject; skip = conn.options.skip_subject_validation)
    reply_s = reply === nothing ? nothing : (conn.options.skip_subject_validation ? String(reply) : validate_publish_subject(reply))
    ensure_headers_supported(conn, headers)
    payload = bytes_payload(data)
    payload_size = length(payload)
    header_size = length(headers_bytes(headers))
    message_size = payload_size + header_size
    if conn.info.max_payload > 0 && message_size > conn.info.max_payload
        throw(MaxPayloadError(conn.info.max_payload, message_size))
    end
    frame = pub_frame(subject_s, payload; reply = reply_s, headers)
    send_frame(conn, frame, allow_draining = true, buffer_on_reconnect = true)
    account_published_message!(conn, payload_size, header_size)
    return nothing
end

function publish_request(conn::Connection, subject::AbstractString, reply::AbstractString, data = nothing; headers::Vector{Pair{String,String}} = Pair{String,String}[])
    publish(conn, subject, data; reply, headers)
    return nothing
end

function new_msg(subject::AbstractString, data = nothing; reply::Union{Nothing, AbstractString} = nothing, headers::Vector{Pair{String,String}} = Pair{String,String}[])
    return Msg(String(subject), 0, reply === nothing ? nothing : String(reply), headers, bytes_payload(data), 200, "")
end

publish_msg(conn::Connection, msg::Msg) =
    publish(conn, msg.subject, msg.data; reply = msg.reply, headers = msg.headers)

publish_msg(::Connection, ::Nothing) = throw(InvalidMsgError())

request_msg(conn::Connection, msg::Msg; timeout::Real = conn.options.request_timeout, mux::Bool = true) =
    request(conn, msg.subject, msg.data; timeout, headers = msg.headers, mux)

request_msg(::Connection, ::Nothing; kwargs...) = throw(InvalidMsgError())

function reply_or_error(msg::Msg)
    if msg.reply === nothing || isempty(msg.reply::String)
        throw(MsgNoReplyError())
    end
    return msg.reply::String
end

function respond(conn::Connection, msg::Msg, data = nothing; headers::Vector{Pair{String,String}} = Pair{String,String}[])
    publish(conn, reply_or_error(msg), data; headers)
    return nothing
end

function respond_msg(conn::Connection, request::Msg, response::Msg)
    publish(conn, reply_or_error(request), response.data; reply = response.reply, headers = response.headers)
    return nothing
end

function flush_terminal_status(conn::Connection, allow_draining::Bool)
    status = connection_status(conn)
    status == CONNECTED && return nothing
    allow_draining && status == DRAINING && return nothing
    return status
end

function throw_flush_status(status)
    status == CLOSED && throw(ConnectionClosedError("connection is closed"))
    status == RECONNECTING && throw(ConnectionReconnectingError())
    status == DRAINING && throw(ConnectionDrainingError())
    throw(ConnectionClosedError("connection is not connected"))
end

function flush(conn::Connection; timeout::Real = conn.options.request_timeout, allow_draining::Bool = false)
    lock(conn.flush_lock)
    try
        while isready(conn.pongs)
            take!(conn.pongs)
        end
        send_frame(conn, ping_frame(), timeout = timeout, operation = "flush", allow_draining = allow_draining)
        result = timedwait(() -> isready(conn.pongs) || flush_terminal_status(conn, allow_draining) !== nothing, timeout; pollint = 0.001)
        result == :ok || throw(ConnectionTimeoutError("flush", Float64(timeout)))
        isready(conn.pongs) && (take!(conn.pongs); return nothing)
        throw_flush_status(flush_terminal_status(conn, allow_draining))
    finally
        unlock(conn.flush_lock)
    end
    return nothing
end

function check_request_response(msg::Msg, subject::AbstractString)
    if is_no_responders_msg(msg)
        throw(NoRespondersError(String(subject)))
    elseif msg.status >= 400
        throw(ServerError("$(msg.status) $(msg.description)"))
    end
    return msg
end

function request_exact(conn::Connection, subject::AbstractString, data = nothing; timeout::Real = conn.options.request_timeout, headers::Vector{Pair{String,String}} = Pair{String,String}[])
    subject_s = validate_publish_subject(subject; skip = conn.options.skip_subject_validation)
    ensure_headers_supported(conn, headers)
    inbox = new_inbox(conn)
    sub = subscribe(conn, inbox; channel_size = 1)
    try
        unsubscribe(conn, sub; max_msgs = 1)
        publish(conn, subject_s, data; reply = inbox, headers)
        return check_request_response(next_msg(sub; timeout), subject_s)
    finally
        try
            unsubscribe(conn, sub)
        catch
        end
    end
end

function request(conn::Connection, subject::AbstractString, data = nothing; timeout::Real = conn.options.request_timeout, headers::Vector{Pair{String,String}} = Pair{String,String}[], mux::Bool = true)
    subject_s = validate_publish_subject(subject; skip = conn.options.skip_subject_validation)
    ensure_headers_supported(conn, headers)
    mux || return request_exact(conn, subject_s, data; timeout, headers)
    prefix = ensure_request_mux(conn)
    token = request_token(conn)
    inbox = "$prefix.$token"
    ch = Channel{Msg}(1)
    lock(conn.lock)
    try
        conn.request_map[token] = ch
    finally
        unlock(conn.lock)
    end
    try
        publish(conn, subject_s, data; reply = inbox, headers)
        result = timedwait(() -> isready(ch) || !isopen(ch), timeout; pollint = 0.001)
        result == :ok || throw(ConnectionTimeoutError("request", Float64(timeout)))
        isready(ch) || throw(ConnectionClosedError("request inbox closed"))
        msg = take!(ch)
        return check_request_response(msg, subject_s)
    finally
        lock(conn.lock)
        try
            delete!(conn.request_map, token)
        finally
            unlock(conn.lock)
        end
        close(ch)
    end
end

function drain(conn::Connection; timeout::Real = conn.options.drain_timeout)
    try
        status_before = CONNECTED
        subs = Subscription[]
        request_mux = nothing
        if connection_status(conn) == CONNECTED && has_subscription_cleanups(conn)
            ensure_request_mux(conn)
        end
        lock(conn.lock)
        try
            status_before = conn.status
            if status_before == CONNECTED
                set_connection_status_locked!(conn, DRAINING)
                request_mux = conn.request_sub
                subs = [
                    sub for sub in values(conn.subscriptions)
                    if request_mux === nothing || sub !== request_mux
                ]
            end
        finally
            unlock(conn.lock)
        end
        status_before == CLOSED && throw(ConnectionClosedError("connection is closed"))
        if status_before in (CONNECTING, RECONNECTING)
            close(conn)
            throw(ConnectionReconnectingError())
        end
        status_before == DRAINING && return nothing
        deadline = time() + Float64(timeout)
        if isempty(subs) && !(request_mux isa Subscription && !request_mux.closed)
            close(conn)
            return nothing
        end
        drain_subscription_group!(conn, subs, deadline, "drain")
        run_subscription_cleanups!(conn, subs, deadline, "drain cleanup")
        if request_mux isa Subscription && !request_mux.closed
            drain_subscription_group!(conn, [request_mux], deadline, "drain request mux")
        end
        close(conn)
    catch err
        if err isa ConnectionTimeoutError
            throw(handle_drain_timeout!(conn, timeout))
        end
        rethrow()
    end
    return nothing
end
