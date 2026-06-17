Base.@kwdef struct TLSOptions
    ca_file::Union{Nothing, String} = nothing
    cert_file::Union{Nothing, String} = nothing
    key_file::Union{Nothing, String} = nothing
    server_name::Union{Nothing, String} = nothing
    verify_peer::Bool = true
    verify_hostname::Bool = verify_peer
    handshake_timeout::Float64 = 2.0
    min_version::Union{Nothing, UInt16} = Reseau.TLS.TLS1_2_VERSION
    max_version::Union{Nothing, UInt16} = nothing
end

struct ServerURL
    raw::String
    scheme::String
    host::String
    port::Int
    user::Union{Nothing, String}
    password::Union{Nothing, String}
    path::String
    query::String
end

function default_port(scheme::AbstractString)
    scheme == "ws" && return 80
    scheme == "wss" && return 443
    return 4222
end

function parse_server_url(raw::AbstractString)
    s = occursin("://", raw) ? String(raw) : "nats://$(raw)"
    uri = URIs.URI(s)
    scheme = isempty(uri.scheme) ? "nats" : lowercase(String(uri.scheme))
    host = String(uri.host)
    isempty(host) && throw(ArgumentError("NATS URL must include a host: $raw"))
    port = isempty(uri.port) ? default_port(scheme) : parse(Int, uri.port)
    user = nothing
    password = nothing
    if !isempty(uri.userinfo)
        parts = split(String(uri.userinfo), ':'; limit = 2)
        user = isempty(parts[1]) ? nothing : parts[1]
        password = length(parts) == 2 ? parts[2] : nothing
    end
    path = isempty(uri.path) ? "" : String(uri.path)
    query = isempty(uri.query) ? "" : String(uri.query)
    return ServerURL(s, scheme, host, port, user, password, path, query)
end

address(url::ServerURL) = "$(url.host):$(url.port)"
is_tls_scheme(url::ServerURL) = url.scheme in ("tls", "wss")
is_websocket_scheme(url::ServerURL) = url.scheme in ("ws", "wss")

duration_ns(seconds::Real) = Int64(round(seconds * 1_000_000_000))

mutable struct WebSocketTransport
    ws::Any
    client::Any
    buffer::Vector{UInt8}
    position::Int
    closed::Bool
end

function WebSocketTransport(ws, client = nothing)
    return WebSocketTransport(ws, client, UInt8[], 1, false)
end

Base.isopen(io::WebSocketTransport) = !io.closed && !HTTP.WebSockets.isclosed(io.ws)

function Base.close(io::WebSocketTransport)
    io.closed = true
    try close(io.ws) catch end
    return nothing
end

function refill!(io::WebSocketTransport)
    while io.position > length(io.buffer)
        msg = HTTP.WebSockets.receive(io.ws)
        io.buffer = msg isa String ? Vector{UInt8}(codeunits(msg)) : Vector{UInt8}(msg)
        io.position = 1
    end
    return nothing
end

function Base.read(io::WebSocketTransport, ::Type{UInt8})
    isopen(io) || throw(EOFError())
    refill!(io)
    b = io.buffer[io.position]
    io.position += 1
    return b
end

function Base.read(io::WebSocketTransport, n::Integer)
    n < 0 && throw(ArgumentError("number of bytes to read must be non-negative"))
    out = Vector{UInt8}(undef, Int(n))
    for i in eachindex(out)
        out[i] = read(io, UInt8)
    end
    return out
end

function Base.write(io::WebSocketTransport, bytes::AbstractVector{UInt8})
    isopen(io) || throw(EOFError())
    HTTP.WebSockets.send(io.ws, bytes)
    return length(bytes)
end

Base.write(io::WebSocketTransport, bytes::Base.CodeUnits{UInt8,<:AbstractString}) =
    write(io, Vector{UInt8}(bytes))

function Base.flush(::WebSocketTransport)
    return nothing
end

function tls_config(url::ServerURL, tls::TLSOptions)
    server_name = something(tls.server_name, url.host)
    return Reseau.TLS.Config(
        server_name = server_name,
        verify_peer = tls.verify_peer,
        verify_hostname = tls.verify_hostname,
        ca_file = tls.ca_file,
        cert_file = tls.cert_file,
        key_file = tls.key_file,
        handshake_timeout_ns = duration_ns(tls.handshake_timeout),
        min_version = tls.min_version,
        max_version = tls.max_version,
    )
end

function normalize_websocket_proxy_path(path::AbstractString)
    path_s = String(path)
    isempty(path_s) && return path_s
    startswith(path_s, "/") || throw(ArgumentError("proxy_path must be empty or start with '/'"))
    has_subject_whitespace(path_s) && throw(ArgumentError("proxy_path must not contain whitespace"))
    return path_s
end

function websocket_url(url::ServerURL; proxy_path::Union{Nothing, AbstractString} = nothing)
    path = proxy_path === nothing ? url.path : normalize_websocket_proxy_path(proxy_path)
    path = isempty(path) ? "/" : path
    query = isempty(url.query) ? "" : "?$(url.query)"
    return "$(url.scheme)://$(url.host):$(url.port)$(path)$(query)"
end

function open_tcp_transport(url::ServerURL; connect_timeout::Real)
    if url.scheme in ("nats", "tls")
        return Reseau.TCP.connect(address(url); timeout_ns = duration_ns(connect_timeout))
    elseif is_websocket_scheme(url)
        throw(UnsupportedTransportError(url.scheme, "WebSocket URLs must be opened with the WebSocket transport"))
    else
        throw(UnsupportedTransportError(url.scheme, "expected nats, tls, ws, or wss"))
    end
end

function websocket_client(url::ServerURL, tls::TLSOptions; connect_timeout::Real)
    url.scheme == "wss" || return nothing
    transport = HTTP.Transport(
        tls_config = tls_config(url, tls),
        max_idle_per_host = 1,
        max_idle_total = 1,
        idle_timeout_ns = Int64(0),
    )
    return HTTP.Client(transport = transport, connect_timeout = connect_timeout)
end

function open_websocket_transport(
        url::ServerURL,
        tls::TLSOptions;
        connect_timeout::Real,
        headers::Vector{Pair{String,String}} = Pair{String,String}[],
        proxy_path::Union{Nothing, AbstractString} = nothing,
)
    client = websocket_client(url, tls; connect_timeout)
    ws = HTTP.WebSockets.open(
        websocket_url(url; proxy_path);
        headers,
        connect_timeout,
        client,
        require_ssl_verification = true,
    )
    return WebSocketTransport(ws, client)
end

function open_tls_transport(url::ServerURL, tls::TLSOptions; connect_timeout::Real)
    return Reseau.TLS.connect(
        "tcp",
        address(url),
        tls_config(url, tls);
        timeout_ns = duration_ns(connect_timeout),
    )
end

function upgrade_to_tls(tcp, url::ServerURL, tls::TLSOptions)
    tls_io = Reseau.TLS.client(tcp, tls_config(url, tls))
    Reseau.TLS.handshake!(tls_io)
    return tls_io
end
