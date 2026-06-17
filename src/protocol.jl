const CRLF = UInt8[0x0d, 0x0a]
const EMPTY_U8 = UInt8[]

abstract type ProtocolMessage end

Base.@kwdef struct ServerInfo <: ProtocolMessage
    server_id::String = ""
    server_name::String = ""
    version::String = ""
    go::String = ""
    host::String = ""
    port::Int = 0
    headers::Bool = false
    max_payload::Int = 0
    proto::Int = 0
    client_id::Union{Nothing, UInt64} = nothing
    auth_required::Union{Nothing, Bool} = nothing
    tls_required::Union{Nothing, Bool} = nothing
    tls_verify::Union{Nothing, Bool} = nothing
    tls_available::Union{Nothing, Bool} = nothing
    connect_urls::Union{Nothing, Vector{String}} = nothing
    ws_connect_urls::Union{Nothing, Vector{String}} = nothing
    ldm::Union{Nothing, Bool} = nothing
    git_commit::Union{Nothing, String} = nothing
    jetstream::Union{Nothing, Bool} = nothing
    acc_is_sys::Union{Nothing, Bool} = nothing
    api_lvl::Union{Nothing, Int} = nothing
    ip::Union{Nothing, String} = nothing
    client_ip::Union{Nothing, String} = nothing
    nonce::Union{Nothing, String} = nothing
    cluster::Union{Nothing, String} = nothing
    domain::Union{Nothing, String} = nothing
end

StructTypes.StructType(::Type{ServerInfo}) = StructTypes.Struct()

struct Ok <: ProtocolMessage end
struct Ping <: ProtocolMessage end
struct Pong <: ProtocolMessage end

struct Err <: ProtocolMessage
    message::String
end

struct Msg <: ProtocolMessage
    subject::String
    sid::Int
    reply::Union{Nothing, String}
    headers::Vector{Pair{String, String}}
    data::Vector{UInt8}
    status::Int
    description::String
end

payload(msg::Msg) = String(msg.data)
reply_subject(msg::Msg) = msg.reply

function header(msg::Msg, key::AbstractString, default = nothing)
    needle = lowercase(String(key))
    for (k, v) in msg.headers
        lowercase(k) == needle && return v
    end
    return default
end

function readline_crlf(io)::String
    bytes = UInt8[]
    while true
        b = read(io, UInt8)
        if b == 0x0a
            if !isempty(bytes) && bytes[end] == 0x0d
                pop!(bytes)
            end
            return String(bytes)
        end
        push!(bytes, b)
        length(bytes) <= 4096 || throw(ProtocolError("control line exceeds 4096 bytes"))
    end
end

function expect_crlf(io)
    suffix = read(io, 2)
    suffix == CRLF || throw(ProtocolError("expected CRLF after payload"))
    return nothing
end

function read_payload(io, n::Integer)::Vector{UInt8}
    n < 0 && throw(ProtocolError("negative payload size"))
    data = read(io, Int(n))
    length(data) == n || throw(EOFError())
    expect_crlf(io)
    return data
end

function parse_server_info(line::AbstractString)
    startswith(line, "INFO ") || throw(ProtocolError("expected INFO, got $(repr(line))"))
    return JSON3.read(SubString(line, 6), ServerInfo)
end

function parse_err(line::AbstractString)
    msg = strip(replace(line[5:end], "'" => ""))
    return Err(msg)
end

function parse_status_line(line::AbstractString)
    s = String(line)
    startswith(s, "NATS/1.0") || throw(ProtocolError("bad header block"))
    rest = strip(s[(ncodeunits("NATS/1.0") + 1):end])
    isempty(rest) && return 200, ""
    if ncodeunits(rest) < 3
        throw(ProtocolError("bad header status line"))
    end
    status_text = rest[1:3]
    all(isdigit, status_text) || throw(ProtocolError("bad header status line"))
    status = parse(Int, status_text)
    description = ncodeunits(rest) > 3 ? strip(rest[4:end]) : ""
    return status, description
end

function parse_header_block(bytes::Vector{UInt8})
    length(bytes) >= 10 || throw(ProtocolError("bad header block"))
    length(bytes) >= 4 && bytes[(end - 3):end] == UInt8[0x0d, 0x0a, 0x0d, 0x0a] || throw(ProtocolError("bad header block"))
    lines = split(String(bytes), "\r\n"; keepempty = true)
    length(lines) >= 3 || throw(ProtocolError("bad header block"))
    status, description = parse_status_line(lines[1])
    headers = Pair{String,String}[]
    for line in lines[2:(end - 2)]
        isempty(line) && throw(ProtocolError("bad header block"))
        i = findfirst(':', line)
        i === nothing && throw(ProtocolError("bad header block"))
        key = strip(line[1:prevind(line, i)])
        validate_header_key(key)
        push!(headers, String(key) => strip(line[nextind(line, i):end]))
    end
    return headers, status, description
end

function parse_msg_line(line::AbstractString, io, has_headers::Bool)
    parts = split(line)
    min_fields = has_headers ? 5 : 4
    length(parts) >= min_fields || throw(ProtocolError("malformed message line: $(repr(line))"))
    subject = String(parts[2])
    sid = parse(Int, parts[3])
    if has_headers
        has_reply = length(parts) == 6
        reply = has_reply ? String(parts[4]) : nothing
        hbytes = parse(Int, parts[has_reply ? 5 : 4])
        total = parse(Int, parts[has_reply ? 6 : 5])
        raw = read_payload(io, total)
        hbytes <= total || throw(ProtocolError("header bytes exceed payload size"))
        headers, status, description = parse_header_block(raw[1:hbytes])
        data = raw[(hbytes + 1):end]
        return Msg(subject, sid, reply, headers, data, status, description)
    else
        has_reply = length(parts) == 5
        reply = has_reply ? String(parts[4]) : nothing
        size = parse(Int, parts[has_reply ? 5 : 4])
        data = read_payload(io, size)
        return Msg(subject, sid, reply, Pair{String,String}[], data, 200, "")
    end
end

function read_protocol_message(io)::ProtocolMessage
    line = readline_crlf(io)
    startswith(line, "INFO ") && return parse_server_info(line)
    line == "+OK" && return Ok()
    line == "PING" && return Ping()
    line == "PONG" && return Pong()
    startswith(line, "-ERR") && return parse_err(line)
    startswith(line, "MSG ") && return parse_msg_line(line, io, false)
    startswith(line, "HMSG ") && return parse_msg_line(line, io, true)
    throw(ProtocolError("unexpected protocol line: $(repr(line))"))
end

function write_ascii!(io::IOBuffer, s::AbstractString)
    write(io, codeunits(s))
    return io
end

function append_crlf!(io::IOBuffer)
    write(io, CRLF)
    return io
end

const HEADER_TOKEN_EXTRA_BYTES = Set{UInt8}(UInt8[
    0x21, # !
    0x23, # #
    0x24, # $
    0x25, # %
    0x26, # &
    0x27, # '
    0x2a, # *
    0x2b, # +
    0x2d, # -
    0x2e, # .
    0x5e, # ^
    0x5f, # _
    0x60, # `
    0x7c, # |
    0x7e, # ~
])

function is_header_token_byte(b::UInt8)
    return (0x30 <= b <= 0x39) ||
           (0x41 <= b <= 0x5a) ||
           (0x61 <= b <= 0x7a) ||
           b in HEADER_TOKEN_EXTRA_BYTES
end

function validate_header_key(key::AbstractString)
    isempty(key) && throw(InvalidHeaderKeyError(String(key)))
    for b in codeunits(key)
        is_header_token_byte(b) || throw(InvalidHeaderKeyError(String(key)))
    end
    return String(key)
end

sanitize_header_value(value::AbstractString) = replace(String(value), '\r' => ' ', '\n' => ' ')

function headers_bytes(headers::Vector{Pair{String, String}})
    isempty(headers) && return EMPTY_U8
    io = IOBuffer()
    write_ascii!(io, "NATS/1.0")
    append_crlf!(io)
    for (k, v) in headers
        write_ascii!(io, validate_header_key(k))
        write_ascii!(io, ": ")
        write_ascii!(io, sanitize_header_value(v))
        append_crlf!(io)
    end
    append_crlf!(io)
    return take!(io)
end

function bytes_payload(data)::Vector{UInt8}
    data === nothing && return UInt8[]
    data isa Vector{UInt8} && return data
    data isa AbstractVector{UInt8} && return collect(UInt8, data)
    data isa AbstractString && return Vector{UInt8}(codeunits(data))
    return Vector{UInt8}(codeunits(string(data)))
end

function pub_frame(subject::AbstractString, data = nothing; reply::Union{Nothing, AbstractString} = nothing, headers::Vector{Pair{String,String}} = Pair{String,String}[])
    subject_s = String(subject)
    payload = bytes_payload(data)
    hdr = headers_bytes(headers)
    io = IOBuffer()
    if isempty(hdr)
        write_ascii!(io, "PUB ")
        write_ascii!(io, subject_s)
        if reply !== nothing
            write_ascii!(io, " ")
            write_ascii!(io, String(reply))
        end
        write_ascii!(io, " ")
        write_ascii!(io, string(length(payload)))
        append_crlf!(io)
        write(io, payload)
        append_crlf!(io)
    else
        total = length(hdr) + length(payload)
        write_ascii!(io, "HPUB ")
        write_ascii!(io, subject_s)
        if reply !== nothing
            write_ascii!(io, " ")
            write_ascii!(io, String(reply))
        end
        write_ascii!(io, " ")
        write_ascii!(io, string(length(hdr)))
        write_ascii!(io, " ")
        write_ascii!(io, string(total))
        append_crlf!(io)
        write(io, hdr)
        write(io, payload)
        append_crlf!(io)
    end
    return take!(io)
end

function sub_frame(subject::AbstractString, queue::Union{Nothing, AbstractString}, sid::Integer)
    io = IOBuffer()
    write_ascii!(io, "SUB ")
    write_ascii!(io, String(subject))
    if queue !== nothing && !isempty(queue)
        write_ascii!(io, " ")
        write_ascii!(io, String(queue))
    end
    write_ascii!(io, " ")
    write_ascii!(io, string(sid))
    append_crlf!(io)
    return take!(io)
end

function unsub_frame(sid::Integer; max_msgs::Union{Nothing, Integer} = nothing)
    io = IOBuffer()
    write_ascii!(io, "UNSUB ")
    write_ascii!(io, string(sid))
    if max_msgs !== nothing
        write_ascii!(io, " ")
        write_ascii!(io, string(max_msgs))
    end
    append_crlf!(io)
    return take!(io)
end

ping_frame() = UInt8['P', 'I', 'N', 'G', 0x0d, 0x0a]
pong_frame() = UInt8['P', 'O', 'N', 'G', 0x0d, 0x0a]
