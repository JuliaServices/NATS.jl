using libsodium_jll: libsodium

const NKEY_PREFIX_ACCOUNT = UInt8(0)
const NKEY_PREFIX_CLUSTER = UInt8(2 << 3)
const NKEY_PREFIX_OPERATOR = UInt8(14 << 3)
const NKEY_PREFIX_SERVER = UInt8(13 << 3)
const NKEY_PREFIX_PRIVATE = UInt8(15 << 3)
const NKEY_PREFIX_SEED = UInt8(18 << 3)
const NKEY_PREFIX_USER = UInt8(20 << 3)
const NKEY_PREFIX_CURVE = UInt8(23 << 3)
const NKEY_BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
const ED25519_SEED_SIZE = 32
const ED25519_PUBLIC_KEY_SIZE = 32
const ED25519_SECRET_KEY_SIZE = 64
const ED25519_SIGNATURE_SIZE = 64
const USER_JWT_RE = r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
const USER_NKEY_SEED_RE = r"S[A-Z2-7]{50,}"

function nkey_valid_prefix(prefix::UInt8)
    return prefix in (
        NKEY_PREFIX_OPERATOR,
        NKEY_PREFIX_SERVER,
        NKEY_PREFIX_CLUSTER,
        NKEY_PREFIX_ACCOUNT,
        NKEY_PREFIX_USER,
        NKEY_PREFIX_SEED,
        NKEY_PREFIX_PRIVATE,
        NKEY_PREFIX_CURVE,
    )
end

function nkey_valid_public_prefix(prefix::UInt8)
    return prefix in (
        NKEY_PREFIX_OPERATOR,
        NKEY_PREFIX_SERVER,
        NKEY_PREFIX_CLUSTER,
        NKEY_PREFIX_ACCOUNT,
        NKEY_PREFIX_USER,
        NKEY_PREFIX_CURVE,
    )
end

function nkey_base32_encode(data::AbstractVector{UInt8})
    out = IOBuffer()
    acc = UInt32(0)
    bits = 0
    for byte in data
        acc = (acc << 8) | UInt32(byte)
        bits += 8
        while bits >= 5
            index = UInt8((acc >> (bits - 5)) & 0x1f)
            write(out, codeunit(NKEY_BASE32_ALPHABET, Int(index) + 1))
            bits -= 5
        end
        acc = bits == 0 ? UInt32(0) : acc & ((UInt32(1) << bits) - UInt32(1))
    end
    if bits > 0
        index = UInt8((acc << (5 - bits)) & 0x1f)
        write(out, codeunit(NKEY_BASE32_ALPHABET, Int(index) + 1))
    end
    return String(take!(out))
end

function nkey_base32_value(ch::Char)
    if 'A' <= ch <= 'Z'
        return UInt8(Int(ch) - Int('A'))
    elseif '2' <= ch <= '7'
        return UInt8(26 + Int(ch) - Int('2'))
    end
    throw(ArgumentError("invalid NKey base32 character '$ch'"))
end

function nkey_base32_decode(encoded::AbstractString)
    clean = uppercase(filter(ch -> !isspace(ch), String(encoded)))
    out = UInt8[]
    acc = UInt32(0)
    bits = 0
    for ch in clean
        acc = (acc << 5) | UInt32(nkey_base32_value(ch))
        bits += 5
        while bits >= 8
            push!(out, UInt8((acc >> (bits - 8)) & 0xff))
            bits -= 8
        end
        acc = bits == 0 ? UInt32(0) : acc & ((UInt32(1) << bits) - UInt32(1))
    end
    bits > 0 && acc != 0 && throw(ArgumentError("invalid NKey base32 padding"))
    return out
end

function nkey_crc16(data::AbstractVector{UInt8})
    crc = UInt16(0)
    for byte in data
        crc = xor(crc, UInt16(byte) << 8)
        for _ in 1:8
            high = (crc & UInt16(0x8000)) != 0
            crc = UInt16((UInt32(crc) << 1) & 0xffff)
            high && (crc = xor(crc, UInt16(0x1021)))
        end
    end
    return crc
end

function nkey_append_crc16!(raw::Vector{UInt8})
    crc = nkey_crc16(raw)
    push!(raw, UInt8(crc & UInt16(0x00ff)))
    push!(raw, UInt8(crc >> 8))
    return raw
end

function nkey_decode_checked(encoded::AbstractString)
    raw = nkey_base32_decode(encoded)
    length(raw) >= 4 || throw(ArgumentError("invalid NKey encoding"))
    expected = UInt16(raw[end - 1]) | (UInt16(raw[end]) << 8)
    payload = raw[1:end - 2]
    nkey_crc16(payload) == expected || throw(ArgumentError("invalid NKey checksum"))
    return payload
end

function nkey_encode(prefix::UInt8, payload::AbstractVector{UInt8})
    nkey_valid_prefix(prefix) || throw(ArgumentError("invalid NKey prefix"))
    raw = UInt8[prefix]
    append!(raw, payload)
    nkey_append_crc16!(raw)
    return nkey_base32_encode(raw)
end

function nkey_decode_seed(seed::AbstractString)
    raw = nkey_decode_checked(seed)
    length(raw) == ED25519_SEED_SIZE + 2 || throw(ArgumentError("invalid NKey seed length"))
    seed_prefix = raw[1] & UInt8(0xf8)
    public_prefix = ((raw[1] & UInt8(0x07)) << 5) | ((raw[2] & UInt8(0xf8)) >> 3)
    seed_prefix == NKEY_PREFIX_SEED || throw(ArgumentError("invalid NKey seed prefix"))
    nkey_valid_public_prefix(public_prefix) || throw(ArgumentError("invalid NKey public prefix"))
    public_prefix == NKEY_PREFIX_CURVE && throw(ArgumentError("curve NKeys cannot sign NATS authentication nonces"))
    return public_prefix, raw[3:end]
end

function nkey_decode_public_key(public_key::AbstractString)
    raw = nkey_decode_checked(public_key)
    length(raw) == ED25519_PUBLIC_KEY_SIZE + 1 || throw(ArgumentError("invalid NKey public key length"))
    prefix = raw[1]
    nkey_valid_public_prefix(prefix) || throw(ArgumentError("invalid NKey public key prefix"))
    prefix == NKEY_PREFIX_CURVE && throw(ArgumentError("curve NKeys cannot sign NATS authentication nonces"))
    return prefix, raw[2:end]
end

function sodium_init!()
    status = ccall((:sodium_init, libsodium), Cint, ())
    status < 0 && error("libsodium initialization failed")
    return nothing
end

function ed25519_keypair_from_seed(seed::AbstractVector{UInt8})
    length(seed) == ED25519_SEED_SIZE || throw(ArgumentError("Ed25519 seed must be 32 bytes"))
    sodium_init!()
    seed_bytes = Vector{UInt8}(seed)
    public = Vector{UInt8}(undef, ED25519_PUBLIC_KEY_SIZE)
    secret = Vector{UInt8}(undef, ED25519_SECRET_KEY_SIZE)
    status = ccall(
        (:crypto_sign_ed25519_seed_keypair, libsodium),
        Cint,
        (Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}),
        public,
        secret,
        seed_bytes,
    )
    status == 0 || error("libsodium failed to derive Ed25519 keypair")
    return public, secret
end

function ed25519_sign_detached(secret::AbstractVector{UInt8}, data::AbstractVector{UInt8})
    length(secret) == ED25519_SECRET_KEY_SIZE || throw(ArgumentError("Ed25519 secret key must be 64 bytes"))
    sodium_init!()
    secret_bytes = Vector{UInt8}(secret)
    data_bytes = Vector{UInt8}(data)
    signature = Vector{UInt8}(undef, ED25519_SIGNATURE_SIZE)
    signature_length = Ref{UInt64}(0)
    status = ccall(
        (:crypto_sign_ed25519_detached, libsodium),
        Cint,
        (Ptr{UInt8}, Ref{UInt64}, Ptr{UInt8}, UInt64, Ptr{UInt8}),
        signature,
        signature_length,
        data_bytes,
        UInt64(length(data_bytes)),
        secret_bytes,
    )
    status == 0 || error("libsodium failed to sign Ed25519 message")
    length(signature) == Int(signature_length[]) || resize!(signature, Int(signature_length[]))
    return signature
end

"""
    nkey_public_from_seed(seed)

Return the public NKey associated with an encoded NATS seed.
"""
function nkey_public_from_seed(seed::AbstractString)
    public_prefix, raw_seed = nkey_decode_seed(seed)
    public, _secret = ed25519_keypair_from_seed(raw_seed)
    return nkey_encode(public_prefix, public)
end

"""
    nkey_sign(seed, data)

Sign a NATS server nonce with an encoded NKey seed and return raw signature bytes.
"""
function nkey_sign(seed::AbstractString, data::AbstractVector{UInt8})
    _public_prefix, raw_seed = nkey_decode_seed(seed)
    _public, secret = ed25519_keypair_from_seed(raw_seed)
    return ed25519_sign_detached(secret, data)
end

nkey_sign(seed::AbstractString, data::AbstractString) = nkey_sign(seed, Vector{UInt8}(codeunits(data)))

function base64url_no_padding(data::AbstractVector{UInt8})
    encoded = base64encode(data)
    encoded = replace(encoded, "+" => "-", "/" => "_")
    return replace(encoded, r"=+$" => "")
end

function signature_bytes(signature)
    if signature isa AbstractVector{UInt8}
        bytes = Vector{UInt8}(signature)
    elseif signature isa AbstractVector{<:Integer}
        bytes = UInt8.(signature)
    else
        throw(ArgumentError("signature_cb must return raw signature bytes"))
    end
    length(bytes) == ED25519_SIGNATURE_SIZE || throw(ArgumentError("NKey signature must be 64 bytes"))
    return bytes
end

function find_user_jwt(content::AbstractString)
    decorated = match(r"-+BEGIN NATS USER JWT-+\s*([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\s*-+END NATS USER JWT-+"s, content)
    decorated !== nothing && return String(decorated.captures[1])
    raw = match(USER_JWT_RE, content)
    raw !== nothing && return String(raw.match)
    throw(ArgumentError("unable to extract NATS user JWT"))
end

function find_user_nkey_seed(content::AbstractString)
    decorated = match(r"-+BEGIN USER NKEY SEED-+\s*(S[A-Z2-7]+)\s*-+END USER NKEY SEED-+"s, content)
    decorated !== nothing && return String(decorated.captures[1])
    raw = match(USER_NKEY_SEED_RE, uppercase(content))
    raw !== nothing && return String(raw.match)
    throw(ArgumentError("unable to extract NATS user NKey seed"))
end

function read_user_jwt(path::AbstractString)
    return find_user_jwt(read(path, String))
end

function read_user_nkey_seed(path::AbstractString)
    return find_user_nkey_seed(read(path, String))
end

function set_auth_value(current, value, name::AbstractString)
    value === nothing && return current
    current === nothing && return value
    current == value && return current
    throw(ArgumentError("multiple $name values were provided"))
end

function jwt_callback_value(cb)
    value = cb()
    value isa AbstractString || throw(ArgumentError("jwt_cb must return an AbstractString"))
    return String(value)
end

function auth_material(options)
    jwt = options.jwt
    nkey = options.nkey
    nkey_seed = options.nkey_seed
    signature_cb = options.signature_cb

    if options.jwt_cb !== nothing
        jwt = set_auth_value(jwt, jwt_callback_value(options.jwt_cb), "jwt")
    end

    if options.credentials !== nothing
        contents = read(options.credentials, String)
        jwt = set_auth_value(jwt, find_user_jwt(contents), "jwt")
        nkey_seed = set_auth_value(nkey_seed, find_user_nkey_seed(contents), "nkey_seed")
    end

    if options.jwt_file !== nothing
        jwt = set_auth_value(jwt, read_user_jwt(options.jwt_file), "jwt")
    end

    if options.nkey_seed_file !== nothing
        nkey_seed = set_auth_value(nkey_seed, read_user_nkey_seed(options.nkey_seed_file), "nkey_seed")
    end

    return (jwt = jwt, nkey = nkey, nkey_seed = nkey_seed, signature_cb = signature_cb)
end

function nats_auth_signature(options, info)
    auth = auth_material(options)
    has_jwt = auth.jwt !== nothing
    has_nkey = auth.nkey !== nothing
    has_seed = auth.nkey_seed !== nothing
    has_signer = auth.signature_cb !== nothing

    has_jwt && has_nkey && throw(ArgumentError("jwt and nkey authentication are mutually exclusive"))
    has_seed && has_signer && throw(ArgumentError("nkey_seed and signature_cb are mutually exclusive; provide nkey with signature_cb for external signing"))
    (has_jwt || has_nkey || has_seed || has_signer) || return nothing
    has_signer && !(has_jwt || has_nkey) && throw(ArgumentError("signature_cb requires jwt or nkey authentication"))
    (has_jwt || has_nkey) && !has_seed && !has_signer && throw(ArgumentError("NKey/JWT authentication requires nkey_seed or signature_cb"))

    nonce = something(info.nonce, "")
    isempty(nonce) && throw(UnsupportedTransportError("auth", "server did not provide a nonce for NKey/JWT authentication"))

    if has_seed
        public = nkey_public_from_seed(auth.nkey_seed)
        if has_nkey
            nkey_decode_public_key(auth.nkey)
            public == auth.nkey || throw(ArgumentError("nkey does not match nkey_seed"))
        end
        signature = nkey_sign(auth.nkey_seed, nonce)
        return (jwt = auth.jwt, nkey = has_jwt ? nothing : public, sig = base64url_no_padding(signature))
    end

    if has_nkey
        nkey_decode_public_key(auth.nkey)
    end
    raw_signature = signature_bytes(auth.signature_cb(nonce))
    return (jwt = auth.jwt, nkey = auth.nkey, sig = base64url_no_padding(raw_signature))
end

function add_auth_fields!(data::Dict{String, Any}, options, info)
    auth = nats_auth_signature(options, info)
    auth === nothing && return data
    auth.jwt === nothing || (data["jwt"] = auth.jwt)
    auth.nkey === nothing || (data["nkey"] = auth.nkey)
    data["sig"] = auth.sig
    return data
end
