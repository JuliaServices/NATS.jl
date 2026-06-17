module JetStream

using Base64
using JSON3
using Random: randstring
using SHA
using StructTypes

import ..NATS

const DEFAULT_API_PREFIX = "\$JS.API."
const DEFAULT_FETCH_BYTES_BATCH = 1_000_000
const DEFAULT_PUBLISH_RETRY_ATTEMPTS = 2
const DEFAULT_PUBLISH_RETRY_WAIT = 0.25
const INVALID_STREAM_NAME_CHARS = Set(['>', '*', '.', ' ', '/', '\\', '\t', '\r', '\n'])

struct JetStreamError <: Exception
    code::Int
    err_code::Int
    description::String
end

function Base.showerror(io::IO, err::JetStreamError)
    print(io, "JetStream error ", err.code, "/", err.err_code, ": ", err.description)
end

struct NoHeartbeatError <: Exception end

function Base.showerror(io::IO, ::NoHeartbeatError)
    print(io, "JetStream no heartbeat received")
end

struct MsgNoAckReplyError <: Exception end

function Base.showerror(io::IO, ::MsgNoAckReplyError)
    print(io, "JetStream message cannot be acknowledged because it has no ack reply subject")
end

const ACK_NONE_CONSUMERS_LOCK = ReentrantLock()
const ACK_NONE_CONSUMERS = Dict{Tuple{UInt,String,String,String}, Int}()

struct NoStreamResponseError <: Exception
    subject::String
end

function Base.showerror(io::IO, err::NoStreamResponseError)
    print(io, "JetStream no stream response for subject ", repr(err.subject))
end

struct PublisherClosedError <: Exception end

function Base.showerror(io::IO, ::PublisherClosedError)
    print(io, "JetStream publisher closed")
end

struct NoMatchingStreamError <: Exception
    subject::String
end

function Base.showerror(io::IO, err::NoMatchingStreamError)
    print(io, "JetStream no stream matches subject ", repr(err.subject))
end

struct StreamNotFoundError <: Exception
    stream::String
end

function Base.showerror(io::IO, err::StreamNotFoundError)
    print(io, "JetStream stream not found: ", repr(err.stream))
end

struct StreamNameAlreadyInUseError <: Exception
    stream::String
end

function Base.showerror(io::IO, err::StreamNameAlreadyInUseError)
    print(io, "JetStream stream name already in use: ", repr(err.stream))
end

struct StreamNameRequiredError <: Exception end

function Base.showerror(io::IO, ::StreamNameRequiredError)
    print(io, "JetStream stream name is required")
end

struct InvalidStreamNameError <: Exception
    stream::String
end

function Base.showerror(io::IO, err::InvalidStreamNameError)
    print(io, "JetStream invalid stream name: ", repr(err.stream))
end

struct ConsumerNotFoundError <: Exception
    stream::String
    consumer::String
end

function Base.showerror(io::IO, err::ConsumerNotFoundError)
    print(io, "JetStream consumer not found: ", repr(err.consumer), " on stream ", repr(err.stream))
end

struct ConsumerExistsError <: Exception
    stream::String
    consumer::String
end

function Base.showerror(io::IO, err::ConsumerExistsError)
    print(io, "JetStream consumer already exists: ", repr(err.consumer), " on stream ", repr(err.stream))
end

struct ConsumerDoesNotExistError <: Exception
    stream::String
    consumer::String
end

function Base.showerror(io::IO, err::ConsumerDoesNotExistError)
    print(io, "JetStream consumer does not exist: ", repr(err.consumer), " on stream ", repr(err.stream))
end

struct OverlappingFilterSubjectsError <: Exception end

function Base.showerror(io::IO, ::OverlappingFilterSubjectsError)
    print(io, "JetStream consumer filter subjects cannot overlap")
end

struct MultipleFilterSubjectsNotSupportedError <: Exception end

function Base.showerror(io::IO, ::MultipleFilterSubjectsNotSupportedError)
    print(io, "JetStream multiple consumer filter subjects are not supported by this server")
end

struct ConsumerNameRequiredError <: Exception end

function Base.showerror(io::IO, ::ConsumerNameRequiredError)
    print(io, "JetStream consumer name is required")
end

struct InvalidConsumerNameError <: Exception
    consumer::String
end

function Base.showerror(io::IO, err::InvalidConsumerNameError)
    print(io, "JetStream invalid consumer name: ", repr(err.consumer))
end

struct KeyNotFoundError <: Exception
    key::String
end

function Base.showerror(io::IO, err::KeyNotFoundError)
    print(io, "JetStream key not found: ", repr(err.key))
end

struct NoKeysFoundError <: Exception end

function Base.showerror(io::IO, ::NoKeysFoundError)
    print(io, "JetStream no keys found")
end

struct BucketNotFoundError <: Exception
    bucket::String
end

function Base.showerror(io::IO, err::BucketNotFoundError)
    print(io, "JetStream bucket not found: ", repr(err.bucket))
end

struct ObjectNotFoundError <: Exception
    name::String
end

function Base.showerror(io::IO, err::ObjectNotFoundError)
    print(io, "JetStream object not found: ", repr(err.name))
end

struct UpdateMetaDeletedError <: Exception
    name::String
end

function Base.showerror(io::IO, err::UpdateMetaDeletedError)
    print(io, "JetStream cannot update metadata for deleted object: ", repr(err.name))
end

struct NoObjectsFoundError <: Exception end

function Base.showerror(io::IO, ::NoObjectsFoundError)
    print(io, "JetStream no objects found")
end

struct BadObjectMetaError <: Exception
    name::String
end

function Base.showerror(io::IO, err::BadObjectMetaError)
    print(io, "JetStream bad object metadata for ", repr(err.name))
end

struct OrderedConsumerResetError <: Exception
    expected_consumer_seq::UInt64
    got_consumer_seq::UInt64
    expected_stream_seq::UInt64
    got_stream_seq::UInt64
    consumer::String
end

struct OrderedConsumerConcurrentRequestsError <: Exception end

struct OrderedConsumerUsedAsConsumeError <: Exception end

struct OrderedConsumerUsedAsFetchError <: Exception end

function Base.showerror(io::IO, err::OrderedConsumerResetError)
    print(
        io,
        "ordered consumer reset: expected consumer sequence ",
        err.expected_consumer_seq,
        " from stream sequence ",
        err.expected_stream_seq,
        ", got consumer sequence ",
        err.got_consumer_seq,
        " at stream sequence ",
        err.got_stream_seq,
        " on ",
        err.consumer,
    )
end

function Base.showerror(io::IO, ::OrderedConsumerConcurrentRequestsError)
    print(io, "ordered consumer cannot run concurrent requests")
end

function Base.showerror(io::IO, ::OrderedConsumerUsedAsConsumeError)
    print(io, "ordered consumer is already used as a consume context")
end

function Base.showerror(io::IO, ::OrderedConsumerUsedAsFetchError)
    print(io, "ordered consumer is already used for fetch requests")
end

Base.@kwdef struct PubAck
    stream::String = ""
    seq::UInt64 = 0
    duplicate::Bool = false
    domain::Union{Nothing, String} = nothing
end

Base.@kwdef struct RawStreamMsg
    stream::String = ""
    subject::String = ""
    seq::UInt64 = 0
    headers::Vector{Pair{String, String}} = Pair{String,String}[]
    data::Vector{UInt8} = UInt8[]
    time::Union{Nothing, String} = nothing
end

struct AsyncPublishMessage
    subject::String
    data::Vector{UInt8}
    headers::Vector{Pair{String, String}}
end

struct PubAckFuture
    subject::String
    reply::String
    msg::AsyncPublishMessage
    task::Task
end

mutable struct PullSubscription
    connection::NATS.Connection
    stream::String
    consumer::String
    api_prefix::String
    inbox::String
    subscription::NATS.Subscription
    info::Any
    max_request_batch::Union{Nothing, Int}
    max_request_expires::Union{Nothing, Int64}
    max_request_max_bytes::Union{Nothing, Int}
    delete_on_close::Bool
    ack_none::Bool
    lock::ReentrantLock
    closed::Bool
end

mutable struct PushSubscription
    connection::NATS.Connection
    stream::String
    consumer::String
    api_prefix::String
    deliver_subject::String
    deliver_group::Union{Nothing, String}
    subscription::NATS.Subscription
    info::Any
    delete_on_close::Bool
    ack_none::Bool
    lock::ReentrantLock
    closed::Bool
end

mutable struct ConsumeContext
    pull::Any
    messages::Channel{NATS.Msg}
    errors::Channel{Any}
    task::Union{Nothing, Task}
    lock::ReentrantLock
    closed::Bool
    owns_pull::Bool
end

struct SequencePair
    stream::UInt64
    consumer::UInt64
end

Base.@kwdef struct MessageMetadata
    sequence::SequencePair
    num_delivered::UInt64
    num_pending::UInt64
    timestamp_ns::UInt64
    stream::String
    consumer::String
    domain::String = ""
end

Base.@kwdef struct SubjectTransformConfig
    source::String
    destination::String
end

Base.@kwdef struct RePublish
    destination::String
    source::Union{Nothing, String} = nothing
    headers_only::Bool = false
end

Base.@kwdef struct Placement
    cluster::Union{Nothing, String} = nothing
    tags::Vector{String} = String[]
end

Base.@kwdef struct ExternalStream
    api_prefix::String
    deliver_prefix::String
end

Base.@kwdef struct StreamSource
    name::String
    opt_start_seq::Union{Nothing, UInt64} = nothing
    opt_start_time::Union{Nothing, String} = nothing
    filter_subject::Union{Nothing, String} = nothing
    subject_transforms::Vector{SubjectTransformConfig} = SubjectTransformConfig[]
    external::Union{Nothing, ExternalStream} = nothing
end

Base.@kwdef struct StreamConsumerLimits
    inactive_threshold::Union{Nothing, Int64} = nothing
    max_ack_pending::Union{Nothing, Int} = nothing
end

Base.@kwdef struct StreamConfig
    name::String
    subjects::Vector{String} = String[]
    description::Union{Nothing, String} = nothing
    retention::String = "limits"
    storage::String = "file"
    replicas::Int = 1
    allow_direct::Bool = false
    max_consumers::Union{Nothing, Int} = nothing
    max_msgs::Union{Nothing, Int64} = nothing
    max_bytes::Union{Nothing, Int64} = nothing
    discard::Union{Nothing, String} = nothing
    discard_new_per_subject::Union{Nothing, Bool} = nothing
    max_age::Union{Nothing, Int64} = nothing
    max_msgs_per_subject::Union{Nothing, Int64} = nothing
    max_msg_size::Union{Nothing, Int} = nothing
    no_ack::Union{Nothing, Bool} = nothing
    duplicate_window::Union{Nothing, Int64} = nothing
    deny_delete::Union{Nothing, Bool} = nothing
    deny_purge::Union{Nothing, Bool} = nothing
    allow_rollup_hdrs::Union{Nothing, Bool} = nothing
    compression::Union{Nothing, String} = nothing
    first_seq::Union{Nothing, UInt64} = nothing
    placement::Union{Nothing, Placement} = nothing
    mirror::Union{Nothing, StreamSource} = nothing
    sources::Union{Nothing, Vector{StreamSource}} = nothing
    sealed::Union{Nothing, Bool} = nothing
    subject_transform::Union{Nothing, SubjectTransformConfig} = nothing
    republish::Union{Nothing, RePublish} = nothing
    mirror_direct::Union{Nothing, Bool} = nothing
    consumer_limits::Union{Nothing, StreamConsumerLimits} = nothing
    metadata::Union{Nothing, Dict{String,String}} = nothing
    allow_msg_ttl::Union{Nothing, Bool} = nothing
    subject_delete_marker_ttl::Union{Nothing, Int64} = nothing
    allow_msg_counter::Union{Nothing, Bool} = nothing
    allow_atomic_publish::Union{Nothing, Bool} = nothing
    allow_msg_schedules::Union{Nothing, Bool} = nothing
    persist_mode::Union{Nothing, String} = nothing
    allow_batch_publish::Union{Nothing, Bool} = nothing
end

json_value(value) = value
json_value(value::Vector) = [json_value(item) for item in value]
json_value(value::Dict) = Dict(String(k) => json_value(v) for (k, v) in pairs(value))
json_value(value::SubjectTransformConfig) = Dict{String, Any}(
    "src" => value.source,
    "dest" => value.destination,
)
function json_value(value::RePublish)
    d = Dict{String, Any}("dest" => value.destination)
    put_if_present!(d, "src", value.source)
    value.headers_only && (d["headers_only"] = true)
    return d
end
function json_value(value::Placement)
    d = Dict{String, Any}()
    put_if_present!(d, "cluster", value.cluster)
    isempty(value.tags) || (d["tags"] = copy(value.tags))
    return d
end
json_value(value::ExternalStream) = Dict{String, Any}(
    "api" => value.api_prefix,
    "deliver" => value.deliver_prefix,
)
function json_value(value::StreamSource)
    d = Dict{String, Any}("name" => value.name)
    put_if_present!(d, "opt_start_seq", value.opt_start_seq)
    put_if_present!(d, "opt_start_time", value.opt_start_time)
    put_if_present!(d, "filter_subject", value.filter_subject)
    isempty(value.subject_transforms) || (d["subject_transforms"] = json_value(value.subject_transforms))
    put_if_present!(d, "external", value.external)
    return d
end
function json_value(value::StreamConsumerLimits)
    d = Dict{String, Any}()
    put_if_present!(d, "inactive_threshold", value.inactive_threshold)
    put_if_present!(d, "max_ack_pending", value.max_ack_pending)
    return d
end

function put_if_present!(d::Dict{String, Any}, key::String, value)
    value === nothing || (d[key] = json_value(value))
    return d
end

function put_if_nonempty!(d::Dict{String, Any}, key::String, value::Union{Nothing, AbstractString})
    value === nothing && return d
    value_s = String(value)
    isempty(value_s) || (d[key] = value_s)
    return d
end

function normalize_api_prefix(prefix::AbstractString)
    s = String(prefix)
    isempty(s) && throw(ArgumentError("JetStream api_prefix must not be empty"))
    return endswith(s, ".") ? s : s * "."
end

function api_prefix_value(; api_prefix::Union{Nothing, AbstractString} = nothing, domain::Union{Nothing, AbstractString} = nothing)
    api_prefix !== nothing && domain !== nothing && throw(ArgumentError("api_prefix and domain are mutually exclusive"))
    if domain !== nothing
        d = String(domain)
        isempty(d) && throw(ArgumentError("JetStream domain must not be empty"))
        return "\$JS.$d.API."
    end
    api_prefix === nothing && return DEFAULT_API_PREFIX
    return normalize_api_prefix(api_prefix)
end

api_subject_from_prefix(api_prefix::AbstractString, suffix::AbstractString) =
    normalize_api_prefix(api_prefix) * String(suffix)

api_subject(suffix::AbstractString; api_prefix::Union{Nothing, AbstractString} = nothing, domain::Union{Nothing, AbstractString} = nothing) =
    api_subject_from_prefix(api_prefix_value(; api_prefix, domain), suffix)

function validate_stream_name(stream::AbstractString)
    stream_s = String(stream)
    isempty(stream_s) && throw(StreamNameRequiredError())
    any(ch -> ch in INVALID_STREAM_NAME_CHARS, stream_s) && throw(InvalidStreamNameError(stream_s))
    return stream_s
end

function validate_consumer_name(consumer::AbstractString)
    consumer_s = String(consumer)
    isempty(consumer_s) && throw(ConsumerNameRequiredError())
    any(ch -> ch in INVALID_STREAM_NAME_CHARS, consumer_s) && throw(InvalidConsumerNameError(consumer_s))
    return consumer_s
end

function consumer_filter_subjects(config)
    single = nothing
    if config.filter_subject !== nothing
        subject = String(config.filter_subject)
        isempty(subject) || (single = NATS.validate_subscribe_subject(subject))
    end
    if config.filter_subjects !== nothing
        subjects = String.(config.filter_subjects)
        if !isempty(subjects)
            single === nothing || throw(ArgumentError("filter_subject and filter_subjects are mutually exclusive"))
            foreach(NATS.validate_subscribe_subject, subjects)
            return nothing, subjects
        end
    end
    return single, nothing
end

function config_dict(config::StreamConfig)
    d = Dict{String, Any}(
        "name" => config.name,
        "subjects" => config.subjects,
        "retention" => config.retention,
        "storage" => config.storage,
        "num_replicas" => config.replicas,
        "allow_direct" => config.allow_direct,
    )
    put_if_present!(d, "description", config.description)
    put_if_present!(d, "max_consumers", config.max_consumers)
    put_if_present!(d, "max_msgs", config.max_msgs)
    put_if_present!(d, "max_bytes", config.max_bytes)
    put_if_present!(d, "discard", config.discard)
    put_if_present!(d, "discard_new_per_subject", config.discard_new_per_subject)
    put_if_present!(d, "max_age", config.max_age)
    put_if_present!(d, "max_msgs_per_subject", config.max_msgs_per_subject)
    put_if_present!(d, "max_msg_size", config.max_msg_size)
    put_if_present!(d, "no_ack", config.no_ack)
    put_if_present!(d, "duplicate_window", config.duplicate_window)
    put_if_present!(d, "deny_delete", config.deny_delete)
    put_if_present!(d, "deny_purge", config.deny_purge)
    put_if_present!(d, "allow_rollup_hdrs", config.allow_rollup_hdrs)
    put_if_present!(d, "compression", config.compression)
    put_if_present!(d, "first_seq", config.first_seq)
    put_if_present!(d, "placement", config.placement)
    put_if_present!(d, "mirror", config.mirror)
    put_if_present!(d, "sources", config.sources)
    put_if_present!(d, "sealed", config.sealed)
    put_if_present!(d, "subject_transform", config.subject_transform)
    put_if_present!(d, "republish", config.republish)
    put_if_present!(d, "mirror_direct", config.mirror_direct)
    put_if_present!(d, "consumer_limits", config.consumer_limits)
    put_if_present!(d, "metadata", config.metadata)
    put_if_present!(d, "allow_msg_ttl", config.allow_msg_ttl)
    put_if_present!(d, "subject_delete_marker_ttl", config.subject_delete_marker_ttl)
    put_if_present!(d, "allow_msg_counter", config.allow_msg_counter)
    put_if_present!(d, "allow_atomic", config.allow_atomic_publish)
    put_if_present!(d, "allow_msg_schedules", config.allow_msg_schedules)
    put_if_present!(d, "persist_mode", config.persist_mode)
    put_if_present!(d, "allow_batched", config.allow_batch_publish)
    return d
end

function throw_if_api_error(obj)
    if haskey(obj, :error)
        err = obj.error
        throw(JetStreamError(
            Base.get(err, :code, 0),
            Base.get(err, :err_code, 0),
            String(Base.get(err, :description, "")),
        ))
    end
    return obj
end

function string_vector_field(obj, field::Symbol)
    (!haskey(obj, field) || obj[field] === nothing) && return String[]
    return [String(item) for item in obj[field]]
end

function object_vector_field(obj, field::Symbol)
    (!haskey(obj, field) || obj[field] === nothing) && return Any[]
    return collect(obj[field])
end

function pub_ack(obj)
    throw_if_api_error(obj)
    domain = haskey(obj, :domain) ? String(obj.domain) : nothing
    return PubAck(
        stream = String(Base.get(obj, :stream, "")),
        seq = UInt64(Base.get(obj, :seq, 0)),
        duplicate = Bool(Base.get(obj, :duplicate, false)),
        domain = domain,
    )
end

function api_request(conn::NATS.Connection, subject::AbstractString, body = UInt8[]; timeout::Real = conn.options.request_timeout)
    msg = NATS.request(conn, subject, body; timeout)
    obj = JSON3.read(NATS.payload(msg))
    return throw_if_api_error(obj)
end

function api_request_msg(conn::NATS.Connection, subject::AbstractString, body = UInt8[]; timeout::Real = conn.options.request_timeout)
    subject_s = NATS.validate_publish_subject(subject; skip = conn.options.skip_subject_validation)
    inbox = NATS.new_inbox(conn)
    sub = NATS.subscribe(conn, inbox; channel_size = 1)
    try
        NATS.unsubscribe(conn, sub; max_msgs = 1)
        NATS.publish(conn, subject_s, body; reply = inbox)
        return NATS.next_msg(sub; timeout)
    finally
        try
            NATS.unsubscribe(conn, sub)
        catch
        end
    end
end

function account_info(
    conn::NATS.Connection;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    return api_request(conn, api_subject("INFO"; api_prefix, domain), UInt8[]; timeout)
end

function create_stream(
    conn::NATS.Connection,
    config::StreamConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream = validate_stream_name(config.name)
    body = JSON3.write(config_dict(config))
    try
        return api_request(conn, api_subject("STREAM.CREATE.$stream"; api_prefix, domain), body; timeout)
    catch err
        stream_name_in_use(err) && throw(StreamNameAlreadyInUseError(stream))
        rethrow()
    end
end

function update_stream(
    conn::NATS.Connection,
    config::StreamConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream = validate_stream_name(config.name)
    body = JSON3.write(config_dict(config))
    try
        return api_request(conn, api_subject("STREAM.UPDATE.$stream"; api_prefix, domain), body; timeout)
    catch err
        stream_not_found(err) && throw(StreamNotFoundError(stream))
        rethrow()
    end
end

function create_or_update_stream(
    conn::NATS.Connection,
    config::StreamConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    try
        return update_stream(conn, config; timeout, api_prefix, domain)
    catch err
        stream_not_found(err) || rethrow()
        return create_stream(conn, config; timeout, api_prefix, domain)
    end
end

function delete_stream(
    conn::NATS.Connection,
    name::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    try
        return api_request(conn, api_subject("STREAM.DELETE.$name_s"; api_prefix, domain), UInt8[]; timeout)
    catch err
        stream_not_found(err) && throw(StreamNotFoundError(name_s))
        rethrow()
    end
end

function stream_info_body(subjects_filter::Union{Nothing, AbstractString}, deleted_details::Union{Nothing, Bool})
    subjects_filter === nothing && deleted_details === nothing && return UInt8[]
    body = Dict{String, Any}()
    subjects_filter === nothing || (body["subjects_filter"] = String(subjects_filter))
    deleted_details === nothing || (body["deleted_details"] = deleted_details)
    return JSON3.write(body)
end

function stream_info(
    conn::NATS.Connection,
    name::AbstractString;
    subjects_filter::Union{Nothing, AbstractString} = nothing,
    deleted_details::Union{Nothing, Bool} = nothing,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    body = stream_info_body(subjects_filter, deleted_details)
    name_s = validate_stream_name(name)
    try
        return api_request(conn, api_subject("STREAM.INFO.$name_s"; api_prefix, domain), body; timeout)
    catch err
        stream_not_found(err) && throw(StreamNotFoundError(name_s))
        rethrow()
    end
end

function stream_not_found(err)
    err isa StreamNotFoundError && return true
    err isa JetStreamError || return false
    err.err_code == 10059 && return true
    return err.code == 404 && occursin("stream not found", lowercase(err.description))
end

function stream_name_in_use(err)
    err isa StreamNameAlreadyInUseError && return true
    err isa JetStreamError || return false
    err.err_code == 10058 && return true
    return err.code == 400 && occursin("stream name already in use", lowercase(err.description))
end

function consumer_not_found(err)
    err isa ConsumerNotFoundError && return true
    err isa JetStreamError || return false
    err.err_code == 10014 && return true
    return err.code == 404 && occursin("consumer not found", lowercase(err.description))
end

function consumer_exists(err)
    err isa ConsumerExistsError && return true
    err isa JetStreamError || return false
    err.err_code == 10105 && return true
    err.err_code == 10148 && return true
    return err.code == 400 && occursin("consumer already exists", lowercase(err.description))
end

function consumer_does_not_exist(err)
    err isa ConsumerDoesNotExistError && return true
    err isa JetStreamError || return false
    err.err_code == 10149 && return true
    return err.code == 400 && occursin("consumer does not exist", lowercase(err.description))
end

function overlapping_filter_subjects(err)
    err isa OverlappingFilterSubjectsError && return true
    err isa JetStreamError || return false
    err.err_code == 10138 && return true
    return err.code == 500 && occursin("filter", lowercase(err.description)) && occursin("overlap", lowercase(err.description))
end

function ensure_multiple_filter_subjects_supported(info, filter_subjects)
    filter_subjects === nothing && return info
    isempty(filter_subjects) && return info
    haskey(info, :config) || throw(MultipleFilterSubjectsNotSupportedError())
    isempty(string_vector_field(info.config, :filter_subjects)) && throw(MultipleFilterSubjectsNotSupportedError())
    return info
end

function stream_subjects(
    conn::NATS.Connection,
    name::AbstractString,
    subjects_filter::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    subjects = Dict{String, UInt64}()
    offset = 0
    while true
        body = JSON3.write(Dict("subjects_filter" => String(subjects_filter), "offset" => offset))
        obj = api_request(conn, api_subject("STREAM.INFO.$name_s"; api_prefix, domain), body; timeout)
        if haskey(obj, :state) && haskey(obj.state, :subjects)
            for (subject, count) in pairs(obj.state.subjects)
                subjects[String(subject)] = UInt64(count)
            end
        end
        total = Int(Base.get(obj, :total, length(subjects)))
        total == 0 && break
        offset = length(subjects)
        offset >= total && break
    end
    return subjects
end

function stream_names(
    conn::NATS.Connection;
    subject_filter::Union{Nothing, AbstractString} = nothing,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    names = String[]
    offset = 0
    while true
        body = Dict{String, Any}("offset" => offset)
        subject_filter === nothing || (body["subject"] = String(subject_filter))
        obj = api_request(conn, api_subject("STREAM.NAMES"; api_prefix, domain), JSON3.write(body); timeout)
        page = string_vector_field(obj, :streams)
        for name in page
            push!(names, name)
        end
        total = Int(Base.get(obj, :total, length(names)))
        total == 0 && break
        offset = length(names)
        offset >= total && break
        isempty(page) && break
    end
    return names
end

function streams(
    conn::NATS.Connection;
    subject_filter::Union{Nothing, AbstractString} = nothing,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    infos = Any[]
    offset = 0
    while true
        body = Dict{String, Any}("offset" => offset)
        subject_filter === nothing || (body["subject"] = String(subject_filter))
        obj = api_request(conn, api_subject("STREAM.LIST"; api_prefix, domain), JSON3.write(body); timeout)
        page = object_vector_field(obj, :streams)
        append!(infos, page)
        total = Int(Base.get(obj, :total, length(infos)))
        total == 0 && break
        offset = length(infos)
        offset >= total && break
        isempty(page) && break
    end
    return infos
end

function stream_name_by_subject(
    conn::NATS.Connection,
    subject::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    subject_s = NATS.validate_publish_subject(subject; skip = conn.options.skip_subject_validation)
    obj = api_request(conn, api_subject("STREAM.NAMES"; api_prefix, domain), JSON3.write(Dict("subject" => subject_s)); timeout)
    streams = string_vector_field(obj, :streams)
    length(streams) == 1 || throw(NoMatchingStreamError(subject_s))
    return only(streams)
end

function stream_msg_get(
    conn::NATS.Connection,
    name::AbstractString,
    body;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    obj = api_request(conn, api_subject("STREAM.MSG.GET.$name_s"; api_prefix, domain), JSON3.write(body); timeout)
    haskey(obj, :message) || throw(JetStreamError(404, 0, "message not found"))
    return obj.message
end

function raw_msg_from_stored(stream::AbstractString, stored)
    headers = haskey(stored, :hdrs) ? header_pairs_from_base64(stored.hdrs) : Pair{String,String}[]
    return RawStreamMsg(
        stream = String(stream),
        subject = String(Base.get(stored, :subject, "")),
        seq = UInt64(Base.get(stored, :seq, 0)),
        headers = headers,
        data = haskey(stored, :data) ? data_from_base64(stored.data) : UInt8[],
        time = haskey(stored, :time) ? String(stored.time) : nothing,
    )
end

function header_value(headers::Vector{Pair{String,String}}, key::AbstractString, default = nothing)
    needle = lowercase(String(key))
    for (k, v) in headers
        lowercase(k) == needle && return v
    end
    return default
end

function direct_msg_error(msg::NATS.Msg, subject::AbstractString)
    msg.status == 503 && throw(NATS.NoRespondersError(String(subject)))
    msg.status >= 400 && throw(JetStreamError(msg.status, 0, isempty(msg.description) ? "unable to get message" : msg.description))
    return nothing
end

function raw_msg_from_direct(stream::AbstractString, subject::AbstractString, msg::NATS.Msg)
    direct_msg_error(msg, subject)
    if isempty(msg.data) && msg.status >= 300
        direct_msg_error(msg, subject)
    end
    isempty(msg.headers) && throw(JetStreamError(500, 0, "direct get response missing headers"))
    stream_header = header_value(msg.headers, "Nats-Stream")
    stream_header === nothing && throw(JetStreamError(500, 0, "direct get response missing stream header"))
    seq_header = header_value(msg.headers, "Nats-Sequence")
    seq_header === nothing && throw(JetStreamError(500, 0, "direct get response missing sequence header"))
    subject_header = header_value(msg.headers, "Nats-Subject")
    subject_header === nothing && throw(JetStreamError(500, 0, "direct get response missing subject header"))
    time_header = header_value(msg.headers, "Nats-Time-Stamp")
    time_header === nothing && throw(JetStreamError(500, 0, "direct get response missing timestamp header"))
    return RawStreamMsg(
        stream = String(stream_header),
        subject = String(subject_header),
        seq = parse(UInt64, seq_header),
        headers = copy(msg.headers),
        data = copy(msg.data),
        time = String(time_header),
    )
end

function direct_msg_get(
    conn::NATS.Connection,
    name::AbstractString,
    body;
    timeout::Real,
    api_prefix::Union{Nothing, AbstractString},
    domain::Union{Nothing, AbstractString},
)
    name_s = validate_stream_name(name)
    subject = api_subject("DIRECT.GET.$name_s"; api_prefix, domain)
    msg = api_request_msg(conn, subject, JSON3.write(body); timeout)
    return raw_msg_from_direct(name_s, subject, msg)
end

function direct_last_msg_get(
    conn::NATS.Connection,
    name::AbstractString,
    subject_filter::AbstractString;
    timeout::Real,
    api_prefix::Union{Nothing, AbstractString},
    domain::Union{Nothing, AbstractString},
)
    name_s = validate_stream_name(name)
    subject = api_subject("DIRECT.GET.$name_s.$subject_filter"; api_prefix, domain)
    msg = api_request_msg(conn, subject, UInt8[]; timeout)
    return raw_msg_from_direct(name_s, subject, msg)
end

function get_msg(
    conn::NATS.Connection,
    name::AbstractString,
    seq::Integer;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
    direct::Bool = false,
    next_subject::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    if direct
        seq >= 0 || throw(ArgumentError("message sequence must be nonnegative"))
        body = Dict{String, Any}("seq" => UInt64(seq))
        next_subject === nothing || (body["next_by_subj"] = String(next_subject))
        return direct_msg_get(conn, name_s, body; timeout, api_prefix, domain)
    end
    seq >= 1 || throw(ArgumentError("message sequence must be at least 1"))
    return raw_msg_from_stored(name_s, stream_msg_get(conn, name_s, Dict("seq" => UInt64(seq)); timeout, api_prefix, domain))
end

function get_last_msg(
    conn::NATS.Connection,
    name::AbstractString,
    subject::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
    direct::Bool = false,
)
    name_s = validate_stream_name(name)
    direct && return direct_last_msg_get(conn, name_s, String(subject); timeout, api_prefix, domain)
    return raw_msg_from_stored(name_s, stream_msg_get(conn, name_s, Dict("last_by_subj" => String(subject)); timeout, api_prefix, domain))
end

function delete_msg(
    conn::NATS.Connection,
    name::AbstractString,
    seq::Integer;
    secure::Bool = false,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    seq >= 1 || throw(ArgumentError("message sequence must be at least 1"))
    body = Dict{String, Any}("seq" => UInt64(seq))
    secure || (body["no_erase"] = true)
    return api_request(conn, api_subject("STREAM.MSG.DELETE.$name_s"; api_prefix, domain), JSON3.write(body); timeout)
end

secure_delete_msg(conn::NATS.Connection, name::AbstractString, seq::Integer; kwargs...) =
    delete_msg(conn, name, seq; secure = true, kwargs...)

function purge_stream(
    conn::NATS.Connection,
    name::AbstractString;
    subject_filter::Union{Nothing, AbstractString} = nothing,
    seq::Union{Nothing, Integer} = nothing,
    keep::Union{Nothing, Integer} = nothing,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    name_s = validate_stream_name(name)
    body = Dict{String, Any}()
    subject_filter === nothing || (body["filter"] = String(subject_filter))
    seq !== nothing && seq < 1 && throw(ArgumentError("purge sequence must be at least 1"))
    keep !== nothing && keep < 0 && throw(ArgumentError("purge keep must be nonnegative"))
    seq !== nothing && keep !== nothing && throw(ArgumentError("purge seq and keep cannot both be provided"))
    seq === nothing || (body["seq"] = UInt64(seq))
    keep === nothing || (body["keep"] = UInt64(keep))
    payload = isempty(body) ? UInt8[] : JSON3.write(body)
    return api_request(conn, api_subject("STREAM.PURGE.$name_s"; api_prefix, domain), payload; timeout)
end

function duration_header(value)
    value isa AbstractString && return String(value)
    value isa Integer && return "$(value)ns"
    value isa Real && return "$(Float64(value))s"
    throw(ArgumentError("duration header value must be a string, integer nanoseconds, or real seconds"))
end

function publish_headers(
    headers::Vector{Pair{String,String}};
    msg_id::Union{Nothing, AbstractString} = nothing,
    expected_stream::Union{Nothing, AbstractString} = nothing,
    expected_last_seq::Union{Nothing, Integer} = nothing,
    expected_last_subject_seq::Union{Nothing, Integer} = nothing,
    expected_last_msg_id::Union{Nothing, AbstractString} = nothing,
    msg_ttl = nothing,
    schedule::Union{Nothing, AbstractString} = nothing,
    schedule_target::Union{Nothing, AbstractString} = nothing,
    schedule_source::Union{Nothing, AbstractString} = nothing,
    schedule_ttl = nothing,
    schedule_timezone::Union{Nothing, AbstractString} = nothing,
)
    out = copy(headers)
    msg_id === nothing || push!(out, "Nats-Msg-Id" => String(msg_id))
    expected_stream === nothing || push!(out, "Nats-Expected-Stream" => String(expected_stream))
    expected_last_seq === nothing || push!(out, "Nats-Expected-Last-Sequence" => string(expected_last_seq))
    expected_last_subject_seq === nothing || push!(out, "Nats-Expected-Last-Subject-Sequence" => string(expected_last_subject_seq))
    expected_last_msg_id === nothing || push!(out, "Nats-Expected-Last-Msg-Id" => String(expected_last_msg_id))
    msg_ttl === nothing || push!(out, "Nats-TTL" => duration_header(msg_ttl))
    schedule === nothing || push!(out, "Nats-Schedule" => String(schedule))
    schedule_target === nothing || push!(out, "Nats-Schedule-Target" => String(schedule_target))
    schedule_source === nothing || push!(out, "Nats-Schedule-Source" => String(schedule_source))
    schedule_ttl === nothing || push!(out, "Nats-Schedule-TTL" => duration_header(schedule_ttl))
    schedule_timezone === nothing || push!(out, "Nats-Schedule-Time-Zone" => String(schedule_timezone))
    return out
end

function validate_publish_retry_controls(retry_attempts::Integer, retry_wait::Real)
    retry_attempts >= 0 || throw(ArgumentError("retry_attempts must be nonnegative"))
    retry_wait > 0 || throw(ArgumentError("retry_wait must be positive"))
    return nothing
end

function request_publish_ack(
    conn::NATS.Connection,
    subject::AbstractString,
    data,
    headers::Vector{Pair{String,String}},
    timeout::Real,
    retry_attempts::Integer,
    retry_wait::Real,
)
    deadline = time() + Float64(timeout)
    attempts = 0
    while true
        remaining = deadline - time()
        remaining <= 0 && throw(NATS.ConnectionTimeoutError("publish", Float64(timeout)))
        try
            return NATS.request(conn, subject, data; timeout = remaining, headers)
        catch err
            if err isa NATS.NoRespondersError
                attempts >= retry_attempts && throw(NoStreamResponseError(String(subject)))
                attempts += 1
                sleep_time = min(Float64(retry_wait), deadline - time())
                sleep_time <= 0 && throw(NATS.ConnectionTimeoutError("publish", Float64(timeout)))
                sleep(sleep_time)
                continue
            end
            rethrow()
        end
    end
end

function publish(
    conn::NATS.Connection,
    subject::AbstractString,
    data = nothing;
    timeout::Real = conn.options.request_timeout,
    headers::Vector{Pair{String,String}} = Pair{String,String}[],
    msg_id::Union{Nothing, AbstractString} = nothing,
    expected_stream::Union{Nothing, AbstractString} = nothing,
    expected_last_seq::Union{Nothing, Integer} = nothing,
    expected_last_subject_seq::Union{Nothing, Integer} = nothing,
    expected_last_msg_id::Union{Nothing, AbstractString} = nothing,
    msg_ttl = nothing,
    schedule::Union{Nothing, AbstractString} = nothing,
    schedule_target::Union{Nothing, AbstractString} = nothing,
    schedule_source::Union{Nothing, AbstractString} = nothing,
    schedule_ttl = nothing,
    schedule_timezone::Union{Nothing, AbstractString} = nothing,
    retry_attempts::Integer = DEFAULT_PUBLISH_RETRY_ATTEMPTS,
    retry_wait::Real = DEFAULT_PUBLISH_RETRY_WAIT,
)
    validate_publish_retry_controls(retry_attempts, retry_wait)
    hdr = publish_headers(
        headers;
        msg_id,
        expected_stream,
        expected_last_seq,
        expected_last_subject_seq,
        expected_last_msg_id,
        msg_ttl,
        schedule,
        schedule_target,
        schedule_source,
        schedule_ttl,
        schedule_timezone,
    )
    msg = request_publish_ack(conn, subject, data, hdr, timeout, retry_attempts, retry_wait)
    return pub_ack(JSON3.read(NATS.payload(msg)))
end

function publish_async_pending(conn::NATS.Connection)
    lock(conn.lock)
    try
        return length(conn.pub_ack_tokens)
    finally
        unlock(conn.lock)
    end
end

function publish_async_complete(conn::NATS.Connection; timeout::Union{Nothing, Real} = nothing)
    if timeout === nothing
        while publish_async_pending(conn) != 0
            sleep(0.001)
        end
    else
        result = timedwait(() -> publish_async_pending(conn) == 0, timeout; pollint = 0.001)
        result == :ok || throw(NATS.ConnectionTimeoutError("publish_async_complete", Float64(timeout)))
    end
    return nothing
end

function cleanup_publisher(conn::NATS.Connection)
    NATS.abort_pub_ack_futures!(conn, PublisherClosedError())
    return nothing
end

function validate_publish_async_controls(max_pending::Union{Nothing, Integer}, stall_wait::Real, retry_attempts::Integer, retry_wait::Real)
    max_pending === nothing || max_pending >= 1 || throw(ArgumentError("max_pending must be at least 1"))
    stall_wait > 0 || throw(ArgumentError("stall_wait must be positive"))
    retry_attempts >= 0 || throw(ArgumentError("retry_attempts must be nonnegative"))
    retry_wait > 0 || throw(ArgumentError("retry_wait must be positive"))
    return nothing
end

function register_pub_ack_future(
    conn::NATS.Connection;
    max_pending::Union{Nothing, Integer} = nothing,
    stall_wait::Real = 0.2,
    retry_attempts::Integer = 2,
    retry_wait::Real = 0.25,
)
    validate_publish_async_controls(max_pending, stall_wait, retry_attempts, retry_wait)
    prefix = NATS.ensure_request_mux(conn)
    deadline = time() + Float64(stall_wait)
    while true
        token = NATS.request_token(conn)
        reply = "$prefix.$token"
        ch = Channel{Any}(2)
        registered = false
        lock(conn.lock)
        try
            if max_pending === nothing || length(conn.pub_ack_tokens) < Int(max_pending)
                conn.request_map[token] = ch
                push!(conn.pub_ack_tokens, token)
                registered = true
            end
        finally
            unlock(conn.lock)
        end
        registered && return token, reply, ch
        close(ch)
        remaining = deadline - time()
        remaining <= 0 && throw(NATS.TooManyStalledMsgsError(Int(max_pending), Float64(stall_wait)))
        sleep(min(remaining, 0.001))
    end
end

function cleanup_pub_ack_future!(conn::NATS.Connection, token::String, ch::Channel)
    lock(conn.lock)
    try
        delete!(conn.request_map, token)
        delete!(conn.pub_ack_tokens, token)
    finally
        unlock(conn.lock)
    end
    isopen(ch) && close(ch)
    return nothing
end

is_no_stream_response(msg::NATS.Msg) = msg.status == 503

function publish_async_error!(conn::NATS.Connection, cb::Union{Nothing, Function}, msg::AsyncPublishMessage, err)
    cb === nothing && return nothing
    try
        cb(conn, msg, err)
    catch callback_err
        NATS.notify_error!(conn, callback_err)
    end
    return nothing
end

function republish_async_msg(conn::NATS.Connection, msg::AsyncPublishMessage, reply::AbstractString)
    NATS.publish(conn, msg.subject, msg.data; reply, headers = msg.headers)
    return nothing
end

function async_ack_result(msg::NATS.Msg)
    if msg.status >= 400
        throw(JetStreamError(msg.status, 0, msg.description))
    end
    return pub_ack(JSON3.read(NATS.payload(msg)))
end

function pub_ack_task(
    conn::NATS.Connection,
    msg::AsyncPublishMessage,
    reply::String,
    token::String,
    ch::Channel,
    timeout::Real;
    retry_attempts::Integer,
    retry_wait::Real,
    error_cb::Union{Nothing, Function},
)
    return @async begin
        try
            retries = 0
            while true
                result = timedwait(() -> isready(ch) || !isopen(ch), timeout; pollint = 0.001)
                if result != :ok
                    err = NATS.ConnectionTimeoutError("publish_async", Float64(timeout))
                    publish_async_error!(conn, error_cb, msg, err)
                    throw(err)
                end
                if !isready(ch)
                    err = NATS.ConnectionClosedError("publish ack inbox closed")
                    publish_async_error!(conn, error_cb, msg, err)
                    throw(err)
                end
                ack_or_err = take!(ch)
                if ack_or_err isa Exception
                    publish_async_error!(conn, error_cb, msg, ack_or_err)
                    throw(ack_or_err)
                elseif !(ack_or_err isa NATS.Msg)
                    err = NATS.ProtocolError("unexpected async publish acknowledgement: $(typeof(ack_or_err))")
                    publish_async_error!(conn, error_cb, msg, err)
                    throw(err)
                end
                ack_msg = ack_or_err::NATS.Msg
                if is_no_stream_response(ack_msg)
                    if retries < retry_attempts
                        retries += 1
                        sleep(Float64(retry_wait))
                        try
                            republish_async_msg(conn, msg, reply)
                        catch err
                            publish_async_error!(conn, error_cb, msg, err)
                            rethrow()
                        end
                        continue
                    end
                    err = NoStreamResponseError(msg.subject)
                    publish_async_error!(conn, error_cb, msg, err)
                    throw(err)
                end
                try
                    return async_ack_result(ack_msg)
                catch err
                    publish_async_error!(conn, error_cb, msg, err)
                    rethrow()
                end
            end
        finally
            cleanup_pub_ack_future!(conn, token, ch)
        end
    end
end

function publish_async(
    conn::NATS.Connection,
    subject::AbstractString,
    data = nothing;
    timeout::Real = conn.options.request_timeout,
    headers::Vector{Pair{String,String}} = Pair{String,String}[],
    msg_id::Union{Nothing, AbstractString} = nothing,
    expected_stream::Union{Nothing, AbstractString} = nothing,
    expected_last_seq::Union{Nothing, Integer} = nothing,
    expected_last_subject_seq::Union{Nothing, Integer} = nothing,
    expected_last_msg_id::Union{Nothing, AbstractString} = nothing,
    msg_ttl = nothing,
    schedule::Union{Nothing, AbstractString} = nothing,
    schedule_target::Union{Nothing, AbstractString} = nothing,
    schedule_source::Union{Nothing, AbstractString} = nothing,
    schedule_ttl = nothing,
    schedule_timezone::Union{Nothing, AbstractString} = nothing,
    max_pending::Union{Nothing, Integer} = nothing,
    stall_wait::Real = 0.2,
    retry_attempts::Integer = 2,
    retry_wait::Real = 0.25,
    error_cb::Union{Nothing, Function} = nothing,
)
    hdr = publish_headers(
        headers;
        msg_id,
        expected_stream,
        expected_last_seq,
        expected_last_subject_seq,
        expected_last_msg_id,
        msg_ttl,
        schedule,
        schedule_target,
        schedule_source,
        schedule_ttl,
        schedule_timezone,
    )
    msg = AsyncPublishMessage(String(subject), NATS.bytes_payload(data), hdr)
    token, reply, ch = register_pub_ack_future(conn; max_pending, stall_wait, retry_attempts, retry_wait)
    try
        republish_async_msg(conn, msg, reply)
    catch
        cleanup_pub_ack_future!(conn, token, ch)
        rethrow()
    end
    task = pub_ack_task(conn, msg, reply, token, ch, timeout; retry_attempts, retry_wait, error_cb)
    return PubAckFuture(String(subject), reply, msg, task)
end

function publish_async(conn::NATS.Connection, msg::AsyncPublishMessage; kwargs...)
    return publish_async(conn, msg.subject, msg.data; headers = msg.headers, kwargs...)
end

function wait_ack(future::PubAckFuture; timeout::Union{Nothing, Real} = nothing)
    if timeout !== nothing
        result = timedwait(() -> istaskdone(future.task), timeout; pollint = 0.001)
        result == :ok || throw(NATS.ConnectionTimeoutError("wait_ack", Float64(timeout)))
    end
    try
        return Base.fetch(future.task)
    catch err
        if err isa TaskFailedException
            stack = Base.current_exceptions(err.task)
            isempty(stack) || throw(stack[1].exception)
        end
        rethrow()
    end
end

Base.@kwdef struct ConsumerConfig
    durable_name::Union{Nothing, String} = nothing
    name::Union{Nothing, String} = durable_name
    description::Union{Nothing, String} = nothing
    deliver_policy::Union{Nothing, String} = nothing
    opt_start_seq::Union{Nothing, UInt64} = nothing
    opt_start_time::Union{Nothing, String} = nothing
    filter_subject::Union{Nothing, String} = nothing
    filter_subjects::Union{Nothing, Vector{String}} = nothing
    ack_policy::String = "explicit"
    ack_wait::Int64 = 30_000_000_000
    max_deliver::Union{Nothing, Int} = nothing
    backoff::Union{Nothing, Vector{Int64}} = nothing
    replay_policy::Union{Nothing, String} = nothing
    rate_limit_bps::Union{Nothing, UInt64} = nothing
    sample_freq::Union{Nothing, String} = nothing
    max_waiting::Union{Nothing, Int} = nothing
    max_ack_pending::Union{Nothing, Int} = nothing
    flow_control::Union{Nothing, Bool} = nothing
    idle_heartbeat::Union{Nothing, Int64} = nothing
    headers_only::Union{Nothing, Bool} = nothing
    max_batch::Union{Nothing, Int} = nothing
    max_expires::Union{Nothing, Int64} = nothing
    max_bytes::Union{Nothing, Int} = nothing
    deliver_subject::Union{Nothing, String} = nothing
    deliver_group::Union{Nothing, String} = nothing
    inactive_threshold::Union{Nothing, Int64} = nothing
    replicas::Union{Nothing, Int} = nothing
    memory_storage::Union{Nothing, Bool} = nothing
    metadata::Union{Nothing, Dict{String,String}} = nothing
    pause_until::Union{Nothing, String} = nothing
    priority_policy::Union{Nothing, String} = nothing
    priority_timeout::Union{Nothing, Int64} = nothing
    priority_groups::Union{Nothing, Vector{String}} = nothing
end

Base.@kwdef struct OrderedConsumerConfig
    filter_subject::Union{Nothing, String} = nothing
    filter_subjects::Union{Nothing, Vector{String}} = nothing
    deliver_policy::String = "all"
    opt_start_seq::Union{Nothing, UInt64} = nothing
    opt_start_time::Union{Nothing, String} = nothing
    replay_policy::Union{Nothing, String} = nothing
    inactive_threshold::Int64 = 300_000_000_000
    headers_only::Bool = false
    max_reset_attempts::Int = 3
    metadata::Union{Nothing, Dict{String,String}} = nothing
    name_prefix::Union{Nothing, String} = nothing
end

mutable struct OrderedConsumer
    connection::NATS.Connection
    stream::String
    api_prefix::String
    config::OrderedConsumerConfig
    name_prefix::String
    serial::Int
    consumer::Union{Nothing, String}
    info::Any
    pull::Union{Nothing, PullSubscription}
    last_stream_seq::UInt64
    last_consumer_seq::UInt64
    operation_mode::Union{Nothing, Symbol}
    operation_running::Bool
    lock::ReentrantLock
    closed::Bool
end

function config_dict(config::ConsumerConfig)
    filter_subject, filter_subjects = consumer_filter_subjects(config)
    d = Dict{String, Any}(
        "ack_policy" => config.ack_policy,
        "ack_wait" => config.ack_wait,
    )
    put_if_nonempty!(d, "durable_name", config.durable_name)
    put_if_nonempty!(d, "name", config.name)
    put_if_present!(d, "description", config.description)
    put_if_present!(d, "deliver_policy", config.deliver_policy)
    put_if_present!(d, "opt_start_seq", config.opt_start_seq)
    put_if_present!(d, "opt_start_time", config.opt_start_time)
    put_if_present!(d, "filter_subject", filter_subject)
    put_if_present!(d, "filter_subjects", filter_subjects)
    put_if_present!(d, "max_deliver", config.max_deliver)
    put_if_present!(d, "backoff", config.backoff)
    put_if_present!(d, "replay_policy", config.replay_policy)
    put_if_present!(d, "rate_limit_bps", config.rate_limit_bps)
    put_if_present!(d, "sample_freq", config.sample_freq)
    put_if_present!(d, "max_waiting", config.max_waiting)
    put_if_present!(d, "max_ack_pending", config.max_ack_pending)
    put_if_present!(d, "flow_control", config.flow_control)
    put_if_present!(d, "idle_heartbeat", config.idle_heartbeat)
    put_if_present!(d, "headers_only", config.headers_only)
    put_if_present!(d, "max_batch", config.max_batch)
    put_if_present!(d, "max_expires", config.max_expires)
    put_if_present!(d, "max_bytes", config.max_bytes)
    put_if_present!(d, "deliver_subject", config.deliver_subject)
    put_if_present!(d, "deliver_group", config.deliver_group)
    put_if_present!(d, "inactive_threshold", config.inactive_threshold)
    put_if_present!(d, "num_replicas", config.replicas)
    put_if_present!(d, "mem_storage", config.memory_storage)
    put_if_present!(d, "metadata", config.metadata)
    put_if_present!(d, "pause_until", config.pause_until)
    put_if_present!(d, "priority_policy", config.priority_policy)
    put_if_present!(d, "priority_timeout", config.priority_timeout)
    put_if_present!(d, "priority_groups", config.priority_groups)
    return d
end

function consumer_name(config::ConsumerConfig)
    consumer = config.name
    if consumer === nothing || isempty(consumer)
        consumer = config.durable_name
    end
    if consumer === nothing || isempty(consumer)
        consumer = randstring(20)
    end
    return validate_consumer_name(consumer)
end

function consumer_create_subject(
    stream::AbstractString,
    consumer::AbstractString,
    config::ConsumerConfig;
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    filter_subject, filter_subjects = consumer_filter_subjects(config)
    if filter_subject !== nothing && filter_subjects === nothing
        return api_subject("CONSUMER.CREATE.$stream.$consumer.$filter_subject"; api_prefix, domain)
    end
    return api_subject("CONSUMER.CREATE.$stream.$consumer"; api_prefix, domain)
end

function consumer_config_body(stream::AbstractString, config, action::Union{Nothing, AbstractString})
    body = Dict{String, Any}("stream_name" => String(stream), "config" => config)
    action === nothing || (body["action"] = String(action))
    return JSON3.write(body)
end

consumer_config_body(stream::AbstractString, config::ConsumerConfig, action::Union{Nothing, AbstractString}) =
    consumer_config_body(stream, config_dict(config), action)

function upsert_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig,
    action::Union{Nothing, AbstractString};
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer = consumer_name(config)
    _, filter_subjects = consumer_filter_subjects(config)
    body = consumer_config_body(stream_s, config, action)
    try
        info = api_request(conn, consumer_create_subject(stream_s, consumer, config; api_prefix, domain), body; timeout)
        return ensure_multiple_filter_subjects_supported(info, filter_subjects)
    catch err
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        overlapping_filter_subjects(err) && throw(OverlappingFilterSubjectsError())
        consumer_exists(err) && throw(ConsumerExistsError(stream_s, consumer))
        consumer_does_not_exist(err) && throw(ConsumerDoesNotExistError(stream_s, consumer))
        rethrow()
    end
end

function create_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    return upsert_consumer(conn, stream, config, "create"; timeout, api_prefix, domain)
end

function update_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    return upsert_consumer(conn, stream, config, "update"; timeout, api_prefix, domain)
end

create_or_update_consumer(conn::NATS.Connection, stream::AbstractString, config::ConsumerConfig; kwargs...) =
    upsert_consumer(conn, stream, config, ""; kwargs...)

function consumer_info(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    try
        return api_request(conn, api_subject("CONSUMER.INFO.$stream_s.$consumer_s"; api_prefix, domain), UInt8[]; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
end

function ensure_consumer_exists(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    timeout::Real,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    consumer_info(conn, stream, consumer; timeout, api_prefix, domain)
    return nothing
end

function delete_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    try
        return api_request(conn, api_subject("CONSUMER.DELETE.$stream_s.$consumer_s"; api_prefix, domain), UInt8[]; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
end

function pause_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString,
    pause_until::Union{Nothing, AbstractString};
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    ensure_consumer_exists(conn, stream_s, consumer_s; timeout, api_prefix, domain)
    body = pause_until === nothing ? UInt8[] : JSON3.write(Dict("pause_until" => String(pause_until)))
    try
        return api_request(conn, api_subject("CONSUMER.PAUSE.$stream_s.$consumer_s"; api_prefix, domain), body; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
end

resume_consumer(conn::NATS.Connection, stream::AbstractString, consumer::AbstractString; kwargs...) =
    pause_consumer(conn, stream, consumer, nothing; kwargs...)

function reset_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    ensure_consumer_exists(conn, stream_s, consumer_s; timeout, api_prefix, domain)
    body = JSON3.write(Dict{String, Any}())
    resp = try
        api_request(conn, api_subject("CONSUMER.RESET.$stream_s.$consumer_s"; api_prefix, domain), body; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
    haskey(resp, :reset_seq) || throw(JetStreamError(500, 0, "consumer reset response missing reset_seq"))
    return resp
end

function reset_consumer_to_sequence(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString,
    seq::Integer;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    seq >= 1 || throw(ArgumentError("consumer reset sequence must be at least 1"))
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    ensure_consumer_exists(conn, stream_s, consumer_s; timeout, api_prefix, domain)
    body = JSON3.write(Dict("seq" => UInt64(seq)))
    resp = try
        api_request(conn, api_subject("CONSUMER.RESET.$stream_s.$consumer_s"; api_prefix, domain), body; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
    haskey(resp, :reset_seq) || throw(JetStreamError(500, 0, "consumer reset response missing reset_seq"))
    return resp
end

function unpin_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString,
    group::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    ensure_consumer_exists(conn, stream_s, consumer_s; timeout, api_prefix, domain)
    body = JSON3.write(Dict("group" => String(group)))
    try
        return api_request(conn, api_subject("CONSUMER.UNPIN.$stream_s.$consumer_s"; api_prefix, domain), body; timeout)
    catch err
        consumer_not_found(err) && throw(ConsumerNotFoundError(stream_s, consumer_s))
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        rethrow()
    end
end

function consumer_names(
    conn::NATS.Connection,
    stream::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    names = String[]
    offset = 0
    while true
        obj = try
            api_request(conn, api_subject("CONSUMER.NAMES.$stream_s"; api_prefix, domain), JSON3.write(Dict("offset" => offset)); timeout)
        catch err
            stream_not_found(err) && throw(StreamNotFoundError(stream_s))
            rethrow()
        end
        page = haskey(obj, :consumers) ? obj.consumers : String[]
        for name in page
            push!(names, String(name))
        end
        total = Int(Base.get(obj, :total, length(names)))
        total == 0 && break
        offset = length(names)
        offset >= total && break
        isempty(page) && break
    end
    sort!(names)
    return names
end

function consumers(
    conn::NATS.Connection,
    stream::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    infos = Any[]
    offset = 0
    while true
        obj = try
            api_request(conn, api_subject("CONSUMER.LIST.$stream_s"; api_prefix, domain), JSON3.write(Dict("offset" => offset)); timeout)
        catch err
            stream_not_found(err) && throw(StreamNotFoundError(stream_s))
            rethrow()
        end
        page = haskey(obj, :consumers) ? obj.consumers : Any[]
        append!(infos, page)
        total = Int(Base.get(obj, :total, length(infos)))
        total == 0 && break
        offset = length(infos)
        offset >= total && break
        isempty(page) && break
    end
    return infos
end

function check_ordered_config(config::OrderedConsumerConfig)
    if config.filter_subject !== nothing && config.filter_subjects !== nothing
        throw(ArgumentError("filter_subject and filter_subjects are mutually exclusive"))
    end
    config.inactive_threshold > 0 || throw(ArgumentError("inactive_threshold must be positive"))
    return config
end

ordered_name_prefix(config::OrderedConsumerConfig) =
    config.name_prefix === nothing ? "OC$(randstring(12))" : String(config.name_prefix)

function ordered_filters(config::OrderedConsumerConfig)
    if config.filter_subject !== nothing
        return String(config.filter_subject), nothing
    elseif config.filter_subjects !== nothing
        subjects = String.(config.filter_subjects)
        if length(subjects) == 1
            return first(subjects), nothing
        end
        return nothing, subjects
    end
    return nothing, nothing
end

function ordered_consumer_config_locked(oc::OrderedConsumer)
    oc.serial += 1
    name = "$(oc.name_prefix)_$(oc.serial)"
    filter_subject, filter_subjects = ordered_filters(oc.config)
    deliver_policy = "by_start_sequence"
    opt_start_seq = oc.last_stream_seq == 0 ? something(oc.config.opt_start_seq, UInt64(1)) : oc.last_stream_seq + UInt64(1)
    opt_start_time = nothing
    replay_policy = nothing

    if oc.last_stream_seq == 0
        deliver_policy = oc.config.deliver_policy
        if deliver_policy in ("all", "last", "new", "last_per_subject")
            opt_start_seq = nothing
            if deliver_policy == "last_per_subject" && filter_subject === nothing && filter_subjects === nothing
                filter_subjects = [">"]
            end
        elseif deliver_policy == "by_start_time"
            opt_start_seq = nothing
            opt_start_time = oc.config.opt_start_time
        elseif deliver_policy == "by_start_sequence"
            opt_start_time = nothing
        else
            throw(ArgumentError("unsupported ordered consumer deliver_policy: $deliver_policy"))
        end
        replay_policy = oc.config.replay_policy
    end

    return ConsumerConfig(
        name = name,
        deliver_policy = deliver_policy,
        opt_start_seq = opt_start_seq,
        opt_start_time = opt_start_time,
        filter_subject = filter_subject,
        filter_subjects = filter_subjects,
        ack_policy = "none",
        max_deliver = -1,
        replay_policy = replay_policy,
        max_waiting = 512,
        inactive_threshold = oc.config.inactive_threshold,
        headers_only = oc.config.headers_only ? true : nothing,
        replicas = 1,
        memory_storage = true,
        metadata = oc.config.metadata,
    )
end

function close_ordered_pull_locked(oc::OrderedConsumer)
    pull = oc.pull
    oc.pull = nothing
    return pull
end

function maybe_close_pull(pull)
    pull === nothing && return nothing
    try
        close(pull)
    catch
    end
    return nothing
end

function maybe_delete_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::Union{Nothing, AbstractString};
    timeout::Real,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    consumer === nothing && return nothing
    try
        delete_consumer(conn, stream, consumer; timeout, api_prefix, domain)
    catch
    end
    return nothing
end

function ack_domain_from_api_prefix(api_prefix::AbstractString)
    parts = split(rstrip(String(api_prefix), '.'), '.')
    length(parts) >= 3 || return ""
    parts[1] == "\$JS" || return ""
    parts[end] == "API" || return ""
    return parts[2] == "API" ? "" : String(parts[2])
end

function ack_none_key(conn::NATS.Connection, domain::AbstractString, stream::AbstractString, consumer::AbstractString)
    return (objectid(conn), String(domain), String(stream), String(consumer))
end

function register_ack_none_consumer!(
    conn::NATS.Connection,
    domain::AbstractString,
    stream::AbstractString,
    consumer::Union{Nothing, AbstractString},
)
    consumer === nothing && return nothing
    key = ack_none_key(conn, domain, stream, consumer)
    lock(ACK_NONE_CONSUMERS_LOCK)
    try
        ACK_NONE_CONSUMERS[key] = Base.get(ACK_NONE_CONSUMERS, key, 0) + 1
    finally
        unlock(ACK_NONE_CONSUMERS_LOCK)
    end
    return nothing
end

function unregister_ack_none_consumer!(
    conn::NATS.Connection,
    domain::AbstractString,
    stream::AbstractString,
    consumer::Union{Nothing, AbstractString},
)
    consumer === nothing && return nothing
    key = ack_none_key(conn, domain, stream, consumer)
    lock(ACK_NONE_CONSUMERS_LOCK)
    try
        count = Base.get(ACK_NONE_CONSUMERS, key, 0)
        if count <= 1
            delete!(ACK_NONE_CONSUMERS, key)
        else
            ACK_NONE_CONSUMERS[key] = count - 1
        end
    finally
        unlock(ACK_NONE_CONSUMERS_LOCK)
    end
    return nothing
end

function consumer_ack_policy(info)
    haskey(info, :config) || return ""
    cfg = info.config
    haskey(cfg, :ack_policy) || return ""
    raw = cfg.ack_policy
    raw === nothing && return ""
    return lowercase(String(raw))
end

is_ack_none_consumer_info(info) = consumer_ack_policy(info) == "none"

function reset_ordered_consumer!(
    oc::OrderedConsumer;
    timeout::Real = oc.connection.options.request_timeout,
    subscribe::Bool = false,
    channel_size::Int = oc.connection.options.subscription_channel_size,
)
    old_consumer = nothing
    old_pull = nothing
    config = nothing
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        old_consumer = oc.consumer
        old_pull = close_ordered_pull_locked(oc)
        config = ordered_consumer_config_locked(oc)
        oc.consumer = config.name
        oc.info = nothing
    finally
        unlock(oc.lock)
    end

    maybe_close_pull(old_pull)
    unregister_ack_none_consumer!(oc.connection, ack_domain_from_api_prefix(oc.api_prefix), oc.stream, old_consumer)
    maybe_delete_consumer(oc.connection, oc.stream, old_consumer; timeout, api_prefix = oc.api_prefix)

    attempts = oc.config.max_reset_attempts == 0 ? -1 : oc.config.max_reset_attempts
    attempt = 0
    delay = 0.1
    while true
        attempt += 1
        info = try
            create_or_update_consumer(oc.connection, oc.stream, config; timeout, api_prefix = oc.api_prefix)
        catch err
            if attempts >= 0 && attempt >= attempts
                rethrow()
            end
            sleep(delay)
            delay = min(delay * 2, 2.0)
            continue
        end
        pull = subscribe ? pull_subscribe(oc.connection, oc.stream, config.name; channel_size, timeout, api_prefix = oc.api_prefix) : nothing
        lock(oc.lock)
        try
            if oc.closed || oc.consumer != config.name
                maybe_close_pull(pull)
                maybe_delete_consumer(oc.connection, oc.stream, config.name; timeout, api_prefix = oc.api_prefix)
                throw(NATS.ConnectionClosedError("ordered consumer is closed"))
            end
            oc.info = info
            oc.pull = pull
            oc.last_consumer_seq = 0
            register_ack_none_consumer!(oc.connection, ack_domain_from_api_prefix(oc.api_prefix), oc.stream, config.name)
        finally
            unlock(oc.lock)
        end
        return info
    end
end

function ordered_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    config::OrderedConsumerConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    check_ordered_config(config)
    stream_s = validate_stream_name(stream)
    oc = OrderedConsumer(
        conn,
        stream_s,
        api_prefix_value(; api_prefix, domain),
        config,
        ordered_name_prefix(config),
        0,
        nothing,
        nothing,
        nothing,
        UInt64(0),
        UInt64(0),
        nothing,
        false,
        ReentrantLock(),
        false,
    )
    reset_ordered_consumer!(oc; timeout)
    return oc
end

ordered_consumer(
    conn::NATS.Connection,
    stream::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
    kwargs...,
) =
    ordered_consumer(conn, stream, OrderedConsumerConfig(; kwargs...); timeout, api_prefix, domain)

function Base.close(oc::OrderedConsumer)
    consumer = nothing
    pull = nothing
    lock(oc.lock)
    try
        oc.closed && return nothing
        oc.closed = true
        oc.operation_running = false
        consumer = oc.consumer
        oc.consumer = nothing
        pull = close_ordered_pull_locked(oc)
    finally
        unlock(oc.lock)
    end
    maybe_close_pull(pull)
    unregister_ack_none_consumer!(oc.connection, ack_domain_from_api_prefix(oc.api_prefix), oc.stream, consumer)
    maybe_delete_consumer(oc.connection, oc.stream, consumer; timeout = oc.connection.options.request_timeout, api_prefix = oc.api_prefix)
    return nothing
end

function consumer_info(oc::OrderedConsumer; timeout::Real = oc.connection.options.request_timeout)
    consumer = nothing
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        consumer = oc.consumer
    finally
        unlock(oc.lock)
    end
    consumer === nothing && throw(NATS.ConnectionClosedError("ordered consumer is not created"))
    info = consumer_info(oc.connection, oc.stream, consumer; timeout, api_prefix = oc.api_prefix)
    lock(oc.lock)
    try
        oc.consumer == consumer && (oc.info = info)
    finally
        unlock(oc.lock)
    end
    return info
end

cached_consumer_info(oc::OrderedConsumer) = oc.info

function ordered_begin_operation!(oc::OrderedConsumer, kind::Symbol)
    kind in (:fetch, :consume) || throw(ArgumentError("unknown ordered consumer operation: $kind"))
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        if oc.operation_mode === nothing
            oc.operation_mode = kind
        elseif oc.operation_mode != kind
            kind === :fetch && throw(OrderedConsumerUsedAsConsumeError())
            throw(OrderedConsumerUsedAsFetchError())
        end
        oc.operation_running && throw(OrderedConsumerConcurrentRequestsError())
        oc.operation_running = true
    finally
        unlock(oc.lock)
    end
    return nothing
end

function ordered_end_operation!(oc::OrderedConsumer)
    lock(oc.lock)
    try
        oc.operation_running = false
    finally
        unlock(oc.lock)
    end
    return nothing
end

function check_pull_request_options(
    batch::Integer,
    expires_ns::Integer,
    max_bytes::Union{Nothing, Integer},
    min_pending::Union{Nothing, Integer},
    min_ack_pending::Union{Nothing, Integer},
    priority::Union{Nothing, Integer},
    heartbeat_ns::Union{Nothing, Integer},
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    expires_ns > 0 || throw(ArgumentError("expires_ns must be positive"))
    max_bytes === nothing || max_bytes > 0 || throw(ArgumentError("max_bytes must be positive"))
    min_pending === nothing || min_pending >= 1 || throw(ArgumentError("min_pending must be at least 1"))
    min_ack_pending === nothing || min_ack_pending >= 1 || throw(ArgumentError("min_ack_pending must be at least 1"))
    priority === nothing || 0 <= priority <= 9 || throw(ArgumentError("priority must be between 0 and 9"))
    heartbeat_ns === nothing || heartbeat_ns >= 0 || throw(ArgumentError("heartbeat_ns must be nonnegative"))
    if heartbeat_ns !== nothing && heartbeat_ns > 0 && expires_ns < 2 * heartbeat_ns
        throw(ArgumentError("expires_ns must be at least twice heartbeat_ns"))
    end
    return nothing
end

function check_pull_no_wait_heartbeat(no_wait::Bool, heartbeat_ns::Union{Nothing, Integer})
    no_wait && heartbeat_ns !== nothing && heartbeat_ns > 0 && throw(ArgumentError("heartbeat_ns cannot be used with no_wait"))
    return nothing
end

function next_request_body(;
    batch::Integer = 1,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    check_pull_request_options(batch, expires_ns, max_bytes, min_pending, min_ack_pending, priority, heartbeat_ns)
    check_pull_no_wait_heartbeat(no_wait, heartbeat_ns)
    body = Dict{String, Any}("batch" => batch)
    max_bytes === nothing || (body["max_bytes"] = Int(max_bytes))
    min_pending === nothing || (body["min_pending"] = Int64(min_pending))
    min_ack_pending === nothing || (body["min_ack_pending"] = Int64(min_ack_pending))
    priority_group === nothing || (body["group"] = String(priority_group))
    priority === nothing || (body["priority"] = Int(priority))
    heartbeat_ns === nothing || heartbeat_ns == 0 || (body["idle_heartbeat"] = Int64(heartbeat_ns))
    if no_wait
        body["no_wait"] = true
    else
        body["expires"] = expires_ns
    end
    return JSON3.write(body)
end

function normalized_status_description(msg::NATS.Msg)
    return lowercase(replace(msg.description, r"[\s_-]+" => ""))
end

function is_pull_max_bytes_terminal(msg::NATS.Msg)
    msg.status == 409 || return false
    return occursin("maxbytes", normalized_status_description(msg))
end

function is_pull_batch_completed_terminal(msg::NATS.Msg)
    msg.status == 409 || return false
    return occursin("batchcompleted", normalized_status_description(msg))
end

is_pull_terminal(msg::NATS.Msg) =
    msg.status == 404 ||
    msg.status == 408 ||
    is_pull_max_bytes_terminal(msg) ||
    is_pull_batch_completed_terminal(msg)
is_pull_control(msg::NATS.Msg) = msg.status != 200 && msg.status < 400

function pull_request_subject(stream::AbstractString, consumer::AbstractString; api_prefix::AbstractString = DEFAULT_API_PREFIX)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    return api_subject_from_prefix(api_prefix, "CONSUMER.MSG.NEXT.$stream_s.$consumer_s")
end

function next_msg(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    batch::Integer = 1,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = conn.options.request_timeout,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    if heartbeat_ns !== nothing && heartbeat_ns > 0
        messages = fetch(
            conn,
            stream,
            consumer;
            batch,
            expires_ns,
            no_wait,
            timeout,
            max_bytes,
            min_pending,
            min_ack_pending,
            priority_group,
            priority,
            heartbeat_ns,
            api_prefix,
            domain,
        )
        isempty(messages) && throw(JetStreamError(no_wait ? 404 : 408, 0, "no messages"))
        return first(messages)
    end
    subject = pull_request_subject(stream, consumer; api_prefix = api_prefix_value(; api_prefix, domain))
    msg = NATS.request(
        conn,
        subject,
        next_request_body(; batch, expires_ns, no_wait, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns);
        timeout,
        mux = false,
    )
    if is_pull_terminal(msg)
        throw(JetStreamError(msg.status, 0, msg.description))
    elseif msg.status >= 400
        throw(JetStreamError(msg.status, 0, msg.description))
    end
    return msg
end

function collect_pull_messages(
    sub::NATS.Subscription,
    batch::Integer,
    timeout::Real;
    return_terminal::Bool = false,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    messages = NATS.Msg[]
    deadline = time() + Float64(timeout)
    heartbeat_timeout = heartbeat_ns === nothing || heartbeat_ns == 0 ? nothing : 2 * Float64(heartbeat_ns) / 1_000_000_000
    while length(messages) < batch
        remaining = deadline - time()
        if remaining <= 0
            isempty(messages) && throw(NATS.ConnectionTimeoutError("fetch", Float64(timeout)))
            break
        end
        read_timeout = heartbeat_timeout === nothing ? remaining : min(remaining, heartbeat_timeout)
        msg = try
            NATS.next_msg(sub; timeout = read_timeout)
        catch err
            if err isa NATS.ConnectionTimeoutError
                heartbeat_timeout !== nothing && heartbeat_timeout < remaining && throw(NoHeartbeatError())
                !isempty(messages) && break
            end
            rethrow()
        end
        if is_pull_terminal(msg)
            return return_terminal ? (messages, msg) : messages
        elseif is_pull_control(msg)
            continue
        elseif msg.status >= 400
            throw(JetStreamError(msg.status, 0, msg.description))
        end
        push!(messages, msg)
    end
    return return_terminal ? (messages, nothing) : messages
end

function fetch(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    batch::Integer = 1,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = conn.options.request_timeout,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    subject = pull_request_subject(stream, consumer; api_prefix = api_prefix_value(; api_prefix, domain))
    inbox = NATS.new_inbox(conn)
    sub = NATS.subscribe(conn, inbox; channel_size = Int(batch) + 1)
    try
        NATS.unsubscribe(conn, sub; max_msgs = Int(batch) + 1)
        NATS.publish(
            conn,
            subject,
            next_request_body(; batch, expires_ns, no_wait, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns);
            reply = inbox,
        )
        return collect_pull_messages(sub, batch, timeout; heartbeat_ns)
    finally
        try
            NATS.unsubscribe(conn, sub)
        catch
        end
    end
end

function fetch_bytes(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString,
    max_bytes::Integer;
    batch::Integer = DEFAULT_FETCH_BYTES_BATCH,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = conn.options.request_timeout,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    max_bytes > 0 || throw(ArgumentError("max_bytes must be positive"))
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    return fetch(
        conn,
        stream,
        consumer;
        batch,
        expires_ns,
        no_wait,
        timeout,
        max_bytes,
        min_pending,
        min_ack_pending,
        priority_group,
        priority,
        heartbeat_ns,
        api_prefix,
        domain,
    )
end

function optional_positive_int_field(obj, field::Symbol, ::Type{T} = Int) where {T <: Integer}
    haskey(obj, field) || return nothing
    raw = getproperty(obj, field)
    raw === nothing && return nothing
    value = try
        T(raw)
    catch
        return nothing
    end
    return value > 0 ? value : nothing
end

function pull_request_limits(info)
    haskey(info, :config) || return nothing, nothing, nothing
    cfg = info.config
    return (
        optional_positive_int_field(cfg, :max_batch, Int),
        optional_positive_int_field(cfg, :max_expires, Int64),
        optional_positive_int_field(cfg, :max_bytes, Int),
    )
end

function consumer_deliver_subject(info)
    haskey(info, :config) || return nothing
    cfg = info.config
    haskey(cfg, :deliver_subject) || return nothing
    raw = cfg.deliver_subject
    raw === nothing && return nothing
    deliver_subject = String(raw)
    return isempty(deliver_subject) ? nothing : deliver_subject
end

function ensure_pull_consumer(info)
    consumer_deliver_subject(info) === nothing || throw(ArgumentError("consumer is not a pull consumer"))
    return nothing
end

function register_subscription_consumer_cleanup!(
    conn::NATS.Connection,
    sub::NATS.Subscription,
    stream::AbstractString,
    consumer::AbstractString,
    api_prefix::AbstractString,
    delete_on_cleanup::Bool,
    ack_none::Bool,
)
    (delete_on_cleanup || ack_none) || return nothing
    stream_s = String(stream)
    consumer_s = String(consumer)
    prefix = String(api_prefix)
    domain = ack_domain_from_api_prefix(prefix)
    NATS.set_subscription_cleanup!(
        conn,
        sub,
        timeout -> begin
            ack_none && unregister_ack_none_consumer!(conn, domain, stream_s, consumer_s)
            delete_on_cleanup && delete_consumer(conn, stream_s, consumer_s; timeout, api_prefix = prefix)
        end,
    )
    return nothing
end

function check_pull_subscription_limits(psub::PullSubscription, batch::Integer, expires_ns::Integer, no_wait::Bool, max_bytes::Union{Nothing, Integer})
    if psub.max_request_batch !== nothing && batch > psub.max_request_batch::Int
        throw(ArgumentError("batch exceeds MaxRequestBatch of $(psub.max_request_batch)"))
    end
    if !no_wait && psub.max_request_expires !== nothing && expires_ns > psub.max_request_expires::Int64
        throw(ArgumentError("expires_ns exceeds MaxRequestExpires of $(psub.max_request_expires)ns"))
    end
    if max_bytes !== nothing && psub.max_request_max_bytes !== nothing && max_bytes > psub.max_request_max_bytes::Int
        throw(ArgumentError("max_bytes exceeds MaxRequestMaxBytes of $(psub.max_request_max_bytes)"))
    end
    return nothing
end

function pull_subscribe(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    channel_size::Int = conn.options.subscription_channel_size,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    consumer_s = validate_consumer_name(consumer)
    prefix = api_prefix_value(; api_prefix, domain)
    info = consumer_info(conn, stream_s, consumer_s; timeout, api_prefix = prefix)
    ensure_pull_consumer(info)
    max_request_batch, max_request_expires, max_request_max_bytes = pull_request_limits(info)
    inbox = NATS.new_inbox(conn)
    sub = NATS.subscribe(conn, inbox; channel_size)
    ack_none = is_ack_none_consumer_info(info)
    ack_none && register_ack_none_consumer!(conn, ack_domain_from_api_prefix(prefix), stream_s, consumer_s)
    register_subscription_consumer_cleanup!(conn, sub, stream_s, consumer_s, prefix, false, ack_none)
    return PullSubscription(
        conn,
        stream_s,
        consumer_s,
        prefix,
        inbox,
        sub,
        info,
        max_request_batch,
        max_request_expires,
        max_request_max_bytes,
        false,
        ack_none,
        ReentrantLock(),
        false,
    )
end

function pull_subscribe(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig;
    channel_size::Int = conn.options.subscription_channel_size,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    config.deliver_subject === nothing || throw(ArgumentError("pull consumers cannot have a deliver_subject"))
    prefix = api_prefix_value(; api_prefix, domain)
    info = create_consumer(conn, stream, config; timeout, api_prefix = prefix)
    consumer = haskey(info, :name) ? String(info.name) : consumer_name(config)
    try
        psub = pull_subscribe(conn, stream, consumer; channel_size, timeout, api_prefix = prefix)
        psub.delete_on_close = true
        register_subscription_consumer_cleanup!(conn, psub.subscription, psub.stream, psub.consumer, psub.api_prefix, true, psub.ack_none)
        return psub
    catch
        maybe_delete_consumer(conn, stream, consumer; timeout, api_prefix = prefix)
        rethrow()
    end
end

function create_push_consumer(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig,
    deliver_subject::AbstractString,
    deliver_group::Union{Nothing, AbstractString};
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    stream_s = validate_stream_name(stream)
    _, filter_subjects = consumer_filter_subjects(config)
    d = config_dict(config)
    d["deliver_subject"] = String(deliver_subject)
    deliver_group === nothing || (d["deliver_group"] = String(deliver_group))
    consumer = consumer_name(config)
    body = consumer_config_body(stream_s, d, "create")
    try
        info = api_request(conn, consumer_create_subject(stream_s, consumer, config; api_prefix, domain), body; timeout)
        return ensure_multiple_filter_subjects_supported(info, filter_subjects)
    catch err
        stream_not_found(err) && throw(StreamNotFoundError(stream_s))
        overlapping_filter_subjects(err) && throw(OverlappingFilterSubjectsError())
        consumer_exists(err) && throw(ConsumerExistsError(stream_s, consumer))
        consumer_does_not_exist(err) && throw(ConsumerDoesNotExistError(stream_s, consumer))
        rethrow()
    end
end

function validate_push_queue_controls(config::ConsumerConfig)
    group = config.deliver_group
    (group === nothing || isempty(group)) && return nothing
    if config.idle_heartbeat !== nothing && config.idle_heartbeat > 0
        throw(ArgumentError("queue push subscriptions cannot use idle_heartbeat"))
    end
    if config.flow_control === true
        throw(ArgumentError("queue push subscriptions cannot use flow_control"))
    end
    return nothing
end

function push_consumer_delivery(info)
    deliver_subject = consumer_deliver_subject(info)
    deliver_subject === nothing && throw(ArgumentError("consumer is not a push consumer"))
    cfg = info.config
    deliver_group = haskey(cfg, :deliver_group) ? String(cfg.deliver_group) : nothing
    return deliver_subject, deliver_group
end

function push_subscription(
    conn::NATS.Connection,
    stream::AbstractString,
    info,
    deliver_subject::AbstractString,
    deliver_group::Union{Nothing, AbstractString},
    sub::NATS.Subscription,
    api_prefix::AbstractString,
    delete_on_close::Bool,
)
    consumer = haskey(info, :name) ? String(info.name) : ""
    isempty(consumer) && throw(JetStreamError(500, 0, "push consumer info missing name"))
    stream_s = String(stream)
    prefix = String(api_prefix)
    ack_none = is_ack_none_consumer_info(info)
    ack_none && register_ack_none_consumer!(conn, ack_domain_from_api_prefix(prefix), stream_s, consumer)
    psub = PushSubscription(conn, stream_s, consumer, prefix, String(deliver_subject), deliver_group === nothing ? nothing : String(deliver_group), sub, info, delete_on_close, ack_none, ReentrantLock(), false)
    register_subscription_consumer_cleanup!(conn, sub, psub.stream, psub.consumer, psub.api_prefix, delete_on_close, ack_none)
    return psub
end

function push_subscribe(
    conn::NATS.Connection,
    stream::AbstractString,
    config::ConsumerConfig;
    channel_size::Int = conn.options.subscription_channel_size,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    prefix = api_prefix_value(; api_prefix, domain)
    deliver_subject = config.deliver_subject === nothing ? NATS.new_inbox(conn) : config.deliver_subject
    deliver_group = config.deliver_group
    validate_push_queue_controls(config)
    sub = NATS.subscribe(conn, deliver_subject; queue = deliver_group, channel_size)
    try
        info = create_push_consumer(conn, stream, config, deliver_subject, deliver_group; timeout, api_prefix = prefix)
        return push_subscription(conn, stream, info, deliver_subject, deliver_group, sub, prefix, true)
    catch
        try NATS.unsubscribe(conn, sub) catch end
        rethrow()
    end
end

function push_subscribe(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    channel_size::Int = conn.options.subscription_channel_size,
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    prefix = api_prefix_value(; api_prefix, domain)
    info = consumer_info(conn, stream, consumer; timeout, api_prefix = prefix)
    deliver_subject, deliver_group = push_consumer_delivery(info)
    sub = NATS.subscribe(conn, deliver_subject; queue = deliver_group, channel_size)
    return push_subscription(conn, stream, info, deliver_subject, deliver_group, sub, prefix, false)
end

function Base.close(psub::PushSubscription)
    should_delete = false
    ack_none = false
    lock(psub.lock)
    try
        psub.closed && return nothing
        psub.closed = true
        should_delete = psub.delete_on_close
        ack_none = psub.ack_none
        (should_delete || ack_none) && NATS.take_subscription_cleanup!(psub.connection, psub.subscription)
        try
            NATS.unsubscribe(psub.connection, psub.subscription)
        catch err
            err isa NATS.BadSubscriptionError || rethrow()
        end
    finally
        unlock(psub.lock)
    end
    try
        should_delete && delete_consumer(psub.connection, psub.stream, psub.consumer; timeout = psub.connection.options.request_timeout, api_prefix = psub.api_prefix)
    finally
        ack_none && unregister_ack_none_consumer!(psub.connection, ack_domain_from_api_prefix(psub.api_prefix), psub.stream, psub.consumer)
    end
    return nothing
end

function ensure_push_subscription_open_locked(psub::PushSubscription)
    psub.closed && throw(NATS.ConnectionClosedError("push subscription is closed"))
    return nothing
end

function consumer_info(psub::PushSubscription; timeout::Real = psub.connection.options.request_timeout)
    conn = nothing
    stream = ""
    consumer = ""
    api_prefix = ""
    lock(psub.lock)
    try
        ensure_push_subscription_open_locked(psub)
        conn = psub.connection
        stream = psub.stream
        consumer = psub.consumer
        api_prefix = psub.api_prefix
    finally
        unlock(psub.lock)
    end
    info = consumer_info(conn, stream, consumer; timeout, api_prefix)
    push_consumer_delivery(info)
    lock(psub.lock)
    try
        ensure_push_subscription_open_locked(psub)
        psub.info = info
    finally
        unlock(psub.lock)
    end
    return info
end

function cached_consumer_info(psub::PushSubscription)
    lock(psub.lock)
    try
        return psub.info
    finally
        unlock(psub.lock)
    end
end

is_push_control(msg::NATS.Msg) = msg.status != 200
is_push_flow_control(msg::NATS.Msg) = msg.status == 100 && occursin("flow", lowercase(msg.description))

function push_idle_heartbeat_ns(psub::PushSubscription)
    haskey(psub.info, :config) || return nothing
    cfg = psub.info.config
    haskey(cfg, :idle_heartbeat) || return nothing
    raw = cfg.idle_heartbeat
    raw === nothing && return nothing
    ns = try
        Int64(raw)
    catch
        return nothing
    end
    return ns > 0 ? ns : nothing
end

push_heartbeat_timeout(psub::PushSubscription) =
    (ns = push_idle_heartbeat_ns(psub); ns === nothing ? nothing : 2 * Float64(ns) / 1_000_000_000)

function handle_push_control(psub::PushSubscription, msg::NATS.Msg)
    if msg.status >= 400
        throw(JetStreamError(msg.status, 0, msg.description))
    elseif is_push_flow_control(msg) && msg.reply !== nothing
        NATS.respond(psub.connection, msg)
    end
    return nothing
end

function next_msg(psub::PushSubscription; timeout::Union{Nothing, Real} = nothing)
    psub.closed && throw(NATS.ConnectionClosedError("push subscription is closed"))
    deadline = timeout === nothing ? nothing : time() + Float64(timeout)
    heartbeat_timeout = push_heartbeat_timeout(psub)
    last_activity = time()
    while true
        now = time()
        remaining = deadline === nothing ? nothing : deadline - now
        if remaining !== nothing && remaining <= 0
            throw(NATS.ConnectionTimeoutError("push next_msg", Float64(timeout)))
        end
        heartbeat_remaining = heartbeat_timeout === nothing ? nothing : heartbeat_timeout - (now - last_activity)
        heartbeat_due = heartbeat_remaining !== nothing && heartbeat_remaining <= 0
        heartbeat_due && throw(NoHeartbeatError())
        read_for_heartbeat = heartbeat_remaining !== nothing && (remaining === nothing || heartbeat_remaining < remaining)
        read_timeout = if remaining === nothing
            heartbeat_remaining
        elseif heartbeat_remaining === nothing
            remaining
        else
            min(remaining, heartbeat_remaining)
        end
        msg = try
            NATS.next_msg(psub.subscription; timeout = read_timeout)
        catch err
            if err isa NATS.ConnectionTimeoutError && read_for_heartbeat
                throw(NoHeartbeatError())
            end
            rethrow()
        end
        last_activity = time()
        if is_push_control(msg)
            handle_push_control(psub, msg)
            continue
        end
        return msg
    end
end

function Base.close(psub::PullSubscription)
    should_delete = false
    ack_none = false
    lock(psub.lock)
    try
        psub.closed && return nothing
        psub.closed = true
        should_delete = psub.delete_on_close
        ack_none = psub.ack_none
        (should_delete || ack_none) && NATS.take_subscription_cleanup!(psub.connection, psub.subscription)
        try
            NATS.unsubscribe(psub.connection, psub.subscription)
        catch err
            err isa NATS.BadSubscriptionError || rethrow()
        end
    finally
        unlock(psub.lock)
    end
    try
        should_delete && delete_consumer(psub.connection, psub.stream, psub.consumer; timeout = psub.connection.options.request_timeout, api_prefix = psub.api_prefix)
    finally
        ack_none && unregister_ack_none_consumer!(psub.connection, ack_domain_from_api_prefix(psub.api_prefix), psub.stream, psub.consumer)
    end
    return nothing
end

function ensure_pull_subscription_open_locked(psub::PullSubscription)
    psub.closed && throw(NATS.ConnectionClosedError("pull subscription is closed"))
    return nothing
end

function consumer_info(psub::PullSubscription; timeout::Real = psub.connection.options.request_timeout)
    conn = nothing
    stream = ""
    consumer = ""
    api_prefix = ""
    lock(psub.lock)
    try
        ensure_pull_subscription_open_locked(psub)
        conn = psub.connection
        stream = psub.stream
        consumer = psub.consumer
        api_prefix = psub.api_prefix
    finally
        unlock(psub.lock)
    end
    info = consumer_info(conn, stream, consumer; timeout, api_prefix)
    ensure_pull_consumer(info)
    max_request_batch, max_request_expires, max_request_max_bytes = pull_request_limits(info)
    lock(psub.lock)
    try
        ensure_pull_subscription_open_locked(psub)
        psub.info = info
        psub.max_request_batch = max_request_batch
        psub.max_request_expires = max_request_expires
        psub.max_request_max_bytes = max_request_max_bytes
    finally
        unlock(psub.lock)
    end
    return info
end

function cached_consumer_info(psub::PullSubscription)
    lock(psub.lock)
    try
        return psub.info
    finally
        unlock(psub.lock)
    end
end

function fetch(
    psub::PullSubscription;
    batch::Integer = 1,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = psub.connection.options.request_timeout,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    lock(psub.lock)
    try
        ensure_pull_subscription_open_locked(psub)
        check_pull_subscription_limits(psub, batch, expires_ns, no_wait, max_bytes)
        NATS.publish(
            psub.connection,
            pull_request_subject(psub.stream, psub.consumer; api_prefix = psub.api_prefix),
            next_request_body(; batch, expires_ns, no_wait, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns);
            reply = psub.inbox,
        )
        return collect_pull_messages(psub.subscription, batch, timeout; heartbeat_ns)
    finally
        unlock(psub.lock)
    end
end

function fetch_bytes(
    psub::PullSubscription,
    max_bytes::Integer;
    batch::Integer = DEFAULT_FETCH_BYTES_BATCH,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = psub.connection.options.request_timeout,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    max_bytes > 0 || throw(ArgumentError("max_bytes must be positive"))
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    return fetch(
        psub;
        batch,
        expires_ns,
        no_wait,
        timeout,
        max_bytes,
        min_pending,
        min_ack_pending,
        priority_group,
        priority,
        heartbeat_ns,
    )
end

function fetch_with_terminal(
    psub::PullSubscription;
    batch::Integer = 1,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = psub.connection.options.request_timeout,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    lock(psub.lock)
    try
        ensure_pull_subscription_open_locked(psub)
        check_pull_subscription_limits(psub, batch, expires_ns, no_wait, max_bytes)
        NATS.publish(
            psub.connection,
            pull_request_subject(psub.stream, psub.consumer; api_prefix = psub.api_prefix),
            next_request_body(; batch, expires_ns, no_wait, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns);
            reply = psub.inbox,
        )
        return collect_pull_messages(psub.subscription, batch, timeout; return_terminal = true, heartbeat_ns)
    finally
        unlock(psub.lock)
    end
end

function next_msg(
    psub::PullSubscription;
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = psub.connection.options.request_timeout,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    messages = fetch(psub; batch = 1, expires_ns, no_wait, timeout, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns)
    isempty(messages) && throw(JetStreamError(no_wait ? 404 : 408, 0, "no messages"))
    return first(messages)
end

function ordered_current_consumer(oc::OrderedConsumer)
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        oc.consumer === nothing && throw(NATS.ConnectionClosedError("ordered consumer is not created"))
        return oc.consumer
    finally
        unlock(oc.lock)
    end
end

function ordered_accept_messages!(oc::OrderedConsumer, messages::Vector{NATS.Msg})
    accepted = NATS.Msg[]
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        current = oc.consumer
        expected_consumer_seq = oc.last_consumer_seq + UInt64(1)
        expected_stream_seq = oc.last_stream_seq + UInt64(1)
        for msg in messages
            meta = metadata(msg)
            current !== nothing && meta.consumer != current && continue
            if meta.sequence.consumer != expected_consumer_seq
                throw(OrderedConsumerResetError(
                    expected_consumer_seq,
                    meta.sequence.consumer,
                    expected_stream_seq,
                    meta.sequence.stream,
                    meta.consumer,
                ))
            end
            push!(accepted, msg)
            oc.last_consumer_seq = meta.sequence.consumer
            oc.last_stream_seq = meta.sequence.stream
            expected_consumer_seq = oc.last_consumer_seq + UInt64(1)
            expected_stream_seq = oc.last_stream_seq + UInt64(1)
        end
    finally
        unlock(oc.lock)
    end
    return accepted
end

function fetch(oc::OrderedConsumer; batch::Integer = 1, expires_ns::Integer = 5_000_000_000, no_wait::Bool = false, timeout::Real = oc.connection.options.request_timeout, heartbeat_ns::Union{Nothing, Integer} = nothing)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    ordered_begin_operation!(oc, :fetch)
    try
        reset_ordered_consumer!(oc; timeout)
        consumer = ordered_current_consumer(oc)
        messages = fetch(oc.connection, oc.stream, consumer; batch, expires_ns, no_wait, timeout, heartbeat_ns, api_prefix = oc.api_prefix)
        try
            return ordered_accept_messages!(oc, messages)
        catch err
            err isa OrderedConsumerResetError || rethrow()
            reset_ordered_consumer!(oc; timeout)
            rethrow()
        end
    finally
        ordered_end_operation!(oc)
    end
end

function fetch_bytes(
    oc::OrderedConsumer,
    max_bytes::Integer;
    batch::Integer = DEFAULT_FETCH_BYTES_BATCH,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = oc.connection.options.request_timeout,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    max_bytes > 0 || throw(ArgumentError("max_bytes must be positive"))
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    ordered_begin_operation!(oc, :fetch)
    try
        reset_ordered_consumer!(oc; timeout)
        consumer = ordered_current_consumer(oc)
        messages = fetch(oc.connection, oc.stream, consumer; batch, expires_ns, no_wait, timeout, max_bytes, heartbeat_ns, api_prefix = oc.api_prefix)
        try
            return ordered_accept_messages!(oc, messages)
        catch err
            err isa OrderedConsumerResetError || rethrow()
            reset_ordered_consumer!(oc; timeout)
            rethrow()
        end
    finally
        ordered_end_operation!(oc)
    end
end

function next_msg(oc::OrderedConsumer; expires_ns::Integer = 5_000_000_000, no_wait::Bool = false, timeout::Real = oc.connection.options.request_timeout, heartbeat_ns::Union{Nothing, Integer} = nothing)
    messages = fetch(oc; batch = 1, expires_ns, no_wait, timeout, heartbeat_ns)
    isempty(messages) && throw(JetStreamError(no_wait ? 404 : 408, 0, "no messages"))
    return first(messages)
end

function is_ordered_consumer_missing_terminal(msg)
    msg === nothing && return false
    msg.status == 404 || return false
    return occursin("consumer", lowercase(msg.description))
end

function consume_closed(ctx::ConsumeContext)
    lock(ctx.lock)
    try
        return ctx.closed
    finally
        unlock(ctx.lock)
    end
end

function finish_consume!(ctx::ConsumeContext)
    lock(ctx.lock)
    try
        ctx.closed = true
        isopen(ctx.messages) && close(ctx.messages)
        isopen(ctx.errors) && close(ctx.errors)
    finally
        unlock(ctx.lock)
    end
    close_owned_consumer!(ctx)
    return nothing
end

function close_owned_consumer!(ctx::ConsumeContext)
    ctx.owns_pull && close(ctx.pull)
    return nothing
end

function ordered_consume_pull(oc::OrderedConsumer)
    lock(oc.lock)
    try
        oc.closed && throw(NATS.ConnectionClosedError("ordered consumer is closed"))
        oc.pull === nothing && throw(NATS.ConnectionClosedError("ordered consumer pull subscription is not active"))
        return oc.pull
    finally
        unlock(oc.lock)
    end
end

function consume_loop(
    ctx::ConsumeContext,
    batch::Integer,
    expires_ns::Integer,
    no_wait::Bool,
    timeout::Real,
    max_bytes::Union{Nothing, Integer},
    min_pending::Union{Nothing, Integer},
    min_ack_pending::Union{Nothing, Integer},
    priority_group::Union{Nothing, AbstractString},
    priority::Union{Nothing, Integer},
    heartbeat_ns::Union{Nothing, Integer},
)
    try
        while !consume_closed(ctx)
            messages = fetch(ctx.pull; batch, expires_ns, no_wait, timeout, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns)
            for msg in messages
                consume_closed(ctx) && return nothing
                put!(ctx.messages, msg)
            end
        end
    catch err
        if !consume_closed(ctx) && !(err isa NATS.ConnectionClosedError)
            try put!(ctx.errors, err) catch end
        end
    finally
        finish_consume!(ctx)
    end
    return nothing
end

function ordered_consume_loop(ctx::ConsumeContext, oc::OrderedConsumer, batch::Integer, expires_ns::Integer, no_wait::Bool, timeout::Real, channel_size::Int)
    try
        reset_ordered_consumer!(oc; timeout, subscribe = true, channel_size = max(channel_size, Int(batch) + 1))
        while !consume_closed(ctx)
            pull = ordered_consume_pull(oc)
            messages, terminal = fetch_with_terminal(pull; batch, expires_ns, no_wait, timeout)
            if is_ordered_consumer_missing_terminal(terminal)
                reset_ordered_consumer!(oc; timeout, subscribe = true, channel_size = max(channel_size, Int(batch) + 1))
                continue
            end
            accepted = try
                ordered_accept_messages!(oc, messages)
            catch err
                if err isa OrderedConsumerResetError
                    try put!(ctx.errors, err) catch end
                    reset_ordered_consumer!(oc; timeout, subscribe = true, channel_size = max(channel_size, Int(batch) + 1))
                    continue
                end
                rethrow()
            end
            for msg in accepted
                consume_closed(ctx) && return nothing
                put!(ctx.messages, msg)
            end
        end
    catch err
        if !consume_closed(ctx) && !(err isa NATS.ConnectionClosedError)
            try put!(ctx.errors, err) catch end
        end
    finally
        finish_consume!(ctx)
        ordered_end_operation!(oc)
    end
    return nothing
end

function push_consume_loop(ctx::ConsumeContext, poll_interval::Real)
    heartbeat_timeout = push_heartbeat_timeout(ctx.pull)
    last_activity = time()
    try
        while !consume_closed(ctx)
            now = time()
            heartbeat_remaining = heartbeat_timeout === nothing ? nothing : heartbeat_timeout - (now - last_activity)
            heartbeat_remaining !== nothing && heartbeat_remaining <= 0 && throw(NoHeartbeatError())
            read_timeout = heartbeat_remaining === nothing ? poll_interval : min(poll_interval, heartbeat_remaining)
            msg = try
                NATS.next_msg(ctx.pull.subscription; timeout = read_timeout)
            catch err
                if err isa NATS.ConnectionTimeoutError
                    if heartbeat_timeout !== nothing && time() - last_activity >= heartbeat_timeout
                        throw(NoHeartbeatError())
                    end
                    continue
                end
                rethrow()
            end
            last_activity = time()
            if is_push_control(msg)
                handle_push_control(ctx.pull, msg)
                continue
            end
            consume_closed(ctx) && return nothing
            put!(ctx.messages, msg)
        end
    catch err
        if !consume_closed(ctx) && !(err isa NATS.ConnectionClosedError)
            try put!(ctx.errors, err) catch end
        end
    finally
        finish_consume!(ctx)
    end
    return nothing
end

function consume(
    psub::PullSubscription;
    batch::Integer = 100,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = psub.connection.options.request_timeout,
    channel_size::Int = max(Int(batch), 1),
    close_pull::Bool = true,
    max_bytes::Union{Nothing, Integer} = nothing,
    min_pending::Union{Nothing, Integer} = nothing,
    min_ack_pending::Union{Nothing, Integer} = nothing,
    priority_group::Union{Nothing, AbstractString} = nothing,
    priority::Union{Nothing, Integer} = nothing,
    heartbeat_ns::Union{Nothing, Integer} = nothing,
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    check_pull_request_options(batch, expires_ns, max_bytes, min_pending, min_ack_pending, priority, heartbeat_ns)
    check_pull_no_wait_heartbeat(no_wait, heartbeat_ns)
    ctx = ConsumeContext(
        psub,
        Channel{NATS.Msg}(channel_size),
        Channel{Any}(1),
        nothing,
        ReentrantLock(),
        false,
        close_pull,
    )
    ctx.task = errormonitor(@async consume_loop(ctx, batch, expires_ns, no_wait, timeout, max_bytes, min_pending, min_ack_pending, priority_group, priority, heartbeat_ns))
    return ctx
end

function consume(
    oc::OrderedConsumer;
    batch::Integer = 100,
    expires_ns::Integer = 5_000_000_000,
    no_wait::Bool = false,
    timeout::Real = oc.connection.options.request_timeout,
    channel_size::Int = max(Int(batch), 1),
    close_ordered::Bool = true,
)
    batch >= 1 || throw(ArgumentError("batch must be at least 1"))
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    ordered_begin_operation!(oc, :consume)
    try
        ctx = ConsumeContext(
            oc,
            Channel{NATS.Msg}(channel_size),
            Channel{Any}(1),
            nothing,
            ReentrantLock(),
            false,
            close_ordered,
        )
        ctx.task = errormonitor(@async ordered_consume_loop(ctx, oc, batch, expires_ns, no_wait, timeout, channel_size))
        return ctx
    catch
        ordered_end_operation!(oc)
        rethrow()
    end
end

function consume(
    psub::PushSubscription;
    poll_interval::Real = 0.1,
    channel_size::Int = psub.connection.options.subscription_channel_size,
    close_push::Bool = true,
)
    poll_interval > 0 || throw(ArgumentError("poll_interval must be positive"))
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    ctx = ConsumeContext(
        psub,
        Channel{NATS.Msg}(channel_size),
        Channel{Any}(1),
        nothing,
        ReentrantLock(),
        false,
        close_push,
    )
    ctx.task = errormonitor(@async push_consume_loop(ctx, poll_interval))
    return ctx
end

function consume(
    conn::NATS.Connection,
    stream::AbstractString,
    consumer::AbstractString;
    channel_size::Int = conn.options.subscription_channel_size,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
    kwargs...,
)
    psub = pull_subscribe(conn, stream, consumer; channel_size, api_prefix, domain)
    try
        return consume(psub; close_pull = true, kwargs...)
    catch
        close(psub)
        rethrow()
    end
end

function Base.close(ctx::ConsumeContext)
    lock(ctx.lock)
    try
        ctx.closed && return nothing
        ctx.closed = true
        isopen(ctx.messages) && close(ctx.messages)
        isopen(ctx.errors) && close(ctx.errors)
    finally
        unlock(ctx.lock)
    end
    close_owned_consumer!(ctx)
    return nothing
end

messages(ctx::ConsumeContext) = ctx.messages

errors(ctx::ConsumeContext) = ctx.errors

function next_msg(ctx::ConsumeContext; timeout::Union{Nothing, Real} = nothing)
    if timeout === nothing
        return take!(ctx.messages)
    end
    result = timedwait(() -> isready(ctx.messages) || !isopen(ctx.messages), timeout; pollint = 0.001)
    result == :ok || throw(NATS.ConnectionTimeoutError("consume next_msg", Float64(timeout)))
    isready(ctx.messages) || throw(NATS.ConnectionClosedError("consume context closed"))
    return take!(ctx.messages)
end

function Base.iterate(ctx::ConsumeContext, state = nothing)
    msg = try
        next_msg(ctx)
    catch err
        err isa NATS.ConnectionClosedError && return nothing
        rethrow()
    end
    return msg, nothing
end

function ack_payload(kind::AbstractString; delay_ns::Union{Nothing, Integer} = nothing, reason::Union{Nothing, AbstractString} = nothing)
    kind in ("+ACK", "-NAK", "+WPI", "+NXT", "+TERM") || throw(ArgumentError("unknown JetStream ack kind: $kind"))
    delay_ns !== nothing && delay_ns < 0 && throw(ArgumentError("delay_ns must be nonnegative"))
    if delay_ns !== nothing && delay_ns > 0
        kind == "-NAK" || throw(ArgumentError("delay_ns is only valid for -NAK acknowledgements"))
        return "$kind {\"delay\": $(Int64(delay_ns))}"
    elseif reason !== nothing
        kind == "+TERM" || throw(ArgumentError("reason is only valid for +TERM acknowledgements"))
        return "$kind $(String(reason))"
    end
    return String(kind)
end

function ack_subject_key(conn::NATS.Connection, reply::AbstractString)
    tokens = split(String(reply), '.')
    if length(tokens) == 9 && tokens[1] == "\$JS" && tokens[2] == "ACK"
        return ack_none_key(conn, "", tokens[3], tokens[4])
    elseif length(tokens) >= 11 && tokens[1] == "\$JS" && tokens[2] == "ACK"
        domain = tokens[3] == "_" ? "" : String(tokens[3])
        return ack_none_key(conn, domain, tokens[5], tokens[6])
    end
    return nothing
end

function is_registered_ack_none_reply(conn::NATS.Connection, reply::AbstractString)
    key = ack_subject_key(conn, reply)
    key === nothing && return false
    lock(ACK_NONE_CONSUMERS_LOCK)
    try
        return haskey(ACK_NONE_CONSUMERS, key)
    finally
        unlock(ACK_NONE_CONSUMERS_LOCK)
    end
end

function ack_reply_or_error(msg::NATS.Msg; require_ackable::Bool = false, conn::Union{Nothing, NATS.Connection} = nothing)
    msg.reply === nothing && throw(MsgNoAckReplyError())
    if require_ackable && conn !== nothing && is_registered_ack_none_reply(conn, msg.reply)
        throw(MsgNoAckReplyError())
    end
    return msg.reply
end

function ack(conn::NATS.Connection, msg::NATS.Msg, kind::AbstractString = "+ACK"; delay_ns::Union{Nothing, Integer} = nothing, reason::Union{Nothing, AbstractString} = nothing)
    reply = ack_reply_or_error(msg; require_ackable = true, conn)
    NATS.publish(conn, reply, ack_payload(kind; delay_ns, reason))
    return nothing
end

function ack_sync(conn::NATS.Connection, msg::NATS.Msg, kind::AbstractString = "+ACK"; timeout::Real = conn.options.request_timeout, delay_ns::Union{Nothing, Integer} = nothing, reason::Union{Nothing, AbstractString} = nothing)
    reply = ack_reply_or_error(msg; require_ackable = true, conn)
    NATS.request(conn, reply, ack_payload(kind; delay_ns, reason); timeout)
    return nothing
end

double_ack(conn::NATS.Connection, msg::NATS.Msg; kwargs...) =
    ack_sync(conn, msg; kwargs...)

nak(conn::NATS.Connection, msg::NATS.Msg; delay_ns::Union{Nothing, Integer} = nothing) =
    ack(conn, msg, "-NAK"; delay_ns)

nak_with_delay(conn::NATS.Connection, msg::NATS.Msg, delay_ns::Integer) =
    nak(conn, msg; delay_ns)

in_progress(conn::NATS.Connection, msg::NATS.Msg) =
    ack(conn, msg, "+WPI")

term(conn::NATS.Connection, msg::NATS.Msg; reason::Union{Nothing, AbstractString} = nothing) =
    ack(conn, msg, "+TERM"; reason)

term_with_reason(conn::NATS.Connection, msg::NATS.Msg, reason::AbstractString) =
    term(conn, msg; reason)

function parse_u64_token(token::AbstractString)
    isempty(token) && return UInt64(0)
    all(isdigit, token) || return UInt64(0)
    return parse(UInt64, token)
end

function metadata(msg::NATS.Msg)
    reply = ack_reply_or_error(msg)
    tokens = split(reply, '.')
    length(tokens) >= 9 || throw(ArgumentError("invalid JetStream ack subject: $reply"))
    tokens[1] == "\$JS" && tokens[2] == "ACK" || throw(ArgumentError("invalid JetStream ack subject: $reply"))
    if length(tokens) == 9
        domain = ""
        stream = tokens[3]
        consumer = tokens[4]
        delivered = tokens[5]
        stream_seq = tokens[6]
        consumer_seq = tokens[7]
        timestamp = tokens[8]
        pending = tokens[9]
    elseif length(tokens) >= 11
        domain = tokens[3] == "_" ? "" : tokens[3]
        stream = tokens[5]
        consumer = tokens[6]
        delivered = tokens[7]
        stream_seq = tokens[8]
        consumer_seq = tokens[9]
        timestamp = tokens[10]
        pending = tokens[11]
    else
        throw(ArgumentError("invalid JetStream ack subject: $(msg.reply)"))
    end
    return MessageMetadata(
        sequence = SequencePair(parse_u64_token(stream_seq), parse_u64_token(consumer_seq)),
        num_delivered = parse_u64_token(delivered),
        num_pending = parse_u64_token(pending),
        timestamp_ns = parse_u64_token(timestamp),
        stream = String(stream),
        consumer = String(consumer),
        domain = String(domain),
    )
end

const KV_BUCKET_PREFIX = "KV_"
const KV_OPERATION_HEADER = "KV-Operation"
const MSG_ROLLUP_HEADER = "Nats-Rollup"
const KV_DEFAULT_PURGE_DELETES_MARKER_THRESHOLD_NS = Int64(30 * 60 * 1_000_000_000)

Base.@kwdef struct KeyValueConfig
    bucket::String
    description::Union{Nothing, String} = nothing
    max_value_size::Int = -1
    history::Int = 1
    ttl_ns::Int64 = 0
    max_bytes::Int = -1
    storage::String = "file"
    replicas::Int = 1
    compression::Bool = false
    limit_marker_ttl_ns::Int64 = 0
    metadata::Union{Nothing, Dict{String,String}} = nothing
    republish::Union{Nothing, RePublish} = nothing
    mirror::Union{Nothing, StreamSource} = nothing
    sources::Union{Nothing, Vector{StreamSource}} = nothing
end

struct KeyValue
    connection::NATS.Connection
    bucket::String
    stream::String
    prefix::String
    put_prefix::String
    put_expected_stream::Union{Nothing, String}
    api_prefix::String
end

Base.@kwdef struct KeyValueEntry
    bucket::String
    key::String
    value::Vector{UInt8}
    revision::UInt64
    operation::Symbol = :put
    headers::Vector{Pair{String,String}} = Pair{String,String}[]
    created::Union{Nothing, String} = nothing
end

Base.@kwdef struct KeyValueStatus
    bucket::String
    values::UInt64
    history::Int
    ttl_ns::Int64
    bytes::UInt64
    storage::String
    replicas::Int
    compressed::Bool
    limit_marker_ttl_ns::Int64
    metadata::Dict{String,String}
    config::KeyValueConfig
    stream_info
end

mutable struct KeyValueWatcher
    key_value::KeyValue
    consumer::String
    subscription::NATS.Subscription
    updates::Channel{Union{Nothing, KeyValueEntry}}
    errors::Channel{Any}
    task::Union{Nothing, Task}
    lock::ReentrantLock
    closed::Bool
end

mutable struct KeyValueKeyLister
    watcher::KeyValueWatcher
    lock::ReentrantLock
    finished::Bool
    closed::Bool
end

value_string(entry::KeyValueEntry) = String(entry.value)

kv_stream(bucket::AbstractString) = "$(KV_BUCKET_PREFIX)$(bucket)"
kv_prefix(bucket::AbstractString) = "\$KV.$bucket."
kv_subject(bucket::AbstractString, key::AbstractString) = kv_prefix(bucket) * String(key)

function kv_source_bucket(source_name::AbstractString)
    name = String(source_name)
    return startswith(name, KV_BUCKET_PREFIX) ? name[nextind(name, 0, ncodeunits(KV_BUCKET_PREFIX) + 1):end] : name
end

kv_source_stream(source_name::AbstractString) =
    startswith(String(source_name), KV_BUCKET_PREFIX) ? String(source_name) : kv_stream(source_name)

function copy_stream_source(source::StreamSource; name::AbstractString = source.name, subject_transforms = source.subject_transforms)
    return StreamSource(
        name = String(name),
        opt_start_seq = source.opt_start_seq,
        opt_start_time = source.opt_start_time,
        filter_subject = source.filter_subject,
        subject_transforms = collect(subject_transforms),
        external = source.external,
    )
end

kv_mirror_source(source::StreamSource) = copy_stream_source(source; name = kv_source_stream(source.name))

function kv_read_prefix(bucket::AbstractString, mirror::Union{Nothing, StreamSource})
    if mirror !== nothing && mirror.external !== nothing && !isempty(mirror.external.api_prefix)
        return kv_prefix(kv_source_bucket(mirror.name))
    end
    return kv_prefix(bucket)
end

function kv_put_prefix(bucket::AbstractString, mirror::Union{Nothing, StreamSource})
    mirror === nothing && return kv_prefix(bucket)
    source_bucket = kv_source_bucket(mirror.name)
    if mirror.external !== nothing && !isempty(mirror.external.api_prefix)
        return "$(mirror.external.api_prefix).$(kv_prefix(source_bucket))"
    end
    return kv_prefix(source_bucket)
end

function kv_source_config(source::StreamSource, bucket::AbstractString)
    isempty(source.subject_transforms) || return copy_stream_source(source)
    source_bucket = kv_source_bucket(source.name)
    transforms = SubjectTransformConfig[]
    if source.external === nothing || source_bucket != String(bucket)
        push!(transforms, SubjectTransformConfig(
            source = kv_subject(source_bucket, ">"),
            destination = kv_subject(bucket, ">"),
        ))
    end
    return copy_stream_source(source; name = kv_source_stream(source.name), subject_transforms = transforms)
end

function bucket_valid(bucket::AbstractString)
    return occursin(r"^[a-zA-Z0-9_-]+$", String(bucket))
end

function key_valid(key::AbstractString)
    s = String(key)
    isempty(s) && return false
    startswith(s, ".") && return false
    endswith(s, ".") && return false
    occursin("..", s) && return false
    return occursin(r"^[-/_=\.a-zA-Z0-9]+$", s)
end

function watch_key_valid(key::AbstractString)
    s = String(key)
    isempty(s) && return false
    startswith(s, ".") && return false
    endswith(s, ".") && return false
    occursin("..", s) && return false
    occursin(r"^[-/_=\.\*>a-zA-Z0-9]+$", s) || return false
    tokens = split(s, '.')
    for (i, token) in pairs(tokens)
        if occursin('>', token)
            token == ">" && i == length(tokens) || return false
        end
        if occursin('*', token)
            token == "*" || return false
        end
    end
    return true
end

function check_bucket(bucket::AbstractString)
    bucket_valid(bucket) || throw(ArgumentError("invalid key-value bucket name: $bucket"))
    return String(bucket)
end

function check_key(key::AbstractString)
    key_valid(key) || throw(ArgumentError("invalid key-value key: $key"))
    return String(key)
end

function check_watch_key(key::AbstractString)
    watch_key_valid(key) || throw(ArgumentError("invalid key-value watch key pattern: $key"))
    return String(key)
end

function kv_config_dict(config::KeyValueConfig)
    bucket = check_bucket(config.bucket)
    1 <= config.history <= 64 || throw(ArgumentError("key-value history must be between 1 and 64"))
    config.limit_marker_ttl_ns >= 0 || throw(ArgumentError("key-value limit_marker_ttl_ns must be nonnegative"))
    d = Dict{String, Any}(
        "name" => kv_stream(bucket),
        "retention" => "limits",
        "max_msgs_per_subject" => config.history,
        "max_msgs" => -1,
        "max_consumers" => -1,
        "max_bytes" => config.max_bytes,
        "max_msg_size" => config.max_value_size,
        "storage" => config.storage,
        "num_replicas" => config.replicas,
        "allow_rollup_hdrs" => true,
        "allow_direct" => true,
        "deny_delete" => true,
        "discard" => "new",
        "duplicate_window" => min(config.ttl_ns > 0 ? config.ttl_ns : 120_000_000_000, 120_000_000_000),
    )
    config.ttl_ns > 0 && (d["max_age"] = config.ttl_ns)
    config.description === nothing || (d["description"] = config.description)
    config.compression && (d["compression"] = "s2")
    if config.limit_marker_ttl_ns > 0
        d["allow_msg_ttl"] = true
        d["subject_delete_marker_ttl"] = config.limit_marker_ttl_ns
    end
    config.metadata === nothing || (d["metadata"] = config.metadata)
    put_if_present!(d, "republish", config.republish)
    if config.mirror !== nothing
        d["mirror"] = json_value(kv_mirror_source(config.mirror))
        d["mirror_direct"] = true
    elseif config.sources !== nothing && !isempty(config.sources)
        d["subjects"] = [kv_subject(bucket, ">")]
        d["sources"] = json_value([kv_source_config(source, bucket) for source in config.sources])
    else
        d["subjects"] = [kv_subject(bucket, ">")]
    end
    return d
end

const KV_REPAIR_COMPARE_KEYS = [
    "name",
    "subjects",
    "retention",
    "storage",
    "num_replicas",
    "allow_direct",
    "description",
    "max_consumers",
    "max_msgs",
    "max_bytes",
    "discard",
    "discard_new_per_subject",
    "max_age",
    "max_msgs_per_subject",
    "max_msg_size",
    "no_ack",
    "duplicate_window",
    "deny_delete",
    "deny_purge",
    "allow_rollup_hdrs",
    "compression",
    "placement",
    "mirror",
    "sources",
    "sealed",
    "subject_transform",
    "republish",
    "mirror_direct",
    "consumer_limits",
    "metadata",
    "allow_msg_ttl",
    "subject_delete_marker_ttl",
    "allow_msg_counter",
    "allow_atomic",
    "allow_msg_schedules",
    "allow_batched",
]

const KV_REPAIR_DEFAULTS = Dict{String, Any}(
    "subjects" => Any[],
    "description" => "",
    "max_consumers" => -1,
    "max_msgs" => -1,
    "max_bytes" => -1,
    "discard" => "old",
    "discard_new_per_subject" => false,
    "max_age" => 0,
    "max_msgs_per_subject" => -1,
    "max_msg_size" => -1,
    "no_ack" => false,
    "duplicate_window" => 0,
    "deny_delete" => false,
    "deny_purge" => false,
    "allow_rollup_hdrs" => false,
    "compression" => "none",
    "placement" => nothing,
    "mirror" => nothing,
    "sources" => Any[],
    "sealed" => false,
    "subject_transform" => nothing,
    "republish" => nothing,
    "mirror_direct" => false,
    "consumer_limits" => Dict{String, Any}(),
    "metadata" => Dict{String, Any}(),
    "allow_msg_ttl" => false,
    "subject_delete_marker_ttl" => 0,
    "allow_msg_counter" => false,
    "allow_atomic" => false,
    "allow_msg_schedules" => false,
    "allow_batched" => false,
)

function jsonish(value)
    value === nothing && return nothing
    value isa AbstractString && return String(value)
    value isa Symbol && return String(value)
    value isa AbstractDict && return Dict(String(k) => jsonish(v) for (k, v) in pairs(value))
    value isa AbstractVector && return [jsonish(item) for item in value]
    if value isa JSON3.Object
        return Dict(String(k) => jsonish(v) for (k, v) in pairs(value))
    elseif value isa JSON3.Array
        return [jsonish(item) for item in value]
    end
    return value
end

function desired_config_value(config::Dict{String, Any}, key::String)
    return haskey(config, key) ? jsonish(config[key]) : deepcopy(Base.get(KV_REPAIR_DEFAULTS, key, nothing))
end

function existing_config_value(config, key::String)
    sym = Symbol(key)
    if !haskey(config, sym) || config[sym] === nothing
        return deepcopy(Base.get(KV_REPAIR_DEFAULTS, key, nothing))
    end
    return jsonish(config[sym])
end

function kv_create_repairable(existing_config, desired::Dict{String, Any})
    for key in KV_REPAIR_COMPARE_KEYS
        existing_value = existing_config_value(existing_config, key)
        desired_value = desired_config_value(desired, key)
        if key == "discard" || key == "allow_direct"
            existing_value = desired_value
        end
        existing_value == desired_value || return false
    end
    return true
end

function key_value_from_stream_info(conn::NATS.Connection, bucket::AbstractString, stream::AbstractString, prefix::AbstractString, info)
    mirror = mirror_from_json(info.config)
    return KeyValue(conn, String(bucket), String(stream), kv_read_prefix(bucket, mirror), kv_put_prefix(bucket, mirror), mirror === nothing ? String(stream) : nothing, String(prefix))
end

function create_key_value(
    conn::NATS.Connection,
    config::KeyValueConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    body = JSON3.write(kv_config_dict(config))
    bucket = check_bucket(config.bucket)
    stream = kv_stream(bucket)
    prefix = api_prefix_value(; api_prefix, domain)
    info = try
        api_request(conn, api_subject_from_prefix(prefix, "STREAM.CREATE.$stream"), body; timeout)
    catch create_err
        create_err isa JetStreamError || rethrow()
        existing = try
            stream_info(conn, stream; timeout, api_prefix = prefix)
        catch
            throw(create_err)
        end
        kv_create_repairable(existing.config, kv_config_dict(config)) || throw(create_err)
        api_request(conn, api_subject_from_prefix(prefix, "STREAM.UPDATE.$stream"), body; timeout)
    end
    return key_value_from_stream_info(conn, bucket, stream, prefix, info)
end

function update_key_value(
    conn::NATS.Connection,
    config::KeyValueConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    body = JSON3.write(kv_config_dict(config))
    bucket = check_bucket(config.bucket)
    stream = kv_stream(bucket)
    prefix = api_prefix_value(; api_prefix, domain)
    info = try
        api_request(conn, api_subject_from_prefix(prefix, "STREAM.UPDATE.$stream"), body; timeout)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(bucket))
        rethrow()
    end
    return key_value_from_stream_info(conn, bucket, stream, prefix, info)
end

function create_or_update_key_value(
    conn::NATS.Connection,
    config::KeyValueConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    try
        return create_key_value(conn, config; timeout, api_prefix, domain)
    catch err
        err isa JetStreamError || rethrow()
        return update_key_value(conn, config; timeout, api_prefix, domain)
    end
end

function key_value(
    conn::NATS.Connection,
    bucket::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    bucket_s = check_bucket(bucket)
    stream = kv_stream(bucket_s)
    prefix = api_prefix_value(; api_prefix, domain)
    info = try
        api_request(conn, api_subject_from_prefix(prefix, "STREAM.INFO.$stream"), UInt8[]; timeout)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(bucket_s))
        rethrow()
    end
    return key_value_from_stream_info(conn, bucket_s, stream, prefix, info)
end

function delete_key_value(
    conn::NATS.Connection,
    bucket::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    bucket_s = check_bucket(bucket)
    try
        return delete_stream(conn, kv_stream(bucket_s); timeout, api_prefix, domain)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(bucket_s))
        rethrow()
    end
end

function key_value_store_names(
    conn::NATS.Connection;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    names = stream_names(conn; subject_filter = "\$KV.*.>", timeout, api_prefix, domain)
    buckets = String[]
    for name in names
        startswith(name, KV_BUCKET_PREFIX) || continue
        push!(buckets, name[(ncodeunits(KV_BUCKET_PREFIX) + 1):end])
    end
    sort!(buckets)
    return buckets
end

function key_value_stores(
    conn::NATS.Connection;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    prefix = api_prefix_value(; api_prefix, domain)
    return [status(key_value(conn, bucket; timeout, api_prefix = prefix); timeout) for bucket in key_value_store_names(conn; timeout, api_prefix = prefix)]
end

function header_pairs_from_base64(encoded)
    encoded === nothing && return Pair{String,String}[]
    s = String(encoded)
    isempty(s) && return Pair{String,String}[]
    headers, _, _ = NATS.parse_header_block(Base64.base64decode(s))
    return headers
end

function data_from_base64(encoded)
    encoded === nothing && return UInt8[]
    s = String(encoded)
    isempty(s) && return UInt8[]
    return Base64.base64decode(s)
end

function stored_msg_request(kv::KeyValue, body; timeout::Real)
    return stream_msg_get(kv.connection, kv.stream, body; timeout, api_prefix = kv.api_prefix)
end

function kv_operation(headers::Vector{Pair{String,String}})
    for (k, v) in headers
        lowercase(k) == lowercase(KV_OPERATION_HEADER) || continue
        v == "DEL" && return :delete
        v == "PURGE" && return :purge
    end
    return :put
end

function entry_from_stored(kv::KeyValue, key::AbstractString, stored)
    headers = haskey(stored, :hdrs) ? header_pairs_from_base64(stored.hdrs) : Pair{String,String}[]
    op = kv_operation(headers)
    return KeyValueEntry(
        bucket = kv.bucket,
        key = String(key),
        value = haskey(stored, :data) ? data_from_base64(stored.data) : UInt8[],
        revision = UInt64(Base.get(stored, :seq, 0)),
        operation = op,
        headers = headers,
        created = haskey(stored, :time) ? String(stored.time) : nothing,
    )
end

function entry_from_raw(kv::KeyValue, key::AbstractString, raw::RawStreamMsg)
    op = kv_operation(raw.headers)
    return KeyValueEntry(
        bucket = kv.bucket,
        key = String(key),
        value = copy(raw.data),
        revision = raw.seq,
        operation = op,
        headers = copy(raw.headers),
        created = raw.time,
    )
end

function direct_get_unavailable(err)
    err isa NATS.NoRespondersError && return true
    err isa JetStreamError && err.code == 503 && return true
    return false
end

function kv_message_not_found(err)
    err isa JetStreamError || return false
    err.code == 404 && return true
    return occursin("message not found", lowercase(err.description))
end

function latest_entry(kv::KeyValue, key_s::AbstractString; timeout::Real)
    subject = kv_subject(kv.bucket, key_s)
    return try
        raw = get_last_msg(kv.connection, kv.stream, subject; timeout, api_prefix = kv.api_prefix, direct = true)
        entry_from_raw(kv, key_s, raw)
    catch err
        direct_get_unavailable(err) || rethrow()
        stored = stored_msg_request(kv, Dict("last_by_subj" => subject); timeout)
        entry_from_stored(kv, key_s, stored)
    end
end

function get(kv::KeyValue, key::AbstractString; timeout::Real = kv.connection.options.request_timeout)
    key_s = check_key(key)
    entry = try
        latest_entry(kv, key_s; timeout)
    catch err
        kv_message_not_found(err) && throw(KeyNotFoundError(key_s))
        rethrow()
    end
    entry.operation == :put || throw(KeyNotFoundError(key_s))
    return entry
end

function get_revision(kv::KeyValue, key::AbstractString, revision::Integer; timeout::Real = kv.connection.options.request_timeout)
    key_s = check_key(key)
    revision_u = UInt64(revision)
    subject = kv_subject(kv.bucket, key_s)
    entry = try
        raw = get_msg(kv.connection, kv.stream, revision_u; timeout, api_prefix = kv.api_prefix, direct = true)
        raw.subject == subject || throw(KeyNotFoundError(key_s))
        entry_from_raw(kv, key_s, raw)
    catch err
        if direct_get_unavailable(err)
            stored = try
                stored_msg_request(kv, Dict("seq" => revision_u); timeout)
            catch stored_err
                kv_message_not_found(stored_err) && throw(KeyNotFoundError(key_s))
                rethrow()
            end
            String(stored.subject) == subject || throw(KeyNotFoundError(key_s))
            entry_from_stored(kv, key_s, stored)
        elseif kv_message_not_found(err)
            throw(KeyNotFoundError(key_s))
        else
            rethrow()
        end
    end
    entry.operation == :put || throw(KeyNotFoundError(key_s))
    return entry
end

function put(kv::KeyValue, key::AbstractString, value; timeout::Real = kv.connection.options.request_timeout)
    key_s = check_key(key)
    ack = publish(kv.connection, kv.put_prefix * key_s, value; timeout, expected_stream = kv.put_expected_stream)
    return ack.seq
end

function create_revision(kv::KeyValue, key::AbstractString, value, revision::Integer; timeout::Real, msg_ttl = nothing)
    ack = publish(kv.connection, kv.put_prefix * String(key), value; timeout, expected_stream = kv.put_expected_stream, expected_last_subject_seq = revision, msg_ttl)
    return ack.seq
end

function create(kv::KeyValue, key::AbstractString, value; timeout::Real = kv.connection.options.request_timeout, msg_ttl = nothing)
    key_s = check_key(key)
    try
        return create_revision(kv, key_s, value, 0; timeout, msg_ttl)
    catch create_err
        entry = try
            latest_entry(kv, key_s; timeout)
        catch
            throw(create_err)
        end
        entry.operation in (:delete, :purge) || throw(create_err)
        return create_revision(kv, key_s, value, entry.revision; timeout, msg_ttl)
    end
end

function update(kv::KeyValue, key::AbstractString, value, revision::Integer; timeout::Real = kv.connection.options.request_timeout)
    key_s = check_key(key)
    return create_revision(kv, key_s, value, revision; timeout)
end

function delete(kv::KeyValue, key::AbstractString; revision::Union{Nothing, Integer} = nothing, purge::Bool = false, timeout::Real = kv.connection.options.request_timeout, msg_ttl = nothing)
    key_s = check_key(key)
    msg_ttl === nothing || purge || throw(ArgumentError("key-value delete marker TTL requires purge=true"))
    headers = Pair{String,String}[KV_OPERATION_HEADER => (purge ? "PURGE" : "DEL")]
    purge && push!(headers, MSG_ROLLUP_HEADER => "sub")
    ack = publish(
        kv.connection,
        kv.put_prefix * key_s,
        UInt8[];
        timeout,
        headers,
        expected_stream = kv.put_expected_stream,
        expected_last_subject_seq = revision,
        msg_ttl,
    )
    return ack.seq
end

purge(kv::KeyValue, key::AbstractString; kwargs...) = delete(kv, key; purge = true, kwargs...)

function marker_timestamp_ns(entry::KeyValueEntry)
    entry.created === nothing && return nothing
    try
        return parse(Int128, entry.created)
    catch
        return nothing
    end
end

function recent_marker(entry::KeyValueEntry, limit_ns::Int128)
    timestamp = marker_timestamp_ns(entry)
    timestamp === nothing && return false
    return timestamp > limit_ns
end

function republish_from_json(cfg)
    haskey(cfg, :republish) || return nothing
    rp = cfg.republish
    return RePublish(
        destination = String(Base.get(rp, :dest, "")),
        source = haskey(rp, :src) ? String(rp.src) : nothing,
        headers_only = Bool(Base.get(rp, :headers_only, false)),
    )
end

function subject_transform_from_json(transform)
    return SubjectTransformConfig(
        source = String(transform.src),
        destination = String(transform.dest),
    )
end

function external_stream_from_json(external)
    return ExternalStream(
        api_prefix = String(external.api),
        deliver_prefix = String(external.deliver),
    )
end

function stream_source_from_json(source)
    transforms = haskey(source, :subject_transforms) ?
        [subject_transform_from_json(transform) for transform in source.subject_transforms] :
        SubjectTransformConfig[]
    return StreamSource(
        name = String(source.name),
        opt_start_seq = haskey(source, :opt_start_seq) ? UInt64(source.opt_start_seq) : nothing,
        opt_start_time = haskey(source, :opt_start_time) ? String(source.opt_start_time) : nothing,
        filter_subject = haskey(source, :filter_subject) ? String(source.filter_subject) : nothing,
        subject_transforms = transforms,
        external = haskey(source, :external) ? external_stream_from_json(source.external) : nothing,
    )
end

mirror_from_json(cfg) = haskey(cfg, :mirror) ? stream_source_from_json(cfg.mirror) : nothing

function sources_from_json(cfg)
    haskey(cfg, :sources) || return nothing
    isempty(cfg.sources) && return nothing
    return [stream_source_from_json(source) for source in cfg.sources]
end

function purge_deletes(
    kv::KeyValue;
    delete_markers_older_than_ns::Integer = 0,
    timeout::Real = kv.connection.options.request_timeout,
)
    older_than_ns = Int64(delete_markers_older_than_ns)
    older_than_ns == 0 && (older_than_ns = KV_DEFAULT_PURGE_DELETES_MARKER_THRESHOLD_NS)
    markers = KeyValueEntry[]
    watcher = watch_all(kv; timeout)
    try
        while true
            entry = next_update(watcher; timeout)
            entry === nothing && break
            entry.operation in (:delete, :purge) && push!(markers, entry)
        end
    finally
        close(watcher)
    end

    limit_ns = older_than_ns > 0 ? Int128(floor(Int64, time() * 1_000_000_000)) - Int128(older_than_ns) : nothing
    for entry in markers
        keep = limit_ns !== nothing && recent_marker(entry, limit_ns) ? 1 : nothing
        purge_stream(kv.connection, kv.stream; subject_filter = kv_subject(kv.bucket, entry.key), keep, timeout, api_prefix = kv.api_prefix)
    end
    return nothing
end

function history(kv::KeyValue, key::AbstractString; timeout::Real = kv.connection.options.request_timeout)
    key_s = check_key(key)
    subject = kv_subject(kv.bucket, key_s)
    entries = KeyValueEntry[]
    next_seq = UInt64(1)
    while true
        stored = try
            stream_msg_get(kv.connection, kv.stream, Dict("seq" => next_seq, "next_by_subj" => subject); timeout, api_prefix = kv.api_prefix)
        catch err
            if err isa JetStreamError && (err.code == 404 || occursin("message not found", lowercase(err.description)))
                break
            end
            rethrow()
        end
        push!(entries, entry_from_stored(kv, key_s, stored))
        seq = UInt64(Base.get(stored, :seq, 0))
        seq == typemax(UInt64) && break
        next_seq = seq + 1
    end
    isempty(entries) && throw(KeyNotFoundError(key_s))
    return entries
end

function finish_key_lister!(lister::KeyValueKeyLister)
    should_close = false
    lock(lister.lock)
    try
        if !lister.finished
            lister.finished = true
            should_close = !lister.closed
        end
    finally
        unlock(lister.lock)
    end
    should_close && close(lister.watcher)
    return nothing
end

function Base.close(lister::KeyValueKeyLister)
    should_close = false
    lock(lister.lock)
    try
        if !lister.closed
            lister.closed = true
            should_close = !lister.finished
        end
    finally
        unlock(lister.lock)
    end
    should_close && close(lister.watcher)
    return nothing
end

errors(lister::KeyValueKeyLister) = errors(lister.watcher)

function next_key(lister::KeyValueKeyLister; timeout::Union{Nothing, Real} = nothing)
    lock(lister.lock)
    try
        lister.finished && return nothing
        lister.closed && throw(NATS.ConnectionClosedError("key-value key lister closed"))
    finally
        unlock(lister.lock)
    end
    entry = next_update(lister.watcher; timeout)
    if entry === nothing
        finish_key_lister!(lister)
        return nothing
    end
    return entry.key
end

function list_keys(kv::KeyValue, filters::AbstractVector; timeout::Real = kv.connection.options.request_timeout, channel_size::Int = 256)
    watcher = watch_filtered(kv, filters; ignore_deletes = true, meta_only = true, timeout, channel_size)
    return KeyValueKeyLister(watcher, ReentrantLock(), false, false)
end

list_keys(kv::KeyValue, filters::AbstractString...; timeout::Real = kv.connection.options.request_timeout, channel_size::Int = 256) =
    list_keys(kv, collect(filters); timeout, channel_size)

function keys(kv::KeyValue, filters::AbstractVector; timeout::Real = kv.connection.options.request_timeout)
    lister = list_keys(kv, filters; timeout)
    out = String[]
    try
        while true
            key = next_key(lister; timeout)
            key === nothing && break
            push!(out, key)
        end
    finally
        close(lister)
    end
    isempty(out) && throw(NoKeysFoundError())
    return sort!(unique(out))
end

keys(kv::KeyValue, filters::AbstractString...; timeout::Real = kv.connection.options.request_timeout) =
    keys(kv, collect(filters); timeout)

function status(kv::KeyValue; timeout::Real = kv.connection.options.request_timeout)
    info = stream_info(kv.connection, kv.stream; timeout, api_prefix = kv.api_prefix)
    cfg = info.config
    state = info.state
    compression = String(Base.get(cfg, :compression, "none"))
    metadata = haskey(cfg, :metadata) ? string_dict(cfg.metadata) : Dict{String,String}()
    limit_marker_ttl_ns = Int64(Base.get(cfg, :subject_delete_marker_ttl, 0))
    config = KeyValueConfig(
        bucket = kv.bucket,
        description = haskey(cfg, :description) ? String(cfg.description) : nothing,
        max_value_size = Int(Base.get(cfg, :max_msg_size, -1)),
        history = Int(Base.get(cfg, :max_msgs_per_subject, 1)),
        ttl_ns = Int64(Base.get(cfg, :max_age, 0)),
        max_bytes = Int(Base.get(cfg, :max_bytes, -1)),
        storage = String(Base.get(cfg, :storage, "file")),
        replicas = Int(Base.get(cfg, :num_replicas, 1)),
        compression = compression != "none",
        limit_marker_ttl_ns = limit_marker_ttl_ns,
        metadata = isempty(metadata) ? nothing : copy(metadata),
        republish = republish_from_json(cfg),
        mirror = mirror_from_json(cfg),
        sources = sources_from_json(cfg),
    )
    return KeyValueStatus(
        bucket = kv.bucket,
        values = UInt64(Base.get(state, :messages, 0)),
        history = config.history,
        ttl_ns = config.ttl_ns,
        bytes = UInt64(Base.get(state, :bytes, 0)),
        storage = config.storage,
        replicas = config.replicas,
        compressed = config.compression,
        limit_marker_ttl_ns = limit_marker_ttl_ns,
        metadata = metadata,
        config = config,
        stream_info = info,
    )
end

function watcher_entry(kv::KeyValue, msg::NATS.Msg)
    startswith(msg.subject, kv.prefix) || throw(ArgumentError("watch update subject is outside key-value bucket: $(msg.subject)"))
    key = msg.subject[(ncodeunits(kv.prefix) + 1):end]
    meta = metadata(msg)
    return KeyValueEntry(
        bucket = kv.bucket,
        key = key,
        value = copy(msg.data),
        revision = meta.sequence.stream,
        operation = kv_operation(msg.headers),
        headers = copy(msg.headers),
        created = string(meta.timestamp_ns),
    ), meta.num_pending
end

function finish_watcher!(watcher::KeyValueWatcher)
    lock(watcher.lock)
    try
        watcher.closed = true
        isopen(watcher.updates) && close(watcher.updates)
        isopen(watcher.errors) && close(watcher.errors)
    finally
        unlock(watcher.lock)
    end
    return nothing
end

function watcher_loop(watcher::KeyValueWatcher, initial_pending::UInt64, updates_only::Bool, ignore_deletes::Bool)
    init_done = updates_only || initial_pending == 0
    received = UInt64(0)
    try
        while true
            msg = NATS.next_msg(watcher.subscription)
            entry, pending = watcher_entry(watcher.key_value, msg)
            if !ignore_deletes || (entry.operation != :delete && entry.operation != :purge)
                put!(watcher.updates, entry)
            end
            if !init_done
                received += UInt64(1)
                if received >= initial_pending || pending == 0
                    init_done = true
                    put!(watcher.updates, nothing)
                end
            end
        end
    catch err
        lock(watcher.lock)
        closed = watcher.closed
        unlock(watcher.lock)
        if !closed && !(err isa NATS.ConnectionClosedError)
            try put!(watcher.errors, err) catch end
        end
    finally
        finish_watcher!(watcher)
    end
    return nothing
end

function close_watcher_remote!(watcher::KeyValueWatcher)
    try
        NATS.unsubscribe(watcher.key_value.connection, watcher.subscription)
    catch
    end
    try
        delete_consumer(watcher.key_value.connection, watcher.key_value.stream, watcher.consumer; timeout = watcher.key_value.connection.options.request_timeout, api_prefix = watcher.key_value.api_prefix)
    catch
    end
    return nothing
end

function Base.close(watcher::KeyValueWatcher)
    should_close = false
    lock(watcher.lock)
    try
        if !watcher.closed
            watcher.closed = true
            should_close = true
            isopen(watcher.updates) && close(watcher.updates)
            isopen(watcher.errors) && close(watcher.errors)
        end
    finally
        unlock(watcher.lock)
    end
    should_close && close_watcher_remote!(watcher)
    return nothing
end

updates(watcher::KeyValueWatcher) = watcher.updates

errors(watcher::KeyValueWatcher) = watcher.errors

function next_update(watcher::KeyValueWatcher; timeout::Union{Nothing, Real} = nothing)
    if timeout === nothing
        return take!(watcher.updates)
    end
    result = timedwait(() -> isready(watcher.updates) || !isopen(watcher.updates), timeout; pollint = 0.001)
    result == :ok || throw(NATS.ConnectionTimeoutError("next_update", Float64(timeout)))
    isready(watcher.updates) || throw(NATS.ConnectionClosedError("key-value watcher closed"))
    return take!(watcher.updates)
end

function watch_filtered(
    kv::KeyValue,
    keys::AbstractVector;
    include_history::Bool = false,
    updates_only::Bool = false,
    ignore_deletes::Bool = false,
    meta_only::Bool = false,
    resume_from_revision::Union{Nothing, Integer} = nothing,
    timeout::Real = kv.connection.options.request_timeout,
    channel_size::Int = 256,
)
    include_history && updates_only && throw(ArgumentError("include_history cannot be used with updates_only"))
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    if resume_from_revision !== nothing && resume_from_revision < 0
        throw(ArgumentError("resume_from_revision must be nonnegative"))
    end
    start_seq = resume_from_revision === nothing || resume_from_revision == 0 ? nothing : UInt64(resume_from_revision)
    deliver_policy = updates_only ? "new" : (include_history ? "all" : "last_per_subject")
    start_seq === nothing || (deliver_policy = "by_start_sequence")
    patterns = isempty(keys) ? [">"] : [check_watch_key(key) for key in keys]
    subjects = [kv.prefix * pattern for pattern in patterns]
    consumer = "NATSJL_KV_$(randstring(12))"
    inbox = NATS.new_inbox(kv.connection)
    sub = NATS.subscribe(kv.connection, inbox; channel_size)
    config = ConsumerConfig(
        name = consumer,
        deliver_policy = deliver_policy,
        opt_start_seq = start_seq,
        replay_policy = "instant",
        ack_policy = "none",
        filter_subject = length(subjects) == 1 ? first(subjects) : nothing,
        filter_subjects = length(subjects) > 1 ? subjects : nothing,
        deliver_subject = inbox,
        headers_only = meta_only ? true : nothing,
        inactive_threshold = 30_000_000_000,
    )
    info = try
        create_consumer(kv.connection, kv.stream, config; timeout, api_prefix = kv.api_prefix)
    catch
        try NATS.unsubscribe(kv.connection, sub) catch end
        rethrow()
    end
    initial_pending = updates_only ? UInt64(0) : UInt64(Base.get(info, :num_pending, 0))
    watcher = KeyValueWatcher(
        kv,
        consumer,
        sub,
        Channel{Union{Nothing, KeyValueEntry}}(channel_size),
        Channel{Any}(1),
        nothing,
        ReentrantLock(),
        false,
    )
    if !updates_only && initial_pending == 0
        put!(watcher.updates, nothing)
    end
    watcher.task = errormonitor(@async watcher_loop(watcher, initial_pending, updates_only, ignore_deletes))
    return watcher
end

function watch(kv::KeyValue, key::AbstractString; kwargs...)
    return watch_filtered(kv, [key]; kwargs...)
end

function watch_all(kv::KeyValue; kwargs...)
    return watch_filtered(kv, String[]; kwargs...)
end

const OBJ_BUCKET_PREFIX = "OBJ_"
const OBJ_DEFAULT_CHUNK_SIZE = 128 * 1024
const OBJ_DIGEST_PREFIX = "SHA-256="

Base.@kwdef struct ObjectStoreConfig
    bucket::String
    description::Union{Nothing, String} = nothing
    ttl_ns::Int64 = 0
    max_bytes::Int64 = -1
    storage::String = "file"
    replicas::Int = 1
    placement::Union{Nothing, Placement} = nothing
    compression::Bool = false
    metadata::Union{Nothing, Dict{String,String}} = nothing
end

struct ObjectStore
    connection::NATS.Connection
    bucket::String
    stream::String
    chunk_prefix::String
    meta_prefix::String
    api_prefix::String
end

Base.@kwdef struct ObjectLink
    bucket::String
    name::Union{Nothing, String} = nothing
end

Base.@kwdef struct ObjectMeta
    name::String
    description::Union{Nothing, String} = nothing
    headers::Vector{Pair{String,String}} = Pair{String,String}[]
    metadata::Dict{String,String} = Dict{String,String}()
    link::Union{Nothing, ObjectLink} = nothing
    chunk_size::Int = OBJ_DEFAULT_CHUNK_SIZE
end

Base.@kwdef struct ObjectInfo
    name::String
    bucket::String
    nuid::String
    size::UInt64
    mtime::Union{Nothing, String} = nothing
    chunks::UInt32
    digest::String = ""
    deleted::Bool = false
    description::Union{Nothing, String} = nothing
    headers::Vector{Pair{String,String}} = Pair{String,String}[]
    metadata::Dict{String,String} = Dict{String,String}()
    link::Union{Nothing, ObjectLink} = nothing
    chunk_size::Int = OBJ_DEFAULT_CHUNK_SIZE
end

mutable struct ObjectWatcher
    store::ObjectStore
    consumer::String
    subscription::NATS.Subscription
    updates::Channel{Union{Nothing, ObjectInfo}}
    errors::Channel{Any}
    task::Union{Nothing, Task}
    lock::ReentrantLock
    closed::Bool
end

mutable struct ObjectInfoLister
    watcher::ObjectWatcher
    lock::ReentrantLock
    finished::Bool
    closed::Bool
end

Base.@kwdef struct ObjectStoreStatus
    bucket::String
    description::Union{Nothing, String}
    ttl_ns::Int64
    size::UInt64
    storage::String
    replicas::Int
    sealed::Bool
    compressed::Bool
    metadata::Dict{String,String}
    stream_info
end

object_stream(bucket::AbstractString) = "$(OBJ_BUCKET_PREFIX)$(bucket)"
object_chunk_prefix(bucket::AbstractString) = "\$O.$bucket.C."
object_meta_prefix(bucket::AbstractString) = "\$O.$bucket.M."
object_chunk_subject(bucket::AbstractString, nuid::AbstractString) = "$(object_chunk_prefix(bucket))$nuid"
object_meta_subject(bucket::AbstractString, name::AbstractString) = "$(object_meta_prefix(bucket))$(object_encode_name(name))"

function object_encode_urlsafe(bytes::AbstractVector{UInt8})
    return replace(Base64.base64encode(bytes), '+' => '-', '/' => '_')
end

function object_encode_name(name::AbstractString)
    return object_encode_urlsafe(Vector{UInt8}(codeunits(String(name))))
end

function object_digest(data::AbstractVector{UInt8})
    return OBJ_DIGEST_PREFIX * object_encode_urlsafe(sha256(data))
end

function object_digest_from_hash(hash::AbstractVector{UInt8})
    return OBJ_DIGEST_PREFIX * object_encode_urlsafe(hash)
end

function decode_object_digest(digest::AbstractString)
    digest_s = String(digest)
    startswith(digest_s, OBJ_DIGEST_PREFIX) || throw(JetStreamError(500, 0, "object digest hash has invalid format"))
    encoded_digest = ncodeunits(digest_s) == ncodeunits(OBJ_DIGEST_PREFIX) ? "" : digest_s[(ncodeunits(OBJ_DIGEST_PREFIX) + 1):end]
    occursin(r"^[A-Za-z0-9_-]*={0,2}$", encoded_digest) || throw(JetStreamError(500, 0, "object digest hash has invalid format"))
    ncodeunits(encoded_digest) % 4 == 0 || throw(JetStreamError(500, 0, "object digest hash has invalid format"))
    encoded = replace(encoded_digest, '-' => '+', '_' => '/')
    try
        return Base64.base64decode(encoded)
    catch
        throw(JetStreamError(500, 0, "object digest hash has invalid format"))
    end
end

function object_not_found(err)
    err isa ObjectNotFoundError && return true
    return err isa JetStreamError && (err.code == 404 || occursin("not found", lowercase(err.description)))
end

function object_store_config_dict(config::ObjectStoreConfig)
    bucket = check_bucket(config.bucket)
    replicas = config.replicas == 0 ? 1 : config.replicas
    max_bytes = config.max_bytes == 0 ? -1 : config.max_bytes
    return config_dict(StreamConfig(
        name = object_stream(bucket),
        subjects = ["$(object_chunk_prefix(bucket))>", "$(object_meta_prefix(bucket))>"],
        description = config.description,
        max_age = config.ttl_ns > 0 ? config.ttl_ns : nothing,
        max_bytes = max_bytes,
        storage = config.storage,
        replicas = replicas,
        placement = config.placement,
        discard = "new",
        allow_rollup_hdrs = true,
        allow_direct = true,
        compression = config.compression ? "s2" : nothing,
        metadata = config.metadata,
    ))
end

function object_store_from_bucket(conn::NATS.Connection, bucket::AbstractString; api_prefix::AbstractString = DEFAULT_API_PREFIX)
    bucket_s = check_bucket(bucket)
    return ObjectStore(conn, bucket_s, object_stream(bucket_s), object_chunk_prefix(bucket_s), object_meta_prefix(bucket_s), normalize_api_prefix(api_prefix))
end

function create_object_store(
    conn::NATS.Connection,
    config::ObjectStoreConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    prefix = api_prefix_value(; api_prefix, domain)
    api_request(conn, api_subject_from_prefix(prefix, "STREAM.CREATE.$(object_stream(check_bucket(config.bucket)))"), JSON3.write(object_store_config_dict(config)); timeout)
    return object_store_from_bucket(conn, config.bucket; api_prefix = prefix)
end

function update_object_store(
    conn::NATS.Connection,
    config::ObjectStoreConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    prefix = api_prefix_value(; api_prefix, domain)
    bucket = check_bucket(config.bucket)
    try
        api_request(conn, api_subject_from_prefix(prefix, "STREAM.UPDATE.$(object_stream(bucket))"), JSON3.write(object_store_config_dict(config)); timeout)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(bucket))
        rethrow()
    end
    return object_store_from_bucket(conn, bucket; api_prefix = prefix)
end

function create_or_update_object_store(
    conn::NATS.Connection,
    config::ObjectStoreConfig;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    try
        return create_object_store(conn, config; timeout, api_prefix, domain)
    catch err
        object_not_found(err) && rethrow()
        err isa JetStreamError || rethrow()
        return update_object_store(conn, config; timeout, api_prefix, domain)
    end
end

function object_store(
    conn::NATS.Connection,
    bucket::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    prefix = api_prefix_value(; api_prefix, domain)
    store = object_store_from_bucket(conn, bucket; api_prefix = prefix)
    try
        stream_info(conn, store.stream; timeout, api_prefix = prefix)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(store.bucket))
        rethrow()
    end
    return store
end

function delete_object_store(
    conn::NATS.Connection,
    bucket::AbstractString;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    bucket_s = check_bucket(bucket)
    try
        return delete_stream(conn, object_stream(bucket_s); timeout, api_prefix, domain)
    catch err
        stream_not_found(err) && throw(BucketNotFoundError(bucket_s))
        rethrow()
    end
end

function object_headers_dict(headers::Vector{Pair{String,String}})
    isempty(headers) && return nothing
    d = Dict{String, Vector{String}}()
    for (k, v) in headers
        push!(get!(d, String(k), String[]), String(v))
    end
    return d
end

function object_meta_options_dict(meta::ObjectMeta)
    d = Dict{String, Any}()
    meta.link === nothing && (d["max_chunk_size"] = meta.chunk_size)
    if meta.link !== nothing
        link = Dict{String, Any}("bucket" => meta.link.bucket)
        meta.link.name === nothing || (link["name"] = meta.link.name)
        d["link"] = link
    end
    return d
end

function object_info_dict(info::ObjectInfo)
    d = Dict{String, Any}(
        "name" => info.name,
        "bucket" => info.bucket,
        "nuid" => info.nuid,
        "size" => info.size,
        "chunks" => info.chunks,
        "digest" => info.digest,
        "options" => object_meta_options_dict(ObjectMeta(
            name = info.name,
            link = info.link,
            chunk_size = info.chunk_size,
        )),
    )
    info.description === nothing || (d["description"] = info.description)
    isempty(info.metadata) || (d["metadata"] = info.metadata)
    hdrs = object_headers_dict(info.headers)
    hdrs === nothing || (d["headers"] = hdrs)
    info.mtime === nothing || (d["mtime"] = info.mtime)
    info.deleted && (d["deleted"] = true)
    return d
end

function string_dict(obj)
    obj === nothing && return Dict{String,String}()
    d = Dict{String,String}()
    for (k, v) in pairs(obj)
        d[String(k)] = String(v)
    end
    return d
end

function object_headers(obj)
    obj === nothing && return Pair{String,String}[]
    headers = Pair{String,String}[]
    for (k, values) in pairs(obj)
        if values isa AbstractVector
            for v in values
                push!(headers, String(k) => String(v))
            end
        else
            push!(headers, String(k) => String(values))
        end
    end
    return headers
end

function object_chunk_size(obj)
    if obj !== nothing && haskey(obj, :max_chunk_size)
        return Int(obj.max_chunk_size)
    end
    return OBJ_DEFAULT_CHUNK_SIZE
end

function object_link(obj)
    obj === nothing && return nothing
    haskey(obj, :link) || return nothing
    link = obj.link
    bucket = String(Base.get(link, :bucket, ""))
    isempty(bucket) && return nothing
    name = haskey(link, :name) ? String(link.name) : nothing
    return ObjectLink(bucket = bucket, name = name)
end

function object_info_from_json(obj; stored_time::Union{Nothing, String} = nothing)
    haskey(obj, :name) || throw(JetStreamError(500, 0, "bad object metadata"))
    haskey(obj, :bucket) || throw(JetStreamError(500, 0, "bad object metadata"))
    opts = haskey(obj, :options) ? obj.options : nothing
    return ObjectInfo(
        name = String(obj.name),
        bucket = String(obj.bucket),
        nuid = String(Base.get(obj, :nuid, "")),
        size = UInt64(Base.get(obj, :size, 0)),
        mtime = stored_time === nothing ? (haskey(obj, :mtime) ? String(obj.mtime) : nothing) : stored_time,
        chunks = UInt32(Base.get(obj, :chunks, 0)),
        digest = String(Base.get(obj, :digest, "")),
        deleted = Bool(Base.get(obj, :deleted, false)),
        description = haskey(obj, :description) ? String(obj.description) : nothing,
        headers = haskey(obj, :headers) ? object_headers(obj.headers) : Pair{String,String}[],
        metadata = haskey(obj, :metadata) ? string_dict(obj.metadata) : Dict{String,String}(),
        link = object_link(opts),
        chunk_size = object_chunk_size(opts),
    )
end

function object_info_from_stored(stored)
    data = haskey(stored, :data) ? data_from_base64(stored.data) : UInt8[]
    info = object_info_from_json(JSON3.read(data); stored_time = haskey(stored, :time) ? String(stored.time) : nothing)
    return info
end

function get_info(store::ObjectStore, name::AbstractString; show_deleted::Bool = false, timeout::Real = store.connection.options.request_timeout)
    isempty(name) && throw(ArgumentError("object name must not be empty"))
    name_s = String(name)
    stored = try
        stream_msg_get(store.connection, store.stream, Dict("last_by_subj" => object_meta_subject(store.bucket, name_s)); timeout, api_prefix = store.api_prefix)
    catch err
        object_not_found(err) && throw(ObjectNotFoundError(name_s))
        rethrow()
    end
    info = object_info_from_stored(stored)
    info.deleted && !show_deleted && throw(ObjectNotFoundError(name_s))
    return info
end

function publish_object_info(store::ObjectStore, info::ObjectInfo; timeout::Real = store.connection.options.request_timeout)
    publish(
        store.connection,
        object_meta_subject(store.bucket, info.name),
        JSON3.write(object_info_dict(info));
        timeout,
        headers = [MSG_ROLLUP_HEADER => "sub"],
        expected_stream = store.stream,
    )
    return info
end

function put(store::ObjectStore, meta::ObjectMeta, data; timeout::Real = store.connection.options.request_timeout)
    isempty(meta.name) && throw(ArgumentError("object name must not be empty"))
    meta.link === nothing || throw(ArgumentError("object links must be created with add_link or add_bucket_link"))
    meta.chunk_size >= 1 || throw(ArgumentError("object chunk size must be positive"))
    old_info = try
        get_info(store, meta.name; show_deleted = true, timeout)
    catch err
        object_not_found(err) ? nothing : rethrow()
    end
    nuid = randstring(22)
    chunk_subject = object_chunk_subject(store.bucket, nuid)
    chunks = UInt32(0)
    size = UInt64(0)
    hash = SHA.SHA256_CTX()
    source = data isa IO ? data : IOBuffer(NATS.bytes_payload(data))
    try
        while true
            chunk = read(source, meta.chunk_size)
            isempty(chunk) && break
            SHA.update!(hash, chunk)
            publish(store.connection, chunk_subject, chunk; timeout, expected_stream = store.stream)
            chunks += UInt32(1)
            size += UInt64(length(chunk))
        end
        info = ObjectInfo(
            name = meta.name,
            bucket = store.bucket,
            nuid = nuid,
            size = size,
            chunks = chunks,
            digest = object_digest_from_hash(SHA.digest!(hash)),
            description = meta.description,
            headers = copy(meta.headers),
            metadata = copy(meta.metadata),
            chunk_size = meta.chunk_size,
        )
        publish_object_info(store, info; timeout)
        if old_info !== nothing && !old_info.deleted && !isempty(old_info.nuid)
            try
                purge_stream(store.connection, store.stream; subject_filter = object_chunk_subject(store.bucket, old_info.nuid), timeout, api_prefix = store.api_prefix)
            catch
            end
        end
        return info
    catch
        if chunks > 0
            try
                purge_stream(store.connection, store.stream; subject_filter = chunk_subject, timeout, api_prefix = store.api_prefix)
            catch
            end
        end
        rethrow()
    end
end

put(store::ObjectStore, name::AbstractString, data; kwargs...) =
    put(store, ObjectMeta(name = String(name)), data; kwargs...)

put_string(store::ObjectStore, name::AbstractString, data::AbstractString; kwargs...) =
    put(store, name, data; kwargs...)

function get_bytes(store::ObjectStore, name::AbstractString; show_deleted::Bool = false, timeout::Real = store.connection.options.request_timeout)
    info = get_info(store, name; show_deleted, timeout)
    info.deleted && return UInt8[]
    isempty(info.nuid) && throw(BadObjectMetaError(info.name))
    if info.link !== nothing
        info.link.name === nothing && throw(JetStreamError(400, 0, "cannot get bucket link as object"))
        linked_store = info.link.bucket == store.bucket ? store : object_store(store.connection, info.link.bucket; timeout, api_prefix = store.api_prefix)
        return get_bytes(linked_store, info.link.name; show_deleted, timeout)
    end
    out = UInt8[]
    next_seq = UInt64(1)
    chunk_subject = object_chunk_subject(store.bucket, info.nuid)
    for _ in 1:Int(info.chunks)
        stored = stream_msg_get(store.connection, store.stream, Dict("seq" => next_seq, "next_by_subj" => chunk_subject); timeout, api_prefix = store.api_prefix)
        append!(out, data_from_base64(stored.data))
        next_seq = UInt64(Base.get(stored, :seq, next_seq)) + 1
    end
    UInt64(length(out)) == info.size || throw(JetStreamError(500, 0, "object size mismatch"))
    if !isempty(info.digest)
        expected_digest = decode_object_digest(info.digest)
        sha256(out) == expected_digest || throw(JetStreamError(500, 0, "object digest mismatch"))
    end
    return out
end

get_string(store::ObjectStore, name::AbstractString; kwargs...) =
    String(get_bytes(store, name; kwargs...))

function delete(store::ObjectStore, name::AbstractString; timeout::Real = store.connection.options.request_timeout)
    info = get_info(store, name; show_deleted = true, timeout)
    isempty(info.nuid) && throw(BadObjectMetaError(info.name))
    deleted = ObjectInfo(
        name = info.name,
        bucket = info.bucket,
        nuid = info.nuid,
        size = UInt64(0),
        chunks = UInt32(0),
        digest = "",
        deleted = true,
        description = info.description,
        headers = info.headers,
        metadata = info.metadata,
        chunk_size = info.chunk_size,
    )
    publish_object_info(store, deleted; timeout)
    isempty(info.nuid) || purge_stream(store.connection, store.stream; subject_filter = object_chunk_subject(store.bucket, info.nuid), timeout, api_prefix = store.api_prefix)
    return nothing
end

function update_meta(store::ObjectStore, name::AbstractString, meta::ObjectMeta; timeout::Real = store.connection.options.request_timeout)
    isempty(meta.name) && throw(ArgumentError("object name must not be empty"))
    meta.link === nothing || throw(ArgumentError("object links must be created with add_link or add_bucket_link"))
    current = get_info(store, name; show_deleted = true, timeout)
    current.deleted && throw(UpdateMetaDeletedError(String(name)))
    if String(name) != meta.name
        existing = try
            get_info(store, meta.name; show_deleted = true, timeout)
        catch err
            object_not_found(err) ? nothing : rethrow()
        end
        existing !== nothing && !existing.deleted && throw(JetStreamError(409, 0, "object already exists"))
    end
    updated = ObjectInfo(
        name = meta.name,
        bucket = current.bucket,
        nuid = current.nuid,
        size = current.size,
        chunks = current.chunks,
        digest = current.digest,
        deleted = current.deleted,
        description = meta.description,
        headers = copy(meta.headers),
        metadata = copy(meta.metadata),
        link = current.link,
        chunk_size = current.chunk_size,
    )
    publish_object_info(store, updated; timeout)
    if String(name) != meta.name
        purge_stream(store.connection, store.stream; subject_filter = object_meta_subject(store.bucket, name), timeout, api_prefix = store.api_prefix)
    end
    return nothing
end

function validate_object_link_target(store::ObjectStore, object::ObjectInfo; timeout::Real)
    isempty(object.name) && throw(ArgumentError("linked object name must not be empty"))
    object.deleted && throw(JetStreamError(400, 0, "cannot link to deleted object"))
    object.link !== nothing && throw(JetStreamError(400, 0, "cannot link to another link"))
    target_store = object.bucket == store.bucket ? store : object_store(store.connection, object.bucket; timeout, api_prefix = store.api_prefix)
    current = get_info(target_store, object.name; show_deleted = true, timeout)
    current.deleted && throw(JetStreamError(400, 0, "cannot link to deleted object"))
    current.link !== nothing && throw(JetStreamError(400, 0, "cannot link to another link"))
    return current
end

function add_link(store::ObjectStore, name::AbstractString, object::ObjectInfo; timeout::Real = store.connection.options.request_timeout)
    isempty(name) && throw(ArgumentError("object link name must not be empty"))
    target = validate_object_link_target(store, object; timeout)
    existing = try
        get_info(store, name; show_deleted = true, timeout)
    catch err
        object_not_found(err) ? nothing : rethrow()
    end
    existing !== nothing && existing.link === nothing && throw(JetStreamError(409, 0, "object already exists"))
    info = ObjectInfo(
        name = String(name),
        bucket = store.bucket,
        nuid = randstring(22),
        size = UInt64(0),
        chunks = UInt32(0),
        link = ObjectLink(bucket = target.bucket, name = target.name),
    )
    return publish_object_info(store, info; timeout)
end

function add_bucket_link(store::ObjectStore, name::AbstractString, target::ObjectStore; timeout::Real = store.connection.options.request_timeout)
    isempty(name) && throw(ArgumentError("bucket link name must not be empty"))
    existing = try
        get_info(store, name; show_deleted = true, timeout)
    catch err
        object_not_found(err) ? nothing : rethrow()
    end
    existing !== nothing && existing.link === nothing && throw(JetStreamError(409, 0, "object already exists"))
    info = ObjectInfo(
        name = String(name),
        bucket = store.bucket,
        nuid = randstring(22),
        size = UInt64(0),
        chunks = UInt32(0),
        link = ObjectLink(bucket = target.bucket),
    )
    return publish_object_info(store, info; timeout)
end

function put_file(store::ObjectStore, path::AbstractString; name::AbstractString = String(path), timeout::Real = store.connection.options.request_timeout)
    open(String(path), "r") do io
        return put(store, ObjectMeta(name = String(name)), io; timeout)
    end
end

function get_file(store::ObjectStore, name::AbstractString, path::AbstractString; show_deleted::Bool = false, timeout::Real = store.connection.options.request_timeout)
    data = get_bytes(store, name; show_deleted, timeout)
    open(String(path), "w") do io
        write(io, data)
    end
    return String(path)
end

function finish_object_lister!(lister::ObjectInfoLister)
    should_close = false
    lock(lister.lock)
    try
        if !lister.finished
            lister.finished = true
            should_close = !lister.closed
        end
    finally
        unlock(lister.lock)
    end
    should_close && close(lister.watcher)
    return nothing
end

function Base.close(lister::ObjectInfoLister)
    should_close = false
    lock(lister.lock)
    try
        if !lister.closed
            lister.closed = true
            should_close = !lister.finished
        end
    finally
        unlock(lister.lock)
    end
    should_close && close(lister.watcher)
    return nothing
end

errors(lister::ObjectInfoLister) = errors(lister.watcher)

function next_object(lister::ObjectInfoLister; timeout::Union{Nothing, Real} = nothing)
    lock(lister.lock)
    try
        lister.finished && return nothing
        lister.closed && throw(NATS.ConnectionClosedError("object lister closed"))
    finally
        unlock(lister.lock)
    end
    info = next_update(lister.watcher; timeout)
    if info === nothing
        finish_object_lister!(lister)
        return nothing
    end
    return info
end

function list_objects(store::ObjectStore; show_deleted::Bool = false, timeout::Real = store.connection.options.request_timeout, channel_size::Int = 256)
    watcher = watch(store; ignore_deletes = !show_deleted, timeout, channel_size)
    return ObjectInfoLister(watcher, ReentrantLock(), false, false)
end

function list(store::ObjectStore; show_deleted::Bool = false, timeout::Real = store.connection.options.request_timeout)
    lister = list_objects(store; show_deleted, timeout)
    infos = ObjectInfo[]
    try
        while true
            info = next_object(lister; timeout)
            info === nothing && break
            push!(infos, info)
        end
    finally
        close(lister)
    end
    isempty(infos) && throw(NoObjectsFoundError())
    return infos
end

function status(store::ObjectStore; timeout::Real = store.connection.options.request_timeout)
    info = stream_info(store.connection, store.stream; timeout, api_prefix = store.api_prefix)
    cfg = info.config
    state = info.state
    compression = String(Base.get(cfg, :compression, "none"))
    return ObjectStoreStatus(
        bucket = store.bucket,
        description = haskey(cfg, :description) ? String(cfg.description) : nothing,
        ttl_ns = Int64(Base.get(cfg, :max_age, 0)),
        size = UInt64(Base.get(state, :bytes, 0)),
        storage = String(Base.get(cfg, :storage, "file")),
        replicas = Int(Base.get(cfg, :num_replicas, 1)),
        sealed = Bool(Base.get(cfg, :sealed, false)),
        compressed = compression != "none",
        metadata = haskey(cfg, :metadata) ? string_dict(cfg.metadata) : Dict{String,String}(),
        stream_info = info,
    )
end

function seal(store::ObjectStore; timeout::Real = store.connection.options.request_timeout)
    info = stream_info(store.connection, store.stream; timeout, api_prefix = store.api_prefix)
    config = JSON3.read(JSON3.write(info.config), Dict{String, Any})
    config["sealed"] = true
    api_request(store.connection, api_subject_from_prefix(store.api_prefix, "STREAM.UPDATE.$(store.stream)"), JSON3.write(config); timeout)
    return nothing
end

function finish_object_watcher!(watcher::ObjectWatcher)
    lock(watcher.lock)
    try
        watcher.closed = true
        isopen(watcher.updates) && close(watcher.updates)
        isopen(watcher.errors) && close(watcher.errors)
    finally
        unlock(watcher.lock)
    end
    return nothing
end

function object_watcher_loop(watcher::ObjectWatcher, initial_pending::UInt64, updates_only::Bool, ignore_deletes::Bool)
    init_done = updates_only || initial_pending == 0
    received = UInt64(0)
    try
        while true
            msg = NATS.next_msg(watcher.subscription)
            info = object_info_from_json(JSON3.read(msg.data))
            meta = metadata(msg)
            info = ObjectInfo(
                name = info.name,
                bucket = info.bucket,
                nuid = info.nuid,
                size = info.size,
                mtime = string(meta.timestamp_ns),
                chunks = info.chunks,
                digest = info.digest,
                deleted = info.deleted,
                description = info.description,
                headers = info.headers,
                metadata = info.metadata,
                link = info.link,
                chunk_size = info.chunk_size,
            )
            if !ignore_deletes || !info.deleted
                put!(watcher.updates, info)
            end
            if !init_done
                received += UInt64(1)
                if received >= initial_pending || meta.num_pending == 0
                    init_done = true
                    put!(watcher.updates, nothing)
                end
            end
        end
    catch err
        lock(watcher.lock)
        closed = watcher.closed
        unlock(watcher.lock)
        if !closed && !(err isa NATS.ConnectionClosedError)
            try put!(watcher.errors, err) catch end
        end
    finally
        finish_object_watcher!(watcher)
    end
    return nothing
end

function watch(
    store::ObjectStore;
    include_history::Bool = false,
    updates_only::Bool = false,
    ignore_deletes::Bool = false,
    timeout::Real = store.connection.options.request_timeout,
    channel_size::Int = 256,
)
    include_history && updates_only && throw(ArgumentError("include_history cannot be used with updates_only"))
    channel_size >= 1 || throw(ArgumentError("channel_size must be positive"))
    inbox = NATS.new_inbox(store.connection)
    sub = NATS.subscribe(store.connection, inbox; channel_size)
    consumer = "NATSJL_OBJ_$(randstring(12))"
    config = ConsumerConfig(
        name = consumer,
        deliver_policy = updates_only ? "new" : (include_history ? "all" : "last_per_subject"),
        replay_policy = "instant",
        ack_policy = "none",
        filter_subject = store.meta_prefix * ">",
        deliver_subject = inbox,
        inactive_threshold = 30_000_000_000,
    )
    info = try
        create_consumer(store.connection, store.stream, config; timeout, api_prefix = store.api_prefix)
    catch
        try NATS.unsubscribe(store.connection, sub) catch end
        rethrow()
    end
    initial_pending = updates_only ? UInt64(0) : UInt64(Base.get(info, :num_pending, 0))
    watcher = ObjectWatcher(
        store,
        consumer,
        sub,
        Channel{Union{Nothing, ObjectInfo}}(channel_size),
        Channel{Any}(1),
        nothing,
        ReentrantLock(),
        false,
    )
    if !updates_only && initial_pending == 0
        put!(watcher.updates, nothing)
    end
    watcher.task = errormonitor(@async object_watcher_loop(watcher, initial_pending, updates_only, ignore_deletes))
    return watcher
end

function Base.close(watcher::ObjectWatcher)
    should_close = false
    lock(watcher.lock)
    try
        if !watcher.closed
            watcher.closed = true
            should_close = true
            isopen(watcher.updates) && close(watcher.updates)
            isopen(watcher.errors) && close(watcher.errors)
        end
    finally
        unlock(watcher.lock)
    end
    if should_close
        try
            NATS.unsubscribe(watcher.store.connection, watcher.subscription)
        catch
        end
        try
            delete_consumer(watcher.store.connection, watcher.store.stream, watcher.consumer; timeout = watcher.store.connection.options.request_timeout, api_prefix = watcher.store.api_prefix)
        catch
        end
    end
    return nothing
end

updates(watcher::ObjectWatcher) = watcher.updates

errors(watcher::ObjectWatcher) = watcher.errors

function next_update(watcher::ObjectWatcher; timeout::Union{Nothing, Real} = nothing)
    if timeout === nothing
        return take!(watcher.updates)
    end
    result = timedwait(() -> isready(watcher.updates) || !isopen(watcher.updates), timeout; pollint = 0.001)
    result == :ok || throw(NATS.ConnectionTimeoutError("next_update", Float64(timeout)))
    isready(watcher.updates) || throw(NATS.ConnectionClosedError("object watcher closed"))
    return take!(watcher.updates)
end

function object_store_names(
    conn::NATS.Connection;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    names = stream_names(conn; subject_filter = "\$O.*.C.>", timeout, api_prefix, domain)
    buckets = String[]
    for name in names
        startswith(name, OBJ_BUCKET_PREFIX) || continue
        push!(buckets, name[(ncodeunits(OBJ_BUCKET_PREFIX) + 1):end])
    end
    sort!(buckets)
    return buckets
end

function object_stores(
    conn::NATS.Connection;
    timeout::Real = conn.options.request_timeout,
    api_prefix::Union{Nothing, AbstractString} = nothing,
    domain::Union{Nothing, AbstractString} = nothing,
)
    prefix = api_prefix_value(; api_prefix, domain)
    return [status(object_store(conn, bucket; timeout, api_prefix = prefix); timeout) for bucket in object_store_names(conn; timeout, api_prefix = prefix)]
end

end
