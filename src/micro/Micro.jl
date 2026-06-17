module Micro

import ..NATS
using JSON3
using Random

const DEFAULT_QUEUE_GROUP = "q"
const API_PREFIX = "\$SRV"

const ERROR_HEADER = "Nats-Service-Error"
const ERROR_CODE_HEADER = "Nats-Service-Error-Code"

const PING = "PING"
const STATS = "STATS"
const INFO = "INFO"

const INFO_RESPONSE_TYPE = "io.nats.micro.v1.info_response"
const PING_RESPONSE_TYPE = "io.nats.micro.v1.ping_response"
const STATS_RESPONSE_TYPE = "io.nats.micro.v1.stats_response"

const NAME_RE = r"^[A-Za-z0-9\-_]+$"
const SEMVER_RE = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
const SUBJECT_RE = r"^[^ >]*[>]?$"

struct ConfigValidationError <: Exception
    message::String
end

struct UnsupportedVerbError <: Exception
    verb::String
end

struct ServiceNameRequiredError <: Exception end

struct RespondError <: Exception
    message::String
end

struct HandlerError <: Exception
    subject::String
    error::Any
end

function Base.showerror(io::IO, err::ConfigValidationError)
    print(io, "NATS micro service config validation failed: ", err.message)
end

function Base.showerror(io::IO, err::UnsupportedVerbError)
    print(io, "NATS micro service control verb is not supported: ", err.verb)
end

Base.showerror(io::IO, ::ServiceNameRequiredError) =
    print(io, "NATS micro service name is required when a service id is provided")

function Base.showerror(io::IO, err::RespondError)
    print(io, "NATS micro service response failed: ", err.message)
end

function Base.showerror(io::IO, err::HandlerError)
    print(io, "NATS micro service handler failed for ", err.subject, ": ")
    showerror(io, err.error)
end

Base.@kwdef struct EndpointConfig
    subject::Union{Nothing, String} = nothing
    handler::Union{Nothing, Function} = nothing
    metadata::Dict{String,String} = Dict{String,String}()
    queue_group::Union{Nothing, String} = nothing
    queue_group_disabled::Bool = false
    channel_size::Union{Nothing, Int} = nothing
    pending_msg_limit::Union{Nothing, Int} = nothing
    pending_bytes_limit::Union{Nothing, Int} = nothing
end

Base.@kwdef struct ServiceConfig
    name::String
    version::String
    description::String = ""
    metadata::Dict{String,String} = Dict{String,String}()
    endpoint::Union{Nothing, EndpointConfig} = nothing
    queue_group::Union{Nothing, String} = nothing
    queue_group_disabled::Bool = false
    stats_handler::Union{Nothing, Function} = nothing
    done_handler::Union{Nothing, Function} = nothing
    error_handler::Union{Nothing, Function} = nothing
end

mutable struct EndpointStats
    name::String
    subject::String
    queue_group::String
    num_requests::Int
    num_errors::Int
    last_error::String
    processing_time::Int64
    average_processing_time::Int64
end

mutable struct Endpoint
    service::Any
    name::String
    subject::String
    handler::Function
    metadata::Dict{String,String}
    queue_group::String
    queue_group_disabled::Bool
    subscription::Union{Nothing, NATS.Subscription}
    stats::EndpointStats
end

mutable struct Service
    connection::NATS.Connection
    config::ServiceConfig
    id::String
    endpoints::Vector{Endpoint}
    verb_subscriptions::Vector{NATS.Subscription}
    lock::ReentrantLock
    started_unix_nanos::Int64
    stopped::Bool
end

struct Group
    service::Service
    prefix::String
    queue_group::Union{Nothing, String}
    queue_group_disabled::Bool
end

mutable struct ServiceRequest
    service::Service
    endpoint::Union{Nothing, Endpoint}
    msg::NATS.Msg
    response_error::Any
end

service_id() = bytes2hex(rand(Random.default_rng(), UInt8, 12))
unix_nanos() = Int64(round(time() * 1_000_000_000))

string_dict(::Nothing) = Dict{String,String}()
string_dict(dict::Dict{String,String}) = copy(dict)
function string_dict(dict)
    out = Dict{String,String}()
    for (key, value) in dict
        out[String(key)] = String(value)
    end
    return out
end

function validate_name(name::AbstractString, label::AbstractString)
    occursin(NAME_RE, name) || throw(ConfigValidationError("$label should be non-empty and contain only alphanumeric characters, dashes, and underscores"))
    return String(name)
end

function validate_semver(version::AbstractString)
    occursin(SEMVER_RE, version) || throw(ConfigValidationError("version should be non-empty and match SemVer"))
    return String(version)
end

function validate_service_subject(subject::AbstractString, label::AbstractString)
    subject_s = String(subject)
    (!isempty(subject_s) && occursin(SUBJECT_RE, subject_s)) ||
        throw(ConfigValidationError("invalid $label"))
    return subject_s
end

function validate_config(config::ServiceConfig)
    validate_name(config.name, "service name")
    validate_semver(config.version)
    if config.queue_group !== nothing && !isempty(config.queue_group::String)
        validate_service_subject(config.queue_group::String, "queue group")
    end
    if config.endpoint !== nothing
        endpoint = config.endpoint::EndpointConfig
        endpoint.handler === nothing && throw(ConfigValidationError("endpoint handler is required"))
        endpoint.subject === nothing && throw(ConfigValidationError("endpoint subject is required"))
        validate_service_subject(endpoint.subject::String, "endpoint subject")
    end
    return nothing
end

function control_subject(verb::AbstractString, name::AbstractString = "", id::AbstractString = "")
    verb_s = uppercase(String(verb))
    verb_s in (PING, STATS, INFO) || throw(UnsupportedVerbError(verb_s))
    name_s = String(name)
    id_s = String(id)
    isempty(name_s) && !isempty(id_s) && throw(ServiceNameRequiredError())
    isempty(name_s) && return "$API_PREFIX.$verb_s"
    isempty(id_s) && return "$API_PREFIX.$verb_s.$name_s"
    return "$API_PREFIX.$verb_s.$name_s.$id_s"
end

function resolve_queue_group(custom, parent, disabled::Bool, parent_disabled::Bool)
    disabled && return "", true
    if custom !== nothing && !isempty(custom::String)
        return custom::String, false
    end
    parent_disabled && return "", true
    if parent !== nothing && !isempty(parent::String)
        return parent::String, false
    end
    return DEFAULT_QUEUE_GROUP, false
end

function endpoint_stats(name::String, subject::String, queue_group::String)
    return EndpointStats(name, subject, queue_group, 0, 0, "", Int64(0), Int64(0))
end

function endpoint_info(endpoint::Endpoint)
    return (
        name = endpoint.name,
        subject = endpoint.subject,
        queue_group = endpoint.queue_group,
        metadata = copy(endpoint.metadata),
    )
end

function service_identity(service::Service)
    return (
        name = service.config.name,
        id = service.id,
        version = service.config.version,
        metadata = copy(service.config.metadata),
    )
end

function info(service::Service)
    lock(service.lock)
    try
        return (
            service_identity(service)...,
            type = INFO_RESPONSE_TYPE,
            description = service.config.description,
            endpoints = endpoint_info.(service.endpoints),
        )
    finally
        unlock(service.lock)
    end
end

function stats_tuple(service::Service, endpoint::Endpoint)
    base = (
        name = endpoint.stats.name,
        subject = endpoint.stats.subject,
        queue_group = endpoint.stats.queue_group,
        num_requests = endpoint.stats.num_requests,
        num_errors = endpoint.stats.num_errors,
        last_error = endpoint.stats.last_error,
        processing_time = endpoint.stats.processing_time,
        average_processing_time = endpoint.stats.average_processing_time,
    )
    service.config.stats_handler === nothing && return base
    return (base..., data = service.config.stats_handler(endpoint))
end

function stats(service::Service)
    lock(service.lock)
    try
        return (
            service_identity(service)...,
            type = STATS_RESPONSE_TYPE,
            started = service.started_unix_nanos,
            endpoints = [stats_tuple(service, endpoint) for endpoint in service.endpoints],
        )
    finally
        unlock(service.lock)
    end
end

function ping(service::Service)
    return (service_identity(service)..., type = PING_RESPONSE_TYPE)
end

data(request::ServiceRequest) = request.msg.data
payload(request::ServiceRequest) = String(request.msg.data)
headers(request::ServiceRequest) = request.msg.headers
subject(request::ServiceRequest) = request.msg.subject
reply(request::ServiceRequest) = request.msg.reply

function respond(request::ServiceRequest, response = nothing; headers::Vector{Pair{String,String}} = Pair{String,String}[])
    try
        NATS.respond(request.service.connection, request.msg, response; headers)
    catch err
        request.response_error = RespondError(sprint(showerror, err))
        throw(request.response_error)
    end
    return nothing
end

respond_json(request::ServiceRequest, response; kwargs...) =
    respond(request, JSON3.write(response); kwargs...)

function respond_error(request::ServiceRequest, code::AbstractString, description::AbstractString, response = nothing; headers::Vector{Pair{String,String}} = Pair{String,String}[])
    isempty(code) && throw(ArgumentError("service error code is required"))
    isempty(description) && throw(ArgumentError("service error description is required"))
    service_headers = Pair{String,String}[ERROR_HEADER => String(description), ERROR_CODE_HEADER => String(code)]
    append!(service_headers, headers)
    respond(request, response; headers = service_headers)
    request.response_error = "$(code):$(description)"
    return nothing
end

function record_handler_error!(service::Service, endpoint::Endpoint, err)
    lock(service.lock)
    try
        endpoint.stats.num_errors += 1
        endpoint.stats.last_error = sprint(showerror, err)
    finally
        unlock(service.lock)
    end
    if service.config.error_handler !== nothing
        @async service.config.error_handler(service, HandlerError(endpoint.subject, err))
    end
    NATS.notify_error!(service.connection, HandlerError(endpoint.subject, err))
    return nothing
end

function handle_endpoint_request(endpoint::Endpoint, msg::NATS.Msg)
    service = endpoint.service::Service
    request = ServiceRequest(service, endpoint, msg, nothing)
    started = time_ns()
    try
        endpoint.handler(request)
    catch err
        record_handler_error!(service, endpoint, err)
    finally
        elapsed_raw = time_ns() - started
        elapsed = Int64(min(elapsed_raw, UInt64(typemax(Int64))))
        lock(service.lock)
        try
            endpoint.stats.num_requests += 1
            endpoint.stats.processing_time += elapsed
            endpoint.stats.average_processing_time = endpoint.stats.processing_time ÷ endpoint.stats.num_requests
            if request.response_error !== nothing
                endpoint.stats.num_errors += 1
                endpoint.stats.last_error = String(request.response_error)
            end
        finally
            unlock(service.lock)
        end
    end
    return nothing
end

function normalize_pending_limits(msg_limit, bytes_limit)
    msg_limit === nothing && bytes_limit === nothing && return nothing
    msg = msg_limit === nothing ? -1 : Int(msg_limit)
    bytes = bytes_limit === nothing ? -1 : Int(bytes_limit)
    msg == 0 && throw(ConfigValidationError("endpoint message pending limit must be nonzero"))
    bytes == 0 && throw(ConfigValidationError("endpoint byte pending limit must be nonzero"))
    return msg, bytes
end

function add_endpoint_impl!(service::Service, name::AbstractString, subject::AbstractString, handler::Function, metadata, queue_group::String, no_queue::Bool, channel_size)
    name_s = validate_name(name, "endpoint name")
    subject_s = validate_service_subject(subject, "endpoint subject")
    !isempty(queue_group) && validate_service_subject(queue_group, "endpoint queue group")
    lock(service.lock)
    try
        service.stopped && throw(ConfigValidationError("service is stopped"))
    finally
        unlock(service.lock)
    end
    endpoint = Endpoint(
        service,
        name_s,
        subject_s,
        handler,
        string_dict(metadata),
        queue_group,
        no_queue,
        nothing,
        endpoint_stats(name_s, subject_s, queue_group),
    )
    sub_kwargs = channel_size === nothing ? (;) : (; channel_size = Int(channel_size))
    sub = if no_queue
        NATS.subscribe(msg -> handle_endpoint_request(endpoint, msg), service.connection, subject_s; sub_kwargs...)
    else
        NATS.subscribe(msg -> handle_endpoint_request(endpoint, msg), service.connection, subject_s; queue = queue_group, sub_kwargs...)
    end
    endpoint.subscription = sub
    lock(service.lock)
    try
        push!(service.endpoints, endpoint)
    finally
        unlock(service.lock)
    end
    return endpoint
end

function add_endpoint_impl!(
        service::Service,
        name::AbstractString,
        subject::AbstractString,
        handler::Function,
        metadata,
        queue_group::String,
        no_queue::Bool,
        channel_size,
        pending_limits,
)
    endpoint = add_endpoint_impl!(service, name, subject, handler, metadata, queue_group, no_queue, channel_size)
    if pending_limits !== nothing
        NATS.set_pending_limits(endpoint.subscription::NATS.Subscription, pending_limits...)
    end
    return endpoint
end

function add_endpoint!(service::Service, name::AbstractString, handler::Function; subject = nothing, metadata = nothing, queue_group = nothing, queue_group_disabled::Bool = false, channel_size = nothing, pending_msg_limit = nothing, pending_bytes_limit = nothing)
    subject_s = subject === nothing ? String(name) : String(subject)
    queue, no_queue = resolve_queue_group(queue_group, service.config.queue_group, queue_group_disabled, service.config.queue_group_disabled)
    pending_limits = normalize_pending_limits(pending_msg_limit, pending_bytes_limit)
    return add_endpoint_impl!(service, name, subject_s, handler, metadata, queue, no_queue, channel_size, pending_limits)
end

add_endpoint!(handler::Function, service::Service, name::AbstractString; kwargs...) =
    add_endpoint!(service, name, handler; kwargs...)

function add_endpoint!(group::Group, name::AbstractString, handler::Function; subject = nothing, metadata = nothing, queue_group = nothing, queue_group_disabled::Bool = false, channel_size = nothing, pending_msg_limit = nothing, pending_bytes_limit = nothing)
    subject_s = subject === nothing ? String(name) : String(subject)
    full_subject = isempty(group.prefix) ? subject_s : "$(group.prefix).$subject_s"
    queue, no_queue = resolve_queue_group(queue_group, group.queue_group, queue_group_disabled, group.queue_group_disabled)
    pending_limits = normalize_pending_limits(pending_msg_limit, pending_bytes_limit)
    return add_endpoint_impl!(group.service, name, full_subject, handler, metadata, queue, no_queue, channel_size, pending_limits)
end

add_endpoint!(handler::Function, group::Group, name::AbstractString; kwargs...) =
    add_endpoint!(group, name, handler; kwargs...)

function add_group(service::Service, prefix::AbstractString; queue_group = nothing, queue_group_disabled::Bool = false)
    queue, no_queue = resolve_queue_group(queue_group, service.config.queue_group, queue_group_disabled, service.config.queue_group_disabled)
    return Group(service, String(prefix), queue, no_queue)
end

function add_group(group::Group, prefix::AbstractString; queue_group = nothing, queue_group_disabled::Bool = false)
    queue, no_queue = resolve_queue_group(queue_group, group.queue_group, queue_group_disabled, group.queue_group_disabled)
    prefix_s = String(prefix)
    full_prefix = isempty(group.prefix) ? prefix_s : isempty(prefix_s) ? group.prefix : "$(group.prefix).$prefix_s"
    return Group(group.service, full_prefix, queue, no_queue)
end

function add_monitor!(service::Service, verb::AbstractString, subject::AbstractString, response)
    sub = NATS.subscribe(msg -> begin
        req = ServiceRequest(service, nothing, msg, nothing)
        respond(req, JSON3.write(response()))
    end, service.connection, subject)
    push!(service.verb_subscriptions, sub)
    return sub
end

function add_monitors!(service::Service)
    for (verb, response) in (
        INFO => () -> info(service),
        PING => () -> ping(service),
        STATS => () -> stats(service),
    )
        add_monitor!(service, verb, control_subject(verb), response)
        add_monitor!(service, verb, control_subject(verb, service.config.name), response)
        add_monitor!(service, verb, control_subject(verb, service.config.name, service.id), response)
    end
    return nothing
end

function add_service(conn::NATS.Connection, config::ServiceConfig)
    validate_config(config)
    normalized = ServiceConfig(
        name = config.name,
        version = config.version,
        description = config.description,
        metadata = string_dict(config.metadata),
        endpoint = config.endpoint,
        queue_group = config.queue_group,
        queue_group_disabled = config.queue_group_disabled,
        stats_handler = config.stats_handler,
        done_handler = config.done_handler,
        error_handler = config.error_handler,
    )
    service = Service(conn, normalized, service_id(), Endpoint[], NATS.Subscription[], ReentrantLock(), unix_nanos(), false)
    if normalized.endpoint !== nothing
        endpoint = normalized.endpoint::EndpointConfig
        queue_group = endpoint.queue_group === nothing ? normalized.queue_group : endpoint.queue_group
        add_endpoint!(
            service,
            "default",
            endpoint.handler::Function;
            subject = endpoint.subject,
            metadata = endpoint.metadata,
            queue_group,
            queue_group_disabled = endpoint.queue_group_disabled,
            channel_size = endpoint.channel_size,
            pending_msg_limit = endpoint.pending_msg_limit,
            pending_bytes_limit = endpoint.pending_bytes_limit,
        )
    end
    try
        add_monitors!(service)
    catch
        try stop(service) catch end
        rethrow()
    end
    return service
end

function add_service(conn::NATS.Connection; kwargs...)
    return add_service(conn, ServiceConfig(; kwargs...))
end

function reset!(service::Service)
    lock(service.lock)
    try
        for endpoint in service.endpoints
            endpoint.stats = endpoint_stats(endpoint.name, endpoint.subject, endpoint.queue_group)
        end
        service.started_unix_nanos = unix_nanos()
    finally
        unlock(service.lock)
    end
    return service
end

function stop(service::Service; timeout::Real = service.connection.options.drain_timeout)
    subs = NATS.Subscription[]
    done_handler = nothing
    lock(service.lock)
    try
        service.stopped && return nothing
        for endpoint in service.endpoints
            endpoint.subscription === nothing || push!(subs, endpoint.subscription)
        end
        append!(subs, service.verb_subscriptions)
        empty!(service.endpoints)
        empty!(service.verb_subscriptions)
        service.stopped = true
        done_handler = service.config.done_handler
    finally
        unlock(service.lock)
    end
    for sub in subs
        try
            NATS.drain(sub; timeout)
        catch err
            if !(err isa NATS.BadSubscriptionError || err isa NATS.ConnectionClosedError || err isa NATS.ConnectionReconnectingError)
                rethrow()
            end
        end
    end
    done_handler === nothing || @async done_handler(service)
    return nothing
end

Base.close(service::Service) = stop(service)
function stopped(service::Service)
    lock(service.lock)
    try
        return service.stopped
    finally
        unlock(service.lock)
    end
end

end
