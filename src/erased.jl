# Type-erased values with statically dispatched operations. A value of any type
# `T` is stored with `@cfunction` pointers compiled for `T`; calling them through
# a fixed-signature `ccall` reaches code compiled for `T` without dispatching on
# the stored value. Code that holds only the erased value therefore compiles
# statically, as `juliac --trim` requires, whatever `T` is.
#
# The pointers are created at runtime and are valid only in the process that
# created them, so erased values must not be serialized into package images.

# The `Ref{T}` argument for `x`, and the object that keeps it valid.
function erased_object(x::T) where {T}
    root = Base.cconvert(Ref{T}, x)
    return Ptr{Cvoid}(Base.unsafe_convert(Ref{T}, root)), root
end

mutable struct CallbackCall
    args::Any
    result::Any
end

# A user's function `f` together with the kind `K` of callback it serves.
struct CallbackTarget{K, F}
    f::F
end

function run_callback(target::CallbackTarget{K}, data::Ptr{Cvoid}) where {K}
    call = unsafe_pointer_to_objref(data)::CallbackCall
    call.result = K(target.f, call.args)
    return nothing
end

callback_pointer(::Type{W}) where {W} = @cfunction(run_callback, Cvoid, (Ref{W}, Ptr{Cvoid}))

"""
    Callback{K}(f)

A user callback `f` stored without its type. `invoke_callback(cb, args)` returns
`K(f, args)`, where `K` calls `f` and normalizes its result, so each kind of
callback produces one result type. Options and setters convert plain functions
to `Callback`s, so users keep passing ordinary functions.

`f` is called with the arguments' runtime values. A callback that receives an
exception gets it with an unknown type; under `juliac --trim`, annotate that
argument with `@nospecialize` or keep the callback small enough to inline.
"""
struct Callback{K}
    fptr::Ptr{Cvoid}
    obj::Ptr{Cvoid}
    fn::Any # the user's function, returned by the handler getters
    root::Any # keeps `obj` valid
end

function Callback{K}(f::F) where {K, F}
    obj, root = erased_object(CallbackTarget{K, F}(f))
    return Callback{K}(callback_pointer(CallbackTarget{K, F}), obj, f, root)
end

Base.convert(::Type{Callback{K}}, f::Function) where {K} = Callback{K}(f)

callback_function(::Nothing) = nothing
callback_function(cb::Callback) = cb.fn

function invoke_callback(cb::Callback, @nospecialize(args::Tuple))
    call = CallbackCall(args, nothing)
    GC.@preserve cb call ccall(cb.fptr, Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), cb.obj, pointer_from_objref(call))
    return call.result
end

erased_write_frame(io, data::Ptr{Cvoid}) = write_frame(io, unsafe_pointer_to_objref(data)::Vector{UInt8})
erased_close(io, ::Ptr{Cvoid}) = (close(io); nothing)
erased_reader_loop(io, data::Ptr{Cvoid}) = reader_loop(unsafe_pointer_to_objref(data)::Connection, io)

"""
    Transport(io)

A connection's transport `io` with `write_frame`, `close`, and the
connection's `reader_loop` compiled for its type. Connections store this
instead of `io` itself, so their code stays statically dispatched for TCP, TLS,
WebSocket, and test transports alike.
"""
struct Transport
    io::Any
    obj::Ptr{Cvoid}
    write::Ptr{Cvoid}
    close::Ptr{Cvoid}
    reader::Ptr{Cvoid}
    root::Any
end

function Transport(io::T) where {T}
    obj, root = erased_object(io)
    return Transport(
        io,
        obj,
        @cfunction(erased_write_frame, Cvoid, (Ref{T}, Ptr{Cvoid})),
        @cfunction(erased_close, Cvoid, (Ref{T}, Ptr{Cvoid})),
        @cfunction(erased_reader_loop, Cvoid, (Ref{T}, Ptr{Cvoid})),
        root,
    )
end

Base.convert(::Type{Transport}, io) = Transport(io)

transport_io(::Nothing) = nothing
transport_io(t::Transport) = t.io

function write_frame(t::Transport, frame::Vector{UInt8})
    GC.@preserve t frame ccall(t.write, Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), t.obj, pointer_from_objref(frame))
    return nothing
end

function Base.close(t::Transport)
    GC.@preserve t ccall(t.close, Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), t.obj, C_NULL)
    return nothing
end

# Runs `reader_loop(conn, t.io)` until the transport fails or is retired.
function run_reader_loop(t::Transport, conn)
    GC.@preserve t conn ccall(t.reader, Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), t.obj, pointer_from_objref(conn))
    return nothing
end

# Closes a connection's transport, if it has one, ignoring errors from a
# transport that already failed.
close_transport(::Nothing) = nothing
close_transport(t::Transport) = (try close(t) catch end; nothing)
