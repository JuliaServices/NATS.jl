module NATS

import Base: close, flush

using HTTP
using JSON3
using Random
using Reseau
using Sockets
using StructTypes
using URIs
using Base64

const DEFAULT_URL = "nats://127.0.0.1:4222"
const CLIENT_LANG = "julia"
const CLIENT_VERSION = "0.1.0"

include("errors.jl")
include("protocol.jl")
include("transport.jl")
include("auth.jl")
include("connection.jl")
include("micro/Micro.jl")
include("jetstream/JetStream.jl")

end
