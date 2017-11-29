using Kafka
import Kafka: writeobj, readobj
using Base.Test

io = IOBuffer()

writeobj(io, "test")
writeobj(io, 42)
writeobj(io, UInt8[1, 2, 3])
writeobj(io, ["hello", "world"])

seek(io, 0)

@test readobj(io, String) == "test"
@test readobj(io, Int64) == 42
@test readobj(io, Vector{UInt8}) == UInt8[1, 2, 3]
@test readobj(io, Vector{String}) == ["hello", "world"]

include("integration.jl")

println("Ok.")
