using Kafka
using Base.Test

include("../src/core.jl")

io = IOBuffer()

writeobj(io, "test")
writeobj(io, 42)
writeobj(io, UInt8[1, 2, 3])
writeobj(io, ["hello", "world"])

seek(io, 0)

@test readobj(io, ASCIIString) == "test"
@test readobj(io, Int64) == 42
@test readobj(io, Vector{UInt8}) == UInt8[1, 2, 3]
@test readobj(io, Vector{ASCIIString}) == ["hello", "world"]

println("Ok.")
