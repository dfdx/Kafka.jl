using Kafka
using Base.Test

io = IOBuffer()

Kafka.writeobj(io, "test")
Kafka.writeobj(io, 42)
Kafka.writeobj(io, UInt8[1, 2, 3])
Kafka.writeobj(io, ["hello", "world"])

seek(io, 0)

@test Kafka.readobj(io, String) == "test"
@test Kafka.readobj(io, Int64) == 42
@test Kafka.readobj(io, Vector{UInt8}) == UInt8[1, 2, 3]
@test Kafka.readobj(io, Vector{String}) == ["hello", "world"]

println("Ok.")
