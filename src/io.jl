
# primitives, strings and arrays

writeobj(io::IO, n::Integer) = write(io, hton(n))
readobj{T<:Integer}(io::IO, ::Type{T}) = ntoh(read(io, T))

function writeobj(io::IO, s::ASCIIString)
    writeobj(io, Int16(length(s)))
    write(io, s)
end
function readobj(io::IO, ::Type{ASCIIString})
    len = readobj(io, Int16)
    return len > 0 ? bytestring(readbytes(io, len)) : ""
end

function writeobj(io::IO, bs::Vector{UInt8})
    writeobj(io, Int32(length(bs)))
    write(io, bs)
end
function readobj(io::IO, ::Type{Vector{UInt8}})
    len = readobj(io, Int32)
    return len > 0 ? readbytes(io, len) : UInt8[]
end

function writeobj{T}(io::IO, arr::Vector{T})
    writeobj(io, Int32(length(arr)))
    for x in arr
        writeobj(io, x)
    end
end
function readobj{T}(io::IO, ::Type{Vector{T}})
    len = readobj(io, Int32)
    if len <= 0
        return T[]
    end
    arr = Array(T, len)
    for i=1:len
        arr[i] = readobj(io, T)
    end
    return arr
end

function writeobj(io::IO, o)
    for f in fieldnames(o)
        writeobj(io, getfield(o, f))
    end
end
function readobj{T}(io::IO, ::Type{T})
    vals = Array(Any, length(T.types))
    for (i, t) in enumerate(T.types)
        vals[i] = readobj(io, t)
    end
    return T(vals...)
end

# requests and responses

function reqsize(req)
    buf = IOBuffer()
    writeobj(buf, req)
    return Int32(length(buf.data))
end

function sendreq(io::IO, req)    
    len = reqsize(req)
    writeobj(io, len)
    writeobj(io, req)
end
function recvresp{T}(io::IO, ::Type{T})
    readobj(io, Int32) # length, do we really need it?
    resp = readobj(io, T)
    return resp
end
