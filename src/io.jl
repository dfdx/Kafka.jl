
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

# tuples
function writeobj(io::IO, tp::Tuple)
    for x in tp
        writeobj(io, x)
    end
end
# there should be a better way to do it, but overloading for
# 1-4 element tuples works too
function readobj{T}(io::IO, ::Type{Tuple{T}})
    return readobj(io, T)
end
function readobj{T1,T2}(io::IO, ::Type{Tuple{T1,T2}})
    v1 = readobj(io, T1)
    v2 = readobj(io, T2)
    return (v1, v2)
end
function readobj{T1,T2,T3}(io::IO, ::Type{Tuple{T1,T2,T3}})
    v1 = readobj(io, T1)
    v2 = readobj(io, T2)
    v3 = readobj(io, T3)
    return (v1, v2, v3)
end
function readobj{T1,T2,T3,T4}(io::IO, ::Type{Tuple{T1,T2,T3,T4}})
    v1 = readobj(io, T1)
    v2 = readobj(io, T2)
    v3 = readobj(io, T3)
    v4 = readobj(io, T4)
    return (v1, v2, v3, v4)
end

# composite types
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

# MessageSet and PartitionFetchResult
# This is the most weird part of the protocol: MessageSet, being an array,
# doesn't have normal Int32 prefix, so isn't self-containing. To read it
# we need an additional parameter - message set size, contained only in
# PartitionFetchResult. Note, that message set size is also Int32, but contains
# number of bytes in message set instead of number of elements
function writeobj(io::IO, message_set::MessageSet)
    # skip array header
    for msg_elem in message_set.elements
        writeobj(io, msg_elem)
    end
end
function readobj(io::IO, ::Type{MessageSet}, message_set_size::Int32)
    message_set_bytes = readbytes(io, message_set_size)
    buf = IOBuffer()
    write(buf, message_set_bytes)
    seek(buf, 0)
    elements = MessageSetElement[]
    while !eof(buf)
        elem = readobj(buf, MessageSetElement)
        push!(elements, elem)
    end
    return MessageSet(elements)
end
function readobj(io::IO, ::Type{PartitionFetchResult})
    partition = readobj(io, Int32)
    error_code = readobj(io, Int16)
    highwater_mark_offset = readobj(io, Int64)
    message_set_size = readobj(io, Int32)
    # we don't know how many messeges there are, so reading that much bytes
    # and trying to parse as many messages as we can    
    message_set = readobj(io, MessageSet, message_set_size)
    return PartitionFetchResult(partition, error_code, highwater_mark_offset,
                                message_set_size, message_set)
end




# requests and responses

function obj_size(objs...)
    buf = IOBuffer()
    for obj in objs
        writeobj(buf, obj)
    end
    return Int32(length(buf.data))
end

function send_request(io::IO, header::RequestHeader, req)
    len = obj_size(header, req)
    writeobj(io, len)
    writeobj(io, header)
    writeobj(io, req)
end
function recv_header(io::IO)
    readobj(io, Int32) # length, do we really need it?
    return readobj(io, ResponseHeader)
end
function recv_response_with_header{T}(io::IO, ::Type{T})
    header = recv_header(io)
    resp = readobj(io, T)
    return header, resp
end
recv_response{T}(io::IO, ::Type{T}) = recv_response_with_header(io, T)[2]
