
# primitives, strings and arrays

writeobj(io::IO, n::Integer) = write(io, hton(n))
readobj{T<:Integer}(io::IO, ::Type{T}) = ntoh(read(io, T))

function writeobj(io::IO, s::String)
    len = Int16(length(s))
    writeobj(io, len > 0 ? len : -1)
    write(io, s)
end
function readobj(io::IO, ::Type{String})
    len = readobj(io, Int16)
    # Fix - sure hope everything is convertible (i.e. not 0x00).
    byteVec = Vector{UInt8}(len)
    byteslen = readbytes!(io, byteVec, len)
    return len > 0 ? String(byteVec[1:byteslen]) : ""
end

function writeobj{T}(io::IO, arr::Vector{T})
    len = Int32(length(arr))
    writeobj(io, len > 0 ? len : -1)
    for x in arr
        writeobj(io, x)
    end
end
function readobj{T}(io::IO, ::Type{Vector{T}})
    len = readobj(io, Int32)
    if len <= 0
        return T[]
    end
    arr = Array{T}(Int(len))
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

# Weird part: OffsetMessage, MessageSet and FetchResponsePartitionData
# Unlike other data types, OffsetMessage and MessageSet can't
# be encoded/decoded as a sum of its parts, but require special rules:
#
# 1. OffsetMessage contains field message_size with actual size (in bytes)
#    of encoded message. In most cases it is the same as if message were decoded
#    normally. But in some specific cases (e.g. when value is null), message
#    may contain additional bytes (for which I couldn't understand the meaning).
#    So we need to read message_size bytes from a stream to a buffer and
#    then parse actual message from that buffer.

function readobj(io::IO, ::Type{OffsetMessage})
    offset = readobj(io, Int64)
    message_size = readobj(io, Int32)
    data = readbytes(io, message_size)
    buf = IOBuffer()
    write(buf, data)
    seek(buf, 0)
    msg = readobj(buf, Message)
    return OffsetMessage(offset, message_size, msg)
end

# 2. MessageSet is essentially an array of OffsetMessages, but unlike other
#    arrays it doesn't contain Int32 prefix of length. Instead, its
#    parent struct (during reading) - FetchResponsePartitionData - contains a field
#    message_set_size. Just as with OffsetMessage, we need first to read
#    that much bytes into a buffer and then deserialize all full instances of
#    OffsetMessage. Note that data is copied from brokers as byte arrays
#    so the resulting buffer may contain some partial messages which we dismiss

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
    elements = OffsetMessage[]
    while !eof(buf)
        try
            elem = readobj(buf, OffsetMessage)
            push!(elements, elem)
        catch EOFError
            # incomplete message
        end
    end
    return MessageSet(elements)
end
function readobj(io::IO, ::Type{FetchResponsePartitionData})
    partition = readobj(io, Int32)
    error_code = readobj(io, Int16)
    highwater_mark_offset = readobj(io, Int64)
    message_set_size = readobj(io, Int32)
    # we don't know how many messeges there are, so reading that much bytes
    # and trying to parse as many messages as we can
    message_set = readobj(io, MessageSet, message_set_size)
    return FetchResponsePartitionData(partition, error_code, highwater_mark_offset,
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
