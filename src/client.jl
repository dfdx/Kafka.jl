
"""
Asynchronous client for Kafka brokers.
Workflow:

    # create KafkaClient using one of constructors
    kc = KafkaClient(...)
    # send requests
    metadata(kc, ...)
    produce(kc, ...)
    fetch(kc, ...)
    # pull results
    take!(kc.results)

"""
type KafkaClient
    sock::TCPSocket
    last_cor_id::Int64
    inprogress::Dict{Int64, Type}
    results::Dict{Int64, Channel{Any}}
end

function KafkaClient(host::AbstractString, port::Int; resp_loop=true)
    sock = connect(host, port)
    inprogress = Dict{Int64, Type}()
    results = Dict{Int64, Channel{Any}}()
    kc = KafkaClient(sock, 0, inprogress, results)
    if resp_loop
        @async handle_responses(kc)
    end
    return kc
end

function handle_response(kc::KafkaClient)
    eof(kc.sock) # stop until there's new data
    header = recv_header(kc.sock)
    cor_id = header.correlation_id
    T = kc.inprogress[cor_id]
    resp = readobj(kc.sock, T)    
    put!(kc.results[cor_id], resp)
    delete!(kc.inprogress, cor_id)
    delete!(kc.results, cor_id) # if nobody listens to a channel,
                                #  GC should delete it as well
end

function handle_responses(kc::KafkaClient)
    while true
        handle_response(kc)
    end
end

function register_request{T}(kc::KafkaClient, ::Type{T})
    kc.last_cor_id += 1
    id = kc.last_cor_id
    kc.inprogress[id] = T
    kc.results[id] = Channel(1)
    return id, kc.results[id]
end

# metadata

function metadata{S<:AbstractString}(kc::KafkaClient, topics::Vector{S})
    cor_id, ch = register_request(kc, TopicMetadataResponse)    
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    send_request(kc.sock, header, req)
    return ch
end

# produce 

# using Message version 0
function make_message(key::Vector{UInt8}, value::Vector{UInt8})
    magic_byte = Int8(1)  # current version of Kafka message binary format
    attributes = Int8(0)  # no compression
    buf = IOBuffer()
    write(buf, magic_byte)
    write(buf, attributes)
    write(buf, key)
    write(buf, value)
    crc = Int64(crc32(buf.data))  # TODO: CRC is calculated differently in Kafka
    if crc >= 2^31
        crc -= 2^32
    end
    return Message(Int32(crc), magic_byte, attributes, key, value)
end

function make_produce_request(topic::AbstractString, partition_id::Integer,
                              kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    partition = Int32(partition_id)
    messages = [make_message(k, v) for (k, v) in kvs]
    message_set_elems = [MessageSetElement(0, obj_size(msg), msg)
                         for msg in messages]
    message_set_size = reduce(+, map(obj_size, message_set_elems))
    partition_data = PartitionData(partition, message_set_size,
                                   MessageSet(message_set_elems))
    topic_data = TopicData(topic, [partition_data])
    required_acks = 1 # broker will send response after writing to a local log
    timeout = 10_000  # in milliseconds
    req = ProduceRequest(required_acks, timeout, [topic_data])
    return req
end

function produce(kc::KafkaClient, topic::AbstractString, partition_id::Integer,
                 kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    partition = Int32(partition_id)
    cor_id, ch = register_request(kc, ProduceResponse)
    header = RequestHeader(PRODUCE, 0, cor_id, DEFAULT_ID)
    req = make_produce_request(topic, partition, kvs)
    send_request(kc.sock, header, req)
    return ch
end

function make_fetch_request(topic::AbstractString, partition_id::Integer,
                            offset::Int64)
    partition = Int32(partition_id)
    partition_fetch = PartitionFetch(partition, offset, MAX_BYTES)
    topic_fetch = TopicFetch(topic, [partition_fetch])
    req = FetchRequest(-1, MAX_WAIT_TIME, MIN_BYTES, [topic_fetch])
    return req
end

function Base.fetch(kc::KafkaClient, topic::AbstractString, partition_id::Integer,
               offset::Int64)
    partition = Int32(partition_id)
    cor_id, ch = register_request(kc, FetchResponse)
    header = RequestHeader(FETCH, 0, cor_id, DEFAULT_ID)
    req = make_fetch_request(topic, partition, offset)
    send_request(kc.sock, header, req)
    return ch
end
