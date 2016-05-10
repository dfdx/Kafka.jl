
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
    results::Channel{Any}
end

function KafkaClient(host::AbstractString, port::Int; resp_loop=true)
    sock = connect(host, port)
    inprogress = Dict{Int64, Type}()
    results = Channel{Any}(10_000)
    kc = KafkaClient(sock, 0, inprogress, results)
    if resp_loop
        @async handle_responses(kc)
    end
    return kc
end

function handle_response(kc::KafkaClient)
    # TODO: handle timed-out requests
    header = recv_header(kc.sock)      
    T = kc.inprogress[header.correlation_id]    
    resp = readobj(kc.sock, T)    
    delete!(kc.inprogress, header.correlation_id)
    put!(kc.results, resp)
end

function handle_responses(kc::KafkaClient)
    while true
        handle_response(kc)
    end
end

function register_response_type{T}(kc::KafkaClient, ::Type{T})
    kc.last_cor_id += 1
    id = kc.last_cor_id
    kc.inprogress[id] = T
    return id
end

function metadata{S<:AbstractString}(kc::KafkaClient, topics::Vector{S})
    cor_id = register_response_type(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    send_request(kc.sock, header, req)
end

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
    message_set = [MessageSetElement(0, obj_size(msg), msg) for msg in messages]
    message_set_size = reduce(+, map(obj_size, message_set))
    partition_data = PartitionData(partition, message_set_size, message_set)
    topic_data = TopicData(topic, [partition_data])
    required_acks = 1 # broker will send response after writing to a local log
    timeout = 10_000  # in milliseconds
    req = ProduceRequest(required_acks, timeout, [topic_data])
    return req
end

function produce(kc::KafkaClient, topic::AbstractString, partition_id::Integer,
                 kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    partition = Int32(partition_id)
    cor_id = register_response_type(kc, ProduceResponse)
    header = RequestHeader(PRODUCE, 0, cor_id, DEFAULT_ID)
    req = make_produce_request(topic, partition, kvs)
    send_request(kc.sock, header, req)
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
    cor_id = register_response_type(kc, FetchResponse)
    header = RequestHeader(FETCH, 0, cor_id, DEFAULT_ID)
    req = make_fetch_request(topic, partition, offset)
    send_request(kc.sock, header, req)
end
