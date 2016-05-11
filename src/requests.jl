
## metadata

function initial_metadata{S<:AbstractString}(sock::TCPSocket, topics::Vector{S})
    header = RequestHeader(METADATA, 0, 0, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    send_request(sock, header, req)
    header = recv_header(sock)       
    resp = readobj(sock, TopicMetadataResponse)
    return parse_response(resp)
end

function _metadata{S<:AbstractString}(kc::KafkaClient, topics::Vector{S})
    cor_id, ch = register_request(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    send_request(random_broker(kc), header, req)
    return ch
end

function metadata{S<:AbstractString}(kc::KafkaClient, topics::Vector{S})
    ch = _metadata(kc, topics)
    return map(parse_response, ch)
end

function parse_metadata(md::PartitionMetadata)
    if md.partition_error_code != 0
        throw(ErrorException("TopicMetadataRequest failed with error code " *
                             "$(md.partition_error_code)"))
    end
    return Dict(md.partition_id =>
                Dict(:leader => md.leader, :replicas => md.replicas,
                     :isr => md.isr))
end

function parse_metadata(md::TopicMetadata)
    if md.topic_error_code != 0
        throw(ErrorException("TopicMetadataRequest failed with error code " *
                             "$(md.topic_error_code)"))
    end
    partition_metadata_dict = merge(map(parse_metadata, md.partition_metadata)...)
    return Dict(md.topic_name => partition_metadata_dict)
end

function parse_response(resp::TopicMetadataResponse)
    topic_metadata_dict = merge(map(parse_metadata, resp.topic_metadata)...)
    return Dict(:brokers => resp.brokers, :topics => topic_metadata_dict)
end


## produce

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


## fetch

function make_fetch_request(topic::AbstractString, prt_id::Integer, offset::Int64,
                            max_wait_time::Int, min_bytes::Int, max_bytes::Int)
    partition = Int32(prt_id)
    partition_fetch = PartitionFetch(partition, offset, max_bytes)
    topic_fetch = TopicFetch(topic, [partition_fetch])
    req = FetchRequest(-1, max_wait_time, min_bytes, [topic_fetch])
    return req
end

function _fetch(kc::KafkaClient, topic::AbstractString,
                prt_id::Integer, offset::Int64;
                max_wait_time=1000, min_bytes=1024, max_bytes=1024*1024)
    partition = Int32(prt_id)
    cor_id, ch = register_request(kc, FetchResponse)
    header = RequestHeader(FETCH, 0, cor_id, DEFAULT_ID)
    req = make_fetch_request(topic, partition, offset,
                             max_wait_time, min_bytes, max_bytes)
    send_request(find_leader(kc, topic, partition), header, req)
    return ch
end

function fetch(kc::KafkaClient, topic::AbstractString,
               partition::Integer, offset::Int64;
               max_wait_time=1000, min_bytes=1024, max_bytes=1024*1024,
               key_type=Vector{UInt8}, value_type=Vector{UInt8})
    ch = _fetch(kc, topic, partition, offset; max_wait_time=max_wait_time,
                min_bytes=min_bytes, max_bytes=max_bytes)
    return map(r -> parse_response(r, key_type, value_type), ch)
end

function parse_response(resp::FetchResponse)
    pr = resp.topic_results[1].partition_results[1]
    if pr.error_code != 0
        throw(ErrorException("FetchRequest failed with error code: " *
                             "$(pr.error_code)"))
    end
    elements = pr.message_set.elements
    results = [(elem.offset, elem.message.key, elem.message.value)
               for elem in elements]
    return results
end

function parse_response{K,V}(resp::FetchResponse, ::Type{K}, ::Type{V})
    return [(offset, convert(K, key), convert(V, value))
            for (offset, key, value) in parse_response(resp)]
end
