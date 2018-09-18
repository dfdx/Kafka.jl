
## metadata

function init_metadata(sock::TCPSocket)
    header = RequestHeader(METADATA, 0, 0, DEFAULT_ID)
    req = AllTopicsMetadataRequest()
    send_request(sock, header, req)
    header = recv_header(sock)
    resp = readobj(sock, TopicMetadataResponse)
    return process_response(resp)
end

function _metadata(kc::KafkaClient, topics::Vector{S}) where S<:AbstractString
    cor_id, ch = register_request(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    send_request(random_broker(kc), header, req)
    return ch
end

function metadata(kc::KafkaClient, topics::Vector{S}) where {S<:AbstractString}
    ch = _metadata(kc, topics)
    return map(process_response, ch)
end

function _metadata(kc::KafkaClient)
    cor_id, ch = register_request(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = AllTopicsMetadataRequest()
    send_request(random_broker(kc), header, req)
    return ch
end

function metadata(kc::KafkaClient)
    ch = _metadata(kc)
    return map(process_response, ch)
end

function parse_metadata(md::PartitionMetadata)
    if md.partition_error_code != 0
        error("TopicMetadataRequest failed with error code " *
              "$(md.partition_error_code)")
    end
    return Dict(md.partition_id =>
                Dict(:leader => md.leader, :replicas => md.replicas,
                     :isr => md.isr))
end

function parse_metadata(md::TopicMetadata)
    if md.topic_error_code != 0
        error("TopicMetadataRequest failed with error code " *
              "$(md.topic_error_code)")
    end
    partition_metadata_dict = merge(map(parse_metadata, md.partition_metadata)...)
    return Dict(md.topic_name => partition_metadata_dict)
end

function process_response(resp::TopicMetadataResponse)
    if !isempty(resp.topic_metadata)
        topic_metadata_dict = merge(map(parse_metadata, resp.topic_metadata)...)
    else
        topic_metadata_dict = Dict{Symbol,Any}()
    end
    return Dict(:brokers => resp.brokers, :topics => topic_metadata_dict)
end


## produce

# using Message version 0
function make_message(key::Vector{UInt8}, value::Vector{UInt8})
    magic_byte = Int8(0)
    # magic_byte = Int8(1)  # current version of Kafka message binary format
    attributes = Int8(0)  # no compression
    buf = IOBuffer()
    writeobj(buf, magic_byte)
    writeobj(buf, attributes)
    writeobj(buf, key)
    writeobj(buf, value)
    crc = reinterpret(Int32, crc32(take!(buf)))
    return Message(Int32(crc), magic_byte, attributes, key, value)
end

function make_produce_request(topic::AbstractString, partition_id::Integer,
                              kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    partition = Int32(partition_id)
    messages = [make_message(k, v) for (k, v) in kvs]
    message_set_elems = [OffsetMessage(0, obj_size(msg), msg)
                         for msg in messages]
    message_set_size = reduce(+, map(obj_size, message_set_elems))
    partition_data = ProduceRequestPartitionData(partition, message_set_size,
                                   MessageSet(message_set_elems))
    topic_data = ProduceRequestTopicData(topic, [partition_data])
    required_acks = 1 # broker will send response after writing to a local log
    timeout = 10_000  # in milliseconds
    req = ProduceRequest(required_acks, timeout, [topic_data])
    return req
end

function _produce(kc::KafkaClient, topic::AbstractString, partition::Integer,
                  kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    pid = Int32(partition)
    cor_id, ch = register_request(kc, ProduceResponse)
    header = RequestHeader(PRODUCE, 0, cor_id, DEFAULT_ID)
    req = make_produce_request(topic, pid, kvs)
    send_request(find_leader(kc, topic, pid), header, req)
    return ch
end

function produce(kc::KafkaClient, topic::AbstractString, partition::Integer,
                 kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    ensure_leader(kc, topic, partition)
    ch = _produce(kc, topic, partition, kvs)
    return map(process_response, ch)
end

function process_response(resp::ProduceResponse)
    partition, error_code, offset = resp.responses[1][2][1]
    if error_code != 0
        error("ProduceRequest failed with error code: $error_code")
    end
    return offset
end


## fetch

function make_fetch_request(topic::AbstractString, prt_id::Integer, offset::Int64,
                            max_wait_time::Int, min_bytes::Int, max_bytes::Int)
    partition = Int32(prt_id)
    partition_fetch = FetchRequestPartitionData(partition, offset, max_bytes)
    topic_fetch = FetchRequestTopicData(topic, [partition_fetch])
    req = FetchRequest(-1, max_wait_time, min_bytes, [topic_fetch])
    return req
end

function _fetch(kc::KafkaClient, topic::AbstractString,
                prt_id::Integer, offset::Int64;
                max_wait_time=100, min_bytes=1024, max_bytes=1024*1024)
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
               max_wait_time=100, min_bytes=1024, max_bytes=1024*1024,
               key_type=Vector{UInt8}, value_type=Vector{UInt8})
    ensure_leader(kc, topic, partition)
    ch = _fetch(kc, topic, partition, offset; max_wait_time=max_wait_time,
                min_bytes=min_bytes, max_bytes=max_bytes)
    return map(r -> process_response(r, key_type, value_type), ch)
end

function process_response(resp::FetchResponse)
    pr = resp.topic_results[1].partition_results[1]
    if pr.error_code != 0
        error("FetchRequest failed with error code: $(pr.error_code)")
    end
    elements = pr.message_set.elements
    results = [(elem.offset, elem.message.key, elem.message.value)
               for elem in elements]
    return results
end

function process_response(resp::FetchResponse, ::Type{K}, ::Type{V}) where {K,V}
    return [(offset, convert(K, key), convert(V, value))
            for (offset, key, value) in process_response(resp)]
end


## offset listing

function make_offset_request(replica_id::Int32,
                             topic::AbstractString, partition::Int32,
                             time::Int64, max_number_of_offsets::Int64)
    partition_data = OffsetRequestPartitionData(partition, time,
                                                max_number_of_offsets)
    topic_data = OffsetRequestTopicData(topic, [partition_data])
    req = OffsetRequest(replica_id, [topic_data])
    return req
end


function _list_offsets(kc::KafkaClient, topic::AbstractString, partition::Integer,
                       time::Int64, max_number_of_offsets::Int64)
    pid = Int32(partition)
    cor_id, ch = register_request(kc, OffsetResponse)
    header = RequestHeader(OFFSETS, 0, cor_id, DEFAULT_ID)
    req = make_offset_request(find_leader_id(kc, topic, partition),
                              topic, pid, time, max_number_of_offsets)
    send_request(find_leader(kc, topic, partition), header, req)
    return ch
end


function list_offsets(kc::KafkaClient, topic::AbstractString, partition::Integer,
                      time::Int64, max_number_of_offsets::Int64)
    ensure_leader(kc, topic, partition)
    ch = _list_offsets(kc, topic, partition, time, max_number_of_offsets)
    return map(process_response, ch)
end


function earliest_offset(kc::KafkaClient,
                         topic::AbstractString, partition::Integer)
    ch = list_offsets(kc, topic, partition, -2, 1)
    return map(offsets -> offsets[1], ch)
end


function latest_offset(kc::KafkaClient,
                       topic::AbstractString, partition::Integer)
    ch = list_offsets(kc, topic, partition, -1, 1)
    return map(offsets -> offsets[1], ch)
end


function process_response(resp::OffsetResponse)
    pd = resp.topic_data[1].partition_data[1]
    if pd.error_code != 0
        error("OffsetRequest failed with error code: $(pd.error_code)")
    end
    return pd.offsets
end


## API versions

function api_versions(kc::KafkaClient)
    sock = random_broker(kc)
    cor_id, ch = register_request(kc, ApiVersionsResponse)
    header = RequestHeader(API_VERSIONS, 1, cor_id, DEFAULT_ID)
    send_request(sock, header)
    resp = take!(ch)
    return resp.api_versions
end
