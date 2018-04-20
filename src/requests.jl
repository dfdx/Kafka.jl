## client requests

## metadata

function init_metadata(sock::TCPSocket)
    header = RequestHeader(METADATA, 0, 0, DEFAULT_ID)
    req = AllTopicsMetadataRequest()
    send_request(sock, header, req)
    header = recv_header(sock)
    resp = readobj(sock, TopicMetadataResponse)
    if !isempty(resp.topic_metadata)
        topic_metadata_dict = merge(map(parse_metadata, resp.topic_metadata)...)
    else
        topic_metadata_dict = Dict{Symbol,Any}()
    end
    return Dict(:brokers => resp.brokers, :topics => topic_metadata_dict)
end


function metadata{S<:AbstractString}(kc::KafkaClient, topics::Vector{S})
    cor_id = register_request(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = TopicMetadataRequest(topics)
    node_id, sock = random_broker(kc)
    send_request(sock, header, req)
    resp = take!(kc.out_channels[node_id])
    if !isempty(resp.topic_metadata)
        topic_metadata_dict = merge(map(parse_metadata, resp.topic_metadata)...)
    else
        topic_metadata_dict = Dict{Symbol,Any}()
    end
    return Dict(:brokers => resp.brokers, :topics => topic_metadata_dict)
end


function metadata(kc::KafkaClient)
    cor_id = register_request(kc, TopicMetadataResponse)
    header = RequestHeader(METADATA, 0, cor_id, DEFAULT_ID)
    req = AllTopicsMetadataRequest()
    node_id, sock = random_broker(kc)
    send_request(sock, header, req)
    resp = take!(kc.out_channels[node_id])
    if !isempty(resp.topic_metadata)
        topic_metadata_dict = merge(map(parse_metadata, resp.topic_metadata)...)
    else
        topic_metadata_dict = Dict{Symbol,Any}()
    end
    return Dict(:brokers => resp.brokers, :topics => topic_metadata_dict)
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

function produce(kc::KafkaClient, topic::AbstractString, partition::Integer,
                 kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    ensure_leader(kc, topic, partition)
    pid = Int32(partition)
    cor_id = register_request(kc, ProduceResponse)
    header = RequestHeader(PRODUCE, 0, cor_id, DEFAULT_ID)
    req = make_produce_request(topic, pid, kvs)
    resp = send_and_read(kc, topic, pid, header, req)
    partition, error_code, offset = resp.responses[1][2][1]
    error_code != 0 && error("ProduceRequest failed with error code: $error_code")
    return offset
    # return ch
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

function fetch(kc::KafkaClient, topic::AbstractString,
               prt_id::Integer, offset::Int64;
               max_wait_time=100, min_bytes=1024, max_bytes=1024*1024)    
    partition = Int32(prt_id)
    ensure_leader(kc, topic, partition)
    cor_id = register_request(kc, FetchResponse)
    header = RequestHeader(FETCH, 0, cor_id, DEFAULT_ID)
    req = make_fetch_request(topic, partition, offset,
                             max_wait_time, min_bytes, max_bytes)
    # send_request(find_leader(kc, topic, partition), header, req)
    resp = send_and_read(kc, topic, partition, header, req)
    pr = resp.topic_results[1].partition_results[1]
    pr.error_code != 0 && error("FetchRequest failed with error code: $(pr.error_code)")
    elements = pr.message_set.elements
    results = [(elem.offset, elem.message.key, elem.message.value)
               for elem in elements]
    return results
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


function list_offsets(kc::KafkaClient, topic::AbstractString, partition::Integer,
                      time::Int64, max_number_of_offsets::Int64)
    pid = Int32(partition)
    cor_id = register_request(kc, OffsetResponse)
    header = RequestHeader(OFFSETS, 0, cor_id, DEFAULT_ID)
    req = make_offset_request(find_leader_id(kc, topic, partition),
                              topic, pid, time, max_number_of_offsets)
    node_id, sock = find_leader(kc, topic, partition)
    send_request(sock, header, req)
    resp = take!(kc.out_channels[node_id])
    pd = resp.topic_data[1].partition_data[1]
    if pd.error_code != 0
        error("OffsetRequest failed with error code: $(pd.error_code)")
    end
    return pd.offsets
end


function earliest_offset(kc::KafkaClient,
                         topic::AbstractString, partition::Integer)
    offsets =  list_offsets(kc, topic, partition, -2, 1)
    return offsets[1]
end


function latest_offset(kc::KafkaClient,
                       topic::AbstractString, partition::Integer)
    offsets = list_offsets(kc, topic, partition, -1, 1)
    return return offsets[1]
end


## API versions

function api_versions(kc::KafkaClient)
    node_id, sock = random_broker(kc)
    cor_id = register_request(kc, ApiVersionsResponse)
    header = RequestHeader(API_VERSIONS, 1, cor_id, DEFAULT_ID)
    send_request(sock, header)
    resp = take!(kc.out_channels[node_id])
    return resp.api_versions
end
