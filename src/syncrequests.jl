
function inc_cor_id!(kc::KClient)
    cor_id = kc.last_cor_id
    kc.last_cor_id += 1
    return cor_id
end

function read_response(sock, T; cor_id=-1)
    header = recv_header(sock)
    if cor_id != -1
        @assert cor_id == header.correlation_id
    end
    return readobj(sock, T)
end


## offset retrieval

function list_offsets(kc::KClient, topic::AbstractString, partition::Integer,
                      time::Int64, max_number_of_offsets::Int64)
    ensure_leader(kc, topic, partition)
    pid = Int32(partition)
    cor_id = inc_cor_id!(kc)
    header = RequestHeader(OFFSETS, 0, cor_id, DEFAULT_ID)
    req = make_offset_request(find_leader_id(kc, topic, partition),
                              topic, pid, time, max_number_of_offsets)
    sock = find_leader(kc, topic, partition)
    send_request(sock, header, req)
    # receive response
    resp = read_response(sock, OffsetResponse, cor_id=cor_id)
    pd = resp.topic_data[1].partition_data[1]
    if pd.error_code != 0
        error("OffsetRequest failed with error code: $(pd.error_code)")
    end
    return pd.offsets
end


function earliest_offset(kc::KClient, topic::AbstractString, partition::Integer)
    return list_offsets(kc, topic, partition, -2, 1)[1]
end


function latest_offset(kc::KClient, topic::AbstractString, partition::Integer)
    return list_offsets(kc, topic, partition, -1, 1)[1]
end


## produce

function produce(kc::KClient, topic::AbstractString, partition::Integer,
                 kvs::Vector{Tuple{Vector{UInt8}, Vector{UInt8}}})
    ensure_leader(kc, topic, partition)
    pid = Int32(partition)
    cor_id = inc_cor_id!(kc)
    header = RequestHeader(PRODUCE, 0, cor_id, DEFAULT_ID)
    req = make_produce_request(topic, pid, kvs)
    sock = find_leader(kc, topic, pid)
    send_request(sock, header, req)
    resp = read_response(sock, ProduceResponse)
    partition, error_code, offset = resp.responses[1][2][1]
    if error_code != 0
        error("ProduceRequest failed with error code: $error_code")
    end
    return offset
end


function produce(kc::KClient, topic::AbstractString, partition::Integer,
                 kvs::Vector{Tuple{K, V}}) where {K, V}
    byte_kvs = [(convert(Vector{UInt8}, k), convert(Vector{UInt8}, v)) for (k, v) in kvs]
    return produce(kc, topic, partition, byte_kvs)
end

## consume

function Base.fetch(kc::KClient, topic::AbstractString,
               partition::Integer, offset::Int64;
               max_wait_time=100, min_bytes=1024, max_bytes=1024*1024)
    ensure_leader(kc, topic, partition)
    pid = Int32(partition)
    cor_id = inc_cor_id!(kc)
    header = RequestHeader(FETCH, 0, cor_id, DEFAULT_ID)
    req = make_fetch_request(topic, partition, offset,
                             max_wait_time, min_bytes, max_bytes)
    sock = find_leader(kc, topic, partition)
    send_request(sock, header, req)
    resp = read_response(sock, FetchResponse)
    pr = resp.topic_results[1].partition_results[1]
    if pr.error_code != 0
        error("FetchRequest failed with error code: $(pr.error_code)")
    end
    elements = pr.message_set.elements
    results = [(elem.offset, elem.message.key, elem.message.value)
               for elem in elements]
    return results
end

# function Base.fetch{K,V}(kc::KClient, topic::AbstractString,
#                     partition::Integer, offset::Int64;
#                     max_wait_time=100, min_bytes=1024, max_bytes=1024*1024) where {K,V}
#     return [(offset, convert(K, key), convert(V, value))
#             for (offset, key, value) in process_response(resp)]
# end


## API versions

function api_versions(kc::KClient)
    sock = random_broker(kc)
    cor_id = inc_cor_id!(kc)
    header = RequestHeader(API_VERSIONS, 1, cor_id, DEFAULT_ID)
    send_request(sock, header)
    resp = read_response(sock, ApiVersionsResponse)
    return resp.api_versions
end
