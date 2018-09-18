
# Kafka wire protocol. See details at:
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

struct RequestHeader
    api_key::Int16
    api_version::Int16
    correlation_id::Int32
    client_id::String
end

struct ResponseHeader
    correlation_id::Int32
end

struct Broker
    node_id::Int32
    host::String
    port::Int32
end

struct PartitionMetadata
    partition_error_code::Int16
    partition_id::Int32
    leader::Int32
    replicas::Vector{Int32}
    isr::Vector{Int32}
end

struct TopicMetadata
    topic_error_code::Int16
    topic_name::String
    partition_metadata::Vector{PartitionMetadata}
end

struct TopicMetadataRequest
    # RequestHeader omitted, write manually header::RequestHeader
    topics::Vector{String}
end

# to obtain metadata for all topics, we need to pass an empty array
# however, Kafka mixes up empty arrays and nulls and rest of API
# transforms Julia's T[] to null (i.e. array with lenght -1)
# to overcome it in this specific case we use special request that 
# emulates empty array (with length 0)
struct AllTopicsMetadataRequest
    # RequestHeader omitted, write manuallyheader::RequestHeader
    pseudo_length::Int32
    AllTopicsMetadataRequest() = new(0)
end

struct TopicMetadataResponse
    # ResponseHeader omitted, read manually
    brokers::Vector{Broker}
    topic_metadata::Vector{TopicMetadata}
end


struct Message_v0
    crc::Int32
    magic_byte::Int8
    attributes::Int8
    key::Vector{UInt8}
    value::Vector{UInt8}
end
struct Message_v1
    crc::Int32
    magic_byte::Int8
    attributes::Int8
    timestamp::Int64
    key::Vector{UInt8}
    value::Vector{UInt8}
end
const Message = Message_v0

struct OffsetMessage
    offset::Int64
    message_size::Int32
    message::Message
end

# NOTE: MessageSet is serialized differently than other types,
# see io.jl for details
struct MessageSet
    elements::Vector{OffsetMessage}
end


# produce

struct ProduceRequestPartitionData
    partition::Int32
    message_set_size::Int32
    message_set::MessageSet
end

struct ProduceRequestTopicData
    topic_name::String
    partition_data::Vector{ProduceRequestPartitionData}
end


struct ProduceRequest
    # RequestHeader omitted, write manually
    required_acks::Int16
    timeout::Int32
    topic_data::Vector{ProduceRequestTopicData}
end

struct ProduceResponse_v0
    # ResponseHeader omitted, read it manually
    # responses format: [TopicName [Partition ErrorCode Offset]]
    responses::Vector{Tuple{String, Vector{Tuple{Int32,Int16,Int64}}}}
end
struct ProduceResponse_v1 # (supported in 0.9.0 or later)
    # ResponseHeader omitted, read it manually
    responses::Vector{Tuple{String, Vector{Tuple{Int32,Int16,Int64}}}}
    throttle_time::Int32
end
struct ProduceResponse_v2 # (supported in 0.10.0 or later)
    # ResponseHeader omitted, read it manually
    responses::Vector{Tuple{String, Vector{Tuple{Int32,Int16,Int64,Int64}}}}
    throttle_time::Int32
end
const ProduceResponse = ProduceResponse_v0

# fetch

struct FetchRequestPartitionData
    partition::Int32
    offset::Int64
    max_bytes::Int32
end

struct FetchRequestTopicData
    topic_name::String
    partition_fetches::Vector{FetchRequestPartitionData}
end

struct FetchRequest
    # RequestHeader omitted, write it manually
    replica_id::Int32 # should always be -1 for clients
    max_wait_time::Int32
    min_bytes::Int32
    topic_fetches::Vector{FetchRequestTopicData}
end

struct FetchResponsePartitionData
    partition::Int32
    error_code::Int16
    highwater_mark_offset::Int64
    message_set_size::Int32
    message_set::MessageSet
end

struct FetchResponseTopicData
    topic_name::String
    partition_results::Vector{FetchResponsePartitionData}
end

struct FetchResponse_v0
    # ResponseHeader omitted, read it manually
    topic_results::Vector{FetchResponseTopicData}
end

struct FetchResponse_v1
    # ResponseHeader omitted, read it manually
    throttle_time::Int32
    topic_results::Vector{FetchResponseTopicData}    
end

const FetchResponse = FetchResponse_v0


# offset listing

struct OffsetRequestPartitionData
    partition::Int32
    time::Int64
    max_number_of_offsets::Int32
end

struct OffsetRequestTopicData
    topic_name::String
    partition_data::Vector{OffsetRequestPartitionData}
end

struct OffsetRequest
    replica_id::Int32
    topic_data::Vector{OffsetRequestTopicData}    
end


struct OffsetResponsePartitionData
    partition::Int32
    error_code::Int16
    offsets::Vector{Int64}
end

struct OffsetResponseTopicData
    topic_name::String
    partition_data::Vector{OffsetResponsePartitionData}
end

struct OffsetResponse
    topic_data::Vector{OffsetResponseTopicData}
end

# API versions

struct ApiVersion
    api_key::Int16
    min_version::Int16
    max_version::Int16
end


struct ApiVersionsResponse
    error_code::Int16
    api_versions::Vector{ApiVersion}
    throttle_time_ms::Int32
end
