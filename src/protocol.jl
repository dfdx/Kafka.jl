
# Kafka wire protocol. See details at:
# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

immutable RequestHeader
    api_key::Int16
    api_version::Int16
    correlation_id::Int32
    client_id::ASCIIString
end

immutable ResponseHeader
    correlation_id::Int32
end

immutable Broker
    node_id::Int32
    host::ASCIIString
    port::Int32
end

immutable PartitionMetadata
    partition_error_code::Int16
    partition_id::Int32
    leader::Int32
    replicas::Vector{Int32}
    isr::Vector{Int32}
end

immutable TopicMetadata
    topic_error_code::Int16
    topic_name::ASCIIString
    partition_metadata::Vector{PartitionMetadata}
end

immutable TopicMetadataRequest
    # RequestHeader omitted, write manuallyheader::RequestHeader
    topics::Vector{ASCIIString}
end

# to obtain metadata for all topics, we need to pass an empty array
# however, Kafka mixes up empty arrays and nulls and rest of API
# transforms Julia's T[] to null (i.e. array with lenght -1)
# to overcome it in this specific case we use special request that 
# emulates empty array (with length 0)
immutable AllTopicsMetadataRequest
    # RequestHeader omitted, write manuallyheader::RequestHeader
    pseudo_length::Int32
    AllTopicsMetadataRequest() = new(0)
end

immutable TopicMetadataResponse
    # ResponseHeader omitted, read manually
    brokers::Vector{Broker}
    topic_metadata::Vector{TopicMetadata}
end


immutable Message_v0
    crc::Int32
    magic_byte::Int8
    attributes::Int8
    key::Vector{UInt8}
    value::Vector{UInt8}
end
immutable Message_v1
    crc::Int32
    magic_byte::Int8
    attributes::Int8
    timestamp::Int64
    key::Vector{UInt8}
    value::Vector{UInt8}
end
typealias Message Message_v0

immutable OffsetMessage
    offset::Int64
    message_size::Int32
    message::Message
end

# NOTE: MessageSet is serialized differently than other types,
# see io.jl for details
immutable MessageSet
    elements::Vector{OffsetMessage}
end


# produce

immutable ProduceRequestPartitionData
    partition::Int32
    message_set_size::Int32
    message_set::MessageSet
end

immutable ProduceRequestTopicData
    topic_name::ASCIIString
    partition_data::Vector{ProduceRequestPartitionData}
end


immutable ProduceRequest
    # RequestHeader omitted, write manually
    required_acks::Int16
    timeout::Int32
    topic_data::Vector{ProduceRequestTopicData}
end

immutable ProduceResponse_v0
    # ResponseHeader omitted, read it manually
    # responses format: [TopicName [Partition ErrorCode Offset]]
    responses::Vector{Tuple{ASCIIString, Vector{Tuple{Int32,Int16,Int64}}}}
end
immutable ProduceResponse_v1 # (supported in 0.9.0 or later)
    # ResponseHeader omitted, read it manually
    responses::Vector{Tuple{ASCIIString, Vector{Tuple{Int32,Int16,Int64}}}}
    throttle_time::Int32
end
immutable ProduceResponse_v2 # (supported in 0.10.0 or later)
    # ResponseHeader omitted, read it manually
    responses::Vector{Tuple{ASCIIString, Vector{Tuple{Int32,Int16,Int64,Int64}}}}
    throttle_time::Int32
end
typealias ProduceResponse ProduceResponse_v0

# fetch

immutable FetchRequestPartitionData
    partition::Int32
    offset::Int64
    max_bytes::Int32
end

immutable FetchRequestTopicData
    topic_name::ASCIIString
    partition_fetches::Vector{FetchRequestPartitionData}
end

immutable FetchRequest
    # RequestHeader omitted, write it manually
    replica_id::Int32 # should always be -1 for clients
    max_wait_time::Int32
    min_bytes::Int32
    topic_fetches::Vector{FetchRequestTopicData}
end

immutable FetchResponsePartitionData
    partition::Int32
    error_code::Int16
    highwater_mark_offset::Int64
    message_set_size::Int32
    message_set::MessageSet
end

immutable FetchResponseTopicData
    topic_name::ASCIIString
    partition_results::Vector{FetchResponsePartitionData}
end

immutable FetchResponse_v0
    # ResponseHeader omitted, read it manually
    topic_results::Vector{FetchResponseTopicData}
end

immutable FetchResponse_v1
    # ResponseHeader omitted, read it manually
    throttle_time::Int32
    topic_results::Vector{FetchResponseTopicData}    
end

typealias FetchResponse FetchResponse_v0


# offset listing

immutable OffsetRequestPartitionData
    partition::Int32
    time::Int64
    max_number_of_offsets::Int32
end

immutable OffsetRequestTopicData
    topic_name::ASCIIString
    partition_data::Vector{OffsetRequestPartitionData}
end

immutable OffsetRequest
    replica_id::Int32
    topic_data::Vector{OffsetRequestTopicData}    
end


immutable OffsetResponsePartitionData
    partition::Int32
    error_code::Int16
    offsets::Vector{Int64}
end

immutable OffsetResponseTopicData
    topic_name::ASCIIString
    partition_data::Vector{OffsetResponsePartitionData}
end

immutable OffsetResponse
    topic_data::Vector{OffsetResponseTopicData}
end

