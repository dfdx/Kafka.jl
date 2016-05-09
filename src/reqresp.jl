
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
    header::RequestHeader
    topics::Vector{ASCIIString}
end

immutable TopicMetadataResponse
    header::ResponseHeader
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

immutable MessageSetElement
    offset::Int64
    message_size::Int32
    message::Message
end
typealias MessageSet Vector{MessageSetElement}
