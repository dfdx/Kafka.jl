module Kafka

export
    KafkaClient,
    KClient,
    metadata,
    produce,
    fetch,
    list_offsets,
    _metadata,
    _produce,
    _fetch,
    _list_offsets,
    earliest_offset,
    latest_offset,
    api_versions

include("core.jl")

end # module
