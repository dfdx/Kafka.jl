module Kafka

export KafkaClient,
       metadata,
       produce,
       fetch,
       list_offsets,
       _metadata,
       _produce,
       _fetch,
       _list_offsets,
       earliest_offset,
       latest_offset

include("core.jl")

end # module
