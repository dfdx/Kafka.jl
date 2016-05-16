# Kafka

[![Build Status](https://travis-ci.org/dfdx/Kafka.jl.svg?branch=master)](https://travis-ci.org/dfdx/Kafka.jl)

Client for Apache Kafka in Julia

## Status

Basically, Kafka provides 4 principal APIs:

 1. Metadata retrieval 
 2. Producing messages
 3. Fetching messages
 4. Listing offsets
 5. Consumer group management (a.k.a. storing offsets)

First 4 are implemented and should be sufficient for most real-life use cases. The last one, however, is somewhat fast-moving target without single approach (e.g. Kafka 0.8.x uses Zookeeper to store offsets, 0.9.x provides broker API, while external systems like Apache Spark and Apache Storm use their own means to store offsets). Given instability and variety of options this part is postponed for now. Though, proposals and discussions are heavily welcome.

## Usage example

Here's short version of what you can do with Kafka.jl. For full example see `examples/all.jl`.

```
using Kafka

# create KafkaClient using single bootstrap broker
kc = KafkaClient("127.0.0.1", 9092)

# get metadata about a topic(s)
md_channel = metadata(kc, ["test"])
md = take!(md_channel)
# if you prefer synchronous logic, use one-linear
take!(metadata(kc, ["test"]))
take!(metadata(kc))

# get earliest and latest available offsets for topic "test" and partition 0
take!(earliest_offset(kc, "test", 0))
take!(latest_offset(kc, "test", 0))

# produce new messages
keys = [convert(Vector{UInt8}, key) for key in ["1", "2", "3"]]
values = [convert(Vector{UInt8}, value) for value in ["feel", "good", "inc."]]
messages = collect(zip(keys, values))
offset = take!(produce(kc, "test", 0, messages))

# fetch messages
start_offset = 0
offset_messages = take!(fetch(kc, "test", 0, start_offset))
```
