# Kafka

[![Build Status](https://travis-ci.org/dfdx/Kafka.jl.svg?branch=master)](https://travis-ci.org/dfdx/Kafka.jl)

Client for Apache Kafka in Julia

## News

 * 2018-04-21: asynchronous API has been deprecated, now all functions return result
   itself and not just channel; switching between tasks is still asynchronous

## Usage example

The following examples assume a Kafka broker running on 127.0.0.1:9092. 

```
using Kafka

# create KafkaClient using single bootstrap broker
kc = KafkaClient("127.0.0.1", 9092)

# get metadata about a topic(s)
md = metadata(kc, ["test"])
md = metadata(kc)

# get earliest and latest available offsets for topic "test" and partition 0
earliest_offset(kc, "test", 0)
latest_offset(kc, "test", 0)

# produce new messages
keys = [convert(Vector{UInt8}, key) for key in ["1", "2", "3"]]
values = [convert(Vector{UInt8}, value) for value in ["feel", "good", "inc."]]
messages = collect(zip(keys, values))
offset = produce(kc, "test", 0, messages)

# fetch messages
start_offset = 0
offset_messages = fetch(kc, "test", 0, start_offset)
```


# TODO

 * Offset commits
 * Consumer group and corresponding `consume` method