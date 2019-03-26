
using Kafka

# create KafkaClient using single bootstrap broker
kc = KafkaClient("127.0.0.1", 9092)

# get supported API versions
vers = api_versions(kc)

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
keys = [unsafe_wrap(Vector{UInt8}, key) for key in ["1", "2", "3"]]
values = [unsafe_wrap(Vector{UInt8}, value) for value in ["feel", "good", "inc."]]
messages = collect(zip(keys, values))
offset = take!(produce(kc, "test", 0, messages))

# fetch messages
start_offset = 0
offset_messages = take!(fetch(kc, "test", 0, start_offset))
