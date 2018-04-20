
# create KafkaClient using single bootstrap broker
kc = KafkaClient("127.0.0.1", 9092)

# get supported API versions
vers = api_versions(kc)

# get metadata about a topic(s)
md = metadata(kc, ["test"])

# get earliest and latest available offsets for topic "test" and partition 0
earliest_offset(kc, "test", 0)
start_offset = latest_offset(kc, "test", 0)

# produce new messages
text_messages = [
    ("1", "feel"),
    ("2", "good"),
    ("3", "inc.")
]

to_bytes(x) = convert(Vector{UInt8}, x)

messages = [map(to_bytes, msg) for msg in text_messages]
offset = produce(kc, "test", 0, messages)
@test latest_offset(kc, "test", 0) - start_offset == length(messages)

# fetch messages
offset_messages = fetch(kc, "test", 0, start_offset)
fetched_messages = [(k, v) for (o, k, v) in offset_messages]
@test messages == fetched_messages
