
include("core.jl")

function main2()
    kc = KafkaClient("127.0.0.1", 9092)
    # kc = KafkaClient("10.1.14.81", 9092)
    mch = metadata(kc, ASCIIString[])
    md = take!(mch)
    kvs = [(b"1", b"hello"), (b"2", b"world")]
    pch = produce(kc, "test", 0, kvs)
    fch = fetch(kc, "test", 0, 0)
    f = fetch(fch)
end
