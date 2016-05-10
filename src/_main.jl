
include("core.jl")

function main2()
    kc = KafkaClient("127.0.0.1", 9092; resp_loop=true)
    mch = metadata(kc, ["test"])    
    kvs = [(b"1", b"hello"), (b"2", b"world")]
    pch = produce(kc, "test", 0, kvs)
    fch = fetch(kc, "test", 0, 0)
end
