
include("core.jl")

function main()
    sock = connect("127.0.0.1", 9092)
    header = RequestHeader(METADATA, 0, 5, "me")
    req = TopicMetadataRequest(["test"])
    send_request(sock, header, req)
    header, resp = recv_response_with_header(sock, TopicMetadataResponse)
end


function main2()
    kc = KafkaClient("127.0.0.1", 9092; resp_loop=true)
    ## metadata(kc, ["test"])
    ## take!(kc.results)
    ## kvs = [(b"1", b"hello"), (b"2", b"world")]
    ## produce(kc, "test", 0, kvs)
    ## take!(kc.results)
    fetch(kc, "test", 0, 0)
    r = take!(kc.results)
end
