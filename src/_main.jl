
include("core.jl")

function main()
    sock = connect("127.0.0.1", 9092)
    req = TopicMetadataRequest(RequestHeader(METADATA, 0, 5, "me"), ["test"])
    sendreq(sock, req)    
    resp = recvresp(sock, TopicMetadataResponse)
end
