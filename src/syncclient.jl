
"""
Synchronous client for Kafka brokers.
"""
mutable struct KClient <: AbstractKafkaClient
    brokers::Dict{Int, TCPSocket}
    meta::Dict{Symbol, Any}
    last_cor_id::Int64
end


function KClient(host::AbstractString, port::Int)
    sock = connect(host, port)
    meta = init_metadata(sock)
    brokers = Dict(b.node_id => connect(b.host, b.port) for b in meta[:brokers])    
    kc = KClient(brokers, meta, 0)
    return kc
end

function Base.show(io::IO, kc::KClient)
    n_brokers = length(kc.brokers)
    broker_str = n_brokers == 1 ? "1 broker" : "$n_brokers brokers"
    print(io, "KClient($broker_str)")
end
