
abstract type AbstractKafkaClient end


"""
Asynchronous client for Kafka brokers.
Workflow:

    # create KafkaClient using one of constructors
    kc = KafkaClient(...)
    # send requests obtaining one-time channels with a response
    channel = metadata(kc, ...)
    # take the result
    resp = take!(channel)

"""
mutable struct KafkaClient <: AbstractKafkaClient
    brokers::Dict{Int, TCPSocket}
    meta::Dict{Symbol, Any}
    last_cor_id::Int64
    inprogress::Dict{Int64, Type}
    results::Dict{Int64, Channel{Any}}
end

function KafkaClient(host::AbstractString, port::Int; resp_loop=true)
    sock = connect(host, port)
    meta = init_metadata(sock)
    brokers = Dict(b.node_id => connect(b.host, b.port) for b in meta[:brokers])
    inprogress = Dict{Int64, Type}()
    results = Dict{Int64, Channel{Any}}()
    kc = KafkaClient(brokers, meta, 0, inprogress, results)
    if resp_loop
        sockets = collect(values(brokers))
        for sock in sockets
            @async begin
                while isopen(sock)
                    Base.start_reading(sock)
                    while nb_available(sock) == 0 sleep(0.1) end
                    handle_response(kc, sock)
                end
            end
        end
    end
    return kc
end

function Base.show(io::IO, kc::KafkaClient)
    n_brokers = length(kc.brokers)
    broker_str = n_brokers == 1 ? "1 broker" : "$n_brokers brokers"
    print(io, "KafkaClient($broker_str)")
end


function register_request{T}(kc::KafkaClient, ::Type{T})
    kc.last_cor_id += 1
    id = kc.last_cor_id
    kc.inprogress[id] = T
    kc.results[id] = Channel(1)
    return id, kc.results[id]
end


function find_leader_id(kc::AbstractKafkaClient, topic::AbstractString, partition::Integer)
    node_id = kc.meta[:topics][topic][partition][:leader]
    return node_id
end

function find_leader(kc::AbstractKafkaClient, topic::AbstractString, partition::Integer)
    node_id = kc.meta[:topics][topic][partition][:leader]
    return kc.brokers[node_id]
end


function random_broker(kc::AbstractKafkaClient)
    sockets = collect(values(kc.brokers))
    return sockets[rand(1:length(sockets))]
end


function handle_response(kc::KafkaClient, sock::TCPSocket)
    header = recv_header(sock)
    cor_id = header.correlation_id
    T = kc.inprogress[cor_id]
    resp = readobj(sock, T)
    put!(kc.results[cor_id], resp)
    delete!(kc.inprogress, cor_id)
    delete!(kc.results, cor_id) # if nobody listens to a channel,
                                #  GC should delete it as well
end


"""
Ensure that KafkaClient has open connection to the leader for these
topic and partition. Reconnect if previous socket has been closed
"""
function ensure_leader(kc::AbstractKafkaClient, topic::AbstractString, partition::Integer)
    node_id = find_leader_id(kc, topic, partition)
    if !isopen(kc.brokers[node_id])
        idx = find(b -> b.node_id == node_id, kc.meta[:brokers])[1]
        host = kc.meta[:brokers][idx].host
        port = kc.meta[:brokers][idx].port
        kc.brokers[node_id] = connect(host, port)
    end
end
