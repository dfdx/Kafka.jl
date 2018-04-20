## KafkaClient core functions


abstract type AbstractKafkaClient end


mutable struct KafkaClient <: AbstractKafkaClient
    brokers::Dict{Int, TCPSocket}
    meta::Dict{Symbol, Any}
    last_cor_id::Int64
    msg_types::Dict{Int, Type}
    out_channels::Dict{Any, Channel{Any}}   # responses to synchronous requests
    bg_channel::Channel{Any}                # background task channel (e.g. ping)
    react_channel::Channel{Any}             # reaction task channel (e.g. for rebalancing)
end


function KafkaClient(host::AbstractString, port::Int; resp_loop=true)
    sock = connect(host, port)
    meta = init_metadata(sock)
    brokers = Dict(b.node_id => connect(b.host, b.port) for b in meta[:brokers])
    msg_types = Dict{Int64, Type}()
    out_channels = Dict(k => Channel{Any}(1024) for k in keys(brokers))
    bg_channel = Channel{Any}(1024)
    react_channel = Channel{Any}(1024)
    kc = KafkaClient(brokers, meta, 0, msg_types, out_channels, bg_channel, react_channel)
    if resp_loop
        for node_id in keys(brokers)
            start_recv_task(kc, node_id)
        end
    end
    return kc
end


function start_recv_task(kc, node_id)
    """
    Start a task for reading from a socket with `node_id` and either
    pushing results to out_channels or processing background messages
    """
    sock = kc.brokers[node_id]
    @async while true
        try
            cor_id = recv_header(sock).correlation_id
            T = kc.msg_types[cor_id]
            resp = readobj(sock, T)
            # put result into out channel for this broker
            put!(kc.out_channels[node_id], resp)
            # delete mapping for this cor_id
            delete!(kc.msg_types, cor_id)           
        catch e
            if e isa EOFError
                # ignore
            else
                throw(e)
            end
        end
    end
end


function Base.show(io::IO, kc::KafkaClient)
    n_brokers = length(kc.brokers)
    broker_str = n_brokers == 1 ? "1 broker" : "$n_brokers brokers"
    print(io, "KafkaClient($broker_str)")
end


function register_request{T}(kc::KafkaClient, ::Type{T})
    """
    Register request type in internal mapping to be able to read correct object later
    """
    kc.last_cor_id += 1
    id = kc.last_cor_id
    kc.msg_types[id] = T    
    return id
end


function find_leader_id(kc::AbstractKafkaClient, topic::AbstractString, partition::Integer)
    node_id = kc.meta[:topics][topic][partition][:leader]
    return node_id
end

function find_leader(kc::AbstractKafkaClient, topic::AbstractString, partition::Integer)
    node_id = kc.meta[:topics][topic][partition][:leader]
    return node_id, kc.brokers[node_id]
end


function random_broker(kc::AbstractKafkaClient)
    node_id = kc.brokers |> keys |> collect |> rand
    return node_id, kc.brokers[node_id]
end


# function handle_response(kc::KafkaClient, sock::TCPSocket)
#     header = recv_header(sock)
#     cor_id = header.correlation_id
#     T = kc.inprogress[cor_id]
#     resp = readobj(sock, T)
#     put!(kc.results[cor_id], resp)
#     delete!(kc.inprogress, cor_id)
#     delete!(kc.results, cor_id) # if nobody listens to a channel,
#                                 #  GC should delete it as well
# end


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


function send_and_read(kc::KafkaClient, topic::AbstractString, pid::Integer, header, req)
    """
    Helper function to send request to a leader and read response from corresponding channel
    """
    node_id, sock = find_leader(kc, topic, pid)
    send_request(sock, header, req)
    resp = take!(kc.out_channels[node_id])
    return resp
end
