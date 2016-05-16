
# this file is not used currently, left here just in case

function select(sockets::Vector{TCPSocket}, readfn::Function;
                sleep_time::Float64=0.1, channel_size::Int64=1024)
    result_channel = Channel(channel_size)
    for s in sockets
        @async begin
            Base.start_reading(s)
            while isopen(s)
                while nb_available(s) == 0 sleep(sleep_time) end
                result = readfn(s)
                put!(result_channel, result)
            end
        end
    end
    return result_channel
end

# test code for select()

function echo_server()
    server = listen(8080)
    while true
        conn = accept(server)
        @async begin
            try
                while true
                    line = readline(conn)
                    write(conn,line)
                end
            catch err
                print("connection ended with error $err")
            end
        end
    end
end


function example()
    @async echo_server()
    s1 = connect("localhost", 8080)
    s2 = connect("localhost", 8080)
    ch = select([s1, s2], sock -> readline(sock))
    write(s1, "hello\n")
    take!(ch)
    
end
