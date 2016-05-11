
type TransformedChannel
    original::AbstractChannel
    transform::Function
end

close(ch::TransformedChannel) = close(ch.original)
isopen(ch::TransformedChannel) = isopen(ch.original)
# put!(ch::TransformedChannel, v) = put!(ch.original, v)
# push!(ch::TransformedChannel, v) = push!(ch.original, v)
fetch(ch::TransformedChannel) = ch.transform(fetch(ch.original))
take!(ch::TransformedChannel) = ch.transform(take!(ch.original))

# TODO: what other methods should be implemented? what methods should be forbidden?

map(f::Function, ch::AbstractChannel) = TransformedChannel(ch, f)
