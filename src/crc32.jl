
# copied from https://github.com/fhs/CRC32.jl/blob/master/src/CRC32.jl to reduce
# dependencies

function maketable(poly::UInt32)
    tab = zeros(UInt32, 256)
    for i in 0:255
        crc = UInt32(i)
        for _ in 1:8
            if (crc&1) == 1
                crc = (crc >> 1) ⊻ poly
            else
                crc >>= 1
            end
        end
        tab[i+1] = crc
    end
    tab
end

const table = maketable(0xedb88320)

function crc32(data::Vector{UInt8}, crc::Integer=0)
    crc = ~UInt32(crc)
    for b in data
        crc = table[(UInt8(crc&0xff) ⊻ b) + 1] ⊻ (crc >> 8)
    end
    ~crc
end

crc32(data::AbstractString, crc::Integer=0) =
    crc32(convert(Vector{UInt8}, data), crc)
