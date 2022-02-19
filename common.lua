maxrange = 1000000
range = 1

function init(args)
    seed = args[1] and args[1] or "randomSeed"
    range = args[2] and tonumber(args[2]) or maxrange

    if (seed == "randomSeed")
    then
        io.write("randomSeed: os.time(), range: " .. range .. "\n")
        math.randomseed(os.time())
    elseif (seed == "fixedSeed")
    then
        io.write("fixedSeed: 9, range: " .. range .. "\n")
        math.randomseed(9) --seed it with any number for predictable math.random()
    else
        error("invalid arguments")
    end
end

function randomProbeId()
    return math.random(1, range)
end

