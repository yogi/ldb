require "common"

counter = 0

request = function()
    wrk.method = "GET"
    counter = (counter + 1)
    count = tostring(counter)
    pid = math.random(1, maxRand)
    probeId = "PRB" .. pid
    path = "/probe/" .. probeId .. "/latest"
    --io.write(string.format("probe %s %d\n", probeId, counter))
    return wrk.format(nil, path)
end

done = function(summary, latency, requests)
    io.write(string.format("total requests: %d\n", summary.requests))
end
