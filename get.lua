counter = 0

request = function()
    wrk.method = "GET"
--     counter = (counter + 1) % 100
    counter = 1
    count = tostring(counter)
    probeId = "PRB" .. count
    path = "/probe/" .. probeId .. "/latest"
    return wrk.format(nil, path)
end

-- done = function(summary, latency, requests)
--     io.write(string.format("total requests: %d\n", summary.requests))
-- end
