counter = 0

request = function()
    wrk.method = "PUT"
    wrk.headers["Content-Type"] = "application/json"
    counter = (counter + 1)
    pid = math.random(10000000)
    probeId = "PRB" .. counter
    eventId = "7707d6a0-61b5-11ec-9f10-0800200c9a66" .. counter
    path = "/probe/" .. probeId .. "/event/" .. eventId
    body = '{\n    \"probeId\": \"' .. probeId .. '\",\n    \"eventId\": \"' .. eventId .. '\",\n    \"messageType\": \"spaceCartography\",\n    \"eventTransmissionTime\": 1640018265951,\n    \"messageData\": [\n        {\n            \"measureName\": \"Spherical coordinate system - euclidean distance\",\n            \"measureCode\": \"SCSED\",\n            \"measureUnit\": \"parsecs\",\n            \"measureValue\": 5399e5,\n            \"measureValueDescription\": \"Euclidean distance from earth\",\n            \"measureType\": \"Positioning\",\n            \"componentReading\": 43e23\n        },\n        {\n            \"measureName\": \"Spherical coordinate system - azimuth angle\",\n            \"measureCode\": \"SCSEAA\",\n            \"measureUnit\": \"degrees\",\n            \"measureValue\": 170.42,\n            \"measureValueDescription\": \"Azimuth angle from earth\",\n            \"measureType\": \"Positioning\",\n            \"componentReading\": 46e2\n        },\n        {\n            \"measureName\": \"Spherical coordinate system - polar angle\",\n            \"measureCode\": \"SCSEPA\",\n            \"measureUnit\": \"degrees\",\n            \"measureValue\": 30.23,\n            \"measureValueDescription\": \"Polar/Inclination angle from earth\",\n            \"measureType\": \"Positioning\",\n            \"componentReading\": 56e42\n        },\n        {\n            \"measureName\": \"Localized electromagnetic frequency reading\",\n            \"measureCode\": \"LER\",\n            \"measureUnit\": \"hz\",\n            \"measureValue\": 3e5,\n            \"measureValueDescription\": \"Electromagnetic frequency reading\",\n            \"measureType\": \"Composition\",\n            \"componentReading\": 3e15\n        },\n        {\n            \"measureName\": \"Probe lifespan estimate\",\n            \"measureCode\": \"PLSE\",\n            \"measureUnit\": \"Years\",\n            \"measureValue\": 2390e2,\n            \"measureValueDescription\": \"Number of years left in probe lifespan\",\n            \"measureType\": \"Probe\",\n            \"componentReading\": 6524e3\n        },\n        {\n            \"measureName\": \"Probe diagnostic logs\",\n            \"measureCode\": \"PDL\",\n            \"measureUnit\": \"Text\",\n            \"measureValue\": \"some log data from probe\",\n            \"measureValueDescription\": \"the diagnostic information from the probe\",\n            \"measureType\": \"Probe\",\n            \"componentReading\": 0.0\n        }\n    ]\n}'
    io.write(string.format("probe %s %d\n", probeId, counter))
    return wrk.format(nil, path, nil, body)
end

done = function(summary, latency, requests)
    io.write(string.format("total requests: %d\n", summary.requests))
end
