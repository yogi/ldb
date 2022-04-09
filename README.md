# LDB

This is an experimental datastore which was developed as part of the Big-O 
competition at [Sahaj Software](https://twitter.com/SahajSoftware) - 
here is the [problem statement](https://bit.ly/3jokUIa).

I referred to this [LevelDB description](https://github.com/google/leveldb/blob/main/doc/impl.md)
to understand it's concepts.

##### To run:
`$ mvn compile exec:java -Dexec.mainClass="app.App"`

##### Run load tests (install [wrk](https://github.com/wg/wrk)):
###### writes:
`$ wrk -t4 -c100 -d10m -s put.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`

###### reads:
` wrk -t4 -c100 -d10m -s get.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`





