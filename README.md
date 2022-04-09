# LDB

This is an experimental datastore that was developed as part of the Big-O 
competition at [Sahaj Software](https://twitter.com/SahajSoftware) - 
here is the [problem statement](https://bit.ly/3jokUIa).

I referred to this [LevelDB description](https://github.com/google/leveldb/blob/main/doc/impl.md)
to understand it's concepts.

LDB implements:
* Set, Get (Note: Delete not implemented) 
* WriteAheadLog
* Memtable
* SortedStringTables
* Levels
* Custom serialization
* Compaction: single-threaded
* Compression: LZ4, Snappy
* Throttling

#### Performance 
On a MacBook Pro (2019, 2.6 GHz 6-Core Intel Core i7, 16 GB RAM)
* 20K writes/sec and 10K reads/sec of 2 KB values
* 50K writes/sec of 100 byte values

##### To run:
`$ mvn compile exec:java -Dexec.mainClass="app.App"`

##### Run load tests 
Install [wrk](https://github.com/wg/wrk)

###### writes:
`$ wrk -t4 -c100 -d10m -s put.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`

###### reads:
` wrk -t4 -c100 -d10m -s get.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`





