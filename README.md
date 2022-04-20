# LDB

This is an experimental datastore which was developed as part of the Big-O 
competition at [Sahaj Software](https://twitter.com/SahajSoftware).

### Problem Statement
Full text of problem statement: ["The space time complexity"](https://bit.ly/3jokUIa).

##### Summary:
- Store JSON events (avg size 5kb) sent by probes
- Retain only latest event for a probe
- Points awarded for peak write-load sustained over 120 min
- Should lose minimum events if process is killed
- p99 < 200 ms for writes
- p99 < 2 sec for reads

### Implementation
I referred to this [LevelDB description](https://github.com/google/leveldb/blob/main/doc/impl.md) 
to understand core concepts (but didn't look at the code so that I could compare 
approaches once done).

##### LDB Features:
* Set, Get 
  * Note: Delete is not implemented since it was not needed for the problem statement 
* WriteAheadLog
* Memtable
* SortedStringTables (called Segments here)
* Levels
* Custom serialization
* Compaction: single-threaded
* Compression: LZ4, Snappy
* Block-based storage within Segments
* Throttling (to prevent runaway compaction backlog)
* Key randomization and sharding (to ensure bounded compaction times)
* Segment caching

### Performance 
Test done using wrk on a MacBook Pro (2019, 2.6 GHz 6-Core Intel Core i7, 16 GB RAM)
* 20K writes/sec and 10K reads/sec of 2 KB values
* 50K writes/sec of 100 byte values

### Run Big-O server with embedded LDB: 
`$ mvn compile exec:java -Dexec.mainClass="app.App"`

### Run load tests 
Install [wrk](https://github.com/wg/wrk)

#### Run load tests (install [wrk](https://github.com/wg/wrk)):
##### writes:
`$ wrk -t4 -c100 -d10m -s put.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`

###### reads:
`$ wrk -t4 -c100 -d10m -s get.lua http://localhost:8080/ --latency -- fixedSeed 1000000000`

### Design
![](/Users/yogik/dev/bigo/ldb.jpg)

##### Set request flow:
1. set(key, value) method called on LDB 
2. Write to wal, and store the key+value in the memtable 
3. If the number of elements in the memtable crosses a threshold 
   1. Switch the live wal and memtable with new instances
   2. Write the full memtable to a new Level0 segment

##### Compaction:
1. Start compaction thread in LDB constructor 
2. Loop forever
   1. Calculate the compactionScore for each level
   2. Pick the level with highest score 
   3. Compact it
      1. If it is Level0 (segments can have overlapping keys) select **all** segments for compaction
      2. Else for LevelN (segments are non-overlapping) select **1** segment for compaction (in a round-robin manner based on maxkey of last compacted segment)
      3. For selected segments
         1. Find the overlapping segments in LevelN+1
         2. Merge combined set of segments and writes out new segments into Level1 with sorted keys
