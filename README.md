I enjoyed reading the BigTable paper so much I decided to go ahead and attempt to implement *some* of the ideas in it.

Concepts worked on: 

* Tablet
  * :x: Scan/filter entire tablet
  * :x: Scan/filter entire row 
  * :white_check_mark: Timestamped values
  * :white_check_mark: Read/Write to memtable
  * :bulb: Commit log 
    * Need to figure out how this is stored and how to checkpoint it....
  * :white_check_mark: Read/Flush to SSTable
    * :white_check_mark: Amazon S3 supported 
    * :white_check_mark: Local filesystem
  * :white_check_mark: Tablet compaction
  * :x: Tablet split
  
* SSTable 
  * :white_check_mark: Blocks compressed with GZIP
  * :white_check_mark: Footer compressed with GZIP
  * :white_check_mark: Configurable block size
  * :white_check_mark: Configurable compression (GZIP, SNAPPY, Uncompressed supported)
  * :white_check_mark: Storage agnostic

* TabletServer
  * :white_check_mark: Each tablet responsible for a row range
  * :x: column family locality 
  * in progress

## SSTable 

![Alt text](/sstable.png?raw=true "SSTable")

Blocks are of fixed length and block size is stored in the header.
All blocks are compressed using GZIP.
Footer is compressed using GZIP.

Reader will

1. Read header
2. Read footer

Blocks are read when a value is requested, and cached if appropriate.
