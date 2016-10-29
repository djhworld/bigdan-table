I enjoyed reading the BigTable paper so much I decided to go ahead and attempt to implement *some* of the ideas in it.

Concepts worked on: 

* Tablet
  * :x: Timestamped values
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
  * :white_check_mark: Storage agnostic
* TabletServer
  * in progress

