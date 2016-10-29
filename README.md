I enjoyed reading the BigTable paper so much I decided to go ahead and attempt to implement *some* of the ideas in it.

Concepts worked on: 

* Tablet
  * :x: Timestamped values
  * :white_check_mark: Read/Write to memtable
  * :white_check_mark: Read/Flush to SSTable
    * :white_check_mark: Amazon S3 supported 
    * :white_check_mark: Local filesystem
  * :white_check_mark: Tablet compaction
  * :x: Tablet split
  
* SSTable 
  * :white_check_mark: Blocks compressed with GZIP
  * :white_check_mark: Footer compressed with GZIP
  * :white_check_mark: Configurable block size
* TabletServer
  * in progress

