I enjoyed reading the BigTable paper so much I decided to go ahead and attempt to implement *some* of the ideas in it.

Concepts worked on: 

* SSTable 
  * :tick: Blocks compressed with GZIP
  * :tick: Footer compressed with GZIP
  * :tick: Configurable block size
* Tablet
  * :tick: Read/Write to memtable
  * :tick: Read/Flush to SSTable
    * :tick: Amazon S3 supported 
    * :tick: Local filesystem
  * :tick: Tablet compaction
  * :cross: Tablet split
* TabletServer
  * in progress

