# SimpleDB
This repo contains the implementation of a simple but feature complete database providing storage, in-memory buffers, transactions, indexes and SQL querying (parsing, planning, optimising and executing). 
It is the result of reading [Database design and implementation](https://www.amazon.co.uk/Database-Design-Implementation-Data-Centric-Applications/dp/3030338355/) by E. Sciore and adapting the code to use modern Java constructs like the [MemorySegment](https://docs.oracle.com/en/java/javase/19/docs/api/java.base/java/lang/foreign/MemorySegment.html) API and asynchronous I/O.
