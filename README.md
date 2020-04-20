# Redis Clone
This is a Python implementation of the Redis Key Value storage, with minimal dependencies.
## Setup
Requirements: 
* Python 3.6+
* sortedcontainers
* ZeroMQ

1. Clone this repository

2. Install dependencies with pip:
`pip install -r requirements.txt`

## Usage
* To run the main engine and use the in-house shell:

    `python engine.py`
    
    This inhouse shell directly interacts with the database object


* [Experimental] : To use the server-client functionality

    `python server.py --port 2340`

    `python client.py --server_ip localhost --server_port 2340`

## Features
* On server redis shell
* Client-Server setup: Serve multiple  clients, even remote connections with robust message-queue based communication
 protocol.
* Various persistence options:
    * Time interval based parallel RDB serialization (Emulates Redis RDB serialization)
    * Log based serialization (Emulates Redis AOF)
    * Hybrid RDB + AOF Journalling (Work in Progress)
  
* Variety of Redis commands supported (All commands supported with all the options supported by Redis)
    * GET
    * SET
    * EXPIRE
    * SELECT
    * DESELECT
    * TTL
    * DEL
    * ZADD
    * ZRANK
    * ZRANGE
    
   Note: Use `-` as a prefix character for options, eg `Redis> SET key val -NX`)
  
* Robust parser for Redis commands. Detects positional and optional arguments, ensures correct argument logic and
 type consistency, just like regular linux utilities.
* Helpful output message for commands. For example, use `Redis> GET -h` to output a helpful description of the COMMAND



## Questions Answered
#### 1. Why did you choose that language ?
> Along with widespread adoption, Python provides well documented native utilities like inbuilt dictionary datastructure, argument parsers and logging, all of which could require considerable effort to implement from scratch. While it suffers from single-threaded nature because of GIL, we use multiprocessing to circumvent this limitation.

#### 2. What are the further improvements that can be made to make it efficient ?
1. One thing in the works during the time of submission was logging based model persistence. This utility would allow the engine to load the latest database snapshot, and reconstruct the last logged state by applying log operations on the loaded snapshot.

2. Currently, serialization relies on python's inbuilt cPickle, which although faster than pickle, is suboptimal for model saving. Other alternatives like ujson could speed up the persistence performance significantly.

3. Currently, logging flushes after every database operation are for every write, which is a known slow-down factor even acknowledged by Redis developers (Cite). A workaround would be to delay the writes using an in-process buffer, or flushing logs at every n'th operation.

4. A distributed key storage which distributes keys on various processes/machines could allow parallel operations. Currently, the database object is locked for each transaction. This model is possible in MongoDB and some other options but not Redis currently.

#### 3. What data structures have you used and why ?
1. Currently, I abstract a simple dictionary to perform the key value storage by wrapping a dictionary object under a Database class. This class provides various functions to provide utilities for redis commands, logging, key expiry and logging backup. The keys of the data dictionary are the redis keys, and the values are the `Value` object, which stores the actual value, and expiry timestamp of the data instance.

2.  I define a `MySortedSet` to emulate the sortedsets facility in Redis. I use `SortedSets` from the sortedcontainers library and wrap it around under the custom class to provide the elementary functions are required by Redis functionalities.
 
3. We also use a dbLock to prevent multiple session access the same db instance, which could result in inconsistencies due to  race conditions on the database.

4. We also use a lock on the parallel process `rdb_serialize`, which prevents more than one invocation of the function at a time. This prevents concurrent serialization of data, which could break the logs and serialized database pickles.

#### 4. Does your implementation support multi threaded operations? If No why can’t it be? If yes then how ?
> Due to the Global Interpreter Lock in python, threads in python are efficient only to paralellize I/O bound
> operations. Also, as in the redis implementation, only one transaction can run at a time on the database. To
> circumvent issues regarding locks on shared objects, we instead rely on multiprocessing. I use it for object serialization. I also use multiprocessing to fork a child process which does parallel serialization while the actual engine can continue to process transactions on the parent database instance.
