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
There are primarily two modes of execution:
* To run the main engine and use the in-house shell:

    `python engine.py`
    
    This inhouse shell directly interacts with the database object


* To use the server-client functionality
    
    * Spin up a server, use `--port` to configure a non-default port.

        `python server.py`
    
    * Spin up multiple clients, use `--server_port` and `--server_host` options to configure non-default connections.

        `python client.py`

Note: Checkout `python FILENAME.py -h` for full range of implemented configuration options.

## Features
* On server redis shell
    
    Access Local Server with a Redis like shell to directly access database objects.
    
* Client-Server setup
    
    Concurrently serve multiple remotely or locally connected clients with robust message-queue based communication.
 protocol.
* Multiple persistence options:
    Like Redis, Redis-Clone also provides a variety of persistence configurations.
    
    * Time interval based parallel RDB serialization (Emulates Redis RDB serialization) 
    
        Set off a parallel serialization process which stores the database snapshot while the main server continues
         to serve clients. Also flushes the current log state to keep log file sizes in check. See `engine.py`'s 
         `--RDB_timeout` and `--RDB_persistence` options for more details.
        
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
   
    Example
    ```BASH
  Redis> SET a
    the following arguments are required: value
    usage: SET [-h] [-EX EX | -PX PX] [-NX | -XX] [-KEEPTTL] key value
  ```
    
* Helpful output message for commands. For example, use `Redis> GET -h` to output a helpful description of the command

    ```BASH
    Redis> SET -h
    usage: SET [-h] [-EX EX | -PX PX] [-NX | -XX] [-KEEPTTL] key value
    
    Set key to hold the string value. If key already holds a value, it is
    overwritten, regardless of its type. Any previous time to live associated with
    the key is discarded on successful SET operation.
    
    positional arguments:
      key         Identifier for the key
      value
    
    optional arguments:
      -h, --help  show this help message and exit
      -EX EX      Set the specified expire time, in seconds.
      -PX PX      Set the specified expire time, in milliseconds.
      -NX         Only set the key if it does not already exist.
      -XX         Only set the key if it already exists.
      -KEEPTTL    Retain the time to live associated with the key.
    
    ```



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
