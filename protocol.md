## The `pressure` Protocol
#### Distributed, reliable, bounded, synchronized, exclusive FIFO queues via Redis

`pressure` is a protocol (and accompanying implementation) that provides distributed, reliable, bounded, synchronized, exclusive FIFO queues via a Redis backend. Buzzword by buzzword:

 - **Distributed** - queues can be used on multiple machines,  queue brokers can be replicated
 - **Reliable** - messages are stored in Redis, which provides some level of fault tolerance
 - **Bounded** - queues can have a maximum length if desired (for coroutine-like behaviour)
 - **Synchronized** - after reaching the maximum length, queue producers block 
 - **Exclusive** - queues can (for now) have at most one producer and one consumer.

This document is considered the canonical specification of the `pressure` protocol. All `pressure` implementations must implement some version of this document.

This document is currently at **version 0.1**. It's written in pseudo-RFC style, with the following words having specific meaning:

 - "*may*" is used to indicate optional behaviour or suggestions that might help ease implementation. Clients that do not implement these clauses can still conform to the `pressure` protocol.
 - "*must*" is used to indicate behaviour that constitutes the core of the protocol. Any client that claims to conform to the protocol must implement this behaviour. Clients that do not implement required behaviour may cause undefined behaviour when used with other conforming clients.  

---

### Message Brokers
`pressure` is based on [Redis](http://redis.io), a simple data structure server that provides `pressure` with most of its functionality. Redis provides the **distributed**, **reliability** and **synchronization** aspects of the protocol.

All `pressure` implementations **must** allow for connection to any arbitrary Redis server. These connection values must be configurable at queue creation time:

 - `REDIS_SERVER`, a hostname or IP address at which a server can be reached. Defaults to `127.0.0.1`.
 - `REDIS_PORT`, the port on which Redis is available on that hostname. Defaults to `6379`
 - `REDIS_DB`, the database number used for this instance of `pressure`. Defaults to `0`.
 - `PRESSURE_PREFIX`, the prefix used for every key. Defaults to `__pressure__`
 
A client *may* choose to implement these configuration options as environment variables passed through to the program. 

If the given connection to a Redis server is not operational, all attempts to perform any operations on a `pressure` queue on that connection **must** fail and report their failure.

### Clients
Clients of `pressure` queues must each have a **unique identifying value** that distinguishes them from other clients. Each client may choose its own value, as long as the value is consistent across multiple operations on any queue. Unique identifying values can be pure or non-pure names, although non-pure names are suggested for human readability. A suggested value generation scheme might contain:

 - The IP address or hostname of the machine
 - The process identifier of the client
 - The thread identifier of the client's current thread
 
Unique identifying values are useful when trying to inspect the current state of queues, including the identities of readers and writers to queues. Should a deadlock situation occur, unique identifying values that help track the location of the blocked code can help aid debugging.

### Queues
A `pressure` queue is composed of **12** Redis keys, where `${REDIS_PREFIX}` is defined as above and `${queue_name}` is an arbitrary identifier. Any characters that are valid in a Redis key name are valid as the `${queue_name}`.

 - `${REDIS_PREFIX}:${queue_name}`, a Redis list that stores the values of the queue.
 - `${REDIS_PREFIX}:${queue_name}:bound`, a Redis string that stores the maximum number of elements in the queue. The default value, 0, indicates no bound.
 - `${REDIS_PREFIX}:${queue_name}:producer`, a Redis string that stores an identifier for the consumer reading from the queue.
 - `${REDIS_PREFIX}:${queue_name}:consumer`, a Redis string that stores an identifier for the producer writing to the queue.
 - `${REDIS_PREFIX}:${queue_name}:producer_free`, a Redis list that stores a single value if the producer is free, and zero values if the queue currently has a producer.
 - `${REDIS_PREFIX}:${queue_name}:consumer_free`, a Redis list that stores a single value if the consumer is free, and zero values if the queue currently has a consumer
 - `${REDIS_PREFIX}:${queue_name}:stats:produced_messages`, a Redis string that stores the number of messages written
 - `${REDIS_PREFIX}:${queue_name}:stats:produced_bytes`, a Redis string that stores the number of bytes written to the queue
 - `${REDIS_PREFIX}:${queue_name}:stats:consumed_messages`, a Redis string that stores the number of messages read
 - `${REDIS_PREFIX}:${queue_name}:stats:consumed_bytes`, a Redis string that stores the number of bytes read from the queue
 - `${REDIS_PREFIX}:${queue_name}:not_full`, a Redis list of length 0 or 1, used to block writers from writing to the queue if the queue is full. A non-full queue results in this list storing one element, while a full queue causes this list to be empty.
 - `${REDIS_PREFIX}:${queue_name}:closed`, a Redis list, used to allow clients to block waiting for a queue to close. This list can contain 0 elements, indicating that the queue is still open, or a non-zero number of elements, indicating that the queue is closed. 
 
A peculiarity of Redis: empty lists do not exist. Any key that does not exist can be addressed as an empty list. Hence, if any of the above-specified lists are empty, they will not appear in the list of Redis keys.

Due to the fact that Redis provides `BLPOP`, `BRPOP` and `BRPOPLPUSH` commands for blocking on lists, all clients **must** insert elements into the left side of a list element (using `LPUSH`) and pop them off of the right side (using `BRPOP` or `BRPOPLPUSH`) to allow for the *possibility* that clients may want to use `BRPOPLPUSH` to get additional data reliability.

**Notation note**: in the rest of this document, many of the above keys are referred to with only a leading colon (i.e.: `:producer`, `:bound`). These are used as a short form for the full key name, including the `REDIS_PREFIX` and `queue_name` variables.
 
### Queue Operations
####Create

Clients that create queues **must** take the following steps to initialize a queue:

 - The client must check the `:bound` key. If the `:bound` key is **not** empty, an error must be raised, as the queue already exists.

 - An integer bound of queue elements must be specified. If the bound is 0, no bound will be applied to the queue. The bound **must** be saved at the `:bound` key, even if it is 0.
 - The `:producer_free` list must be initialized with one element. The value of this element is left undefined, and is not used in the protocol.
 - The `:consumer_free` list must be initialized with one element. The value of this element is left undefined, and is not used in the protocol.
 - The `:not_full` list must be initialized with one element. The value of this element is left undefined, and is not used in the protocol.

Queues **must** be initialized prior to their use. If a queue has been created, then its corresponding `:bound` key will be initialized. (This is the only way to tell if a queue has been created.)

####Put

Clients that initiate a Put operation assume the role of the producer of the queue. Clients **must** implement the following behaviour to put a value onto a queue:

 - The client must check the `:bound` key. If the `:bound` key is empty, an error must be raised, as the queue does not exist.

 - The client must check the `:closed` key. If the key is not empty, an error must be raised, as closed queues cannot be pushed onto.

 - The client must check the `:producer_free` key of its queue. If the list at that key is empty, then another producer is currently acting on the queue and the current client **must** not put its element onto the queue. In this situation, the client **must** do one of two things:
   - The client may block until the `:producer_free` key has one element in its list. (Redis provides the `BRPOP` primitive for blocking while waiting for a list to contain an element.)
   - The client may return an error indicating that another producer is using the queue. In this case, the client *should* try multiple times and only send an error after some fixed timeout.
   
    Once the `:producer_free` key is available, the client **must** pop the element from the list to indicate that it is taking over the producer role.
    
 - The client must set the `:producer` key to its unique identifying value, replacing any value that already exists.
   
 - The client must attempt to pop from the `:not_full` key. If the key is empty, the client **must** do one of two things:
   - The client may block until the key exists.
   - The client may return an error after some fixed timeout. If an error is returned, an element must be pushed onto the `:producer_free` list by the client.
   
 - The client must push its data element onto the `${queue_name}` list.
 - The client must increment the `:stats:produced_messages` key.
 - The client **may** increment the `:stats:produced_bytes` key with the number of bytes in the latest data element. If computing the length of the latest element is prohibitively costly, this step may be omitted.
 - The client must compare the `:bound` key with the length of the queue. If the queue length is strictly less than the bound or the bound is zero, the client must push a value to the `:not_full` key. If the `:not_full` key contains more than one element after this operation, it must be reduced to one element.
 - The client must push a value to the `:producer_free` key.


####Get

Clients that initiate a Get operation assume the role of the consumer of the queue. Clients **must** implement the following behaviour to get a value from a queue:

 - The client must check the `:bound` key. If the `:bound` key is empty, an error must be raised, as the queue does not exist.

 - The client must check the `:consumer_free` key of its queue. If the list at that key is empty, then another consumer is currently acting on the queue and the current client **must** not pop an element from the queue. In this situation, the client **must** do one of two things:
   - The client may block until the `:consumer_free` key has one element in its list.
   - The client may return an error indicating that another consumer is using the queue. In this case, the client *should* try multiple times and only send an error after some fixed timeout.
   
    Once the `:consumer_free` key is available, the client **must** pop the element from the list to indicate that it is taking over the producer role.
    
 - The client must set the `:consumer` key to its unique identifying value, replacing any value that already exists.
   
 - The client must attempt to pop from the `${queue_name}` list. If the list is empty and the `:closed` key is also empty, the client **must** do one of two things:
   - The client may block until the key exists or the `:closed` key has elements.
   - The client may return an error after some fixed timeout. If an error is returned, an element must be pushed onto the `:consumer_free` list by the client.
 - The client must ensure that the `:not_full` list has a value.
   - The client **may** choose to implement the above two steps in one call with Redis' `BRPOPLPUSH` command, as the value stored in the `:not_full` list is undefined.
   
 - The client must increment the `:stats:consumed_messages` key with the number of messages returned.
 - The client **may** increment the `:stats:consumed_bytes` key with the number of bytes in the latest data element. If computing the length of the latest element is prohibitively costly, this step may be omitted.
 - The client must push a value to the `:consumer_free` key.

####Close

Clients that initiate a Close operation assume the role of the producer. Clients **must** implement the following behaviour to close a queue:

 - The client must check the `:bound` key. If the `:bound` key is empty, an error must be raised, as the queue does not exist.

 - The client must check the `:producer_free` key of its queue. If the list at that key is empty, then another producer is currently acting on the queue and the current client **must** not put its element onto the queue. In this situation, the client **must** do one of two things:
   - The client may block until the `:producer_free` key has one element in its list. (Redis provides the `BRPOP` primitive for blocking while waiting for a list to contain an element.)
   - The client may return an error indicating that another producer is using the queue. In this case, the client *should* try multiple times and only send an error after some fixed timeout.
   
    Once the `:producer_free` key is available, the client **must** pop the element from the list to indicate that it is taking over the producer role.
    
 - The client must set the `:producer` key to its unique identifying value, replacing any value that already exists.
   
 - The client must read the value of the list at the `:closed` key. If the list contains zero elements, the client must push two elements of undefined value onto the list. If the list contains one or more elements, this is an error.
   
 - The client must push a value to the `:producer_free` key.
 
Queues can be closed at most once.

####Delete

Clients that initiate a Delete operation assume the role of the consumer. Clients **must** implement the following behaviour **in order** to delete a queue:

 - The client must check the `:bound` key. If the `:bound` key is empty, an error must be raised, as the queue does not exist.
 
 - The client must delete the `:bound` key.
 - The client must push a value to the `:not_full` key.
 - The client must push two values to the `:closed` key.
 - The client must block waiting for an element to exist at the `:producer_free` key of its queue.
 - The client must delete the `:producer_free` and `:producer` keys.
 - The client must block waiting for an element to exist at the `:consumer_free` key of its queue.
 - The client must delete the `:consumer_free` and `:consumer` keys.
 - The client must delete the `:not_full` key.
 - The client must delete the `:closed` key.
 - The client must delete the `:stats:produced_messages`, `:stats:produced_bytes`, `:stats:consumed_messages` and `:stats:consumed_bytes` keys.
 - The client must delete the `${queue_name}` queue.
 
### Redis Reference Implementation

The following sequences of Redis commands (and pseudocode) are provided as a reference implementation of the above-described behaviour.

####Create

    result = SETNX ${REDIS_PREFIX}:${queue_name}:bound bound_value
    if result == 0
      raise QueueAlreadyExistsError
    end
    
    LPUSH ${REDIS_PREFIX}:${queue_name}:producer_free 1
    LPUSH ${REDIS_PREFIX}:${queue_name}:consumer_free 1
    LPUSH ${REDIS_PREFIX}:${queue_name}:not_full 1

####Put

    if EXISTS ${REDIS_PREFIX}:${queue_name}:bound
       BRPOP ${REDIS_PREFIX}:${queue_name}:producer_free
       SET ${REDIS_PREFIX}:${queue_name}:producer producer_value
       
       if EXISTS ${REDIS_PREFIX}:${queue_name}:closed
         raise QueueClosedError
       else
         BRPOP ${REDIS_PREFIX}:${queue_name}:not_full
         
         len = LPUSH ${REDIS_PREFIX}:${queue_name} data_value
         bound = GET ${REDIS_PREFIX}:${queue_name}:bound
         if bound == 0 or len < bound
           LPUSH ${REDIS_PREFIX}:${queue_name}:not_full
           LTRIM ${REDIS_PREFIX}:${queue_name}:not_full 0 1
         end
         
         INCR ${REDIS_PREFIX}:${queue_name}:produced_messages
         INCRBY ${REDIS_PREFIX}:${queue_name}:produced_bytes bytes_value
       end
       LPUSH ${REDIS_PREFIX}:${queue_name}:producer_free 0
    else
      raise QueueDoesNotExistError
    end

####Get

    if EXISTS ${REDIS_PREFIX}:${queue_name}:bound
       BRPOP ${REDIS_PREFIX}:${queue_name}:consumer_free 0  
       SET ${REDIS_PREFIX}:${queue_name}:consumer consumer_value
         
       if EXISTS ${REDIS_PREFIX}:${queue_name}:closed
           if LLEN ${REDIS_PREFIX}:${queue_name} == 0
             raise QueueClosedError
           else
             res = BRPOP ${REDIS_PREFIX}:${queue_name} 0
           end
       else 
         res = BRPOP ${REDIS_PREFIX}:${queue_name} ${REDIS_PREFIX}:${queue_name}:closed 0
         if res[0] == "${REDIS_PREFIX}:${queue_name}:closed"
           raise QueueClosedError
         end
       end
                  
       LPUSH ${REDIS_PREFIX}:${queue_name}:not_full 0
       LTRIM ${REDIS_PREFIX}:${queue_name}:not_full 0 1
         
       INCR ${REDIS_PREFIX}:${queue_name}:consumed_messages
       INCRBY ${REDIS_PREFIX}:${queue_name}:consumed_bytes bytes_value
         
       LPUSH ${REDIS_PREFIX}:${queue_name}:consumer_free 0
    else
      raise QueueDoesNotExistError
    end

####Close


    if EXISTS ${REDIS_PREFIX}:${queue_name}:bound
       BRPOP ${REDIS_PREFIX}:${queue_name}:producer_free
       SET ${REDIS_PREFIX}:${queue_name}:producer producer_value
       
       if EXISTS ${REDIS_PREFIX}:${queue_name}:closed
         raise QueueClosedError
       else
         LPUSH ${REDIS_PREFIX}:${queue_name}:closed 0 0
       end

       LPUSH ${REDIS_PREFIX}:${queue_name}:producer_free 0
    else
      raise QueueDoesNotExistError
    end

####Delete

    if EXISTS ${REDIS_PREFIX}:${queue_name}:bound
      DEL ${REDIS_PREFIX}:${queue_name}:bound
      LPUSH ${REDIS_PREFIX}:${queue_name}:not_full 0
      LPUSH ${REDIS_PREFIX}:${queue_name}:closed 0 0 
    
      BRPOP ${REDIS_PREFIX}:${queue_name}:producer_free 0
      DEL ${REDIS_PREFIX}:${queue_name}:producer
      DEL ${REDIS_PREFIX}:${queue_name}:producer_free
      
      BRPOP ${REDIS_PREFIX}:${queue_name}:consumer_free 0
      DEL ${REDIS_PREFIX}:${queue_name}:consumer
      DEL ${REDIS_PREFIX}:${queue_name}:consumer_free
      
      DEL ${REDIS_PREFIX}:${queue_name}:not_full
      DEL ${REDIS_PREFIX}:${queue_name}:closed
      
      DEL ${REDIS_PREFIX}:${queue_name}:stats:produced_messages
      DEL ${REDIS_PREFIX}:${queue_name}:stats:produced_bytes
      DEL ${REDIS_PREFIX}:${queue_name}:stats:consumed_messages
      DEL ${REDIS_PREFIX}:${queue_name}:stats:consumed_bytes
    
      DEL ${REDIS_PREFIX}:${queue_name}
    else
      raise QueueDoesNotExistError
    end