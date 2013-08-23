#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>

#include <hiredis/hiredis.h>

#include "pressure.h"

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

#ifdef DEBUG
    #define dbprintf(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
    #define dbprintf(fmt, ...)
#endif

char *pressure_key(const char *prefix, const char *name, const char *key) {
    if (key == NULL || key[0] == 0) {
        unsigned int len = strlen(prefix) + 1 + strlen(name);
        char *final_key = malloc(len + 1);
        snprintf(final_key, len + 1, "%s:%s", prefix, name);
        return final_key;
    } else {
        unsigned int len = strlen(prefix) + 1 + strlen(name) + 1 + strlen(key);
        char *final_key = malloc(len + 1);
        snprintf(final_key, len + 1, "%s:%s:%s", prefix, name, key);
        return final_key;
    }
}

char *pressure_uid() {
    char *buf = malloc(1024);

    struct addrinfo hints, *info, *p;
    int gai_result;

    char hostname[1024];
    hostname[1023] = 0;
    gethostname(hostname, 1023);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;

    if ((gai_result = getaddrinfo(hostname, "http", &hints, &info)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai_result));
        exit(1);
    }

    for(p = info; p != NULL; p = p->ai_next) {
        snprintf(buf, 1024, "%s_pid%d", p->ai_canonname, (int) getpid());
        break;
    }

    freeaddrinfo(info);

    return buf;
}

pressureQueue *pressure_connect(redisContext *context, const char *prefix, const char *name) {
    if (context == NULL || context->err) {
        return NULL;
    }

    pressureQueue r = {
        .context = context,
        .name = malloc(strlen(name) + 1),
        .exists = false,
        .connected = false,
        .bound = BOUND_NOT_SET,
        .client_uid = pressure_uid(),

        .keys = {
            .queue = pressure_key(prefix, name, NULL),
            .bound = pressure_key(prefix, name, "bound"),

            .producer = pressure_key(prefix, name, "producer"),
            .consumer = pressure_key(prefix, name, "consumer"),

            .producer_free = pressure_key(prefix, name, "producer_free"),
            .consumer_free = pressure_key(prefix, name, "consumer_free"),

            .stats_produced_messages = pressure_key(prefix, name, "stats:produced_messages"),
            .stats_produced_bytes = pressure_key(prefix, name, "stats:produced_bytes"),

            .stats_consumed_messages = pressure_key(prefix, name, "stats:consumed_messages"),
            .stats_consumed_bytes = pressure_key(prefix, name, "stats:consumed_bytes"),

            .not_full = pressure_key(prefix, name, "not_full"),
            .closed = pressure_key(prefix, name, "closed"),
        }
    };

    strncpy(r.name, name, strlen(name) + 1);
    
    //  Make sure the server is available.
    redisReply *reply = redisCommand(context, "PING");
    r.connected = !strcmp(reply->str, "PONG");
    freeReplyObject(reply);

    //  Check if the queue already exists.
    reply = redisCommand(context, "GET %s", r.keys.bound);
    if (reply->type == REDIS_REPLY_STRING) {
        r.bound = atoi(reply->str);
        r.exists = true;
    } else {
        r.bound = BOUND_NOT_SET;
        r.exists = false;
    }
    freeReplyObject(reply);

    reply = redisCommand(context, "EXISTS %s", r.keys.closed);
    r.closed = reply->integer;
    freeReplyObject(reply);

    pressureQueue *queue = malloc(sizeof(r));
    memcpy(queue, &r, sizeof(r));
    return queue;
}

pressureStatus pressure_create(pressureQueue* queue, int bound) {
    //  Check if the queue already exists, or create it atomically.
    redisReply *reply = redisCommand(queue->context, "SETNX %s %d", queue->keys.bound, bound);
    bool key_was_set = reply->integer;
    freeReplyObject(reply);

    if (key_was_set) {
        queue->exists = true;
        queue->bound = bound;
        {
            redisReply *reply = redisCommand(queue->context, "LPUSH %s %d", queue->keys.producer_free, 0);
            int length = reply->integer;
            freeReplyObject(reply);

            if (length != 1) {
                return kPressureStatus_UnexpectedFailure;
            }
        }
        {
            redisReply *reply = redisCommand(queue->context, "LPUSH %s %d", queue->keys.consumer_free, 0);
            int length = reply->integer;
            freeReplyObject(reply);
            
            if (length != 1) {
                return kPressureStatus_UnexpectedFailure;
            }
        }
        {
            redisReply *reply = redisCommand(queue->context, "LPUSH %s %d", queue->keys.not_full, 0);
            int length = reply->integer;
            freeReplyObject(reply);
            
            if (length != 1) {
                return kPressureStatus_UnexpectedFailure;
            }
        }
        return kPressureStatus_Success;
    } else {
        return kPressureStatus_QueueAlreadyExistsError;
    }
}

pressureStatus pressure_put(pressureQueue* queue, char *buf, int bufsize) {
    //  Check if the queue exists.
    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
        queue->exists = reply->integer;
        freeReplyObject(reply);
    }

    if (!queue->exists) {
        return kPressureStatus_QueueDoesNotExistError;
    }

    {
        dbprintf("Waiting on a producer_free key...\n");
        redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.producer_free);
        freeReplyObject(reply);
        dbprintf("Got a producer_free key!\n");
    }

    {
        redisReply *reply = redisCommand(queue->context, "SET %s %s", queue->keys.producer, queue->client_uid);
        freeReplyObject(reply);
        dbprintf("Set producer tag '%s' to '%s'.\n", queue->keys.producer, queue->client_uid);
    }

    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.closed);
        bool queue_closed = reply->integer;
        freeReplyObject(reply);
        if (queue_closed) {
            freeReplyObject(redisCommand(
                queue->context, "LPUSH %s 0", queue->keys.producer_free
            ));
            return kPressureStatus_QueueClosed;
        } else {
            if (queue->bound > 0) {
                dbprintf("Waiting on not_full key...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.not_full);
                freeReplyObject(reply);
                dbprintf("Got not_full key!\n");
            }
            {
                dbprintf("Pushing binary data to queue...\n");
                redisReply *reply = redisCommand(queue->context, "LPUSH %s %b", queue->keys.queue, buf, bufsize);
                int queue_length = reply->integer;
                dbprintf("Done! Queue length is now %d.\n", queue_length);
                freeReplyObject(reply);

                if (queue->bound > 0 && queue_length < queue->bound) {
                    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0", queue->keys.not_full));
                    freeReplyObject(redisCommand(queue->context, "LTRIM %s 0 0", queue->keys.not_full));
                }

                freeReplyObject(redisCommand(queue->context, "INCR %s", queue->keys.stats_produced_messages));
                freeReplyObject(redisCommand(queue->context, "INCRBY %s %d", queue->keys.stats_produced_bytes, bufsize));
            }
        }
    }
    freeReplyObject(redisCommand(
        queue->context, "LPUSH %s 0", queue->keys.producer_free
    ));
    return kPressureStatus_Success;
}

pressureStatus pressure_get(pressureQueue* queue, char **buf, int *bufsize) {
    //  Check if the queue exists.
    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
        queue->exists = reply->integer;
        freeReplyObject(reply);
    }

    if (!queue->exists) {
        return kPressureStatus_QueueDoesNotExistError;
    }

    {
        dbprintf("Waiting on a consumer_free key...\n");
        redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.consumer_free);
        freeReplyObject(reply);
        dbprintf("Got a consumer_free key!\n");
    }

    {
        redisReply *reply = redisCommand(queue->context, "SET %s %s", queue->keys.consumer, queue->client_uid);
        freeReplyObject(reply);
        dbprintf("Set consumer tag '%s' to '%s'.\n", queue->keys.consumer, queue->client_uid);
    }

    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.closed);
        queue->closed = reply->integer;
        freeReplyObject(reply);
        if (queue->closed) {
            redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.queue);
            bool queue_empty = !reply->integer;
            freeReplyObject(reply);

            if (queue_empty) {
                freeReplyObject(redisCommand(
                    queue->context, "LPUSH %s 0", queue->keys.consumer_free
                ));
                return kPressureStatus_QueueClosed;
            } else {
                dbprintf("Waiting on data...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.queue);
                int data_length = reply->element[1]->len;

                if (*buf == NULL) {
                    *buf = malloc(data_length);
                    *bufsize = data_length;
                } else {
                    *bufsize = min(*bufsize, data_length);
                }
                memcpy(*buf, reply->element[1]->str, *bufsize);

                freeReplyObject(reply);
                dbprintf("Got data!\n");
            }

        } else {
            {
                dbprintf("Pulling binary data from queue...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s %s 0", queue->keys.queue, queue->keys.closed);
                
                if (!strcmp(queue->keys.closed, reply->element[0]->str)) {
                    //  Queue is closed.
                    queue->closed = true;
                    freeReplyObject(reply);

                    freeReplyObject(redisCommand(
                        queue->context, "LPUSH %s 0", queue->keys.consumer_free
                    ));
                    return kPressureStatus_QueueClosed;
                } else {
                    int data_length = reply->element[1]->len;

                    if (*buf == NULL) {
                        *buf = malloc(data_length);
                        *bufsize = data_length;
                    } else {
                        *bufsize = min(*bufsize, data_length);
                    }
                    memcpy(*buf, reply->element[1]->str, *bufsize);

                    dbprintf("Got %d bytes of data!\n", data_length);
                    freeReplyObject(reply);

                    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0", queue->keys.not_full));
                    freeReplyObject(redisCommand(queue->context, "LTRIM %s 0 0", queue->keys.not_full));

                    freeReplyObject(redisCommand(queue->context, "INCR %s", queue->keys.stats_consumed_messages));
                    freeReplyObject(redisCommand(queue->context, "INCRBY %s %d", queue->keys.stats_consumed_bytes, data_length));
                }
            }
        }
    }
    freeReplyObject(redisCommand(
        queue->context, "LPUSH %s 0", queue->keys.consumer_free
    ));
    return kPressureStatus_Success;
}

pressureStatus pressure_close(pressureQueue *queue) {
    //  Check if the queue exists.
    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
        queue->exists = reply->integer;
        freeReplyObject(reply);
    }

    if (!queue->exists) {
        return kPressureStatus_QueueDoesNotExistError;
    }

    {
        dbprintf("Waiting on a producer_free key...\n");
        redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.producer_free);
        freeReplyObject(reply);
        dbprintf("Got a producer_free key!\n");
    }

    {
        redisReply *reply = redisCommand(queue->context, "SET %s %s", queue->keys.producer, queue->client_uid);
        freeReplyObject(reply);
        dbprintf("Set producer tag '%s' to '%s'.\n", queue->keys.producer, queue->client_uid);
    }

    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.closed);
        queue->closed = reply->integer;
        freeReplyObject(reply);
        if (queue->closed) {
            freeReplyObject(redisCommand(
                queue->context, "LPUSH %s 0", queue->keys.producer_free
            ));
            return kPressureStatus_QueueClosed;
        } else {
            redisReply *reply = redisCommand(queue->context, "LPUSH %s 0 0", queue->keys.closed);
            freeReplyObject(reply);
            dbprintf("Pushed two keys to closed!\n");
        }
    }
    freeReplyObject(redisCommand(
        queue->context, "LPUSH %s 0", queue->keys.producer_free
    ));
    return kPressureStatus_Success;
}

pressureStatus pressure_delete(pressureQueue *queue) {
    //  Check if the queue exists.
    redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
    queue->exists = reply->integer;
    freeReplyObject(reply);

    if (!queue->exists) {
        return kPressureStatus_QueueDoesNotExistError;
    }

    freeReplyObject(redisCommand(queue->context, "DEL %s", queue->keys.bound));
    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0", queue->keys.not_full));
    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0 0", queue->keys.closed));

    freeReplyObject(redisCommand(queue->context, "BRPOP %s 0", queue->keys.producer_free));
    freeReplyObject(redisCommand(queue->context, "DEL %s %s", queue->keys.producer, queue->keys.producer_free));

    freeReplyObject(redisCommand(queue->context, "BRPOP %s 0", queue->keys.consumer_free));
    freeReplyObject(redisCommand(queue->context, "DEL %s %s", queue->keys.consumer, queue->keys.consumer_free));

    freeReplyObject(redisCommand(queue->context, "DEL %s %s %s %s %s %s %s",
                                 queue->keys.not_full, 
                                 queue->keys.closed,
                                 queue->keys.stats_produced_messages,
                                 queue->keys.stats_produced_bytes,
                                 queue->keys.stats_consumed_messages,
                                 queue->keys.stats_consumed_bytes,
                                 queue->keys.queue));
    queue->exists = false;
    
    return kPressureStatus_Success;
}

bool pressure_exists(pressureQueue* queue) {
    redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
    queue->exists = reply->integer;
    freeReplyObject(reply);

    return queue->exists;
}

pressureStatus pressure_length(pressureQueue *queue, int *length) {
    redisReply *reply = redisCommand(queue->context, "LLEN %s", queue->keys.queue);

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        
        reply = redisCommand(queue->context, "EXISTS %s", queue->keys.bound);
        queue->exists = reply->integer;

        freeReplyObject(reply);
        if (queue->exists) {
            *length = 0;
            return kPressureStatus_Success;
        } else {
            return kPressureStatus_QueueDoesNotExistError;
        }
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        *length = reply->integer;

        freeReplyObject(reply);
        return kPressureStatus_Success;
    }

    freeReplyObject(reply);
    return kPressureStatus_UnexpectedFailure;
}

pressureStatus pressure_closed(pressureQueue *queue, bool *closed) {
    if (pressure_exists(queue)) {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.closed);
        queue->closed = reply->integer;
        freeReplyObject(reply);
        
        *closed = queue->closed;

        return kPressureStatus_Success;
    } else {
        return kPressureStatus_QueueDoesNotExistError;
    }
}

void pressure_disconnect(pressureQueue *queue) {
    if (queue->connected) {
        queue->connected = false;

        free(queue->name);                        
        free(queue->client_uid);                  

        free(queue->keys.queue);                  
        free(queue->keys.bound);                  
        free(queue->keys.producer);               
        free(queue->keys.consumer);               
        free(queue->keys.producer_free);          
        free(queue->keys.consumer_free);          

        free(queue->keys.stats_produced_messages);
        free(queue->keys.stats_produced_bytes);   
        free(queue->keys.stats_consumed_messages);
        free(queue->keys.stats_consumed_bytes);   

        free(queue->keys.not_full);               
        free(queue->keys.closed);                 
    }
    free(queue);
}

void pressure_print(pressureQueue *queue) {
    dbprintf("pressure queue {\n");
    dbprintf("\tname\t%s\n", queue->name);
    dbprintf("\texists?\t%s\n", queue->exists ? "yes" : "no");
    dbprintf("\tconnected?\t%s\n", queue->connected ? "yes" : "no");
    dbprintf("\tclosed?:\t%s\n", queue->closed ? "yes" : "no");
    if (queue->exists) {
        dbprintf("\tbound\t%d\n", queue->bound);
    }
    dbprintf("\tclient_uid:\t%s\n", queue->client_uid);
    dbprintf("\tkeys:\n");
    dbprintf("\t\t%s\n", queue->keys.queue);
    dbprintf("\t\t%s\n", queue->keys.bound);
    dbprintf("\t\t%s\n", queue->keys.producer);
    dbprintf("\t\t%s\n", queue->keys.consumer);
    dbprintf("\t\t%s\n", queue->keys.producer_free);
    dbprintf("\t\t%s\n", queue->keys.consumer_free);
    dbprintf("\t\t%s\n", queue->keys.stats_produced_messages);
    dbprintf("\t\t%s\n", queue->keys.stats_produced_bytes);
    dbprintf("\t\t%s\n", queue->keys.stats_consumed_messages);
    dbprintf("\t\t%s\n", queue->keys.stats_consumed_bytes);
    dbprintf("\t\t%s\n", queue->keys.not_full);
    dbprintf("\t\t%s\n", queue->keys.closed);
    dbprintf("}\n");
}