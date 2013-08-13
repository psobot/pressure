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

const char *pressure_key(const char *prefix, const char *name, const char *key) {
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

const char *pressure_uid() {
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
        .name = name,
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
    
    //  Make sure the server is available.
    redisReply *reply = redisCommand(context, "PING");
    r.connected = !strcmp(reply->str, "PONG");
    freeReplyObject(reply);

    //  Check if the queue already exists.
    reply = redisCommand(context, "EXISTS %s", r.keys.bound);
    r.exists = reply->integer;
    freeReplyObject(reply);

    if (r.exists) {
        reply = redisCommand(context, "GET %s", r.keys.bound);
        if (reply->type == REDIS_REPLY_STRING) {
            r.bound = atoi(reply->str);
        } else {
        }
        freeReplyObject(reply);
    }

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
        printf("Waiting on a producer_free key...\n");
        redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.producer_free);
        freeReplyObject(reply);
        printf("Got a producer_free key!\n");
    }

    {
        redisReply *reply = redisCommand(queue->context, "SET %s %s", queue->keys.producer, queue->client_uid);
        freeReplyObject(reply);
        printf("Set producer tag '%s' to '%s'.\n", queue->keys.producer, queue->client_uid);
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
                printf("Waiting on not_full key...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.not_full);
                freeReplyObject(reply);
                printf("Got not_full key!\n");
            }
            {
                printf("Pushing binary data to queue...\n");
                redisReply *reply = redisCommand(queue->context, "LPUSH %s %b", queue->keys.queue, buf, bufsize);
                int queue_length = reply->integer;
                printf("Done! Queue length is now %d.\n", queue_length);
                freeReplyObject(reply);

                if (queue->bound > 0 && queue_length < queue->bound) {
                    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0", queue->keys.not_full));
                    freeReplyObject(redisCommand(queue->context, "LTRIM %s 0 1", queue->keys.not_full));
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

pressureStatus pressure_get(pressureQueue* queue, char *buf, int *bufsize) {
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
        printf("Waiting on a consumer_free key...\n");
        redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.consumer_free);
        freeReplyObject(reply);
        printf("Got a consumer_free key!\n");
    }

    {
        redisReply *reply = redisCommand(queue->context, "SET %s %s", queue->keys.consumer, queue->client_uid);
        freeReplyObject(reply);
        printf("Set consumer tag '%s' to '%s'.\n", queue->keys.consumer, queue->client_uid);
    }

    {
        redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.closed);
        bool queue_closed = reply->integer;
        freeReplyObject(reply);
        if (queue_closed) {
            redisReply *reply = redisCommand(queue->context, "EXISTS %s", queue->keys.queue);
            bool queue_empty = !reply->integer;
            freeReplyObject(reply);

            if (queue_empty) {
                freeReplyObject(redisCommand(
                    queue->context, "LPUSH %s 0", queue->keys.consumer_free
                ));
                return kPressureStatus_QueueClosed;
            } else {
                printf("Waiting on data...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s 0", queue->keys.queue);
                int data_length = reply->element[1]->len;
                *bufsize = min(*bufsize, data_length);
                memcpy(buf, reply->element[1]->str, *bufsize);
                freeReplyObject(reply);
                printf("Got data!\n");
            }

        } else {
            {
                printf("Pulling binary data from queue...\n");
                redisReply *reply = redisCommand(queue->context, "BRPOP %s %s 0", queue->keys.queue, queue->keys.closed);
                
                if (!strcmp(queue->keys.closed, reply->element[0]->str)) {
                    //  Queue is closed.
                    freeReplyObject(reply);

                    freeReplyObject(redisCommand(
                        queue->context, "LPUSH %s 0", queue->keys.consumer_free
                    ));
                    return kPressureStatus_QueueClosed;
                } else {
                    int data_length = reply->element[1]->len;
                    *bufsize = min(*bufsize, data_length);
                    memcpy(buf, reply->element[1]->str, *bufsize);

                    printf("Got %d bytes of data!\n", data_length);
                    freeReplyObject(reply);

                    freeReplyObject(redisCommand(queue->context, "LPUSH %s 0", queue->keys.not_full));
                    freeReplyObject(redisCommand(queue->context, "LTRIM %s 0 1", queue->keys.not_full));

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

void pressure_print(pressureQueue *queue) {
    printf("pressure queue {\n");
    printf("\tname\t%s\n", queue->name);
    printf("\texists?\t%s\n", queue->exists ? "yes" : "no");
    printf("\tconnected?\t%s\n", queue->connected ? "yes" : "no");
    if (queue->exists) {
        printf("\tbound\t%d\n", queue->bound);
    }
    printf("\tclient_uid:\t%s\n", queue->client_uid);
    printf("\tkeys:\n");
    printf("\t\t%s\n", queue->keys.queue);
    printf("\t\t%s\n", queue->keys.bound);
    printf("\t\t%s\n", queue->keys.producer);
    printf("\t\t%s\n", queue->keys.consumer);
    printf("\t\t%s\n", queue->keys.producer_free);
    printf("\t\t%s\n", queue->keys.consumer_free);
    printf("\t\t%s\n", queue->keys.stats_produced_messages);
    printf("\t\t%s\n", queue->keys.stats_produced_bytes);
    printf("\t\t%s\n", queue->keys.stats_consumed_messages);
    printf("\t\t%s\n", queue->keys.stats_consumed_bytes);
    printf("\t\t%s\n", queue->keys.not_full);
    printf("\t\t%s\n", queue->keys.closed);
    printf("}\n");
}

int main(int argc, char **argv) {
    unsigned int j;
    redisContext *c;
    redisReply *reply;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? atoi(argv[2]) : 6379;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    pressureQueue *queue = pressure_connect(c, "__pressure__", "test");
    pressure_print(queue);
    switch (pressure_create(queue, 5)) {
        case kPressureStatus_QueueAlreadyExistsError:
            printf("Queue already exists!\n");
            break;
        case kPressureStatus_UnexpectedFailure:
            printf("Unexpected failure!\n");
            break;
        case kPressureStatus_Success:
            printf("Successfully created queue!\n");
            break;
    }
    pressure_print(queue);

    char *item = "test";
    printf("Putting one item: '%s'\n", item);
    switch (pressure_put(queue, item, strlen(item))) {
        case kPressureStatus_QueueDoesNotExistError:
            printf("Queue does not exist!\n");
            break;
        case kPressureStatus_QueueClosed:
            printf("Queue closed!\n");
            break;
        case kPressureStatus_Success:
            printf("Successfully created queue!\n");
            break;
    }

    item = malloc(5);
    printf("Getting one item.\n");
    int bufsize = 5;
    switch (pressure_get(queue, item, &bufsize)) {
        case kPressureStatus_QueueDoesNotExistError:
            printf("Queue does not exist!\n");
            break;
        case kPressureStatus_QueueClosed:
            printf("Queue closed!\n");
            break;
        case kPressureStatus_Success:
            printf("Successfully created queue!\n");
            break;
    }
    if (bufsize < 5) {
        //  Null terminate the string that we get back.
        item[4] = 0;
    }
    printf("Got item: '%s'\n", item);
    free(item);

    /* Disconnects and frees the context */
    redisFree(c);

    return 0;
}