#pragma once

#include <stdbool.h>

struct redisContext;
struct redisReply;

static const int BOUND_NOT_SET = -1;
static const int UNBOUNDED = 0;

typedef enum pressureStatus {
    kPressureStatus_Success,
    kPressureStatus_QueueClosed,
    kPressureStatus_QueueAlreadyExistsError,
    kPressureStatus_QueueDoesNotExistError,
    kPressureStatus_UnexpectedFailure,
} pressureStatus;

typedef struct pressureQueue {
    redisContext *context;
    char *name;
    char *client_uid;

    bool exists;
    bool connected;
    bool closed;
    int bound;

    struct keys {
        char *queue;
        char *bound;

        char *producer;
        char *consumer;

        char *producer_free;
        char *consumer_free;

        char *stats_produced_messages;
        char *stats_produced_bytes;
        char *stats_consumed_messages;
        char *stats_consumed_bytes;

        char *not_full;
        char *closed;
    } keys;
} pressureQueue;

pressureQueue *pressure_connect(redisContext *context, const char *prefix, const char *name);
pressureStatus pressure_create(pressureQueue* queue, int bound);

pressureStatus pressure_get(pressureQueue* queue, char **buf, int *bufsize);
pressureStatus pressure_put(pressureQueue* queue, char *buf, int bufsize);

pressureStatus pressure_close(pressureQueue* queue);
pressureStatus pressure_delete(pressureQueue* queue);

void pressure_disconnect(pressureQueue* queue);

void pressure_print(pressureQueue *queue);
