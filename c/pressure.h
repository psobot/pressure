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
    const char *name;
    const char *client_uid;

    bool exists;
    bool connected;
    int bound;

    struct keys {
        const char *queue;
        const char *bound;

        const char *producer;
        const char *consumer;

        const char *producer_free;
        const char *consumer_free;

        const char *stats_produced_messages;
        const char *stats_produced_bytes;
        const char *stats_consumed_messages;
        const char *stats_consumed_bytes;

        const char *not_full;
        const char *closed;
    } keys;
} pressureQueue;

pressureQueue *pressure_connect(redisContext *context, const char *prefix, const char *name);
pressureStatus pressure_create(pressureQueue* queue, int bound);

pressureStatus pressure_get(pressureQueue* queue, char *buf, int *bufsize);
pressureStatus pressure_put(pressureQueue* queue, char *buf, int bufsize);

void pressure_disconnect(pressureQueue* queue);

void pressure_print(pressureQueue *queue);
