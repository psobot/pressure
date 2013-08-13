#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hiredis/hiredis.h>
#include "pressure.h"

int main(int argc, char **argv) {
    unsigned int j;
    redisContext *c;
    redisReply *reply;
    const char *hostname = "127.0.0.1";
    int port = 6379;

    if (argc <= 1) {
        printf("usage: %s <queue_name>\n", argv[0]);
        exit(0);
    }

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

    pressureQueue *queue = pressure_connect(c, "__pressure__", argv[1]);

    switch (pressure_create(queue, 5)) {
        case kPressureStatus_UnexpectedFailure:
            printf("Unexpected failure!\n");
            exit(1);
            break;
        case kPressureStatus_QueueAlreadyExistsError:
        case kPressureStatus_Success:
            break;
    }

    char *line = NULL;
    int bytes_read = 0;
    bool stop = false;
    int len;

    while ((bytes_read = getline(&line, &len, stdin)) != -1 && !stop) {
        //  Remove the trailing newline character.
        if (line[strlen(line) - 1] == '\n') {
            line[strlen(line) - 1] = 0;
        }

        switch (pressure_put(queue, line, strlen(line))) {
            case kPressureStatus_QueueDoesNotExistError:
            case kPressureStatus_QueueClosed:
                stop = true;
                break;
            case kPressureStatus_Success:
            default:
                break;
        }

        free(line);
        line = NULL;
    }

    if (line != NULL) free(line);

    switch (pressure_close(queue)) {
        case kPressureStatus_QueueDoesNotExistError:
            printf("Queue does not exist!\n");
            break;
        case kPressureStatus_QueueClosed:
            printf("Queue closed already!\n");
            break;
        case kPressureStatus_Success:
            break;
    }   

    pressure_disconnect(queue);
    redisFree(c);

    return 0;
}