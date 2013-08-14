LIB_SOURCES = $(wildcard pressure*.c)
LIB_OBJECTS = $(LIB_SOURCES:.c=.o)
LIB_DEPENDS = $(LIB_SOURCES:.c=.d)
LIB_O = pressure.o
LIB = libpressure.a

PUT_SOURCES = $(wildcard put*.c)
PUT_OBJECTS = $(PUT_SOURCES:.c=.o)
PUT_DEPENDS = $(PUT_SOURCES:.c=.d)
PUT = put

GET_SOURCES = $(wildcard get*.c)
GET_OBJECTS = $(GET_SOURCES:.c=.o)
GET_DEPENDS = $(GET_SOURCES:.c=.d)
GET = get

CFLAGS = -Wall -MMD -ftrapv -lhiredis
CC = clang

.PHONY: debug clean clients

debug: CFLAGS = -Wall -lhiredis -g
debug: clients

${PUT}: ${PUT_OBJECTS} libpressure.a
	${CC} ${CFLAGS} $^ -o $@ -L. -lpressure

${GET}: ${GET_OBJECTS} libpressure.a
	${CC} ${CFLAGS} $^ -o $@ -L. -lpressure

clients: ${PUT} ${GET}

clean:
	rm -rf *.d *.o ${PUT} ${GET} ${LIB} ${LIB_O} *.dSYM

${LIB}: ${LIB_O}
	${AR} rcs $@ $^

-include ${LIB_DEPENDS}
-include ${PUT_DEPENDS}
-include ${GET_DEPENDS}