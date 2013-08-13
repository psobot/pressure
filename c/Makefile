SOURCES = $(wildcard *.c)
OBJECTS = $(SOURCES:.c=.o)
DEPENDS = $(SOURCES:.c=.d)
CFLAGS = -Wall -MMD -ftrapv -lhiredis
CC = clang
EXEC = pressure

.PHONY: debug fast clean

debug: CFLAGS += -g
debug: ${EXEC}

fast: CFLAGS += -O3 -DNDEBUG
fast: ${EXEC}

clean:
	rm -f *.d *.o ${EXEC}

${EXEC}: ${OBJECTS}
	${CC} ${CFLAGS} $^ -o $@

-include ${DEPENDS}
