VERSION = 1.2.01

CC = gcc
CFLAGS = -O2 -Wall 
CPPFLAGS = "-DVERSION=\"$(VERSION)\""

DB_OBJS = fileio.o util.o dbench.o child.o
TB_OBJS = sockio.o util.o dbench.o child.o socklib.o
SRV_OBJS = util.o tbench_srv.o socklib.o

all: dbench tbench tbench_srv

dbench: $(DB_OBJS)
	$(CC) -o $@ $(DB_OBJS)

tbench: $(TB_OBJS)
	$(CC) -o $@ $(TB_OBJS)

tbench_srv: $(SRV_OBJS)
	$(CC) -o $@ $(SRV_OBJS)

clean:
	rm -f *.o *~ dbench tbench tbench_srv

proto:
	cat *.c | awk -f mkproto.awk > proto.h
