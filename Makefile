CC = cc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -O2 -pthread

all: kv_server kv_bench

kv_server: server.c kv_store.c kv_store.h
	$(CC) $(CFLAGS) server.c kv_store.c -o kv_server

kv_bench: kv_bench.c hash.c hash.h
	$(CC) $(CFLAGS) kv_bench.c hash.c -o kv_bench

clean:
	rm -f kv_server kv_bench

.PHONY: all clean
