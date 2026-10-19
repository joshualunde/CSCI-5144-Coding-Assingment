CC = cc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -O2 -pthread

all: kv_server

kv_server: server.c kv_store.c kv_store.h
	$(CC) $(CFLAGS) server.c kv_store.c -o kv_server

clean:
	rm -f kv_server

.PHONY: all clean
