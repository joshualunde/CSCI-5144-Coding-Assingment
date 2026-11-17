#define _POSIX_C_SOURCE 200809L
#include "hash.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* Benchmark for CSCI 5144: compare 1, 2, or 3 KV servers.
   Run the servers separately on ports 8081, 8082, 8083.
   Each server must use its own working directory (separate data.wal).
   The client uses the existing consistent hash ring to select a server. */
#define FIRST_PORT 8081
#define DEFAULT_KEYS 300
#define DEFAULT_THREADS 3
#define MAX_THREADS 64
#define REPLY_LIMIT 8192

typedef enum { PUT_PHASE, GET_PHASE } Phase;
typedef enum { OK, MISMATCH, ERROR } Outcome;

typedef struct {
    Outcome outcome;
    double latency_ms;
} Result;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int go;
} StartGate;

typedef struct {
    hash_ring *ring;
    int thread_id;
    int thread_count;
    int key_count;
    Phase phase;
    Result *results;
    StartGate *gate;
} WorkerArgs;

static pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;
static int problems_printed = 0;

static double elapsed_ms(const struct timespec *start, const struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1000000.0;
}

/* Print at most five problems per benchmark so errors don't flood the output. */
static void log_problem(const char *phase, const char *key, const char *why) {
    pthread_mutex_lock(&print_mutex);
    if (problems_printed < 5)
        fprintf(stderr, "%s %s: %s\n", phase, key, why);
    problems_printed++;
    pthread_mutex_unlock(&print_mutex);
}

static int send_all(int sock, const char *text, size_t length) {
    size_t sent = 0;
    while (sent < length) {
        ssize_t n = send(sock, text + sent, length - sent, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

/* Send one request and read its response. One TCP connection per request,
   matching the server's "Connection: close" behavior. */
static int http_request(int port, Phase phase, const char *key,
                        const char *value, int *status,
                        char *body, size_t body_size) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct timeval timeout = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)port);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (connect(sock, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(sock);
        return -1;
    }

    char request[512];
    int length;
    if (phase == PUT_PHASE) {
        char json[128];
        snprintf(json, sizeof(json), "{\"value\":\"%s\"}", value);
        length = snprintf(request, sizeof(request),
                          "POST /%s HTTP/1.1\r\n"
                          "Host: 127.0.0.1\r\n"
                          "Content-Type: application/json\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n%s",
                          key, strlen(json), json);
    } else {
        length = snprintf(request, sizeof(request),
                          "GET /%s HTTP/1.1\r\n"
                          "Host: 127.0.0.1\r\n"
                          "Connection: close\r\n\r\n", key);
    }
    if (length < 0 || (size_t)length >= sizeof(request) ||
        send_all(sock, request, (size_t)length) != 0) {
        close(sock);
        return -1;
    }

    char reply[REPLY_LIMIT];
    size_t used = 0;
    while (used < sizeof(reply) - 1) {
        ssize_t n = recv(sock, reply + used, sizeof(reply) - 1 - used, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) { close(sock); return -1; }
        if (n == 0) break;
        used += (size_t)n;
    }
    close(sock);
    reply[used] = '\0';

    char http_version[16];
    if (used == sizeof(reply) - 1 ||
        sscanf(reply, "%15s %d", http_version, status) != 2 ||
        strncmp(http_version, "HTTP/1.", 7) != 0) return -1;
    const char *separator = strstr(reply, "\r\n\r\n");
    if (!separator) return -1;
    separator += 4;
    if (strlen(separator) >= body_size) return -1;
    strcpy(body, separator);
    return 0;
}

/* The benchmark writes plain strings (value_0, value_1, ...).
   Accept optional JSON whitespace; no full JSON library is needed here. */
static void skip_ws(const char **p) {
    while (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n') ++*p;
}

static int value_matches(const char *body, const char *expected) {
    const char *p = body;
    skip_ws(&p);
    if (*p++ != '{') return 0;
    skip_ws(&p);
    if (strncmp(p, "\"value\"", 7) != 0) return 0;
    p += 7;
    skip_ws(&p);
    if (*p++ != ':') return 0;
    skip_ws(&p);
    if (*p++ != '"') return 0;
    size_t length = strlen(expected);
    if (strncmp(p, expected, length) != 0) return 0;
    p += length;
    if (*p++ != '"') return 0;
    skip_ws(&p);
    if (*p++ != '}') return 0;
    skip_ws(&p);
    return *p == '\0';
}

static Outcome do_operation(hash_ring *ring, Phase phase,
                            const char *key, const char *value) {
    /* Use the SAME hash.c as the group implementation; don't use key % N. */
    char node[HASH_NAME_LEN];
    if (hash_lookup_name(ring, key, strlen(key), node, sizeof(node)) != 0)
        return ERROR;

    const char *colon = strrchr(node, ':');
    if (!colon || strncmp(node, "127.0.0.1:", 10) != 0)
        return ERROR;
    int port = atoi(colon + 1);
    if (port < 1 || port > 65535) return ERROR;

    int status = 0;
    char body[REPLY_LIMIT];
    if (http_request(port, phase, key, value, &status, body, sizeof(body)) != 0) {
        log_problem(phase == PUT_PHASE ? "PUT" : "GET", key,
                    "Could not connect or read HTTP response");
        return ERROR;
    }
    if (status != 200) {
        char reason[80];
        snprintf(reason, sizeof(reason), "HTTP status %d (port %d)", status, port);
        log_problem(phase == PUT_PHASE ? "PUT" : "GET", key, reason);
        return ERROR;
    }
    if (phase == GET_PHASE && !value_matches(body, value)) {
        log_problem("GET", key, "Returned value did not match expected value");
        return MISMATCH;
    }
    return OK;
}

static void *worker(void *arg) {
    WorkerArgs *w = arg;
    pthread_mutex_lock(&w->gate->mutex);
    while (!w->gate->go) pthread_cond_wait(&w->gate->cond, &w->gate->mutex);
    pthread_mutex_unlock(&w->gate->mutex);

    for (int i = w->thread_id; i < w->key_count; i += w->thread_count) {
        char key[40], value[40];
        snprintf(key, sizeof(key), "bench_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        struct timespec begin, end;
        clock_gettime(CLOCK_MONOTONIC, &begin);
        Outcome outcome = do_operation(w->ring, w->phase, key, value);
        clock_gettime(CLOCK_MONOTONIC, &end);
        w->results[i].outcome = outcome;
        w->results[i].latency_ms = elapsed_ms(&begin, &end);
    }
    return NULL;
}

static int compare_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static int run_phase(hash_ring *ring, Phase phase, int keys, int threads) {
    Result *results = calloc((size_t)keys, sizeof(*results));
    WorkerArgs *args = calloc((size_t)threads, sizeof(*args));
    pthread_t *ids = calloc((size_t)threads, sizeof(*ids));
    double *latencies = malloc((size_t)keys * sizeof(*latencies));
    if (!results || !args || !ids || !latencies) {
        fprintf(stderr, "Out of memory\n");
        free(results); free(args); free(ids); free(latencies);
        return -1;
    }
    StartGate gate;
    pthread_mutex_init(&gate.mutex, NULL);
    pthread_cond_init(&gate.cond, NULL);
    gate.go = 0;

    int created = 0;
    for (int t = 0; t < threads; t++) {
        args[t] = (WorkerArgs){.ring = ring, .thread_id = t,
            .thread_count = threads, .key_count = keys, .phase = phase,
            .results = results, .gate = &gate};
        if (pthread_create(&ids[t], NULL, worker, &args[t]) != 0) break;
        created++;
    }

    struct timespec begin, end;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    pthread_mutex_lock(&gate.mutex);
    gate.go = 1;
    pthread_cond_broadcast(&gate.cond);
    pthread_mutex_unlock(&gate.mutex);
    for (int t = 0; t < created; t++) pthread_join(ids[t], NULL);
    clock_gettime(CLOCK_MONOTONIC, &end);
    double seconds = elapsed_ms(&begin, &end) / 1000.0;

    int ok = 0, mismatch = 0, error = 0;
    if (created == threads) {
        for (int i = 0; i < keys; i++) {
            if (results[i].outcome == OK) latencies[ok++] = results[i].latency_ms;
            else if (results[i].outcome == MISMATCH) mismatch++;
            else error++;
        }
    }

    if (ok) qsort(latencies, (size_t)ok, sizeof(*latencies), compare_double);
    double sum = 0;
    for (int i = 0; i < ok; i++) sum += latencies[i];
    size_t rank = ((size_t)99 * (size_t)ok + 99) / 100; /* ceil(0.99 * ok) */
    double avg = ok ? sum / ok : 0;
    double p99 = ok ? latencies[rank - 1] : 0;
    printf("%s: ok=%d mismatch=%d error=%d time=%.3fs "
           "throughput=%.1f ops/s avg=%.2f ms p99=%.2f ms\n",
           phase == PUT_PHASE ? "PUT" : "GET", ok, mismatch, error,
           seconds, seconds > 0 ? ok / seconds : 0.0, avg, p99);
    if (created != threads) fprintf(stderr, "Could not start all worker threads\n");

    pthread_cond_destroy(&gate.cond);
    pthread_mutex_destroy(&gate.mutex);
    free(results); free(args); free(ids); free(latencies);
    return (created == threads && error == 0 && mismatch == 0 && ok == keys) ? 0 : -1;
}

static int positive_number(const char *text, int maximum) {
    char *end;
    errno = 0;
    long number = strtol(text, &end, 10);
    if (errno || *text == '\0' || *end != '\0' || number < 1 || number > maximum)
        return -1;
    return (int)number;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN); /* A closed connection should be an error, not a crash. */
    if (argc < 2 || argc > 4) {
        fprintf(stderr, "Usage: %s <stores:1-3> [keys:300] [threads:3]\n", argv[0]);
        return 1;
    }
    int stores = positive_number(argv[1], 3);
    int keys = argc > 2 ? positive_number(argv[2], 1000000) : DEFAULT_KEYS;
    int threads = argc > 3 ? positive_number(argv[3], MAX_THREADS) : DEFAULT_THREADS;
    if (stores < 0 || keys < 0 || threads < 0) {
        fprintf(stderr, "Invalid stores, keys, or threads argument\n");
        return 1;
    }
    hash_ring *ring = hash_create(0);
    if (!ring) { fprintf(stderr, "Could not create hash ring\n"); return 1; }

    for (int n = 0; n < stores; n++) {
        char node[HASH_NAME_LEN];
        snprintf(node, sizeof(node), "127.0.0.1:%d", FIRST_PORT + n);
        if (hash_add(ring, node) < 0) {
            fprintf(stderr, "Could not add %s to hash ring\n", node);
            hash_free(ring);
            return 1;
        }
    }

    printf("KV stores: %d | keys: %d | client threads: %d\n", stores, keys, threads);
    printf("Distribution (keys assigned by consistent hashing):\n");
    int distribution[3] = {0};
    for (int i = 0; i < keys; i++) {
        char key[40], node[HASH_NAME_LEN];
        snprintf(key, sizeof(key), "bench_%d", i);
        if (hash_lookup_name(ring, key, strlen(key), node, sizeof(node)) != 0) {
            fprintf(stderr, "Hash lookup failed\n");
            hash_free(ring);
            return 1;
        }
        const char *colon = strrchr(node, ':');
        int index = colon ? atoi(colon + 1) - FIRST_PORT : -1;
        if (index < 0 || index >= stores) {
            fprintf(stderr, "Unexpected hash node: %s\n", node);
            hash_free(ring);
            return 1;
        }
        distribution[index]++;
    }
    for (int n = 0; n < stores; n++)
        printf("  Port %d: %d keys (%.1f%%)\n", FIRST_PORT + n,
               distribution[n], distribution[n] * 100.0 / keys);

    problems_printed = 0;
    int put_result = run_phase(ring, PUT_PHASE, keys, threads);
    int get_result = run_phase(ring, GET_PHASE, keys, threads);
    hash_free(ring);
    return (put_result == 0 && get_result == 0) ? 0 : 1;
}
