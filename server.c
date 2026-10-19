#define _POSIX_C_SOURCE 200809L
#include "kv_store.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define REQUEST_LIMIT 16384
#define DEFAULT_PORT 8080

static FILE *operations_log;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

static void record_operation(const char *method, const char *key, int code) {
    time_t now = time(NULL);
    struct tm local_time;
    localtime_r(&now, &local_time);
    char timestamp[40];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S%z", &local_time);

    pthread_mutex_lock(&log_mutex);
    fprintf(operations_log, "%s %s key=%s status=%d\n", timestamp,
            method, key, code);
    fflush(operations_log);
    pthread_mutex_unlock(&log_mutex);
}

static int send_all(int fd, const char *data, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t n = send(fd, data + offset, length - offset, 0);
        if (n <= 0) return -1;
        offset += (size_t)n;
    }
    return 0;
}

static int respond(int fd, int status, const char *reason, const char *json) {
    char header[300];
    int n = snprintf(header, sizeof(header),
                     "HTTP/1.1 %d %s\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n\r\n",
                     status, reason, strlen(json));
    if (n < 0 || (size_t)n >= sizeof(header)) return -1;
    if (send_all(fd, header, (size_t)n) != 0) return -1;
    return send_all(fd, json, strlen(json));
}

static void skip_spaces(const char **cursor) {
    while (**cursor == ' ' || **cursor == '\t' || **cursor == '\r' ||
           **cursor == '\n') ++*cursor;
}

// Educational subset: parse a JSON string, including common backslash escapes
// Unicode escape sequences are deliberately unsupported in this starter
static int parse_json_string(const char **cursor, char *out, size_t capacity) {
    if (**cursor != '"') return -1;
    ++*cursor;
    size_t used = 0;
    while (**cursor != '\0' && **cursor != '"') {
        unsigned char c = (unsigned char)**cursor;
        ++*cursor;
        if (c == '\\') {
            c = (unsigned char)**cursor;
            if (c == '\0') return -1;
            ++*cursor;
            switch (c) {
                case '"': case '\\': case '/': break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                default: return -1;
            }
        } else if (c < 32) {
            return -1;
        }
        if (used + 1 >= capacity) return -1;
        out[used++] = (char)c;
    }
    if (**cursor != '"') return -1;
    ++*cursor;
    out[used] = '\0';
    return 0;
}

// Accepts JSON objects of the form {"value":"some string"}
static int extract_value(const char *body, char value[KV_MAX_VALUE + 1]) {
    const char *cursor = body;
    char property[32];
    skip_spaces(&cursor);
    if (*cursor++ != '{') return -1;
    skip_spaces(&cursor);
    if (parse_json_string(&cursor, property, sizeof(property)) != 0 ||
        strcmp(property, "value") != 0) return -1;
    skip_spaces(&cursor);
    if (*cursor++ != ':') return -1;
    skip_spaces(&cursor);
    if (parse_json_string(&cursor, value, KV_MAX_VALUE + 1) != 0) return -1;
    skip_spaces(&cursor);
    if (*cursor++ != '}') return -1;
    skip_spaces(&cursor);
    return *cursor == '\0' ? 0 : -1;
}

// Read one HTTP/1.1 request with Content-Length (not chunked encoding)
static int read_request(int fd, char *buffer, size_t capacity,
                        size_t *body_at, size_t *body_length,
                        char method[16], char path[KV_MAX_KEY + 2]) {
    size_t used = 0, header_end = 0;
    for (;;) {
        if (used == capacity - 1) return 413;
        ssize_t n = recv(fd, buffer + used, capacity - 1 - used, 0);
        if (n <= 0) return 400;
        used += (size_t)n;
        buffer[used] = '\0';
        char *separator = strstr(buffer, "\r\n\r\n");
        if (separator) {
            header_end = (size_t)(separator - buffer) + 4;
            break;
        }
    }

    // Parse a copy so the already received request body stays intact
    char headers[REQUEST_LIMIT + 1];
    memcpy(headers, buffer, header_end);
    headers[header_end] = '\0';
    char *saveptr = NULL;
    char *line = strtok_r(headers, "\r\n", &saveptr);
    char version[16];
    if (!line || sscanf(line, "%15s %256s %15s", method, path, version) != 3 ||
        strncmp(version, "HTTP/1.", 7) != 0) return 400;

    size_t content_length = 0;
    int saw_length = 0;
    while ((line = strtok_r(NULL, "\r\n", &saveptr))) {
        if (strncasecmp(line, "Content-Length:", 15) == 0) {
            if (saw_length) return 400;
            saw_length = 1;
            const char *digits = line + 15;
            while (*digits == ' ' || *digits == '\t') ++digits;
            if (!isdigit((unsigned char)*digits)) return 400;
            errno = 0;
            char *endptr;
            unsigned long long amount = strtoull(digits, &endptr, 10);
            while (*endptr == ' ' || *endptr == '\t') ++endptr;
            if (errno || *endptr || amount > capacity - header_end - 1)
                return 413;
            content_length = (size_t)amount;
        }
    }
    if (used - header_end > content_length) {
        // Extra bytes (e.g. pipelined requests) are intentionally ignored
    }
    while (used - header_end < content_length) {
        ssize_t n = recv(fd, buffer + used, capacity - 1 - used, 0);
        if (n <= 0) return 400;
        used += (size_t)n;
    }
    if (memchr(buffer + header_end, '\0', content_length)) return 400;
    buffer[header_end + content_length] = '\0';
    *body_at = header_end;
    *body_length = content_length;
    return 0;
}

static int allowed_key(const char *key) {
    size_t length = strlen(key);
    if (length == 0 || length > KV_MAX_KEY) return 0;
    for (size_t i = 0; i < length; i++) {
        unsigned char c = (unsigned char)key[i];
        if (!(isalnum(c) || c == '_' || c == '-' || c == '.')) return 0;
    }
    return 1;
}

static void *handle_client(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);
    struct timeval timeout = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    char request[REQUEST_LIMIT + 1];
    char method[16] = "UNKNOWN", path[KV_MAX_KEY + 2] = "?";
    size_t body_at = 0, body_length = 0;
    int error = read_request(client_fd, request, sizeof(request),
                             &body_at, &body_length, method, path);
    if (error != 0) {
        int code = error;
        respond(client_fd, code, code == 413 ? "Payload Too Large" : "Bad Request",
                code == 413 ? "{\"error\":\"Request too large\"}" :
                              "{\"error\":\"Malformed HTTP request\"}");
        record_operation(method, path, code);
        close(client_fd);
        return NULL;
    }

    if (path[0] != '/' || !allowed_key(path + 1)) {
        respond(client_fd, 400, "Bad Request", "{\"error\":\"Invalid key\"}");
        record_operation(method, path, 400);
    } else if (strcmp(method, "POST") == 0 || strcmp(method, "PUT") == 0) {
        // The assignment calls this PUT; the provided benchmark uses POST
        char value[KV_MAX_VALUE + 1];
        if (extract_value(request + body_at, value) != 0 || strchr(value, '\n')) {
            respond(client_fd, 400, "Bad Request",
                    "{\"error\":\"Expected JSON string without newlines\"}");
            record_operation(method, path + 1, 400);
        } else if (kv_put(path + 1, value) == 0) {
            respond(client_fd, 200, "OK", "{\"message\":\"Value stored\"}");
            record_operation(method, path + 1, 200);
        } else {
            respond(client_fd, 500, "Internal Server Error",
                    "{\"error\":\"Could not store value\"}");
            record_operation(method, path + 1, 500);
        }
    } else if (strcmp(method, "GET") == 0 || strcmp(method, "DELETE") == 0) {
        // These routes are intentionally left for other group members
        respond(client_fd, 501, "Not Implemented",
                "{\"error\":\"Endpoint not implemented yet\"}");
        record_operation(method, path + 1, 501);
    } else {
        respond(client_fd, 405, "Method Not Allowed",
                "{\"error\":\"Unsupported method\"}");
        record_operation(method, path + 1, 405);
    }
    close(client_fd);
    return NULL;
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    if (argc > 2) {
        fprintf(stderr, "Usage: %s [port]\n", argv[0]);
        return 1;
    }
    if (argc == 2) {
        char *end;
        long parsed = strtol(argv[1], &end, 10);
        if (*end || parsed < 1 || parsed > 65535) {
            fprintf(stderr, "Port must be between 1 and 65535.\n");
            return 1;
        }
        port = (int)parsed;
    }

    signal(SIGPIPE, SIG_IGN);
    if (kv_init("data.wal") != 0) {
        perror("Could not initialize persistent storage");
        return 1;
    }
    operations_log = fopen("operations.log", "a");
    if (!operations_log) {
        perror("Could not open operations.log");
        kv_shutdown();
        return 1;
    }

    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) { perror("socket"); return 1; }
    int reuse = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)port);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (bind(listener, (struct sockaddr *)&address, sizeof(address)) != 0 ||
        listen(listener, 64) != 0) {
        perror("bind/listen");
        close(listener);
        fclose(operations_log);
        kv_shutdown();
        return 1;
    }

    printf("C key-value server running on http://127.0.0.1:%d\n", port);
    fflush(stdout);

    for (;;) {
        int client = accept(listener, NULL, NULL);
        if (client < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }
        int *socket_arg = malloc(sizeof(*socket_arg));
        if (!socket_arg) { close(client); continue; }
        *socket_arg = client;
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_client, socket_arg) != 0) {
            free(socket_arg);
            close(client);
            continue;
        }
        pthread_detach(thread);
    }
    // Stop the server with Ctrl-C after tests
    close(listener);
    return 0;
}
