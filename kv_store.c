#define _POSIX_C_SOURCE 200809L
#include "kv_store.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Each node contains one key-value pair
typedef struct Entry {
    char *key;
    char *value;
    struct Entry *next;
} Entry;


static Entry *store = NULL;        // Head of linked list 
static FILE *data_file = NULL;     // File used for persistence
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Return entry for this key, or NULL if it does not exist
static Entry *find_entry(const char *key) {
    for (Entry *entry = store; entry != NULL; entry = entry->next) {
        if (strcmp(entry->key, key) == 0) {
            return entry;
        }
    }
    return NULL;
}

// Change memory only
// Used when loading saved PUTs at startup
static int put_in_memory(const char *key, const char *value) {
    Entry *entry = find_entry(key);
    char *value_copy = strdup(value);
    if (!value_copy) return -1;

    if (entry != NULL) {
        free(entry->value);
        entry->value = value_copy;
        return 0;
    }

    Entry *new_entry = malloc(sizeof(*new_entry));
    if (!new_entry) {
        free(value_copy);
        return -1;
    }
    new_entry->key = strdup(key);
    if (!new_entry->key) {
        free(new_entry);
        free(value_copy);
        return -1;
    }
    new_entry->value = value_copy;
    new_entry->next = store;
    store = new_entry;
    return 0;
}

// Load previous PUT operations from disk when server starts
int kv_init(const char *filename) {
    if (!filename || data_file) return -1;
    data_file = fopen(filename, "a+");
    if (!data_file) return -1;
    rewind(data_file);

    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    int result = 0;

    // File records look like: P<TAB>key<TAB>value<NEWLINE>
    while ((length = getline(&line, &capacity, data_file)) != -1) {
        if (length < 4 || line[length - 1] != '\n' ||
            strncmp(line, "P\t", 2) != 0) {
            result = -1;
            break;
        }
        line[length - 1] = '\0';
        char *key = line + 2;
        char *separator = strchr(key, '\t');
        if (!separator) {
            result = -1;
            break;
        }
        *separator = '\0';
        char *value = separator + 1;
        if (!*key || strlen(key) > KV_MAX_KEY ||
            strlen(value) > KV_MAX_VALUE ||
            put_in_memory(key, value) != 0) {
            result = -1;
            break;
        }
    }
    if (ferror(data_file)) result = -1;
    free(line);
    if (result != 0) {
        kv_shutdown();
        return -1;
    }
    return 0;
}

// Store new key or replace its existing value
int kv_put(const char *key, const char *value) {
    if (!key || !value || !*key || strlen(key) > KV_MAX_KEY ||
        strlen(value) > KV_MAX_VALUE ||
        strchr(key, '\t') || strchr(key, '\n') ||
        strchr(value, '\n')) {
        return -1;
    }

    pthread_mutex_lock(&mutex);
    if (!data_file) {
        pthread_mutex_unlock(&mutex);
        return -1;
    }

    Entry *entry = find_entry(key);
    char *value_copy = strdup(value);
    Entry *new_entry = entry ? NULL : malloc(sizeof(*new_entry));
    char *key_copy = entry ? NULL : strdup(key);
    if (!value_copy || (!entry && (!new_entry || !key_copy))) {
        free(value_copy);
        free(new_entry);
        free(key_copy);
        pthread_mutex_unlock(&mutex);
        return -1;
    }

    // Save the PUT before changing the in-memory store
    if (fprintf(data_file, "P\t%s\t%s\n", key, value) < 0 ||
        fflush(data_file) != 0) {
        free(value_copy);
        free(new_entry);
        free(key_copy);
        pthread_mutex_unlock(&mutex);
        return -1;
    }

    if (entry) {
        free(entry->value);
        entry->value = value_copy;
    } else {
        new_entry->key = key_copy;
        new_entry->value = value_copy;
        new_entry->next = store;
        store = new_entry;
    }
    pthread_mutex_unlock(&mutex);
    return 0;
}

// Free memory and close the persistence file
void kv_shutdown(void) {
    pthread_mutex_lock(&mutex);
    if (data_file) fclose(data_file);
    data_file = NULL;
    while (store) {
        Entry *next = store->next;
        free(store->key);
        free(store->value);
        free(store);
        store = next;
    }
    pthread_mutex_unlock(&mutex);
}
