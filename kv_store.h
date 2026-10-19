#ifndef KV_STORE_H
#define KV_STORE_H

#define KV_MAX_KEY 255
#define KV_MAX_VALUE 4096

/* Start the store and load previously saved values. */
int kv_init(const char *filename);

/* Add a new key-value pair or update an existing one. */
int kv_put(const char *key, const char *value);

/* Free resources when the server stops. */
void kv_shutdown(void);

#endif
