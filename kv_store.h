#ifndef KV_STORE_H
#define KV_STORE_H

#define KV_MAX_KEY 255
#define KV_MAX_VALUE 4096

/* Start the store and load previously saved values. */
int kv_init(const char *filename);

/* Add a new key-value pair or update an existing one. */
int kv_put(const char *key, const char *value);

//get key-value pair
int kv_get(const char *key,  char *value);

/* Remove a key. Returns 0 if deleted, 1 if the key does not exist, -1 on error. */
int kv_delete(const char *key);

/* Free resources when the server stops. */
void kv_shutdown(void);

#endif
