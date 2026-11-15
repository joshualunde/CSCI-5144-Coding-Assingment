#ifndef HASH_H
#define HASH_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Consistent hash ring.
 *
 * Every physical KV store ("node") is placed on a 64-bit ring many times
 * using virtual nodes. A key belongs to the first virtual node found walking
 * clockwise from the key's hash. Adding or removing a node only moves the keys
 * that land next to that node's points, about 1/N of all keys, instead of
 * reshuffling everything the way `hash % N` would.
 *
 * Thread safety: every public function is safe to call from any number of
 * threads at once. The ring has a read/write lock inside it, so lookups
 * run in parallel and only add/remove take it exclusively. Callers never
 * need their own lock around the ring.
 *
 * Because another thread can change the ring at any time, use
 * chash_lookup_name() to get the owner of a key. It copies the store's name
 * while still holding the lock, so the answer can't be changed halfway
 * through reading it.
 *
 * Like the rest of the project, any .c file that includes this header needs
 * `#define _POSIX_C_SOURCE 200809L` before its first #include (otherwise
 * -std=c11 hides pthread_rwlock_t).
 */

#define HASH_MAX_NODES 16
#define HASH_NAME_LEN 128          // "host:port" fits easily

// Measured with 300k keys on 3 nodes: 160 vnodes -> 6.9% worst imbalance,
// 1000 -> 2.6%. Lookups are a binary search, so 3000 points is still only
// ~12 comparisons per key and the extra memory (~48 KB) doesn't matter.
#define HASH_DEFAULT_VNODES 1000

typedef struct hash_point {
    uint64_t hash;
    int node;                       // index into chash_ring.names
} hash_point;

// Treat the fields as private and go through the functions below, which
// take the lock for you.
typedef struct hash_ring {
    pthread_rwlock_t lock;          // readers: lookups, writers: add/remove
    char names[HASH_MAX_NODES][HASH_NAME_LEN];
    int in_use[HASH_MAX_NODES];    // 1 if the slot holds a live node
    int node_count;
    int vnodes;                     // virtual nodes per physical node
    hash_point *points;            // sorted by hash
    size_t point_count;
} hash_ring;

/* 64-bit hash used for both keys and virtual node labels. */
uint64_t hash_hash(const char *data, size_t length);

/* Create an empty ring. vnodes <= 0 picks CHASH_DEFAULT_VNODES. */
hash_ring *hash_create(int vnodes);

/* Deep copy (with its own lock). Handy for remembering the "before" ring
   while keys are moved after a membership change. */
hash_ring *hash_clone(hash_ring *ring);

/* Only call once no other thread is using the ring. */
void hash_free(hash_ring *ring);

/* Add a node. Returns its slot index, or -1 if it is full or a duplicate. */
int hash_add(hash_ring *ring, const char *name);

/* Remove a node by name. Returns 0 on success, -1 if it is not on the ring. */
int hash_remove(hash_ring *ring, const char *name);

/* Slot index of a node by name, or -1. */
int hash_find(hash_ring *ring, const char *name);

/* Number of nodes currently on the ring. */
int hash_node_count(hash_ring *ring);

/*
 * Copy the name of the node that owns this key into `out` (size `out_size`).
 * Returns 0, or -1 if the ring is empty. This is the lookup to use when
 * other threads may be adding or removing nodes.
 */
int hash_lookup_name(hash_ring *ring, const char *key, size_t length,
                      char *out, size_t out_size);

/*
 * Slot index of the node that owns this key, or -1 if the ring is empty.
 * The index is only meaningful while membership isn't changing; with
 * concurrent add/remove, prefer chash_lookup_name().
 */
int hash_lookup(hash_ring *ring, const char *key, size_t length);

#endif
