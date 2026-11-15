#define _POSIX_C_SOURCE 200809L
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * FNV-1a is fast and simple, but its high bits don't avalanche well for
 * short, similar strings like "user1", "user2", ... That matters here because
 * ring position is decided by the full 64-bit value. Running the result
 * through the MurmurHash3 64-bit finalizer fixes that for a few extra cycles.
 */
uint64_t hash (const char *data, size_t length) {
    uint64_t h = 1469598103934665603ULL;           // FNV offset basis
    for (size_t i = 0; i < length; i++) {
        h ^= (unsigned char)data[i];
        h *= 1099511628211ULL;                      // FNV prime
    }
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

static int compare_points(const void *a, const void *b) {
    const hash_point *pa = a, *pb = b;
    if (pa->hash < pb->hash) return -1;
    if (pa->hash > pb->hash) return 1;
    // Equal hashes are astronomically unlikely, but break ties by node so
    // the order (and therefore ownership) is deterministic.
    return pa->node - pb->node;
}

/* ---- helpers below assume the caller already holds ring->lock ---- */

// Recompute the sorted point array from the live nodes. Membership changes
// are rare (a few per experiment), so a full rebuild keeps this simple.
// Called with the write lock held.
static int rebuild(hash_ring *ring) {
    size_t needed = (size_t)ring->node_count * (size_t)ring->vnodes;
    hash_point *points = NULL;
    if (needed > 0) {
        points = malloc(needed * sizeof(*points));
        if (!points) return -1;
    }

    size_t used = 0;
    char label[HASH_NAME_LEN + 16];
    for (int node = 0; node < HASH_MAX_NODES; node++) {
        if (!ring->in_use[node]) continue;
        for (int v = 0; v < ring->vnodes; v++) {
            int n = snprintf(label, sizeof(label), "%s#%d", ring->names[node], v);
            points[used].hash = hash (label, (size_t)n);
            points[used].node = node;
            used++;
        }
    }
    qsort(points, used, sizeof(*points), compare_points);

    free(ring->points);
    ring->points = points;
    ring->point_count = used;
    return 0;
}

static int find_locked(const hash_ring *ring, const char *name) {
    for (int i = 0; i < HASH_MAX_NODES; i++) {
        if (ring->in_use[i] && strcmp(ring->names[i], name) == 0) return i;
    }
    return -1;
}

// Binary search for the first point with hash >= hash(key)
static int lookup_locked(const hash_ring *ring, const char *key, size_t length) {
    if (ring->point_count == 0) return -1;
    uint64_t h = hash (key, length);

    size_t lo = 0, hi = ring->point_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (ring->points[mid].hash < h) lo = mid + 1;
        else hi = mid;
    }
    // Past the last point we wrap around to the start of the ring
    if (lo == ring->point_count) lo = 0;
    return ring->points[lo].node;
}

/* ---- public functions: each one takes the lock it needs ---- */

hash_ring *hash_create(int vnodes) {
    hash_ring *ring = calloc(1, sizeof(*ring));
    if (!ring) return NULL;
    if (pthread_rwlock_init(&ring->lock, NULL) != 0) {
        free(ring);
        return NULL;
    }
    ring->vnodes = vnodes > 0 ? vnodes : HASH_DEFAULT_VNODES;
    return ring;
}

hash_ring *hash_clone(hash_ring *ring) {
    hash_ring *copy = calloc(1, sizeof(*copy));
    if (!copy) return NULL;
    if (pthread_rwlock_init(&copy->lock, NULL) != 0) {
        free(copy);
        return NULL;
    }

    // Read lock so the source can't change while we copy it
    pthread_rwlock_rdlock(&ring->lock);
    memcpy(copy->names, ring->names, sizeof(copy->names));
    memcpy(copy->in_use, ring->in_use, sizeof(copy->in_use));
    copy->node_count = ring->node_count;
    copy->vnodes = ring->vnodes;
    copy->point_count = ring->point_count;
    if (ring->point_count > 0) {
        copy->points = malloc(ring->point_count * sizeof(*copy->points));
        if (copy->points) {
            memcpy(copy->points, ring->points,
                   ring->point_count * sizeof(*copy->points));
        }
    }
    pthread_rwlock_unlock(&ring->lock);

    if (copy->point_count > 0 && !copy->points) {
        pthread_rwlock_destroy(&copy->lock);
        free(copy);
        return NULL;
    }
    return copy;
}

void hash_free(hash_ring *ring) {
    if (!ring) return;
    pthread_rwlock_destroy(&ring->lock);
    free(ring->points);
    free(ring);
}

int hash_find(hash_ring *ring, const char *name) {
    pthread_rwlock_rdlock(&ring->lock);
    int slot = find_locked(ring, name);
    pthread_rwlock_unlock(&ring->lock);
    return slot;
}

int hash_node_count(hash_ring *ring) {
    pthread_rwlock_rdlock(&ring->lock);
    int count = ring->node_count;
    pthread_rwlock_unlock(&ring->lock);
    return count;
}

int hash_add(hash_ring *ring, const char *name) {
    if (!name || !*name || strlen(name) >= HASH_NAME_LEN) return -1;

    // The duplicate check and the insert happen under the same write lock,
    // so two threads adding the same node at once can't both succeed
    pthread_rwlock_wrlock(&ring->lock);
    int result = -1;
    if (find_locked(ring, name) < 0) {
        for (int i = 0; i < HASH_DEFAULT_VNODES; i++) {
            if (ring->in_use[i]) continue;
            strcpy(ring->names[i], name);
            ring->in_use[i] = 1;
            ring->node_count++;
            if (rebuild(ring) == 0) {
                result = i;
            } else {
                // Out of memory: undo so the ring stays consistent
                ring->in_use[i] = 0;
                ring->node_count--;
            }
            break;
        }
    }
    pthread_rwlock_unlock(&ring->lock);
    return result;
}

int hash_remove(hash_ring *ring, const char *name) {
    pthread_rwlock_wrlock(&ring->lock);
    int result = -1;
    int slot = find_locked(ring, name);
    if (slot >= 0) {
        ring->in_use[slot] = 0;
        ring->node_count--;
        if (rebuild(ring) == 0) {
            result = 0;
        } else {
            ring->in_use[slot] = 1;
            ring->node_count++;
        }
    }
    pthread_rwlock_unlock(&ring->lock);
    return result;
}

int hash_lookup_name(hash_ring *ring, const char *key, size_t length,
                      char *out, size_t out_size) {
    pthread_rwlock_rdlock(&ring->lock);
    int slot = lookup_locked(ring, key, length);
    // Copy before unlocking: once we let go, a remove could reuse the slot
    if (slot >= 0) snprintf(out, out_size, "%s", ring->names[slot]);
    pthread_rwlock_unlock(&ring->lock);
    return slot >= 0 ? 0 : -1;
}

int hash_lookup(hash_ring *ring, const char *key, size_t length) {
    pthread_rwlock_rdlock(&ring->lock);
    int slot = lookup_locked(ring, key, length);
    pthread_rwlock_unlock(&ring->lock);
    return slot;
}