#include <stdio.h>
#include <stdlib.h>

#include "misc.h"
#include "mempool.h"

static void init_chunk(void *chunk, int obj_size, int chunk_size)
{
	int i = 0;
	for (i = 0; i < chunk_size / obj_size; i++) {
		unsigned long *ptr = chunk + i * obj_size;
		if (i == chunk_size / obj_size - 1) {
			*ptr = 0;
		} else {
			*ptr = (unsigned long) (chunk
						+ i * obj_size  + obj_size);
		}
	}
}

void mempool_init(struct mempool *pool, int obj_size, size_t chunk_size)
{
	pool->name = NULL;
	pool->obj_size = obj_size;
	pool->nr_chunks = 1;
	pool->chunks[0] = malloc(chunk_size);
	pool->chunk_heads[0] = pool->chunks[0];
	pool->chunk_size = chunk_size;
	init_chunk(pool->chunks[0], obj_size, pool->chunk_size);
}

void mempool_destroy(struct mempool *pool)
{
	int i = 0;
	for (i = 0; i < pool->nr_chunks; i++) {
		free(pool->chunks[i]);
	}
	pool->nr_chunks = 0;
}

void *mempool_alloc(struct mempool *pool)
{
	int i = 0;
	for (i = 0; i < pool->nr_chunks; i++) {
		if (pool->chunk_heads[i] == NULL)
			continue;
		unsigned long *ptr = pool->chunk_heads[i];
		PREFETCH(ptr);
		pool->chunk_heads[i] = (void *) *ptr;
		return ptr;
	}
	if (likely(pool->nr_chunks < MAX_CHUNK - 1)) {
		fprintf(stderr, "%s mempool enlarge\n", pool->name);
		pool->chunks[pool->nr_chunks] = malloc(pool->chunk_size);
		PREFETCH(pool->chunks[pool->nr_chunks]);
		init_chunk(pool->chunks[pool->nr_chunks], pool->obj_size,
			   pool->chunk_size);
		pool->chunk_heads[pool->nr_chunks] = (void *)
			* (unsigned long *) pool->chunks[pool->nr_chunks];
		pool->nr_chunks++;
		return pool->chunks[pool->nr_chunks - 1];
	}
	fprintf(stderr, "%s mempool allocation failed!\n",
		pool->name);
	return NULL;
}

void mempool_free(struct mempool *pool, void *ptr)
{
	int i = 0;
	for (i = 0; i < pool->nr_chunks; i++) {
		if (ptr >= pool->chunks[i]
		    && ptr < (void*) ((u8*) (pool->chunks[i])
				      + pool->chunk_size)) {
			* (unsigned long *) ptr =
				(unsigned long) pool->chunk_heads[i];
			pool->chunk_heads[i] = ptr;
			return;
		}
	}
	fprintf(stderr, "error in mempool_free(), mempool %s"
		"cannot free ptr outside the pool\n", pool->name);
}
