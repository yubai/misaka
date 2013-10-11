#ifndef _MEMPOOL_H_
#define _MEMPOOL_H_

#define MAX_CHUNK 8

struct mempool {
	const char *name;
	int obj_size;
	int nr_chunks;
	size_t chunk_size;
	void *chunk_heads[MAX_CHUNK];
	void *chunks[MAX_CHUNK];
};

void mempool_init(struct mempool *pool, int obj_size, size_t chunk_size);
void mempool_destroy(struct mempool *pool);

static inline void mempool_set_name(struct mempool *pool, const char *name)
{
	pool->name = name;
}

void mempool_free(struct mempool *pool, void *ptr);
void *mempool_alloc(struct mempool *pool);

#endif /* _MEMPOOL_H_ */
