#include <string.h>
#include "misc.h"

struct queue_chunk {
	struct list_head head;
	unsigned char data[];
};

void queue_init(struct queue *q, int obj_size, int chunk_capacity)
{
	q->nchunks = q->nobjs = 0;
	q->obj_size = obj_size;
	q->chunk_capacity = chunk_capacity;

	q->nobjs_per_chunk =
		(chunk_capacity - sizeof(struct queue_chunk)) / obj_size;
	list_init(&q->chunk_list);
}

void queue_append(struct queue *q, void *obj)
{
	int idx = q->nobjs % q->nobjs_per_chunk;
	struct queue_chunk *chunk = NULL;

	chunk = container_of(q->chunk_list.prev, struct queue_chunk, head);
	if (idx == 0) {
		chunk = malloc(q->chunk_capacity);
		list_add_tail(&chunk->head, &q->chunk_list);
		q->nchunks++;
	}
	memcpy(chunk->data + idx * q->obj_size, obj, q->obj_size);
	q->nobjs++;
}

void queue_destroy(struct queue *q)
{
	struct list_head *cur = NULL;
	struct list_head *next = q->chunk_list.next;
	while (next != &q->chunk_list) {
		cur = next;
		next = cur->next;
		/* free up the whole chunk */
		free(cur);
	}
}
