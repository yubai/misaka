#ifndef _OPERATOR_H_
#define _OPERATOR_H_

#include <pthread.h>

#include "misc.h"
#include "core.h"
#include "btree.h"
#include "hashtable.h"
#include "mempool.h"

/* number of threads */
#define NR_SHADOW 12

/* query table hashtable size */
#define QUERY_TABLE_BUCKET (10 << 22)

/* mempool per shadow/thread */
#define SHADOW_MEMPOOL_SIZE (1 << 20)

/* query id bitmap, maxium size of qid actually */
#define MAX_QID (1 << 19)
#define SHADOW_BITMAP_NR_BITS MAX_QID

#define DEDUP_TABLE_CAP (1 << 22)

/* reserve space for query_struct mempool */
#define QUERY_MEMPOOL_SIZE (4 << 22)

/* reserve space for operator mempool */
#define OP_MEMPOOL_SIZE (10 << 22)

struct query_ref_head {
	struct list_head head;
	int pos; /* position of this ref head */
};

struct query_shadow {
	struct list_head head;
	int active;
};

struct query_struct {
	struct query_shadow shadow[NR_SHADOW];
	unsigned int qid;
	struct btree *alias;
	MatchType mt;
	unsigned int threshold;

	u8 ops_len;
	struct operator *ops[MAX_QUERY_WORDS];
	struct query_ref_head ref_heads[MAX_QUERY_WORDS];
};

struct operator_shadow {
	struct list_head zombie_list;
	struct list_head dirty_ops;
	int refcnt;
	int nr_refs[3][4];
	/* current context state for preemptive scheduling */
	struct {
		int need_hamming;
		int min_distance;
		int level;
	} ctx;
};

struct operator {
	struct operator_shadow shadow[NR_SHADOW];
	int refcnt;
	word_t word;
	int len;
	struct list_head dirty_head; /* dirty list to avoid double insertion
				      * on constructing query plan */
	int nr_refs[3][4];
	struct list_head query_refs[3][4]; /* references to querys */
};

struct refcnt_operator_key {
	int refcnt;
	struct operator *operator;
};

/* query plan index */
struct plan {
	/* global op_rank and query_mask tree. they're just mem-tree, no cow */
	struct btree *op_rank;
	// struct btree *query_mask;
	struct btree *query_table;
	struct hashtable *query_dedup;
	struct mempool query_pool;
	struct mempool op_pool;

	/* mem-tree, `word->struct operator` */
	struct btree *word_index;
	/* some stat counter */
	unsigned long tot_words;

	/* mempools for shadows */
	struct mempool shadow_mempool[NR_SHADOW];
	u8 *bitmap_mem[NR_SHADOW];
	struct list_head dirty_ops;
};

void plan_init(struct plan *plan);
void plan_destroy(struct plan *plan);
/* plan is a singleton */
struct plan *plan_get();

void operator_create_shadow(struct operator *op, int idx);
void operator_destroy_shadow(struct operator *op, int idx);

static inline int operator_shadow_is_zombie(struct operator_shadow *shadow)
{
	return !(shadow->zombie_list.prev == &shadow->zombie_list
		 && shadow->zombie_list.next == &shadow->zombie_list);
}

static inline int operator_is_shadow_active(struct operator *op, int idx)
{
	return op->shadow[idx].refcnt != -1;
}

static inline
struct operator_shadow *operator_shadow_raw(struct operator *op, int idx)
{
	return &op->shadow[idx];
}

static inline
struct operator_shadow *operator_shadow(struct operator *op, int idx)
{
	if (!operator_is_shadow_active(op, idx))
		operator_create_shadow(op, idx);
	return &op->shadow[idx];
}

static inline void query_create_shadow(struct query_struct *q, int idx)
{
	q->shadow[idx].active = 1;
	list_init(&q->shadow[idx].head);
}

static inline void query_destroy_shadow(struct query_struct *q, int idx)
{
	q->shadow[idx].active = 0;
	list_init(&q->shadow[idx].head);
}

static inline int query_is_shadow_active(struct query_struct *q, int idx)
{
	return q->shadow[idx].active;
}

static inline struct query_shadow *query_shadow_raw(struct query_struct *q,
						    int idx)
{
	return &q->shadow[idx];
}

static inline struct query_shadow *query_shadow(struct query_struct *q, int idx)
{
	if (!q->shadow[idx].active)
		query_create_shadow(q, idx);
	return &q->shadow[idx];
}

void plan_add_query(struct plan *plan, unsigned int qid, const char *str,
		    MatchType mt, unsigned int threshold);
void plan_del_query(struct plan *plan, unsigned int qid);
void plan_rebuild(struct plan *plan);

/* misc compare functions */
int uint_compare(const void *p, const void *q);
int ptr_compare(const void *p, const void *q);
int word_compare(const void *p, const void *q);
int refcnt_operator_compare(const void *p, const void *q);

#endif /* _OPERATOR_H_ */
