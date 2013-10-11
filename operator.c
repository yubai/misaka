#include <limits.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include "btree.h"
#include "operator.h"

int word_compare(const void *p, const void *q)
{
	return strncmp(p, q, sizeof(word_t));
}

unsigned long word_hash(const void *key)
{
	const u64 *p = key;
	// static unsigned long h = 2166136261;
	// int i = 0;
	// for (i = 0; i < sizeof(word_t); i++) {
	//  	if (p[i] == 0)
	//  		break;
	//  	h = (h * 16777619) ^ p[i];
	// }
	// return h;
	return p[0] ^ p[1] ^ p[2];
}

int refcnt_operator_compare(const void *p, const void *q)
{
	const struct refcnt_operator_key *a = p;
	const struct refcnt_operator_key *b = q;

	if (a->refcnt > b->refcnt) return -1;
	else if (a->refcnt < b->refcnt) return 1;

	return (unsigned long) a->operator - (unsigned long) b->operator;
}

int uint_compare(const void *p, const void *q)
{
	const unsigned int *a = p;
	const unsigned int *b = q;
	return (*a) - (*b);
}

unsigned long uint_hash(const void *p)
{
	const unsigned int *a = p;
	return *a;
}

int ptr_compare(const void *p, const void *q)
{
	const unsigned long *a = p;
	const unsigned long *b = q;

	// return (*a) - (*b);
	if (*a < *b)
		return -1;
	else if (*a > *b)
		return 1;
	return 0;
}

int query_dedup_compare(const void *p, const void *q)
{
	const struct query_struct *a = *(const struct query_struct **) p;
	const struct query_struct *b = *(const struct query_struct **) q;
	int i = 0;

	if (a == NULL) return -1;
	if (b == NULL) return 1;

	if (a->mt != b->mt)
		return a->mt - b->mt;

	if (a->threshold != b->threshold)
		return a->threshold - b->threshold;

	if (a->ops_len != b->ops_len)
		return a->ops_len - b->ops_len;
	for (i = 0; i < a->ops_len; i++) {
		if (a->ops[i] > b->ops[i]) {
			return 1;
		} else if (a->ops[i] < b->ops[i]) {
			return -1;
		}
	}
	return 0;
}

unsigned long query_dedup_hash(const void *p)
{
	const struct query_struct *a = *(const struct query_struct **) p;
	int i = 0;
	unsigned long base = (a->ops_len << 4) | (a->mt << 2) | (a->threshold);
	for (i = 0; i < a->ops_len; i++) {
		base ^= (((unsigned long) a->ops[i]) << 7);
	}
	return base;
}

void plan_init(struct plan *plan)
{
	plan->op_rank = btree_mem_new(sizeof(struct refcnt_operator_key), 1,
				      refcnt_operator_compare);
	plan->query_table = btree_mem_new(sizeof(int),
					  sizeof(struct query_struct *),
					  uint_compare);
	plan->query_dedup = hashtable_new(sizeof(struct query_struct *),
					  sizeof(struct query_struct *),
					  DEDUP_TABLE_CAP,
					  query_dedup_hash,
					  query_dedup_compare);
	// plan->query_table = hashtable_new(sizeof(int),
	//  				  sizeof(struct query_struct*),
	//  				  10 << 20,
	//  				  uint_hash,
	//  				  uint_compare);
	plan->word_index = btree_mem_new(sizeof(word_t),
					 sizeof(struct operator*),
					 word_compare);
	// plan->word_index = hashtable_new(sizeof(word_t),
	//  				 sizeof(struct operator*),
	//  				 256 << 20,
	//  				 word_hash,
	//  				 word_compare);
	int i = 0;
	for (i = 0; i < NR_SHADOW; i++) {
		mempool_init(&plan->shadow_mempool[i], BTREE_NODE_SIZE,
			     SHADOW_MEMPOOL_SIZE);
		plan->bitmap_mem[i] = malloc(SHADOW_BITMAP_NR_BITS >> 3);
		bitmap_reset(plan->bitmap_mem[i], SHADOW_BITMAP_NR_BITS);
	}
	mempool_init(&plan->query_pool, sizeof(struct query_struct),
		     QUERY_MEMPOOL_SIZE);
	mempool_set_name(&plan->query_pool, "query-pool");
	mempool_init(&plan->op_pool, sizeof(struct operator),
		     OP_MEMPOOL_SIZE);
	mempool_set_name(&plan->op_pool, "operator-pool");
	list_init(&plan->dirty_ops);
}

static void free_all_queries(struct plan *plan)
{
	void *key_ptr = NULL;
	void *value_ptr = NULL;
	unsigned int qid = 0;

	while (plan->query_table->sb.size > 0) {
		btree_first_pair(plan->query_table, &key_ptr, &value_ptr);
		qid = *(unsigned int *) key_ptr;
		plan_del_query(plan, qid);
	}
}

void plan_destroy(struct plan *plan)
{
	free_all_queries(plan);
	btree_mem_destroy(plan->op_rank);
	btree_mem_destroy(plan->query_table);
	hashtable_destroy(plan->query_dedup);
	btree_mem_destroy(plan->word_index);
	int i = 0;
	for (i = 0; i < NR_SHADOW; i++) {
		mempool_destroy(&plan->shadow_mempool[i]);
	}
	mempool_destroy(&plan->query_pool);
	mempool_destroy(&plan->op_pool);
}

static int get_next_word(const char *str, int idx, word_t word, int *len)
{
	int start = idx, end = idx + 1;
	memset(word, 0, sizeof(word_t));
	if (str[start] == 0)
		return -1;
	while (str[start] == ' ') {
		start++;
		if (str[start] == 0)
			return -1;
	}
	end = start + 1;
	while (str[end] != ' ' && str[end] != 0) end++;
	memcpy(word, str + start, end - start);
	*len = end - start;
	return end;
}

static struct operator* operator_new(struct plan *plan, word_t word, int len)
{
	struct operator *op = mempool_alloc(&plan->op_pool);
	int i = 0, j = 0;
	memcpy(op->word, word, sizeof(word_t));
	op->len = len;
	op->refcnt = 0;
	memset(&op->dirty_head, 0, sizeof(struct list_head));

	for (i = 0; i < 3; i++) {
		for (j = 0; j < 4; j++) {
			list_init(&op->query_refs[i][j]);
			op->nr_refs[i][j] = 0;
		}
	}
	/* set the shadows as uninitialized */
	for (i = 0; i < NR_SHADOW; i++) {
		op->shadow[i].refcnt = -1;
	}
	return op;
}

static void operator_destroy(struct plan *plan, struct operator *op)
{
	int i = 0;
	for (i = 0; i < NR_SHADOW; i++) {
		assert(op->shadow[i].refcnt == -1);
	}
	mempool_free(&plan->op_pool, op);
}

void plan_add_query(struct plan *plan, unsigned int qid, const char *str,
		    MatchType mt, unsigned int threshold)
{
	int idx = 0;
	int i = 0;
	int ops_len = 0;
	int len[MAX_QUERY_WORDS + 1];
	word_t words[MAX_QUERY_WORDS + 1];
	/* lookup in the word_index */
	struct operator *op = NULL;
	struct operator **val = NULL;
	struct query_struct* qstruct = mempool_alloc(&plan->query_pool);
	u8 dummy = 0;
	int dedup_flag = 1;

	if (threshold == 0) mt = MT_EXACT_MATCH;

	qstruct->qid = qid;
	qstruct->mt = mt;
	qstruct->threshold = threshold;
	qstruct->ops_len = 0;

	for (i = 0; i < NR_SHADOW; i++) {
		qstruct->shadow[i].active = 0;
		list_init(&qstruct->shadow[i].head);
	}

	while (1) {
		idx = get_next_word(str, idx, words[qstruct->ops_len],
				    &len[qstruct->ops_len]);
		if (idx < 0)
			break;
		qstruct->ops_len++;
	}

	// printf("%s qid %u %p threshold %d size %d\n", __FUNCTION__, qid,
	//        qstruct, threshold, qstruct->ops_len);

	for (i = 0; i < qstruct->ops_len; i++) {
		val = btree_search(plan->word_index, words[i]);
		if (val == NULL) {
			op = operator_new(plan, words[i], len[i]);
			btree_insert(plan->word_index, words[i], &op);
			dedup_flag = 0;
		} else {
			op = *val;
		}
		qstruct->ops[i] = op;
	}
	qsort(qstruct->ops, qstruct->ops_len, sizeof(struct operator *),
	      ptr_compare);
	/* remove duplicate words */
	ops_len = 1;
	for (i = 1; i < qstruct->ops_len; i++) {
		if (qstruct->ops[i] != qstruct->ops[i - 1]) {
			qstruct->ops[ops_len] = qstruct->ops[i];
			ops_len++;
		}
	}
	qstruct->ops_len = ops_len;
	if (dedup_flag) {
		/* try to dedup this qstruct! */
		struct query_struct *dup = NULL;
		struct query_struct **dup_val =
			hashtable_search(plan->query_dedup, &qstruct);
		if (dup_val == NULL)
			goto nodup;

		dup = *dup_val;
		if (dup->alias == NULL)
			dup->alias = btree_mem_new(sizeof(unsigned int), 1,
						   uint_compare);
		// printf("aliasing %u to %u %p\n", qid, dup->qid, dup);
		if (qid == dup->qid) {
			abort();
		}
		btree_insert(dup->alias, &qid, &dummy);
		btree_insert(plan->query_table, &qid, &dup);
		goto free;
	}
nodup:
	plan->tot_words += qstruct->ops_len;
	for (i = 0; i < qstruct->ops_len; i++) {
		struct operator *op = qstruct->ops[i];
		if (op->dirty_head.next == NULL) {
			if (op->refcnt > 0) {
				struct refcnt_operator_key refkey = {
					op->refcnt, op
				};
				btree_delete(plan->op_rank, &refkey);
			}
			list_add(&op->dirty_head, &plan->dirty_ops);
		}
		op->refcnt++;
		qstruct->ref_heads[i].pos = i;
		list_add(&qstruct->ref_heads[i].head,
			 &op->query_refs[mt][threshold]);
		op->nr_refs[mt][threshold]++;
	}
	/* used for dedup, but lazily initialized */
	qstruct->alias = NULL;
	btree_insert(plan->query_table, &qid, &qstruct);
	// printf("inserting unique query %u %p\n", qstruct->qid, qstruct);
	hashtable_insert(plan->query_dedup, &qstruct, &qstruct);
	return;
free:
	mempool_free(&plan->query_pool, qstruct);
	return;
}

void plan_del_query(struct plan *plan, unsigned int qid)
{
	// struct query_struct **val = btree_search(plan->query_mask, &qid);
	struct query_struct **val = btree_search(plan->query_table, &qid);
	struct query_struct *qstruct = NULL;
	struct refcnt_operator_key refkey;
	int i = 0;

	if (unlikely(val == NULL)) {
		fprintf(stderr, "error, cannot find %u\n", qid);
		return;
	}
	// printf("%s qid %u\n", __FUNCTION__, qid);
	qstruct = *val;

	btree_delete(plan->query_table, &qid);
	if (qstruct->qid != qid) {
		/* remove it from alias */
		// printf("un-alias %u to %u %p\n", qid, qstruct->qid, qstruct);
		btree_delete(qstruct->alias, &qid);
		return;
	}

	if (qstruct->alias != NULL && qstruct->alias->sb.size > 0) {
		/* select a new primary qid for this qstruct */
		void *key = NULL;
		void *dummy = NULL;
		unsigned int new_qid = 0;
		btree_last_pair(qstruct->alias, &key, &dummy);
		new_qid = *(unsigned int *) key;
		qstruct->qid = new_qid;
		btree_delete(qstruct->alias, &new_qid);
		// printf("qid of %p now is %u\n", qstruct, qstruct->qid);
		return;
	}

	hashtable_delete(plan->query_dedup, &qstruct);

	for (i = 0; i < qstruct->ops_len; i++) {
		struct operator *op = qstruct->ops[i];
		if (op->dirty_head.next == NULL) {
			refkey.refcnt = op->refcnt;
			refkey.operator = op;
			btree_delete(plan->op_rank, &refkey);
			list_add(&op->dirty_head, &plan->dirty_ops);
		}
		op->refcnt--;
		if (op->refcnt == 0) {
			btree_delete(plan->word_index, op->word);
		} else {
			// printf("remove %p from %s %p level %d thre %d tree %p\n",
			//        qstruct, op->word, op, qstruct->mt,
			//        qstruct->threshold,
			//        op->query_refs[qstruct->mt][qstruct->threshold]);
			list_del(&qstruct->ref_heads[i].head);
			op->nr_refs[qstruct->mt][qstruct->threshold]--;
		}
	}
	plan->tot_words -= qstruct->ops_len;
	if (qstruct->alias)
		btree_mem_destroy(qstruct->alias);
	// printf("free %d %p\n", qid, qstruct);
	mempool_free(&plan->query_pool, qstruct);
}

void plan_rebuild(struct plan *plan)
{
	struct list_head *ent = plan->dirty_ops.next;
	struct refcnt_operator_key refkey;
	u8 dummy = 0;

	while (ent != &plan->dirty_ops) {
		struct operator *op =
			container_of(ent, struct operator, dirty_head);
		ent = op->dirty_head.next;
		list_del(&op->dirty_head);
		memset(&op->dirty_head, 0, sizeof(struct list_head));
		if (op->refcnt > 0) {
			refkey.operator = op;
			refkey.refcnt = op->refcnt;
			btree_insert(plan->op_rank, &refkey, &dummy);
		} else {
			operator_destroy(plan, op);
		}
	}
}

void operator_create_shadow(struct operator *op, int idx)
{
	op->shadow[idx].refcnt = op->refcnt;
	memset(&op->shadow[idx].dirty_ops, 0, sizeof(struct list_head));
	memcpy(op->shadow[idx].nr_refs, op->nr_refs, 12 * sizeof(int));
	list_init(&op->shadow[idx].zombie_list);

	op->shadow[idx].ctx.level = 0;
	op->shadow[idx].ctx.need_hamming = 0;
	op->shadow[idx].ctx.min_distance = MAX_DIST + 1;
}

void operator_destroy_shadow(struct operator *op, int idx)
{
	op->shadow[idx].refcnt = -1;
	list_init(&op->shadow[idx].zombie_list);
}
