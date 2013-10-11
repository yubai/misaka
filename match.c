#include <limits.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "btree.h"
#include "match.h"
#include "core.h"

/**
 * create a document_match object -- take a snapshot of the current plan.
 */
void match_init(struct document_match *match, DocID doc_id, const char *doc_str)
{
	/* op_rank is highly contented, needs to be created in a threaded
	 * environment */
	match->op_rank = NULL;
	// match->query_mask = btree_cow_new(plan->query_mask);
	// strncpy(match->doc_str, doc_str, MAX_DOC_LENGTH);
	memcpy(match->doc_str, doc_str, strlen(doc_str) + 1);
	/* docent_new is slow, create it in a threaded environment */
	match->docent = NULL;
	match->doc_id = doc_id;
	match->shadow_id = -1;
}

/**
 * pick the first operator, which have the highest reference count.
 * if this operation is negated, then a large amount of queries are omitted.
 */
static struct operator *match_pick_operator(struct document_match *match)
{
	void *key = NULL;
	struct operator *op;
	void *value = NULL;
	struct refcnt_operator_key *op_key = NULL;
	btree_first_pair(match->op_rank, &key, &value);
	op_key = key;
	op = op_key->operator;
	assert(op_key->refcnt == operator_shadow(op, match->shadow_id)->refcnt);
	// printf("picking %p %s\n", op, op->word);
	return op;
}

static void match_remove_operator(struct document_match *match,
				  struct operator *op)
{
	struct refcnt_operator_key key = {
		operator_shadow_raw(op, match->shadow_id)->refcnt,
		op,
	};
	btree_delete(match->op_rank, &key);
}

static int match_exclude_query(struct document_match *match,
			       struct match_result *result,
			       struct query_struct *qstruct,
			       struct operator *src_op)
{
	struct operator *op = NULL;
	struct operator_shadow *shadow = NULL;
	struct refcnt_operator_key key;
	int i = 0;

	for (i = 0; i < qstruct->ops_len; i++) {
		op = qstruct->ops[i];
		if (op == src_op)
			continue;
		shadow = operator_shadow(op, match->shadow_id);
		if (operator_shadow_is_zombie(shadow))
			continue;
		if (qstruct->mt < shadow->ctx.level)
			continue;
		/* unref this operator */
		if (shadow->dirty_ops.next == NULL) {
			key.refcnt = shadow->refcnt;
			key.operator = op;
			btree_delete(match->op_rank, &key);
			list_add(&shadow->dirty_ops, &result->dirty_head);
		}
		// printf("removing %d(%p) from op %p(%s) shadow refcnt %d\n",
		//        qstruct->qid, qstruct, op, op->word, shadow->refcnt);
		shadow->refcnt--;
		shadow->nr_refs[qstruct->mt][qstruct->threshold]--;
		if (shadow->refcnt == 0) {
			list_del(&shadow->dirty_ops);
			operator_destroy_shadow(op, match->shadow_id);
		}
	}
	result->range.min_qid = result->range.min_qid < qstruct->qid ?
		result->range.min_qid : qstruct->qid;
	result->range.max_qid = result->range.max_qid > qstruct->qid ?
		result->range.max_qid : qstruct->qid;
	bitmap_set_bit(match->bitmap, qstruct->qid);
	result->nr_queries--;
	if (qstruct->alias) result->nr_queries -= qstruct->alias->sb.size;
	if (query_is_shadow_active(qstruct, match->shadow_id)) {
		/* remove it from the match_list */
		list_del(&query_shadow_raw(qstruct, match->shadow_id)->head);
		query_destroy_shadow(qstruct, match->shadow_id);
	}
	return 0;
}

static int match_include_query(struct document_match *match,
			       struct match_result *result,
			       struct query_struct *qstruct,
			       struct operator *src_op)
{
	struct list_head *shadow_head =
		&query_shadow(qstruct, match->shadow_id)->head;
	if (shadow_head->next == shadow_head
	    && shadow_head->prev == shadow_head) {
		list_add(shadow_head, &result->match_head);
		// printf("include query %d, nr %d\n", qstruct->qid,
		//  	result->nr_queries);
	}
	return 0;
}

struct query_refs_accessor {
	struct operator *op;
	struct document_match *match;
	struct match_result *result;
	int min_dis;
	int phase; /* release or add back or match */
};

static int operator_query_refs_key_callback(struct list_head *node, void* ptr)
{
	struct query_refs_accessor *refs = ptr;
	int pos = 0;
	struct query_struct *qstruct = NULL;

	pos = *(int *) ((u8 *) node + sizeof(struct list_head));
	qstruct = container_of(node, struct query_struct,
			       ref_heads[pos].head);
	if (bitmap_is_bit_set(refs->match->bitmap, qstruct->qid)) {
		/* this query has been negated! */
		return 0;
	}
	// printf("visiting query %u[%d](%p) on op %s(%p)\n", qstruct->qid, pos,
	//        qstruct, refs->op->word, refs->op);
	switch (refs->phase) {
	case 0:
		match_exclude_query(refs->match, refs->result, qstruct,
				    refs->op);
		break;
	case 2:
		match_include_query(refs->match, refs->result, qstruct,
				    refs->op);
		break;
	}
	return 0;
}

/**
 * do we need to reduce the cur_dis to this new level?
 */
static int need_to_reduce_distance(struct document_match *match, int cur_dis,
				   struct operator_shadow *shadow, int level,
				   int *min_threshold)
{
	for (*min_threshold = 0; *min_threshold < cur_dis; (*min_threshold)++) {
		if (shadow->nr_refs[level][*min_threshold] != 0) {
			return 1;
		}
	}
	/* all thresholds are larger or equal than the current min-
	 * distance, this means this level of distance reduction
	 * is useless because no query will be negated!
	 */
	return 0;
}

/**
 * we notice that the distance type are level-by-level. topest level are the
 * largest distance (EXACT_MATCH). they're strictly ordered, and we have
 * D0 >= D1 >= D2. more interestingly, the cost of caculating these distance
 * is also Cost0 >= Cost1 >= Cost2.
 *
 * also, if D0 = oo, then 1 <= D2 <= D1 < oo .this one could be used to reduce
 * a lot of distance calls. so D0 is always evaluated, and it's very cheap
 *
 * we will caculate minimum distance for this document each time, but if the
 * minmum distance at the previous level is able to match all the queries in
 * this level, we won't waste our time to calculate a new minimum distance.
 */
static int match_exec_round(struct document_match *match, struct operator *op,
			    struct operator_shadow *shadow,
			    struct match_result *result)
{
	int i = 0;
	u8 dummy = 0;
	struct query_refs_accessor access_struct;
	struct list_head *entry = NULL;
	struct list_head (*query_refs)[4] = op->query_refs;
	int lower_bound = 0;

	access_struct.match = match;
	access_struct.result = result;
	access_struct.op = op;

	result->dis_calc_cnt[shadow->ctx.level]++;
	if (shadow->ctx.need_hamming) {
		/* calculate hamming distance */
		shadow->ctx.min_distance = match_min_dist(match, 1,
							  op->word,
							  op->len, 1,
							  MAX_DIST + 1);
		if (!need_to_reduce_distance(match, shadow->ctx.min_distance,
					     shadow, 2, &lower_bound))
			goto done_exclude;
	}
	shadow->ctx.min_distance = match_min_dist(match, shadow->ctx.level,
						  op->word,
						  op->len, lower_bound,
						  shadow->ctx.min_distance);
	if (shadow->ctx.min_distance > MAX_DIST)
		shadow->ctx.min_distance = MAX_DIST + 1;
	access_struct.min_dis = shadow->ctx.min_distance;
	/* exclude the mismatched queries */
	access_struct.phase = 0;
	list_init(&result->dirty_head);
	for (i = 0; i < shadow->ctx.min_distance; i++) {
		list_visit(&query_refs[shadow->ctx.level][i],
			   operator_query_refs_key_callback, &access_struct);
	}
	/* re-insert into the op_rank tree from diry_head */
	entry = result->dirty_head.next;
	while (entry != &result->dirty_head) {
		struct list_head *next = entry->next;
		struct operator_shadow *dirty_shadow =
			container_of(entry, struct operator_shadow, dirty_ops);
		struct operator *dirty_op =
			(void*) ((u8*) dirty_shadow
				 - match->shadow_id * sizeof(struct operator_shadow));
		struct refcnt_operator_key key = {
			dirty_shadow->refcnt, dirty_op
		};
		btree_insert(match->op_rank, &key, &dummy);
		memset(&dirty_shadow->dirty_ops, 0, sizeof(struct list_head));
		entry = next;
	}

done_exclude:
	/* trying to prune the ctx.level more aggressively */
	while (1) {
		access_struct.phase = 2;
		for (i = shadow->ctx.min_distance; i <= MAX_DIST; i++) {
			list_visit(&query_refs[shadow->ctx.level][i],
				   operator_query_refs_key_callback,
				   &access_struct);
		}
		shadow->ctx.level++;
		if (shadow->ctx.level >= 3)
			break;
		if (need_to_reduce_distance(match, shadow->ctx.min_distance,
					    shadow, shadow->ctx.level,
					    &lower_bound)) {
			/* this level needs reduce the distance,
			 * update the refcnt and mark it as reschedule
			 * needed
			 */
			return -1;
		}
	}
	return 0;
}

static void match_reschedule_operator(struct document_match *match,
				      struct operator *op)
{
	struct operator_shadow *shadow =
		operator_shadow_raw(op, match->shadow_id);
	struct refcnt_operator_key key = {
		operator_shadow_raw(op, match->shadow_id)->refcnt,
		op,
	};
	u8 dummy = 0;
	int i = 0, j = 0;

	btree_delete(match->op_rank, &key);
	shadow->refcnt = 0;
	for (i = shadow->ctx.level; i < 3; i++) {
		for (j = 1; j < 4; j++) {
			shadow->refcnt += shadow->nr_refs[i][j];
		}
	}
	key.refcnt = shadow->refcnt;
	btree_insert(match->op_rank, &key, &dummy);
}

static int collect_qid_callback(struct btree *tree, struct btree_node *node,
				void *key, void *ptr)
{
	struct match_result *result = ptr;
	unsigned int *qid = key;
	if (node->header.level != 0)
		return 0;
	result->queries[result->nr_queries++] = *qid;
	// printf(" %u", *qid);
	return 0;
}

void match_exec(struct document_match *match, struct match_result *result,
		int shadow_id)
{
	struct operator *op = NULL;
	struct operator_shadow *shadow = NULL;
	struct list_head zombies;
	struct list_head *entry = NULL;
	int task_level = -1;
	int task_ret = -1;

	list_init(&zombies);
	memset(result, 0, sizeof(struct match_result));
	list_init(&result->match_head);
	result->match = match;
	result->range.min_qid = INT_MAX;
	result->range.max_qid = 0;
	result->nr_queries = plan_get()->query_table->sb.size;
	match->shadow_id = shadow_id;
	match->docent = docent_new(match->doc_str);
	/* create a shadow */
	match->op_rank = btree_cow_new(plan_get()->op_rank,
				       &plan_get()->shadow_mempool[shadow_id]);
	match->bitmap = plan_get()->bitmap_mem[shadow_id];
	// printf("start matching...\n");
	while (match->op_rank->sb.size > 0) {
		op = match_pick_operator(match);
		shadow = operator_shadow(op, shadow_id);
		// printf("op %p op_rank size %d\n", op, match->op_rank->sb.size);
	exec:
		task_level = shadow->ctx.level;
		task_ret = match_exec_round(match, op, shadow, result);
		if (task_ret == 0) {
			/* operator done */
			match_remove_operator(match, op);
			// operator_destroy_shadow(op, shadow_id);
			list_add(&operator_shadow_raw(op, shadow_id)->zombie_list,
				 &zombies);
			result->match_round_nr++;
		} else if (task_ret == -1) {
			if (shadow->ctx.level == 2) {
				if (task_level == 0) {
					/* we skiped the hamming and now we
					 * need to go for edit let's tell edit
					 * to prefetch hamming first.
					 */
					shadow->ctx.need_hamming = 1;
				}
			}
			// match_reschedule_operator(match, op);
			goto exec;
		}
	}

	entry = zombies.next;
	while (entry != &zombies) {
		struct operator_shadow *shadow =
			container_of(entry, struct operator_shadow,
				     zombie_list);
		struct operator *op =
			(void*) ((u8*) shadow
				 - shadow_id * sizeof(struct operator_shadow));
		entry = shadow->zombie_list.next;
		list_del(&shadow->zombie_list);
		operator_destroy_shadow(op, shadow_id);
	}

	/*
	printf("%s(): %d rounds %d/%d/%d dis_calc for doc %d\n", __FUNCTION__,
	       result->match_round_nr, result->dis_calc_cnt[0],
	       result->dis_calc_cnt[1], result->dis_calc_cnt[2],
	       match->doc_id);
	*/

	result->queries = malloc(sizeof(unsigned int) * result->nr_queries);
	entry = result->match_head.next;
	result->nr_queries = 0;
	while (entry != &result->match_head) {
		struct query_shadow *shadow =
			container_of(entry, struct query_shadow, head);
		struct query_struct *qstruct =
			(void*) ((u8*) shadow
				 - shadow_id * sizeof(struct query_shadow));
		entry = shadow->head.next;
		list_del(&shadow->head);
		query_destroy_shadow(qstruct, shadow_id);
		result->queries[result->nr_queries++] = qstruct->qid;
		if (qstruct->alias && qstruct->alias->sb.size > 0) {
			// printf("collect %u", qstruct->qid);
			btree_visit(qstruct->alias, NULL, collect_qid_callback,
				    NULL, result);
			// puts("");
		}
	}
	qsort(result->queries, result->nr_queries, sizeof(int), uint_compare);
	bitmap_reset_range(match->bitmap, result->range.min_qid,
	 		   result->range.max_qid);

	/* free up thread specific resources */
	btree_cow_destroy(match->op_rank);
	docent_destroy(match->docent);
}
