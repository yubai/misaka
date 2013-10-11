#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "btree.h"
#include "match.h"
#include "operator.h"
#include "worker.h"

#include "misc.h"
#include "core.h"

/* implements the core.h interface */

static struct plan global_plan;
static struct worker_manager worker_mgr;
static struct mempool match_pool;
static k42lock match_pool_lock;
static long maxrss;

struct plan *plan_get() { return &global_plan; }

ErrorCode InitializeIndex()
{
	plan_init(&global_plan);
	worker_manager_init(&worker_mgr);
	/* this is a really big pool, wow! */
	mempool_init(&match_pool, sizeof(struct document_match),
		     sizeof(struct document_match) * 48);
	mempool_set_name(&match_pool, "match-pool");
	return EC_SUCCESS;
}

#define COUNTER 24

static volatile unsigned long clock_cnt;
static volatile unsigned long counter[COUNTER];

unsigned long start_timer()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000000 + tv.tv_usec;
}

void end_timer(unsigned long last)
{
	__sync_fetch_and_add(&clock_cnt, start_timer() - last);
}

void inc_cnt(int idx, long c)
{
	__sync_fetch_and_add(&counter[idx], c);
}

static void update_mem_usage()
{
	struct rusage rs;
	if (getrusage(RUSAGE_SELF, &rs) == 0) {
		if (rs.ru_maxrss > maxrss) {
			maxrss = rs.ru_maxrss;
		}
	}
}

ErrorCode DestroyIndex()
{
	/* hack for the fix....*/
	struct match_result *result = NULL;
	int i = 0;
	while ((result = worker_manager_pop(&worker_mgr)) != NULL) {
		free(result->queries);
		free(result);
	}
	plan_destroy(&global_plan);
	mempool_destroy(&match_pool);
	printf("timer:%luus thr:%d per-thr: %luus\n", clock_cnt, NR_SHADOW,
	       clock_cnt / NR_SHADOW);
	printf("nr_counter:%d ", COUNTER);
	for (i = 0; i < COUNTER; i++) {
		printf("%lu ", counter[i]);
	}
	puts("");
	printf("maxrss: %ld\n", maxrss);
	return EC_SUCCESS;
}

ErrorCode StartQuery(QueryID qid, const char *str, MatchType match_type,
		     unsigned int threshold)
{
	if (match_type == MT_EXACT_MATCH)
		threshold = 0;
	plan_add_query(&global_plan, qid, str, match_type, threshold);
	return EC_SUCCESS;
}

ErrorCode EndQuery(QueryID qid)
{
	plan_del_query(&global_plan, qid);
	return EC_SUCCESS;
}

void *alloc_match_obj()
{
	void *match = NULL;
again:
	k42_lock(&match_pool_lock);
	match = mempool_alloc(&match_pool);
	k42_unlock(&match_pool_lock);
	if (match == NULL) {
		goto again;
	}
	return match;
}

void free_match_obj(void *ptr)
{
	k42_lock(&match_pool_lock);
	mempool_free(&match_pool, ptr);
	k42_unlock(&match_pool_lock);
}

ErrorCode MatchDocument(DocID doc_id, const char *str)
{
	struct document_match *match = NULL;
	if (!list_empty(&plan_get()->dirty_ops)) {
		plan_rebuild(plan_get());
	}

	match = alloc_match_obj();
	match_init(match, doc_id, str);

	worker_manager_push(&worker_mgr, match);
	return EC_SUCCESS;
}

ErrorCode GetNextAvailRes(DocID *doc_id_ret, unsigned int *nr_ret,
			  QueryID **q_ret)
{
	struct match_result *result = worker_manager_pop(&worker_mgr);
	if (result == NULL)
		return EC_NO_AVAIL_RES;
	// update_mem_usage();
	*doc_id_ret = result->doc_id;
	*nr_ret = result->nr_queries;
	*q_ret = result->queries;
	// printf("done for doc %d\n", result->doc_id);
	free(result);

	return EC_SUCCESS;
}
