#include "worker.h"


extern void free_match_obj(void *ptr);

static void *worker_routine(void *arg)
{
	struct worker* worker = arg;
	struct worker_manager *mgr = worker->mgr;
	struct document_match *match = NULL;
	struct match_result *result = NULL;

process:
	pthread_mutex_lock(&mgr->doc_mutex);
	while (list_empty(&mgr->doc_queue)) {
		pthread_cond_wait(&mgr->doc_cond, &mgr->doc_mutex);
	}
	match = container_of(mgr->doc_queue.prev, struct document_match, head);
	list_del(&match->head);
	pthread_mutex_unlock(&mgr->doc_mutex);

	result = malloc(sizeof(struct match_result));
	match_exec(match, result, worker->shadow_id);
	result->doc_id = match->doc_id;
	free_match_obj(match);

	pthread_mutex_lock(&mgr->result_mutex);
	list_add(&result->head, &mgr->result_queue);
	pthread_cond_signal(&mgr->result_cond);
	__sync_fetch_and_sub(&mgr->nr_pending, 1);
	pthread_mutex_unlock(&mgr->result_mutex);
	goto process;

	return NULL;
}

void worker_manager_init(struct worker_manager *mgr)
{
	int i = 0;

	pthread_mutex_init(&mgr->doc_mutex, NULL);
	pthread_cond_init(&mgr->doc_cond, NULL);
	list_init(&mgr->doc_queue);
	pthread_mutex_init(&mgr->result_mutex, NULL);
	pthread_cond_init(&mgr->result_cond, NULL);
	list_init(&mgr->result_queue);
	mgr->nr_pending = 0;

	/* creating worker threads */
	for (i = 0; i < NR_SHADOW; i++) {
		struct worker *worker = &mgr->workers[i];
		worker->shadow_id = i;
		worker->mgr = mgr;
		pthread_create(&worker->id, NULL, worker_routine, worker);
	}
}

void worker_manager_push(struct worker_manager *mgr,
			 struct document_match *match)
{
	pthread_mutex_lock(&mgr->doc_mutex);
	list_add(&match->head, &mgr->doc_queue);
	pthread_cond_signal(&mgr->doc_cond);
	__sync_fetch_and_add(&mgr->nr_pending, 1);
	pthread_mutex_unlock(&mgr->doc_mutex);
}

struct match_result *worker_manager_pop(struct worker_manager *mgr)
{
	struct match_result *result = NULL;
	pthread_mutex_lock(&mgr->result_mutex);
	while (list_empty(&mgr->result_queue)) {
		if (mgr->nr_pending == 0) {
			pthread_mutex_unlock(&mgr->result_mutex);
			return NULL;
		}
		pthread_cond_wait(&mgr->result_cond, &mgr->result_mutex);
	}
	result = container_of(mgr->result_queue.prev, struct match_result,
			      head);
	list_del(&result->head);
	pthread_mutex_unlock(&mgr->result_mutex);
	return result;
}

/* destroy function? TBD....-_- */
