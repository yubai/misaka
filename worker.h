#ifndef _WORKER_H_
#define _WORKER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "operator.h"
#include "match.h"

struct worker {
	pthread_t id;
	struct worker_manager *mgr;
	int shadow_id;
};

struct worker_manager {
	pthread_mutex_t doc_mutex;
	pthread_cond_t doc_cond;
	struct list_head doc_queue;

	struct worker workers[NR_SHADOW];

	pthread_mutex_t result_mutex;
	pthread_cond_t result_cond;
	struct list_head result_queue;

	volatile long nr_pending;
};

void worker_manager_init(struct worker_manager *mgr);

void worker_manager_push(struct worker_manager *mgr,
			 struct document_match *match);

struct match_result *worker_manager_pop(struct worker_manager *mgr);

#endif /* _WORKER_H_ */
