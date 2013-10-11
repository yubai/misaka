#ifndef _MATCH_H_
#define _MATCH_H_

#include "misc.h"
#include "core.h"
#include "operator.h"
#include "document.h"

/**
 * this represent the internal data structure of a document. we might
 * need to build some sort of index for document to speed up the matching
 * process.
 *
 */
struct document_match {
	/**
	 * document index
	 */
	struct docent *docent;
 	/**
 	 * op_rank key is a "<refcnt, ptr>", value is useless, a cow tree
 	 */
 	struct btree *op_rank;
	DocID doc_id;
	int shadow_id;
	struct plan *plan; /* backref */

	struct list_head head; /* head on the global queue */
	char doc_str[MAX_DOC_LENGTH];
	u8 *bitmap;
};

struct match_result {
	unsigned int doc_id;
	int nr_queries;
	struct list_head dirty_head;
	struct list_head match_head;
	unsigned int *queries;
	/* some stat numbers */
	int match_round_nr;
	int dis_calc_cnt[3];

		struct {
		unsigned int min_qid;
		unsigned int max_qid;
	} range;

	struct document_match *match;
	struct list_head head;
};

/**
 * this function return the minimum distance to the target word among all the
 * words in the documents.
 * @doc_id the document id
 * @match_type MATCH TYPE, it could be MT_EXTACT_MATCH,
 * MT_HAMMING_DIST or MT_EDIT_DIST
 * @match_dist match distance
 * @word the query word.
 *
 * @return the minimum distance to the target word specified.
 *
 */
int match_min_dist(struct document_match *match, MatchType match_type,
		   const char *word, int len, int lower_bound, int upper_bound);

void match_init(struct document_match *match, DocID doc_id, const char *doc_str);

void            match_exec(struct document_match *match,
			   struct match_result *result, int shadow_id);

#endif /* _MATCH_H_ */
