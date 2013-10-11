#define _GNU_SOURCE

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>

#include "document.h"
#include "match.h"
#include "util.h"
#include "core.h"
#include "misc.h"

static unsigned long doc_word_hash(const void *p)
{
	const char *str = *(const char **) p;
	char ch = 0;
	unsigned long res = 0;
	while ((ch = *(str++))) {
		res = (res << 8) + (res << 4) - res + ch - 'a';
	}
	return res;
}

static int compare(const void* a, const void* b)
{
	return strcmp(*(char**)a, *(char**)b);
}

struct docent *docent_new(char *doc_str)
{
	int i = 0;
	u8 dummy = 0;
	struct docent* docent = malloc(sizeof(struct docent));
	memset(docent, 0, sizeof(struct docent));

	docent->strents = malloc(sizeof(struct strent) * WORD_LENGTH_RANGE);
	memset(docent->strents, 0, sizeof(struct strent) * WORD_LENGTH_RANGE);

	docent->doc_str = doc_str;

	int done = 0;
	char *ptr = doc_str;
	while (*ptr) {
		while(*ptr == ' ') ptr++;
		if(!ptr) break;

		char *start = ptr;
		while(*ptr && *ptr != ' ') ptr++;

		// char dt = docent->doc_str[id];
		// docent->doc_str[id] = '\0';

		int ld = ptr - start;
		docent->strents[ld-MIN_WORD_LENGTH].num++;

		// docent->doc_str[id]=dt;
		if (((ptr - doc_str) & ((1 << 9) - 1)) == 0) PREFETCH(ptr);
	}

	int total_word_num = 0;
	unsigned int word_cnt[WORD_LENGTH_RANGE];
	memset(word_cnt, 0, sizeof(word_cnt));

	for (i = 0; i < WORD_LENGTH_RANGE; i++) {
		total_word_num += docent->strents[i].num;
	}

	docent->strpool = malloc(sizeof(char*) * total_word_num);

	char* *strptr = docent->strpool;
	docent->strents[0].ptr = strptr;
	for (i = 1; i < WORD_LENGTH_RANGE; i++) {
		strptr += docent->strents[i-1].num;
		docent->strents[i].ptr = strptr;
	}

	ptr = docent->doc_str;
	done = 0;
	while (!done && *ptr) {
		while(*ptr == ' ') ptr++;
		if(!*ptr) break;

		char* start = ptr;
		while(*ptr && *ptr != ' ') ptr++;
		if (!*ptr) {
			done = 1;
		} else {
			*ptr = '\0';
		}

		int ld = ptr - start;
		int bucket = ld - MIN_WORD_LENGTH;
		int cnt = word_cnt[bucket];

		docent->strents[bucket].ptr[cnt] = start;
		word_cnt[bucket]++;

		ptr++;
	}

	for (i = 0; i < WORD_LENGTH_RANGE; i++) {
		int j = 0, cnt = 0;
		struct strent *ent = &docent->strents[i];
		ent->htbl = hashtable_new(sizeof(char *), 1,
					  DOC_HASHTABLE_CAP,
					  doc_word_hash, compare);
		for (j = 0; j < ent->num; j++) {
			int unique =
				hashtable_insert(ent->htbl, &ent->ptr[j],
						 &dummy);
			if (unique) {
				ent->ptr[cnt] = ent->ptr[j];
				cnt++;
			}
		}
		ent->num = cnt;
	}
	return docent;
}

void docent_destroy(struct docent *ent)
{
	int i = 0;
	for (i = 0; i < WORD_LENGTH_RANGE; i++) {
		hashtable_destroy(ent->strents[i].htbl);
	}
	free(ent->strents);
	free(ent->strpool);
	free(ent);
}

static int min_edit_strent(struct docent *ent,
			   int len, const char *word, int wlen,
			   int lower_bound, int *upper_bound)
{
	struct strent *strent = &ent->strents[len - MIN_WORD_LENGTH];
	int i = 0;
	long cnt = 0;
	PREFETCH(strent->ptr);
    unsigned long s = start_timer();
	for (i = 0; i < strent->num; i++) {
		int dist = 0;
		if (wlen < len) {
			dist = EditDistance(word, wlen, strent->ptr[i], len,
					    *upper_bound);
		} else {
			dist = EditDistance(strent->ptr[i], len, word, wlen,
					    *upper_bound);
		}
		cnt++;
	done:
		if (dist <= lower_bound) {
            end_timer(s);
			inc_cnt(0, cnt);
			return dist;
		}
		*upper_bound = dist < *upper_bound ? dist : *upper_bound;
	}
    end_timer(s);
	inc_cnt(0, cnt);
	return *upper_bound;
}

int match_min_dist(struct document_match *doc_match, MatchType match_type,
		   const char* word, int len,
		   int lower_bound, int upper_bound)
{
	int ret = upper_bound;

	switch (match_type) {
	case MT_EXACT_MATCH: {
		struct strent* ent
			= &doc_match->docent->strents[len - MIN_WORD_LENGTH];
		if (hashtable_search(ent->htbl, &word))
			return 0;
		else
			return MAX_DIST + 1;
		break;
	}
	case MT_HAMMING_DIST: {
		int i;
		struct strent* strent
			= &doc_match->docent->strents[len - MIN_WORD_LENGTH];
		char** pstr = strent->ptr;
		int cnt = 0;
		/* prefetch pstr into cache line */
		PREFETCH(pstr);
		for (i = 0; i < strent->num; ++i) {
			int dist = HammingDistance(pstr[i], len, word, len, ret);
			cnt++;
			if (dist <= lower_bound) {
				inc_cnt(1, cnt);
				return lower_bound;
			}
			ret = dist < ret ? dist: ret;
		}
		inc_cnt(1, cnt);
		break;
	}
	case MT_EDIT_DIST: {
		struct docent *docent = doc_match->docent;
		int dist = 0;
		int i = 0;
		for (i = 1; i < ret; ++i) {
			int lb = i;
			if (lower_bound > lb) lb = lower_bound;
			if (len - i >= MIN_WORD_LENGTH) {
				dist = min_edit_strent(docent, len - i,
						       word, len, lb, &ret);
				if (dist <= lb) {
					return dist;
				}
			}
			if (i == 2) {
				/* if we assume hamming prefetching... */
				dist = min_edit_strent(docent, len, word,
						       len, lb, &ret);
				if (dist <= lb) {
					return dist;
				}
			}
			if (len + i <= MAX_WORD_LENGTH) {
				dist = min_edit_strent(docent, len + i,
						       word, len, lb, &ret);
				if (dist <= lb) {
					return dist;
				}
			}
		}
		break;
	}
	};

	return ret;
}
