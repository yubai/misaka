#ifndef __SIGMOD_DOCUMENT_H_
#define __SIGMOD_DOCUMENT_H_

#include "core.h"
#include "misc.h"
#include "hashtable.h"

#define WORD_LENGTH_RANGE (MAX_WORD_LENGTH - MIN_WORD_LENGTH + 1)
#define DOC_HASHTABLE_CAP ((1 << 12) - 1)

/**
 * string entry type
 * @ptr pointer to the first string in the strpool
 * @num number of strings with the same length
 */
struct strent {
	char** ptr; /* indirect pointers of each word */
	unsigned int num;
	struct hashtable *htbl; /* hashtable of all words */
};

/**
 * document entry type
 * @strents string entry type array
 * @strpool document string pool, where the document strings are stored
 */
struct docent {
	struct strent* strents; /* buckets */
	char** strpool; /* strpool or ptr pool? @_@ */
	char* doc_str; /* borrowed reference, do not free it */
};

struct docent *docent_new(char *doc_str);
void           docent_destroy(struct docent *ent);

#endif // __SIGMOD_DOCUMENT_H_
