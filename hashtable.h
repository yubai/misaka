#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_

#include "misc.h"

struct hash_entry {
	struct list_head head;
	unsigned char key[];
};

typedef unsigned long (*hash_func)(const void *);
typedef int (*compare_func)(const void *, const void *);

struct hashtable {
	int size;
	unsigned long capacity;
	int key_len;
	int value_len;

	hash_func hash;
	compare_func cmp;

	struct list_head *bucket;
};

struct hashtable *hashtable_new(int key_len, int value_len, int capacity,
				hash_func func, compare_func cmp);
void hashtable_destroy(struct hashtable *table);

int  hashtable_insert_with_hash(struct hashtable *table, unsigned long hash,
				void *key, void *value);

int  hashtable_insert(struct hashtable *table, void *key, void *value);
void *hashtable_search(struct hashtable *table, void *key);
void *hashtable_search_with_hash(struct hashtable *table, unsigned long hash,
				 void *key);
void hashtable_delete(struct hashtable *table, void *key);

#endif /* _HASHTABLE_H_ */
