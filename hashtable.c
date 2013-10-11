#include <string.h>
#include "hashtable.h"

struct hashtable *hashtable_new(int key_len, int value_len, int capacity,
				hash_func func, compare_func cmp)
{
	struct hashtable *table = malloc(sizeof(struct hashtable));
	table->size = 0;
	table->key_len = key_len;
	table->value_len = value_len;
	table->capacity = capacity;
	table->hash = func;
	table->cmp = cmp;

	table->bucket = calloc(capacity, sizeof(struct list_head));
	return table;
}

void hashtable_destroy(struct hashtable *table)
{
	int i = 0;
	for (i = 0; i < table->capacity; i++) {
		struct list_head *bucket = &table->bucket[i];
		struct list_head *node = bucket->next;
		if (node == NULL)
			continue;
		while (node != bucket) {
			struct hash_entry *ent =
				container_of(node, struct hash_entry, head);
			node = node->next;
			free(ent);
		}
	}
	free(table->bucket);
	free(table);
}

static struct hash_entry *hash_entry_new(struct hashtable *table, void *key,
					 void *value)
{
	struct hash_entry *ent = malloc(sizeof(struct hash_entry)
					+ table->key_len + table->value_len);
	memcpy(ent->key, key, table->key_len);
	memcpy(ent->key + table->key_len, value, table->value_len);
	return ent;
}

int hashtable_insert(struct hashtable *table, void *key, void *value)
{
	return hashtable_insert_with_hash(table, table->hash(key), key, value);
}

int hashtable_insert_with_hash(struct hashtable *table, unsigned long hash,
				void *key, void *value)
{
	unsigned long h = hash % table->capacity;
	struct list_head *bucket = &table->bucket[h];
	struct hash_entry *ent = NULL;

	if (bucket->prev == bucket->next && bucket->next == NULL) {
		list_init(bucket);
	} else {
		/* traverse the link list */
		struct list_head *node = bucket->next;
		while (node != bucket) {
			struct hash_entry *ent =
				container_of(node, struct hash_entry, head);
			if (table->cmp(ent->key, key) == 0) {
				memcpy(ent->key + table->key_len,
				       value, table->value_len);
				return 0;
			}
			node = node->next;
		}
	}
	ent = hash_entry_new(table, key, value);
	list_add(&ent->head, bucket);
	return 1;
}

void *hashtable_search(struct hashtable *table, void *key)
{
	return hashtable_search_with_hash(table, table->hash(key), key);
}

void *hashtable_search_with_hash(struct hashtable *table, unsigned long hash,
				 void *key)
{
	unsigned long h = hash % table->capacity;
	struct list_head *bucket = &table->bucket[h];
	struct list_head *node = bucket->next;
	if (node == NULL)
		return NULL;
	while (node != bucket) {
		struct hash_entry *ent =
			container_of(node, struct hash_entry, head);
		if (table->cmp(ent->key, key) == 0)
			return ent->key + table->key_len;
		node = node->next;
	}
	return NULL;
}

void hashtable_delete(struct hashtable *table, void *key)
{
	unsigned long h = table->hash(key) % table->capacity;
	struct list_head *bucket = &table->bucket[h];
	struct list_head *node = bucket->next;
	if (node == NULL)
		return;
	while (node != bucket) {
		struct hash_entry *ent =
			container_of(node, struct hash_entry, head);
		if (table->cmp(ent->key, key) == 0) {
			list_del(node);
			free(ent);
			return;
		}
		node = node->next;
	}
	return;
}
