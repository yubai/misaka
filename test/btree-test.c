#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <string.h>

#include "../btree.h"

static int int_compare(const void *p, const void *q)
{
	const int *a = p;
	const int *b = q;
	return (*a) - (*b);
}

struct level_keys {
	int len;
	int *keys;
};

static int collect_key_cb(struct btree *tree, struct btree_node *node,
			   void *key_ptr, void *ptr)
{
	int *k = key_ptr;
	struct level_keys *lvl = ptr;
	struct level_keys *cur_lvl = &lvl[node->header.level];
	if (cur_lvl->keys == NULL) {
		cur_lvl->keys =	malloc(sizeof(int) * tree->sb.size);
	}
	if (cur_lvl->len > tree->sb.size)
		fprintf(stderr, "nr elements inconsistent!!!\n");
	cur_lvl->keys[cur_lvl->len++] = *k;
	return 0;
}

static void visit_and_verify(struct btree *tree)
{

	struct level_keys lvl[tree->sb.level + 1];
	int l = 0;
	int i = 0;
	memset(lvl, 0, sizeof(struct level_keys) * (tree->sb.level + 1));
	btree_visit(tree, NULL, collect_key_cb, NULL, &lvl);
	for (l = 0; l <= tree->sb.level; l++) {
		if (l == 0) {
			printf("verifying tree size %lu\n", tree->sb.size);
			assert(tree->sb.size == lvl[l].len);
		}
		for (i = 1; i < lvl[l].len; i++) {
			int left = lvl[l].keys[i - 1];
			int right = lvl[l].keys[i];
			// assert(left < right);
			if (left >= right) {
				fprintf(stderr,
					"error in order %d >= %d level %d\n",
					left, right, l);
			}
		}
		free(lvl[l].keys);
	}
}

#define KEY_MAX 100000000

static void setup_insert(struct btree *tree, int cnt)
{
	int i = 0;
	for (i = 0; i < cnt; i++) {
		int key = rand() % KEY_MAX;
		// printf("inserting key %d\n", key);
		btree_insert(tree, &key, &key);
	}
}

static void setup_delete(struct btree *tree, int cnt)
{
	int i = 0;
	for (i = 0; i < cnt; i++) {
		int key = rand() % KEY_MAX;
		if (btree_search(tree, &key) != NULL) {
			// printf("deleting key %d\n", key);
			btree_delete(tree, &key);
		}
	}
}

#define INSERT_CNT 1000000
#define DELETE_CNT 1000000

int main(int argc, char *argv[])
{
	struct btree *mem_tree = btree_mem_new(sizeof(int), sizeof(int),
					       int_compare);
	struct btree *cow_tree = NULL;

	srand(time(NULL));
	puts("testing mem-tree insertion");
	setup_insert(mem_tree, INSERT_CNT);
	visit_and_verify(mem_tree);
	puts("testing mem-tree deletion");
	setup_delete(mem_tree, DELETE_CNT);
	visit_and_verify(mem_tree);
	/* test the cow tree */
	cow_tree = btree_cow_new(mem_tree, NULL);
	puts("testing cow-tree insertion/deletion");
	setup_insert(cow_tree, 2000);
	visit_and_verify(cow_tree);
	setup_delete(cow_tree, DELETE_CNT);
	visit_and_verify(cow_tree);
	btree_cow_destroy(cow_tree);
	visit_and_verify(mem_tree);
	btree_mem_destroy(mem_tree);
	return 0;
}
