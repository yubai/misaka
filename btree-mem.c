#include <stdio.h>
#include "misc.h"
#include "btree.h"

static blkptr_t mem_alloc_block(struct btree *tree)
{
	struct btree_node *node = malloc(BTREE_NODE_SIZE);
	node->header.generation = tree->sb.generation;
	return (blkptr_t) node;
}

static void mem_free_block(struct btree *tree, blkptr_t blk)
{
	free((void*) blk);
}

struct btree *btree_mem_new(u8 key_len, u8 value_len,
			    int (*compare)(const void *, const void *))
{
	struct btree *tree = malloc(sizeof(struct btree));

	tree->alloc_block = mem_alloc_block;
	tree->free_block = mem_free_block;

	tree->sb.key_len = key_len;
	tree->sb.value_len = value_len;
	tree->sb.root = 0;
	tree->sb.level = 0;
	tree->sb.generation = 0;
	tree->sb.size = 0;

	btree_init(tree, compare);
	return tree;
}

static void btree_free_node(struct btree *tree, blkptr_t blk)
{
	struct btree_node *node = BLK2PTR(blk);
	int i = 0;

	if (node->header.level > 0) {
		for (i = 0; i < node->header.size; i++) {
			btree_free_node(tree, *btree_node_ptrref(node, i));
		}
	}
	tree->free_block(tree, blk);
}

void btree_mem_destroy(struct btree *tree)
{
	if (tree->sb.root != 0)
		btree_free_node(tree, tree->sb.root);
	free(tree);
}
