#include "misc.h"
#include "btree.h"
#include "mempool.h"

struct btree_cow_info {
	/* whether this tree was cloned from previous tree? */
	struct btree *original_tree;
	struct list_head alloc_head;
	struct mempool *mempool;
};

#define COW_INFO(tree) ((struct btree_cow_info*) tree->priv)

static blkptr_t cow_alloc_block(struct btree *tree)
{
	struct btree_cow_info *info = (struct btree_cow_info *) tree->priv;
	struct btree_node *node = NULL;
	blkptr_t ret = 0;
	if (unlikely(info->mempool == NULL)) {
		node = malloc(BTREE_NODE_SIZE);
	} else {
		node = mempool_alloc(info->mempool);
	}
	if (unlikely(node == NULL)) {
		abort(); /* OOM */
	}
	node->header.generation = tree->sb.generation;
	ret = (blkptr_t) node;
	list_add(&node->header.alloc_head, &info->alloc_head);
	return ret;
}

static void cow_free_block(struct btree *tree, blkptr_t blk)
{
	struct btree_cow_info *info = (struct btree_cow_info *) tree->priv;
	struct btree_node *node = (void *) blk;
	if (node->header.generation == tree->sb.generation) {
		/* short path free, this is how we can save memory */
		list_del(&node->header.alloc_head);
		if (unlikely(info->mempool == NULL)) {
			free(node);
		} else {
			mempool_free(info->mempool, node);
		}
	}
}

struct btree *btree_cow_new(struct btree *orig_tree, struct mempool *pool)
{
	struct btree *tree = NULL;
	if (unlikely(pool == NULL)) {
		tree = malloc(sizeof(struct btree)
			      + sizeof(struct btree_cow_info));
	} else {
		tree = mempool_alloc(pool);
	}
	struct btree_cow_info *info = (struct btree_cow_info *) tree->priv;
	/* copy the original superblock */
	tree->sb = orig_tree->sb;
	tree->sb.generation++;

	tree->alloc_block = cow_alloc_block;
	tree->free_block = cow_free_block;

	info->mempool = pool;
	info->original_tree = orig_tree;
	list_init(&info->alloc_head);

	btree_init(tree, orig_tree->key_compare);
	return tree;
}

void btree_cow_destroy(struct btree *tree)
{
	struct btree_cow_info *info = (struct btree_cow_info *) tree->priv;
	struct list_head *entry = info->alloc_head.next;
	while (entry != &info->alloc_head) {
		struct btree_header *header =
			container_of(entry, struct btree_header, alloc_head);
		struct btree_node *node = (struct btree_node *) header;
		entry = header->alloc_head.next;
		if (unlikely(info->mempool == NULL)) {
			free(node);
		} else {
			mempool_free(info->mempool, node);
		}
	}
	if (unlikely(info->mempool == NULL)) {
		free(tree);
	} else {
		mempool_free(info->mempool, tree);
	}
}
