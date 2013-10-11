#ifndef _BTREE_H_
#define _BTREE_H_

#include "misc.h"
#include "mempool.h"

/**
 * btree implementation.
 * this btree is a weaked typed btree however keys and values are of the
 * fixed length. (all keys are of the same length, all values are of the same
 * length).
 */

/* pointer to btree block */
typedef u64 blkptr_t;
#define BTREE_NODE_SIZE 512

/**
 * level 0 means leaf
 * size is the number of elements in this node. either value or pointer
 * generation number is the current block generation, this is used to identify
 * whether this block need to be COWed.
 */
struct btree_header {
	u8 level;
	u16 size;
	u8 generation;
	struct list_head alloc_head;
} __attribute__((packed));

/**
 * layout of a btree node is like the following:
 *
 * -----------------------------------------------------------
 * |K|K|K|K|K|K| | | | | | | | | | | | | | | | | |P|P|P|P|P|P|
 * -----------------------------------------------------------
 *
 * K is the key. P is the pointer or the value, value are packed at the end of a
 * node, due to better locality in binary search. Number of K and P are equal,
 * with a dummy key at the begining of the array -- keys[0]. keys[0] is the
 * parent pointer's key or 0. Therefore, keys[0] is always the smallest in
 * the node.
 *
 * after the header, are the array of keys. pointer/values are stored reverse
 * at the end of the node. this is to better utilize the cache.
 */
struct btree_node {
	struct btree_header header;
	u8 keys[];
} __attribute__((packed));

/**
 * btree superblock. this is the only block that's been udpated in place.
 */
struct btree_sb {
	blkptr_t root;
	u64 generation; /* current generation number */
	u16 level; /* level of the root block */
	u8  key_len; /* length of key */
	u8  value_len; /* length of value */
	u64 size; /* number of elements in the btree */
} __attribute__((packed));

/**
 * This btree impl should support both COW and in-memory, and therefore we have
 * a lot of callback functions here.
 *
 * alloc_block() is used for allocation a block, it could scan the bitmap for
 * physical storage block, or just call malloc() if it's in-memory based.
 *
 * free_block() actually mark the block as garbage, but not necessarily have to
 * reclaim it right now, because in COW you could only reclaim the garbage after
 * tx commits. But for in-memory btree, free_block() could just be free().
 *
 * commit() is only being used to when need to persist the btree. It's therefore
 * only supported by COW backend.
 *
 * offset field is the return value mmap(), which is the starting address of our
 * "buffer pool". For in-memory based btree, this could just be 0. By default,
 * this should set to 0.
 *
 */
struct btree {
	struct btree_sb sb;

	/* backend specific ops and data */
	blkptr_t (*alloc_block)(struct btree *tree);
	void     (*free_block) (struct btree *tree, blkptr_t blkptr);
	int      (*commit) (struct btree *tree);
	int      (*key_compare) (const void *, const void *);

	// int      (*trigger_commit) (struct btree *tree);
	// void     (*recover_value)(struct btree*, struct btree_node*,
	//  			  void*, void*);

	/* balance threshold */
	int max_nodes;
	int max_values;
	int min_nodes;
	int min_values;

	u8 priv[]; /* any private data that ops needs */
};

static inline blkptr_t* btree_node_ptrref(struct btree_node *node, int idx)
{
	u8 *ptr = (void*) node;
	ptr += BTREE_NODE_SIZE - (idx * sizeof(blkptr_t)) - sizeof(blkptr_t);
	return (blkptr_t*) ptr;
}

static inline void* btree_node_keyref(struct btree *tree,
				      struct btree_node *node,
				      int idx)
{
	return node->keys + idx * tree->sb.key_len;
}

static inline void* btree_node_valueref(struct btree *tree,
					struct btree_node *node,
					int idx)
{
	u8 *ptr = (void*) node;
	ptr += BTREE_NODE_SIZE - (idx * tree->sb.value_len)
		- tree->sb.value_len;
	return ptr;
}

#define BTREE_DATA_AREA (BTREE_NODE_SIZE - sizeof(struct btree_header))

#define PTR2BLK(ptr) ((blkptr_t) (ptr))
#define BLK2PTR(blk) ((void*) (blk))

static inline int btree_node_is_full(struct btree *tree,
				     struct btree_node *node)
{
	if (node->header.level == 0) {
		return node->header.size == tree->max_values;
	} else {
		return node->header.size == tree->max_nodes;
	}
}

static inline int btree_node_need_rebalance(struct btree *tree,
					    struct btree_node *node)
{
	if (node->header.level == 0) {
		return node->header.size <= tree->min_values - 1;
	} else {
		return node->header.size <= tree->min_nodes - 1;
	}
}

static inline int btree_node_can_rebalance_to(struct btree *tree,
					      struct btree_node *node)
{
	if (node->header.level == 0) {
		return node->header.size <= tree->max_values - 2;
	} else {
		return node->header.size <= tree->max_nodes - 2;
	}
}

static inline int btree_node_can_rebalance_from(struct btree *tree,
						struct btree_node *node)
{
	if (node->header.level == 0) {
		return node->header.size >= tree->min_values + 1;
	} else {
		return node->header.size >= tree->min_nodes + 1;
	}
}

static inline int btree_node_is_root(struct btree *tree,
				     struct btree_node *node)
{
	return tree->sb.level == node->header.level;
}

static inline int btree_node_need_copy(struct btree *tree,
				       struct btree_node *node)
{
	if (tree->sb.generation > node->header.generation)
		return 1;
	return 0;
}

static inline int blkptr_compare(const void *p, const void *q)
{
	const blkptr_t *a = p;
	const blkptr_t *b = q;
	return (*a) - (*b);
}

typedef int(*btree_pointer_cb)(struct btree*, blkptr_t, void*);
typedef int(*btree_key_cb)(struct btree*, struct btree_node*, void*, void*);
typedef int(*btree_value_cb)(struct btree*, struct btree_node*, void*, void*);

/* common operations */
void btree_insert(struct btree *tree, void *key, void *valueref);
void btree_delete(struct btree *tree, void *key);
void btree_visit(struct btree *tree, btree_pointer_cb pointer_cb,
		 btree_key_cb key_cb, btree_value_cb value_cb, void *ptr);

void *btree_search(struct btree *tree, const void *key);
void btree_first_pair(struct btree *tree, void **key_ret, void **valueref_ret);
void btree_last_pair(struct btree *tree, void **key_ret, void **valueref_ret);

/* init btree struct after superblock is setup */
void btree_init(struct btree *tree, int (*compare)(const void*, const void*));

/* memory based btree */
struct btree *btree_mem_new(u8 key_len, u8 value_len,
			    int (*compare)(const void *, const void *));
void          btree_mem_destroy(struct btree *tree);

/* cow based btree, for cloning an old tree */
struct btree *btree_cow_new(struct btree *orig_mem_tree, struct mempool *pool);
void          btree_cow_destroy(struct btree *cow_tree);

#endif /* _BTREE_H_ */
