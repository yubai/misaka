#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "btree.h"

#define POLICY_SPLIT 1
#define POLICY_REBALANCE_LEFT 2
#define POLICY_REBALANCE_RIGHT 3
#define POLICY_MERGE_LEFT 4
#define POLICY_MERGE_RIGHT 5

struct btree_ops_ret {
	int nrblks;
	blkptr_t blks[2];
	int pol;
	void *keys[2];
};

static blkptr_t btree_block_copy(struct btree *tree, struct btree_node *node)
{
	blkptr_t old_blknr = PTR2BLK(node);
	blkptr_t new_blknr = tree->alloc_block(tree);
	struct btree_node * newnode = BLK2PTR(new_blknr);
	struct btree_header newheader = newnode->header;

	newheader.level = node->header.level;
	newheader.size = node->header.size;
	memcpy(newnode, node, BTREE_NODE_SIZE);
	newnode->header = newheader;

	if (old_blknr != 0)
		tree->free_block(tree, old_blknr);

	return new_blknr;
}

static blkptr_t test_do_copy(struct btree *tree, blkptr_t blknr)
{
	struct btree_node *node = BLK2PTR(blknr);
	if (!btree_node_need_copy(tree, node)) {
		return blknr;
	} else {
		return btree_block_copy(tree, node);
	}
}

static void ops_ret_init(struct btree_ops_ret *ret, void *key_ptr,
			 blkptr_t oldblk, blkptr_t newblk)
{
	ret->pol = 0;
	ret->keys[0] = key_ptr;
	if (oldblk == newblk) {
		ret->nrblks = 0;
	} else {
		ret->nrblks = 1;
		ret->blks[0] = newblk;
	}
}

#define PTRREF btree_node_ptrref
#define KEYREF(node, idx) btree_node_keyref(tree, node, idx)
#define VALUEREF(node, idx) btree_node_valueref(tree, node, idx)
#define KEY_SIZE (tree->sb.key_len)
#define VALUE_SIZE (tree->sb.value_len)

static void btree_node_split(struct btree *tree, blkptr_t node_blknr,
			     struct btree_ops_ret *ret)
{
	struct btree_node *node = NULL;
	struct btree_node *newnode = NULL;
	u16 halfsize = 0;
	u16 newsize = 0;

	/* setup the space */
	ret->pol = POLICY_SPLIT;
	ret->nrblks = 2;
	ret->blks[0] = test_do_copy(tree, node_blknr);
	ret->blks[1] = tree->alloc_block(tree);
	node = BLK2PTR(ret->blks[0]);
	newnode = BLK2PTR(ret->blks[1]);
	newnode->header.level = node->header.level;

	halfsize = node->header.size / 2;
	newsize = node->header.size - halfsize;

	/* memmove to newnodes */
	memcpy(KEYREF(newnode, 0), KEYREF(node, halfsize), newsize * KEY_SIZE);
	if (node->header.level > 0) {
		memcpy(PTRREF(newnode, newsize - 1),
		       PTRREF(node, node->header.size - 1),
		       newsize * sizeof(blkptr_t));
	} else {
		memcpy(VALUEREF(newnode, newsize - 1),
		       VALUEREF(node, node->header.size - 1),
		       newsize * VALUE_SIZE);
	}
	node->header.size = halfsize;
	newnode->header.size = newsize;
	ret->keys[0] = KEYREF(node, 0);
	ret->keys[1] = KEYREF(newnode, 0);
}

/* rebalance the left node to right node */
static void btree_node_rebalance_left(struct btree *tree,
				      struct btree_node *left,
				      struct btree_node *right,
				      struct btree_ops_ret *ret)
{
	u16 halfsize = (left->header.size + right->header.size) / 2;
	u16 mov_amount = left->header.size - halfsize;

	memmove(KEYREF(right, mov_amount), KEYREF(right, 0),
		right->header.size * KEY_SIZE);
	memcpy(KEYREF(right, 0), KEYREF(left, halfsize),
	       mov_amount * KEY_SIZE);
	if (left->header.level > 0) {
		memmove(PTRREF(right, right->header.size + mov_amount - 1),
			PTRREF(right, right->header.size - 1),
			right->header.size * sizeof(blkptr_t));
		memcpy(PTRREF(right, mov_amount - 1),
		       PTRREF(left, left->header.size - 1),
		       mov_amount * sizeof(blkptr_t));
	} else {
		memmove(VALUEREF(right, right->header.size + mov_amount - 1),
			VALUEREF(right, right->header.size - 1),
			right->header.size * VALUE_SIZE);
		memcpy(VALUEREF(right, mov_amount - 1),
		       VALUEREF(left, left->header.size - 1),
		       mov_amount * VALUE_SIZE);
	}
	left->header.size -= mov_amount;
	right->header.size += mov_amount;
	ret->pol = POLICY_REBALANCE_LEFT;
}


/* rebalance the right node to left node */
static void btree_node_rebalance_right(struct btree *tree,
				       struct btree_node *left,
				       struct btree_node *right,
				       struct btree_ops_ret *ret)
{
	u16 halfsize = (left->header.size + right->header.size) / 2;
	u16 mov_amount = right->header.size - halfsize;

	memcpy(KEYREF(left, left->header.size), KEYREF(right, 0),
	       mov_amount * KEY_SIZE);
	memmove(KEYREF(right, 0), KEYREF(right, mov_amount),
		halfsize * KEY_SIZE);
	if (left->header.level > 0) {
		memcpy(PTRREF(left, left->header.size + mov_amount - 1),
		       PTRREF(right, mov_amount - 1),
		       mov_amount * sizeof(blkptr_t));
		memmove(PTRREF(right, right->header.size - mov_amount - 1),
			PTRREF(right, right->header.size - 1),
			halfsize * sizeof(blkptr_t));
	} else {
		memcpy(VALUEREF(left, left->header.size + mov_amount - 1),
		       VALUEREF(right, mov_amount - 1),
		       mov_amount * VALUE_SIZE);
		memmove(VALUEREF(right, right->header.size - mov_amount - 1),
			VALUEREF(right, right->header.size - 1),
			halfsize * VALUE_SIZE);
	}
	left->header.size += mov_amount;
	right->header.size -= mov_amount;
	ret->pol = POLICY_REBALANCE_RIGHT;
}

static void btree_node_rebalance(struct btree *tree, blkptr_t left_blknr,
				 blkptr_t right_blknr,
				 struct btree_ops_ret *ret)
{
	struct btree_node *left = NULL, *right = NULL;

	ret->nrblks = 2;
	ret->blks[0] = test_do_copy(tree, left_blknr);
	ret->blks[1] = test_do_copy(tree, right_blknr);

	left = BLK2PTR(ret->blks[0]);
	right = BLK2PTR(ret->blks[1]);

	if (left->header.size > right->header.size) {
		btree_node_rebalance_left(tree, left, right, ret);
	} else if (right->header.size > left->header.size) {
		btree_node_rebalance_right(tree, left, right, ret);
	} else {
		assert(0); /* shouldn't be here */
	}
	ret->keys[0] = KEYREF(left, 0);
	ret->keys[1] = KEYREF(right, 0);
}

/**
 * mode could be either POLICY_MERGE_LEFT (merge with left)
 * or POLICY_MERGE_RIGHT (merge with right)
 */
static void btree_node_merge(struct btree *tree, int mode, blkptr_t left_blknr,
			     blkptr_t right_blknr, struct btree_ops_ret *ret)
{
	struct btree_node *left = NULL, *right = NULL;
	ret->pol = mode;
	ret->nrblks = 1;
	ret->blks[0] = test_do_copy(tree, left_blknr);

	left = BLK2PTR(ret->blks[0]);
	right = BLK2PTR(right_blknr);

	memcpy(KEYREF(left, left->header.size), KEYREF(right, 0),
	       right->header.size * KEY_SIZE);
	if (left->header.level > 0) {
		memcpy(PTRREF(left, left->header.size + right->header.size - 1),
		       PTRREF(right, right->header.size - 1),
		       right->header.size * sizeof(blkptr_t));
	} else {
		memcpy(VALUEREF(left, left->header.size + right->header.size - 1),
		       VALUEREF(right, right->header.size - 1),
		       right->header.size * VALUE_SIZE);
	}
	left->header.size += right->header.size;
	ret->keys[0] = KEYREF(left, 0);
	tree->free_block(tree, right_blknr);
}

static int
btree_node_search(struct btree *tree, struct btree_node *node, const void *key)
{
	/* TODO: use SIMD to speed this up? */
	int start = 0;
	int end = node->header.size;
	int mid = 0;
	int cmp = 0;

	if (end == 0) {
		return -1;
	}

	while (start < end) {
		mid = (start + end) / 2;
		cmp = tree->key_compare(key, KEYREF(node, mid));
		if (cmp == 0) {
			return mid;
		} else if (cmp < 0) {
			end = mid;
		} else {
			start = mid + 1;
		}
	}
	assert(start <= node->header.size);
	if (start == node->header.size
	    || tree->key_compare(key, KEYREF(node, start)) < 0) {
		return start - 1;
	} else {
		return start;
	}
}

void *btree_search(struct btree *tree, const void *key)
{
	blkptr_t next_blknr = tree->sb.root;
	struct btree_node *node = NULL;
	int idx = -1;

	if (tree->sb.root == 0)
		return NULL;

	while (1) {
		node = BLK2PTR(next_blknr);
		next_blknr = 0;
		idx = btree_node_search(tree, node, key);
		if (idx < 0) return NULL;
		if (node->header.level > 0) {
			next_blknr = *PTRREF(node, idx);
		} else if (idx < node->header.size && idx >= 0
			   && tree->key_compare(key, KEYREF(node,idx)) == 0) {
			return VALUEREF(node, idx);
		} else {
			return NULL;
		}
	}
}

void btree_first_pair(struct btree *tree, void **key_ret, void **valueref_ret)
{
	blkptr_t next_blknr = tree->sb.root;
	struct btree_node *node = NULL;
	while (1) {
		node = BLK2PTR(next_blknr);
		next_blknr = 0;
		if (node->header.level > 0) {
			next_blknr = *PTRREF(node, 0);
		} else {
			*key_ret = btree_node_keyref(tree, node, 0);
			*valueref_ret = btree_node_valueref(tree, node, 0);
			return;
		}
	}
}

void btree_last_pair(struct btree *tree, void **key_ret, void **valueref_ret)
{
	blkptr_t next_blknr = tree->sb.root;
	struct btree_node *node = NULL;
	int idx = 0;
	while (1) {
		node = BLK2PTR(next_blknr);
		next_blknr = 0;
		idx = node->header.size - 1;
		if (node->header.level > 0) {
			next_blknr = *PTRREF(node, idx);
		} else {
			*key_ret = btree_node_keyref(tree, node, idx);
			*valueref_ret = btree_node_valueref(tree, node, idx);
			return;
		}
	}
}

static void init_siblings(struct btree_node *node, int idx,
			  blkptr_t siblings[2])
{
	if (idx == 0) {
		siblings[0] = 0;
	} else {
		siblings[0] = *PTRREF(node, idx - 1);
	}
	if (idx == node->header.size - 1) {
		siblings[1] = 0;
	} else {
		siblings[1] = *PTRREF(node, idx + 1);
	}
}

static void btree_node_adjust_after_insert(struct btree *tree,
					   blkptr_t node_blknr,
					   blkptr_t siblings[2],
					   struct btree_ops_ret *ret)
{
	struct btree_node *node = BLK2PTR(node_blknr);
	struct btree_node *left = BLK2PTR(siblings[0]);
	struct btree_node *right = BLK2PTR(siblings[1]);

	(void) node;

	// fprintf(stderr, "adjusting...");
	if (left && btree_node_can_rebalance_to(tree, left)) {
		// fprintf(stderr, "rebalance with left\n");
		btree_node_rebalance(tree, siblings[0], node_blknr, ret);
	} else if (right && btree_node_can_rebalance_to(tree, right)) {
		// fprintf(stderr, "rebalance with right\n");
		btree_node_rebalance(tree, node_blknr, siblings[1], ret);
	} else {
		// fprintf(stderr, "split\n");
		btree_node_split(tree, node_blknr, ret);
	}
}

static void btree_node_insert(struct btree *tree, blkptr_t node_blknr,
			      blkptr_t siblings[2], void *key,
			      void *valueref, struct btree_ops_ret *ret);

static void btree_internal_insert(struct btree *tree, struct btree_node *node,
				  blkptr_t node_blknr, blkptr_t siblings[2],
				  int idx, void *key, void *valueref,
				  struct btree_ops_ret *ret)
{
	blkptr_t new_blknr = node_blknr;
	struct btree_node *new_node = NULL;
	blkptr_t next_level_blknr = *PTRREF(node, idx);
	blkptr_t child_siblings[2];
	struct btree_ops_ret child_ret;
	blkptr_t *ptrref = NULL;

	(void) ptrref;

	/* insert in the next level */
	init_siblings(node, idx, child_siblings);
	btree_node_insert(tree, next_level_blknr, child_siblings, key, valueref,
			  &child_ret);

	if (child_ret.nrblks != -1) {
		new_blknr = test_do_copy(tree, node_blknr);
		new_node = BLK2PTR(new_blknr);
		if (child_ret.nrblks == 1 && child_ret.pol == 0)
			*PTRREF(new_node, idx) = child_ret.blks[0];
	} else {
		ret->pol = 0;
		ret->nrblks = -1;
		return;
	}

	switch (child_ret.pol) {
	case POLICY_SPLIT:
		memmove(KEYREF(new_node, idx + 2), KEYREF(new_node, idx + 1),
			(new_node->header.size - idx - 1) * KEY_SIZE);
		memmove(PTRREF(new_node, new_node->header.size),
			PTRREF(new_node, new_node->header.size - 1),
			(new_node->header.size - idx - 1) * sizeof(blkptr_t));
		new_node->header.size++;

		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[0];
		memcpy(KEYREF(new_node, idx + 1), child_ret.keys[1], KEY_SIZE);
		*PTRREF(new_node, idx + 1) = child_ret.blks[1];
		break;
	case POLICY_REBALANCE_LEFT:
		/* left to right */
		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[0];
		memcpy(KEYREF(new_node, idx + 1), child_ret.keys[1], KEY_SIZE);
		*PTRREF(new_node, idx + 1) = child_ret.blks[1];
		break;
	case POLICY_REBALANCE_RIGHT:
		/* right to left */
		memcpy(KEYREF(new_node, idx - 1), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx - 1) = child_ret.blks[0];
		memcpy(KEYREF(new_node, idx), child_ret.keys[1], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[1];
		break;
	default:
		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		break;
	}

	if (child_ret.nrblks == 0) {
		ret->pol = ret->nrblks = 0;
		ret->keys[0] = new_node->keys;
	} else if (!btree_node_is_full(tree, new_node)) {
		ops_ret_init(ret, new_node->keys, node_blknr, new_blknr);
	} else {
		btree_node_adjust_after_insert(tree, new_blknr,
					       siblings, ret);
	}
}

static void btree_leaf_insert(struct btree *tree, struct btree_node *node,
			      blkptr_t node_blknr, blkptr_t sibilings[2],
			      int idx, void *key, void *valueref,
			      struct btree_ops_ret *ret)
{
	blkptr_t new_blknr = node_blknr;
	struct btree_node *new_node = NULL;
	if (idx >= 0 && tree->key_compare(key, KEYREF(node, idx)) == 0) {
		/* replace */
		if (memcmp(valueref, VALUEREF(node, idx), VALUE_SIZE) == 0) {
			ret->nrblks = -1;
			ret->pol = 0;
			return;
		}
		new_blknr = test_do_copy(tree, node_blknr);
		new_node = BLK2PTR(new_blknr);
		memcpy(VALUEREF(node, idx), valueref, VALUE_SIZE);
		goto nosplit;
	} else {
		new_blknr = test_do_copy(tree, node_blknr);
		new_node = BLK2PTR(new_blknr);
		memmove(KEYREF(new_node, idx + 2), KEYREF(new_node, idx + 1),
			(new_node->header.size - idx - 1) * KEY_SIZE);
		memcpy(KEYREF(new_node, idx + 1), key, KEY_SIZE);
		memmove(VALUEREF(new_node, new_node->header.size),
			VALUEREF(new_node, new_node->header.size - 1),
			(new_node->header.size - idx - 1) * VALUE_SIZE);
		memcpy(VALUEREF(new_node, idx + 1), valueref,
		       VALUE_SIZE);
		new_node->header.size++;
		tree->sb.size++;
		if (!btree_node_is_full(tree, new_node)) {
			goto nosplit;
		} else {
			goto adjust;
		}
	}
nosplit:
	ops_ret_init(ret, new_node->keys, node_blknr, new_blknr);
	return;
adjust:
	/* try to rebalance if possible, optimize for search :) */
	btree_node_adjust_after_insert(tree, new_blknr, sibilings, ret);
	return;
}

static void btree_node_insert(struct btree *tree, blkptr_t node_blknr,
			      blkptr_t siblings[2], void *key, void *valueref,
			      struct btree_ops_ret *ret)
{
	struct btree_node *node = BLK2PTR(node_blknr);
	int idx = btree_node_search(tree, node, key);

	if (node->header.level > 0) {
		if (idx < 0) idx = 0;
		btree_internal_insert(tree, node, node_blknr, siblings, idx,
				      key, valueref, ret);
	} else {
		btree_leaf_insert(tree, node, node_blknr, siblings, idx, key,
				  valueref, ret);
	}
}

void btree_insert(struct btree *tree, void *key, void *valueref)
{
	blkptr_t siblings[2];
	struct btree_ops_ret ret;

	/* because root is laze allocated */
	if (tree->sb.root == 0) {
		struct btree_node *root_node = NULL;
		tree->sb.root = tree->alloc_block(tree);
		root_node = BLK2PTR(tree->sb.root);
		root_node->header.level = 0;
		root_node->header.size = 0;
		root_node->header.generation = tree->sb.generation;
	}

	memset(siblings, 0, sizeof(blkptr_t) * 2);
	btree_node_insert(tree, tree->sb.root, siblings, key, valueref, &ret);
	if (ret.nrblks == 1) {
		/* update the superblock */
		tree->sb.root = ret.blks[0];
	} else if (ret.nrblks == 2) {
		/* tree root is full and need split */
		blkptr_t new_root = tree->alloc_block(tree);
		struct btree_node *new_node = BLK2PTR(new_root);

		tree->sb.level++;
		tree->sb.root = new_root;

		new_node->header.level = tree->sb.level;
		new_node->header.size = 2;
		memcpy(KEYREF(new_node, 0), ret.keys[0], KEY_SIZE);
		memcpy(KEYREF(new_node, 1), ret.keys[1], KEY_SIZE);
		memcpy(PTRREF(new_node, 0), &ret.blks[0], sizeof(blkptr_t));
		memcpy(PTRREF(new_node, 1), &ret.blks[1], sizeof(blkptr_t));
	}
}

static int btree_visit_node(struct btree *tree, struct btree_node *node,
			     btree_pointer_cb pointer_cb, btree_key_cb key_cb,
			     btree_value_cb value_cb, void *ptr)
{
	int i = 0;
	for (i = 0; i < node->header.size; i++) {
		if (key_cb && key_cb(tree, node, KEYREF(node, i), ptr) < 0)
			return -1;

		if (node->header.level == 0) {
			if (value_cb && value_cb(tree, node, VALUEREF(node, i),
						 ptr) < 0)
					return -1;
		} else {
			blkptr_t *ptrref = PTRREF(node, i);
			struct btree_node *child = BLK2PTR(*ptrref);
			if (pointer_cb && pointer_cb(tree, *ptrref, ptr) < 0)
				return -1;
			if (btree_visit_node(tree, child, pointer_cb, key_cb,
					     value_cb, ptr) < 0)
				return -1;
		}
	}
	return 0;
}

void btree_visit(struct btree *tree, btree_pointer_cb pointer_cb,
		 btree_key_cb key_cb, btree_value_cb value_cb, void *ptr)
{
	struct btree_node *root_node = NULL;

	if (tree->sb.root == 0)
		return;

	root_node = BLK2PTR(tree->sb.root);
	if (pointer_cb) pointer_cb(tree, tree->sb.root, ptr);
	btree_visit_node(tree, root_node, pointer_cb, key_cb, value_cb, ptr);
}

static void btree_node_adjust_after_delete(struct btree *tree,
					   blkptr_t node_blknr,
					   blkptr_t siblings[2],
					   struct btree_ops_ret *ret)
{
	struct btree_node *node = BLK2PTR(node_blknr);
	struct btree_node *left = BLK2PTR(siblings[0]);
	struct btree_node *right = BLK2PTR(siblings[1]);

	(void) node;

	// printf("adjusting after delete...");
	if (right && btree_node_can_rebalance_from(tree, right)) {
		// printf("rebalance with right sibling\n");
		btree_node_rebalance(tree, node_blknr, siblings[1], ret);
	} else if (left && btree_node_can_rebalance_from(tree, left)) {
		// printf("distribute with left sibling\n");
		btree_node_rebalance(tree, siblings[0], node_blknr, ret);
	} else if (left) {
		// printf("merge with left sibling\n");
		btree_node_merge(tree, POLICY_MERGE_LEFT, siblings[0],
				 node_blknr, ret);
	} else if (right) {
		// printf("merge with right siblings\n");
		btree_node_merge(tree, POLICY_MERGE_RIGHT, node_blknr,
				 siblings[1], ret);
	} else {
		assert(0);
	}
}


static void btree_node_delete(struct btree *tree, blkptr_t node_blknr,
			      blkptr_t siblings[2], void *key,
			      struct btree_ops_ret *ret);

static void btree_leaf_delete(struct btree *tree, struct btree_node *node,
			      blkptr_t node_blknr, blkptr_t siblings[2],
			      int idx, void *key,
			      struct btree_ops_ret *ret)
{
	blkptr_t new_blknr = 0;
	struct btree_node *new_node = NULL;

	assert(idx < node->header.size);
	if (idx < 0 || idx >= node->header.size
	    || tree->key_compare(KEYREF(node, idx), key) != 0) {
		ret->pol = 0;
		ret->nrblks = -1;
		return;
	}

 	new_blknr = test_do_copy(tree, node_blknr);
	new_node = BLK2PTR(new_blknr);

	memmove(KEYREF(new_node, idx), KEYREF(new_node, idx + 1),
		(new_node->header.size - idx - 1) * KEY_SIZE);
	memmove(VALUEREF(new_node, new_node->header.size - 2),
		VALUEREF(new_node, new_node->header.size - 1),
		(new_node->header.size - idx - 1) * VALUE_SIZE);
	new_node->header.size--;
	tree->sb.size--;

	if (node_blknr == tree->sb.root
	    || !btree_node_need_rebalance(tree, new_node)) {
		ops_ret_init(ret, new_node->keys, node_blknr, new_blknr);
	} else {
		btree_node_adjust_after_delete(tree, new_blknr, siblings, ret);
	}
}

static void btree_internal_delete(struct btree *tree, struct btree_node *node,
				  blkptr_t node_blknr, blkptr_t siblings[2],
				  int idx, void *key, struct btree_ops_ret *ret)
{
	blkptr_t new_blknr = node_blknr;
	struct btree_node *new_node = NULL;
	blkptr_t next_level_blknr = *PTRREF(node, idx);
	blkptr_t child_siblings[2];
	struct btree_ops_ret child_ret;
	blkptr_t *ptrref = NULL;

	(void) ptrref;

	init_siblings(node, idx, child_siblings);
	btree_node_delete(tree, next_level_blknr, child_siblings, key,
			  &child_ret);

	if (child_ret.nrblks != -1) {
		new_blknr = test_do_copy(tree, node_blknr);
		new_node = BLK2PTR(new_blknr);
		if (child_ret.nrblks == 1 && child_ret.pol == 0)
			*PTRREF(new_node, idx) = child_ret.blks[0];
	} else {
		ret->pol = 0;
		ret->nrblks = -1;
		return;
	}

	switch (child_ret.pol) {
	case POLICY_REBALANCE_LEFT:
		memcpy(KEYREF(new_node, idx - 1), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx - 1) = child_ret.blks[0];
		memcpy(KEYREF(new_node, idx), child_ret.keys[1], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[1];
		break;
	case POLICY_REBALANCE_RIGHT:
		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[0];
		memcpy(KEYREF(new_node, idx + 1), child_ret.keys[1], KEY_SIZE);
		*PTRREF(new_node, idx + 1) = child_ret.blks[1];
		break;
	case POLICY_MERGE_LEFT:
		memcpy(KEYREF(new_node, idx - 1), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx - 1) = child_ret.blks[0];
		memmove(KEYREF(new_node, idx), KEYREF(new_node, idx + 1),
			(new_node->header.size - idx - 1) * KEY_SIZE);
		memmove(PTRREF(new_node, new_node->header.size - 2),
			PTRREF(new_node, new_node->header.size - 1),
			(new_node->header.size - idx - 1) * sizeof(blkptr_t));
		new_node->header.size--;
		break;
	case POLICY_MERGE_RIGHT:
		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		*PTRREF(new_node, idx) = child_ret.blks[0];
		memmove(KEYREF(new_node, idx + 1), KEYREF(new_node, idx + 2),
			(new_node->header.size - idx - 2) * KEY_SIZE);
		memmove(PTRREF(new_node, new_node->header.size - 2),
			PTRREF(new_node, new_node->header.size - 1),
			(new_node->header.size - idx - 2) * sizeof(blkptr_t));
		new_node->header.size--;
		break;
	default:
		memcpy(KEYREF(new_node, idx), child_ret.keys[0], KEY_SIZE);
		break;
	}

	if (child_ret.nrblks == 0) {
		ret->pol = ret->nrblks = 0;
		ret->keys[0] = new_node->keys;
	} else if (node_blknr == tree->sb.root
		   || !btree_node_need_rebalance(tree, new_node)) {
		ops_ret_init(ret, new_node->keys, node_blknr, new_blknr);
	} else {
		btree_node_adjust_after_delete(tree, new_blknr, siblings, ret);
	}
}

static void btree_node_delete(struct btree *tree, blkptr_t node_blknr,
			      blkptr_t siblings[2], void *key,
			      struct btree_ops_ret *ret)
{
	struct btree_node *node = BLK2PTR(node_blknr);
	int idx = btree_node_search(tree, node, key);

	// printf("%s(): %p idx %d\n", __FUNCTION__, tree, idx);

	if (idx < 0) {
		/* cannot find a key to delete */
		fprintf(stderr, "cannot find key to delete\n");
		return;
	}

	if (node->header.level > 0) {
		btree_internal_delete(tree, node, node_blknr, siblings, idx,
				      key, ret);
	} else {
		btree_leaf_delete(tree, node, node_blknr, siblings, idx, key,
				  ret);
	}
}

void btree_delete(struct btree *tree, void *key)
{
	blkptr_t siblings[2];
	struct btree_ops_ret ret;
	struct btree_node *node = NULL;

	if (tree->sb.root == 0)
		return;

	memset(siblings, 0, sizeof(blkptr_t) * 2);
	btree_node_delete(tree, tree->sb.root, siblings, key, &ret);
	if (ret.nrblks == 1) {
		// printf("updated root %llu->%llu\n", tree->sb.root, ret.blks[0]);
		tree->sb.root = ret.blks[0];
	}
	node = BLK2PTR(tree->sb.root);
	if (node->header.size == 1 && node->header.level > 0) {
		blkptr_t oldroot = tree->sb.root;
		tree->sb.root = *PTRREF(node, 0);
		tree->free_block(tree, oldroot);
	} else if (node->header.level == 0 && node->header.size == 0) {
		tree->free_block(tree, tree->sb.root);
		tree->sb.root = 0;
	}
}

void btree_init(struct btree *tree, int (*compare)(const void*, const void*))
{
	tree->key_compare = compare;
	tree->max_nodes = BTREE_DATA_AREA
		/ (tree->sb.key_len + sizeof(blkptr_t));
	tree->max_values = BTREE_DATA_AREA
		/ (tree->sb.key_len + tree->sb.value_len);
	tree->min_nodes = tree->max_nodes / 2;
	tree->min_values = tree->max_values / 2;
}
