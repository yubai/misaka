#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "../btree.h"
#include "../misc.h"

static int *arr = NULL;
static int arr_len = 0;

static void gen_arr(int len)
{
	int i = 0;
	if (arr == NULL)
		arr = malloc(sizeof(int) * len);
	for (i = 0; i < len; i++) {
		arr[i] = rand();
	}
	arr_len = len;
}

static int int_compare(const void *p, const void *q)
{
	const int *a = p;
	const int *b = q;
	return *a - *b;
}

static void test_qsort()
{
	struct timeval s, e;
	gettimeofday(&s, NULL);
	qsort(arr, arr_len, sizeof(int), int_compare);
	gettimeofday(&e, NULL);
	printf("%ld ms\n", (e.tv_sec - s.tv_sec) * 1000
	       + (e.tv_usec - s.tv_usec) / 1000);
}

static void test_btree()
{
	struct btree *tree = btree_mem_new(sizeof(int), 1, int_compare);
	int i = 0;
	u8 dummy = 0;
	struct timeval s, e;
	gettimeofday(&s, NULL);
	for (i = 0; i < arr_len; i++) {
		btree_insert(tree, &arr[i], &dummy);
	}
	gettimeofday(&e, NULL);
	btree_mem_destroy(tree);
	printf("%ld ms\n", (e.tv_sec - s.tv_sec) * 1000
	       + (e.tv_usec - s.tv_usec) / 1000);
}

#define MAX_LEN 800000

int main(int argc, char* argv[])
{
	gen_arr(MAX_LEN);
	test_qsort();
	gen_arr(MAX_LEN);
	test_btree();
	free(arr);
	return 0;
}
