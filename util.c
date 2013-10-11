#include <limits.h>
#include <string.h>

#include "util.h"
#include "core.h"
#include "misc.h"

/**
 * Computes edit distance between a null-terminated string "a" with length "na"
 * and a null-terminated string "b" with length "nb"
 *
 * assuming na >= nb
 */
int EditDistance(const char* a, int na, const char* b, int nb, int curr_dist)
{
	int T[2][MAX_WORD_LENGTH+1];
	int ia, ib;
	int eb = curr_dist;
	int cur=0;

	for(ib = 0; ib <= nb; ib++)
		T[cur][ib] = ib;

	cur = 1-cur;

	for (ia = 1; ia <= na; ia++) {
		// for(ib=0;ib<=nb;ib++)
		//  	T[cur][ib]= MAX_DIST + 1;
		int min_line = INT_MAX;
		T[cur][0] = ia;
		for (ib = 1; ib <= eb; ib++) {
			int ret = T[1-cur][ib] + 1;
			int d2 = T[cur][ib-1]+1;
			int d3 = T[1-cur][ib-1] + (a[ia-1]!=b[ib-1]);

			if (d2<ret) ret=d2;
			if (d3<ret) ret=d3;

			T[cur][ib] = ret;
			if (ret < min_line)
				min_line = ret;
		}

		if (min_line >= curr_dist) {
			return MAX_DIST + 1;
		}

		for (ib = eb + 1; ib <= nb; ib++) {
			T[cur][ib] = MAX_DIST + 1;
		}
		if (++eb > nb) eb = nb;
		cur = 1 - cur;
	}

	int ret = T[1 - cur][nb];

	return ret;
}

/**
 * Computes Hamming distance between a null-terminated string "a" with length "na"
 * and a null-terminated string "b" with length "nb"
 */
unsigned int HammingDistance(const char* a, int na, const char* b, int nb,
			     int curr_dist)
{
	int j;
	unsigned int num_mismatches=0;
	for(j=0;j<na;j++) {
		if(a[j]!=b[j]) num_mismatches++;
		// if (num_mismatches >= curr_dist)
		//  	return MAX_DIST + 1;
	}
	return num_mismatches;
}
