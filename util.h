#ifndef _UTIL_H_
#define _UTIL_H_

#include "misc.h"

int EditDistance(const char* a, int na, const char* b, int nb, int k);

unsigned int HammingDistance(const char* a, int na, const char* b, int nb,
			     int cur_dist);

#endif /* _UTIL_H_ */
