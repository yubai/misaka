#ifndef _PSTRING_H_
#define _PSTRING_H_

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#include "core.h"

/* threads and mutexes */
#define lock pthread_mutex_lock
#define trylock pthread_mutex_trylock
#define unlock pthread_mutex_unlock

typedef pthread_mutex_t mutex_t;

/* integer types */
typedef u_int64_t u64;
typedef u_int32_t u32;
typedef u_int16_t u16;
typedef u_int8_t  u8;

/* pascal string implementation */
/* the reason to choose pascal-string instead of c-string, is because we
 * need the length of the string so often that we want to
 *
 * 1. sort by the lenght of the string
 * 2. eliminate the cost of strlen as much as possible.
 */

static inline int
pstrlen(const char *s)
{
	return (unsigned char) s[0];
}

/* so that you can dump this into qsort or something... */
static inline int
pstrcmp(const void *p, const void *q)
{
	const char *a = p;
	const char *b = q;
	int i = 0;
	/* this rocks for pascal string! */
	int len = pstrlen(a) < pstrlen(b) ? pstrlen(a) : pstrlen(b);
	for (i = 0; i <= len; i++) {
		if (a[i] == b[i]) continue;
		else if (a[i] < b[i]) return -1;
		else return 1;
	}
	return 0;
}

typedef char word_t[MAX_WORD_LENGTH + 1];

#undef offsetof
#ifdef __compiler_offsetof
#define offsetof(TYPE,MEMBER) __compiler_offsetof(TYPE,MEMBER)
#else
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

#define container_of(ptr, type, member)                                 \
	({                                                              \
		const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
		(type *) ( (char *)__mptr - offsetof(type, member) );}) \

struct list_head {
	struct list_head *prev, *next;
};

static inline void
list_init(struct list_head *head)
{
	head->prev = head->next = head;
}

static inline void
list_add(struct list_head *new, struct list_head *head)
{
	new->next = head->next;
	new->prev = head;
	head->next->prev = new;
	head->next = new;
}

static inline void
list_add_tail(struct list_head *new, struct list_head *head)
{
	new->next = head;
	new->prev = head->prev;
	head->prev->next = new;
	head->prev = new;
}

static inline void
list_del(struct list_head *entry)
{
	entry->prev->next = entry->next;
	entry->next->prev = entry->prev;
	entry->next = entry->prev = NULL;
}

static inline int
list_empty(const struct list_head * head)
{
    return head->next == head;
}

static inline void
list_visit(struct list_head *head, int (*routine)(struct list_head *, void *),
	   void *ptr)
{
	struct list_head *ent = head->next;
	while (ent != head) {
		struct list_head *next = ent->next;
		if (routine(ent, ptr) < 0)
			break;
		ent = next;
	}
}

static inline void bitmap_set_bit(u8 *mem, unsigned long pos)
{
	unsigned long byte_pos = (pos >> 3);
	unsigned long idx = (pos & 0x07);
	unsigned long mask = (1 << idx);
	mem[byte_pos] |= mask;
}

static inline void bitmap_clear_bit(u8 *mem, unsigned long pos)
{
	unsigned long byte_pos = (pos >> 3);
	unsigned long idx = (pos & 0x07);
	unsigned long mask = (1 << idx);
	mem[byte_pos] &= ~mask;
}

static inline int bitmap_is_bit_set(u8 *mem, unsigned long pos)
{
	unsigned long byte_pos = (pos >> 3);
	unsigned long idx = (pos & 0x07);
	unsigned long mask = (1 << idx);
	return (mem[byte_pos] & mask) != 0;
}

static inline void bitmap_reset(u8 *mem, unsigned long nr_bits)
{
	/* nr_bits should be aligned */
	assert(nr_bits % 8 == 0);
	memset(mem, 0, nr_bits / 8);
}

static inline void bitmap_reset_range(u8 *mem, unsigned long start,
				      unsigned long end)
{
	unsigned long sb = start >> 3;
	unsigned long eb = ((end - 1) >> 3) + 1;
	memset(mem + sb, 0, eb - sb + 1);
}


/* an append only queue */
struct queue {
	int nobjs;
	int obj_size;
	int chunk_capacity;
	int nchunks;
	int nobjs_per_chunk;
	struct list_head chunk_list;
};

void queue_init(struct queue *q, int obj_size, int chunk_capacity);
void queue_append(struct queue *q, void *obj);
void queue_destroy(struct queue *q);

#define PREFETCH(ptr)				\
	asm ("nopl %0"				\
	     : :"m"(ptr))			\

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

/* spinlocks */

#define barrier() asm volatile("": : :"memory")

/* Atomic exchange (of various sizes) */
static inline void *xchg_64(void *ptr, void *x)
{
	__asm__ __volatile__("xchgq %0,%1"
				:"=r" ((unsigned long long) x)
				:"m" (*(volatile long long *)ptr), "0" ((unsigned long long) x)
				:"memory");

	return x;
}

typedef struct k42lock k42lock;

struct k42lock {
	k42lock *next;
	k42lock *tail;
};

#define cmpxchg(P, O, N) __sync_val_compare_and_swap((P), (O), (N))
#define atomic_xadd(P, V) __sync_fetch_and_add((P), (V))
#define cpu_relax() asm volatile("pause\n": : :"memory")

static inline void k42_lock(k42lock *l)
{
	k42lock me;
	k42lock *pred, *succ;
	me.next = NULL;

	barrier();

	pred = xchg_64(&l->tail, &me);
	if (pred) {
		me.tail = (void *) 1;

		barrier();
		pred->next = &me;
		barrier();

		while (me.tail) cpu_relax();
	}

	succ = me.next;

	if (!succ) {
		barrier();
		l->next = NULL;

		if (cmpxchg(&l->tail, &me, &l->next) != &me)
		{
			while (!me.next) cpu_relax();

			l->next = me.next;
		}
	} else {
		l->next = succ;
	}
}

static inline void k42_unlock(k42lock *l)
{
	k42lock *succ = l->next;

	barrier();

	if (!succ) {
		if (cmpxchg(&l->tail, &l->next, NULL) == (void *) &l->next)
			return;

		while (!l->next) cpu_relax();
		succ = l->next;
	}

	succ->tail = NULL;
}

static inline int k42_trylock(k42lock *l)
{
	if (!cmpxchg(&l->tail, NULL, &l->next)) return 0;
	return EBUSY;
}

typedef union ticketlock ticketlock;

union ticketlock
{
	unsigned u;
	struct
	{
		unsigned short ticket;
		unsigned short users;
	} s;
};

static inline void ticket_lock(ticketlock *t)
{
	unsigned short me = atomic_xadd(&t->s.users, 1);

	while (t->s.ticket != me) cpu_relax();
}

static inline void ticket_unlock(ticketlock *t)
{
	barrier();
	t->s.ticket++;
}

static inline int ticket_trylock(ticketlock *t)
{
	unsigned short me = t->s.users;
	unsigned short menew = me + 1;
	unsigned cmp = ((unsigned) me << 16) + me;
	unsigned cmpnew = ((unsigned) menew << 16) + me;

	if (cmpxchg(&t->u, cmp, cmpnew) == cmp) return 0;

	return EBUSY;
}

static inline int ticket_lockable(ticketlock *t)
{
	ticketlock u = *t;
	barrier();
	return (u.s.ticket == u.s.users);
}

#endif /* _PSTRING_H_ */
