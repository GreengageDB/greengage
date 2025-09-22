#ifndef BLOOM_H
#define BLOOM_H

#include "postgres.h"

#include <stdint.h>

#define MAX_BLOOM_HASH_FUNCS 6
#define TOTAL_ELEMENTS 10000000UL

typedef struct
{
	uint8	   *current_bloom;
	uint32		size;			/* equal to bloom_size, half a map size */
	uint8		is_set_all;		/* indicates all bits are set */
	uint8		map[FLEXIBLE_ARRAY_MEMBER]; /* core bit array */
}	bloom_t;

static inline Size
full_bloom_size(uint32 size)
{
	return (offsetof(bloom_t, map) + size);
}

extern uint64 bloom_hash_seed;
extern int	bloom_hash_num;

void		bloom_init(const uint32 bloom_size, bloom_t *bloom);
void		init_bloom_invariants(void);
bool		bloom_isset(bloom_t *bloom, Oid relnode);
void		bloom_set_bits(bloom_t *bloom, Oid relnode);
void		bloom_set_all(bloom_t *bloom);
void		bloom_clear(bloom_t *bloom);
void		bloom_merge(bloom_t *dst, bloom_t *src);
void		bloom_copy(bloom_t *dst, bloom_t *src);
void		bloom_switch_current(bloom_t *bloom);
uint8	   *bloom_get_other(bloom_t *bloom);
void		bloom_merge_internal(bloom_t *bloom);

#endif   /* BLOOM_H */
