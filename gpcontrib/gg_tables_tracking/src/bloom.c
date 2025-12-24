/*
 * Simple bloom filter without using postgres primitives.
 */

#include <math.h>

#include "gg_tables_tracking_guc.h"
#include "hashimpl.h"
#include "track_bloom.h"
#include "tf_shmem.h"

uint64		bloom_hash_seed;
int			bloom_hash_num;

static inline uint32
mod_m(uint32 val, uint64 m)
{
	if (((m - 1) & m) == 0)
		return val & (m - 1);
	else
		return val % m;
}

/*
 * Generate k independent bit positions in a Bloom filter.
 *
 * Implements Enhanced Double Hashing technique (Dillinger & Manolios, 2004) which
 * generates k hash values using only 2 independent hash functions. This approach
 * provides comparable performance to using k independent hash functions while
 * being more computationally efficient.
 *
 * Algorithm:
 * 1. Generate two independent 32-bit hashes (x, y) from a 64-bit wyhash
 * 2. Apply modulo operation to fit within filter size
 * 3. Generate subsequent indices using linear combination: x = (x + y) mod m
 *														  y = (y + i) mod m
 *
 * Parameters:
 * node		   - relation file node OID to hash
 * bloom_size  - size of Bloom filter in bytes
 * out_hashes  - output array to store k bit positions
 *
 * Reference: GPDB7 codebase.
 */
static void
tracking_hashes(Oid node, uint32 bloom_size, uint32 *out_hashes)
{
	uint64		hash;
	uint32		x,
				y;
	uint64		m;
	int			i;

	/* Use 64-bit hashing to get two independent 32-bit hashes */
	hash = wyhash(node, bloom_hash_seed);
	x = (uint32) hash;
	y = (uint32) (hash >> 32);
	m = bloom_size * 8;

	x = mod_m(x, m);
	y = mod_m(y, m);

	/* Accumulate hashes */
	out_hashes[0] = x;
	for (i = 1; i < bloom_hash_num; i++)
	{
		x = mod_m(x + y, m);
		y = mod_m(y + i, m);

		out_hashes[i] = x;
	}
}

/*
* Test membership of an element in Bloom filter
*
* Implements standard Bloom filter membership test by checking k different bit
* positions. The function provides probabilistic set membership with controllable
* false positive rate.
*
* Returns true if element might be in set, false if definitely not in set.
*/
bool
bloom_isset(bloom_t *bloom, Oid relnode)
{
	uint32		hashes[MAX_BLOOM_HASH_FUNCS];

	if (bloom->is_set_all)
		return true;

	tracking_hashes(relnode, bloom->size, hashes);

	for (int i = 0; i < bloom_hash_num; ++i)
	{
		if (!(bloom->current_bloom[hashes[i] >> 3] & (1 << (hashes[i] & 7))))
			return false;
	}
	return true;
}

/*
 * Insert an element into Bloom filter
 *
 * Sets k bits in the Bloom filter's bit array corresponding to the k hash
 * values generated for the input element. This operation is irreversible -
 * elements cannot be removed without rebuilding the entire filter.
 *
 * Parameters:
 * bloom	- pointer to Bloom filter structure
 * relnode	- relation file node OID to insert
 */
void
bloom_set_bits(bloom_t *bloom, Oid relnode)
{
	uint32		hashes[MAX_BLOOM_HASH_FUNCS];

	tracking_hashes(relnode, bloom->size, hashes);
	for (int i = 0; i < bloom_hash_num; ++i)
	{
		bloom->current_bloom[hashes[i] >> 3] |= 1 << (hashes[i] & 7);
	}
}

void
bloom_init(const uint32 bloom_size, bloom_t *bloom)
{
	bloom->size = bloom_size;
	bloom->current_bloom = bloom->map;
	bloom_clear(bloom);
}

/*
 * Initialize optimal Bloom filter parameters
 *
 * This function calculates and sets optimal parameters for the Bloom filter
 * based on established widespread principles.
 *
 * Calculates the optimal number of hash functions using the formula:
 * k = (m/n)ln(2), which minimizes the false positive probability
 * p = (1 - e^(-kn/m))^k.
 * where:
 * - m = total_bits (size of bit array)
 * - n = TOTAL_ELEMENTS (expected number of insertions)
 *
 * Initializes bloom_hash_seed with a random value to prevent deterministic
 * hash collisions and ensure independent hash distributions across runs.
 */
void
init_bloom_invariants(void)
{
	int			k = rint(log(2.0) * (bloom_size * 8) / TOTAL_ELEMENTS);

	bloom_hash_num = Max(1, Min(k, MAX_BLOOM_HASH_FUNCS));
	bloom_hash_seed = (uint64) random();
}

void
bloom_set_all(bloom_t *bloom)
{
	memset(bloom->current_bloom, 0xFF, bloom->size);
	bloom->is_set_all = 1;
}

void
bloom_clear(bloom_t *bloom)
{
	memset(bloom->current_bloom, 0, bloom->size);
	bloom->is_set_all = 0;
}

void
bloom_merge(bloom_t *dst, bloom_t *src)
{
	if (src->is_set_all)
	{
		memset(dst->current_bloom, 0xFF, dst->size);
		dst->is_set_all = src->is_set_all;
		return;
	}

	for (uint32 i = 0; i < dst->size; i++)
		dst->current_bloom[i] |= src->current_bloom[i];
}

void
bloom_copy(bloom_t *dest, bloom_t *src)
{
	dest->size = src->size;
	memcpy(dest->current_bloom, src->current_bloom, src->size);
	dest->is_set_all = src->is_set_all;
}

void
bloom_switch_current(bloom_t *bloom)
{
	uint8	   *map_base = bloom->map;
	uint8	   *map_off = bloom->map + bloom->size;

	bloom->current_bloom = (bloom->current_bloom == map_base) ? map_off : map_base;
	bloom->is_set_all = false;
}

uint8 *
bloom_get_other(bloom_t *bloom)
{
	uint8	   *map_base = bloom->map;
	uint8	   *map_off = bloom->map + bloom->size;

	return (bloom->current_bloom == map_base) ? map_off : map_base;
}

void
bloom_merge_internal(bloom_t *bloom)
{
	if (bloom->is_set_all)
		return;

	uint8	   *bloom_other = bloom_get_other(bloom);

	for (uint32 i = 0; i < bloom->size; i++)
		bloom->current_bloom[i] |= bloom_other[i];
}
