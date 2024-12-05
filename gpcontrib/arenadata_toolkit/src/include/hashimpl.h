#ifndef HASHIMPL_H
#define HASHIMPL_H

#include "c.h"

#define _wyrot(x) (((x)>>32)|((x)<<32))

/*
 * This is an adaptation of WyHash - a fast, modern non-cryptographic hash function.
 *
 * Originally designed by Wang Yi, whose hash implementation is published
 * without license.
 *
 * Core components:
 * _wymum: Implements multiplication-based mixing
 * _wymix: Two-step mixing function
 *	 - Combines _wymum multiplication with XOR operations
 *	 - Enhances bit diffusion and avalanche properties
 * wyhash: Main hashing function
 *	 - Processes 32-bit keys with a seed value
 * For Bloom filter implementation this hash is considered having
 * better statistical properties than Postgres's default Jenkins hash.
 */
static inline void
_wymum(uint64 *A, uint64 *B)
{
#if(SIZEOF_VOID_P < 8)
	uint64		hh = (*A >> 32) * (*B >> 32),
				hl = (*A >> 32) * (uint32) *B,
				lh = (uint32) *A * (*B >> 32),
				ll = (uint64) (uint32) *A * (uint32) *B;

	*A = _wyrot(hl) ^ hh;
	*B = _wyrot(lh) ^ ll;
#elif defined(HAVE_INT128)
	uint128		r = *A;

	r *= *B;
	*A = (uint64) r;
	*B = (uint64) (r >> 64);
#else
	uint64		ha = *A >> 32,
				hb = *B >> 32,
				la = (uint32) *A,
				lb = (uint32) *B,
				hi,
				lo;
	uint64		rh = ha * hb,
				rm0 = ha * lb,
				rm1 = hb * la,
				rl = la * lb,
				t = rl + (rm0 << 32),
				c = t < rl;

	lo = t + (rm1 << 32);
	c += lo < t;
	hi = rh + (rm0 >> 32) + (rm1 >> 32) + c;
	*A = lo;
	*B = hi;
#endif
}

static inline uint64 _wymix(uint64 A, uint64 B)
{
	_wymum(&A, &B);
	return A ^ B;
}

static inline uint64
wyhash(uint32 key, uint64 seed)
{
	seed ^= _wymix(seed ^ 0x2d358dccaa6c78a5ull, 0x8bb84b93962eacc9ull);
#if (WORDS_BIGENDIAN)
#if defined(HAVE__BUILTIN_BSWAP32)
	key = __builtin_bswap32(key);
#else
	key = (((key >> 24) & 0xff) | ((key >> 8) & 0xff00) | ((key << 8) & 0xff0000) | ((key << 24) & 0xff000000));
#endif
#endif
	uint64		a = ((uint64) key << 32) | key;
	uint64		b = 0;

	a ^= 0x8bb84b93962eacc9ull;
	b ^= seed;
	_wymum(&a, &b);
	return _wymix(a ^ 0x2d358dccaa6c78a5ull ^ 4, b ^ 0x8bb84b93962eacc9ull);
}

#endif   /* HASHIMPL_H */
