/*-------------------------------------------------------------------------
 *
 * memtup.h
 *	  In Memory Tuple format
 *
 * Portions Copyright (c) 2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/memtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMTUP_H
#define MEMTUP_H

#include "access/tupdesc.h"
#include "access/tupmacs.h"

typedef enum MemTupleBindFlag
{
	MTB_ByVal_Native = 1,	/* Fixed len, native (returned as datum ) */
	MTB_ByVal_Ptr    = 2,	/* Fixed len, convert to pointer for datum */
	MTB_ByRef  	 = 3,	/* var len */
	MTB_ByRef_CStr   = 4,   /* varlen, CString type */
} MemTupleBindFlag;

typedef struct MemTupleAttrBinding
{
	int offset; 		/* offset of attr in memtuple */
	int len;				/* attribute length */
	int len_aligned;		/* attribute length, padded for aligning the physically following attribute */
	MemTupleBindFlag flag;	/* binding flag */
	int null_byte;		/* which byte holds the null flag for the attr */
	unsigned char null_mask;		/* null bit mask */
} MemTupleAttrBinding;

typedef struct MemTupleBindingCols
{
	uint32 var_start; 	/* varlen fields start */
	MemTupleAttrBinding *bindings; /* bindings for attrs (cols) */
	short *null_saves;		/* saved space from each attribute when null - uses aligned length */
} MemTupleBindingCols;

typedef struct MemTupleBinding
{
	TupleDesc tupdesc;
	int column_align;
	int null_bitmap_extra_size;  /* extra bytes required by null bitmap */
	int natts; 			/* number of attributes in memtuple (note: it could be smaller than tupdesc->natts) */

	MemTupleBindingCols bind;  	/* 2 bytes offsets */
	MemTupleBindingCols large_bind; /* large tup, 4 bytes offsets */
} MemTupleBinding;

typedef struct MemTupleData
{
	uint32 PRIVATE_mt_len;
	unsigned char PRIVATE_mt_bits[1]; 	/* varlen */
} MemTupleData;

typedef MemTupleData *MemTuple;

#define MEMTUP_LEAD_BIT 0x80000000
#define MEMTUP_LEN_MASK 0x3FFFFFF8
#define MEMTUP_HASNULL   1
#define MEMTUP_LARGETUP  2
#define MEMTUP_HASEXTERNAL 	 4

#define MEMTUP_ALIGN(LEN) TYPEALIGN(8, (LEN)) 
#define MEMTUPLE_LEN_FITSHORT 0xFFF0

static inline bool is_len_memtuplen(uint32 len)
{
	return (len & MEMTUP_LEAD_BIT) != 0;
}
static inline bool memtuple_lead_bit_set(MemTuple tup)
{
	return (tup->PRIVATE_mt_len & MEMTUP_LEAD_BIT) != 0;
}
static inline uint32 memtuple_get_size(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_LEN_MASK);
}
static inline void memtuple_set_mtlen(MemTuple mtup, uint32 mtlen)
{
	Assert((mtlen & MEMTUP_LEAD_BIT) != 0);
	mtup->PRIVATE_mt_len = mtlen;
}
static inline bool memtuple_get_hasnull(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_HASNULL) != 0;
}
static inline void memtuple_set_hasnull(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_HASNULL;
}
static inline bool memtuple_get_islarge(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_LARGETUP) != 0;
}
static inline void memtuple_set_islarge(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_LARGETUP; 
}
static inline bool memtuple_get_hasext(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	return (mtup->PRIVATE_mt_len & MEMTUP_HASEXTERNAL) != 0;
}
static inline void memtuple_set_hasext(MemTuple mtup)
{
	Assert(memtuple_lead_bit_set(mtup));
	mtup->PRIVATE_mt_len |= MEMTUP_HASEXTERNAL;
}


/* Compute how much bytes are saved by one byte in the null bit map */
static inline short
compute_null_save_b(short *null_saves, unsigned char b)
{
	unsigned int blow = (b & 0xF);
	unsigned int bhigh = (b >> 4);

	return null_saves[blow] + null_saves[16 + bhigh];
}

/* compute the null saved bytes by the whole null bit map, by the attribute
 * physically precedes the one.
 */
static inline int
compute_null_save(short *null_saves, unsigned char *nullbitmaps, int nbyte, unsigned char nbit)
{
	int			ret = 0;
	int			curr_byte = 0;

	while (curr_byte < nbyte)
	{
		ret += compute_null_save_b(null_saves, nullbitmaps[curr_byte]);
		null_saves += 32;
		++curr_byte;
	}

	ret += compute_null_save_b(null_saves, (nullbitmaps[nbyte] & (nbit - 1)));
	return ret;
}

static pg_attribute_always_inline char *
memtuple_get_attr_ptr(char *start, MemTupleAttrBinding *bind, short *null_saves, unsigned char *nullp)
{
	int			ns = 0;

	if (nullp)
		ns = compute_null_save(null_saves, nullp, bind->null_byte, bind->null_mask);
	return start + bind->offset - ns;
}

static pg_attribute_always_inline char *
memtuple_get_attr_data_ptr(char *start, MemTupleAttrBinding *bind, short *null_saves, unsigned char *nullp)
{
	if (bind->flag == MTB_ByVal_Native || bind->flag == MTB_ByVal_Ptr)
		return memtuple_get_attr_ptr(start, bind, null_saves, nullp);

	if (bind->len == 2)
		return start + (*(uint16 *) memtuple_get_attr_ptr(start, bind, null_saves, nullp));

	Assert(bind->len == 4);
	return start + (*(uint32 *) memtuple_get_attr_ptr(start, bind, null_saves, nullp));
}

static inline unsigned char *
memtuple_get_nullp(MemTuple mtup)
{
	return mtup->PRIVATE_mt_bits;
}

/*
 * The value of an attribute of a memtuple, with attnum 1-based. A scan calls
 * this for every value, also from an extension, where only an inline function
 * can be inlined.
 */
static pg_attribute_always_inline Datum
memtuple_getattr_inline(MemTuple mtup, MemTupleBinding *pbind, int attnum, bool *isnull)
{
	bool		hasnull = memtuple_get_hasnull(mtup);
	unsigned char *nullp = hasnull ? memtuple_get_nullp(mtup) : NULL;
	char	   *start = (char *) mtup + (hasnull ? pbind->null_bitmap_extra_size : 0);

	Datum		ret;
	MemTupleBindingCols *colbind = memtuple_get_islarge(mtup) ? &pbind->large_bind : &pbind->bind;
	MemTupleAttrBinding *attrbind;

	Assert(mtup && pbind && pbind->tupdesc);
	Assert(attnum > 0 && attnum <= pbind->tupdesc->natts);

	if (isnull)
		*isnull = false;

	/* input attnum is 1 based.  Make it 0 based */
	--attnum;
	attrbind = &(colbind->bindings[attnum]);

	/* null check */
	if (hasnull && (nullp[attrbind->null_byte] & attrbind->null_mask))
	{
		if (isnull)
			*isnull = true;
		return 0;
	}

	short	   *null_saves = colbind->null_saves;

	Assert(null_saves);

	ret = fetchatt(TupleDescAttr(pbind->tupdesc, attnum), memtuple_get_attr_data_ptr(start, attrbind, null_saves, nullp));

	return ret;
}

extern void destroy_memtuple_binding(MemTupleBinding *pbind);
extern MemTupleBinding* create_memtuple_binding(TupleDesc tupdesc, int expected_natts);

extern Datum memtuple_getattr(MemTuple mtup, MemTupleBinding *pbind, int attnum, bool *isnull);

extern uint32 compute_memtuple_size(MemTupleBinding *pbind, Datum *values, bool *isnull, uint32 *nullsaves, bool *has_nulls);

extern MemTuple memtuple_form(MemTupleBinding *pbind, Datum *values, bool *isnull);
extern MemTuple memtuple_form_to(MemTupleBinding *pbind, Datum *values, bool *isnull,
								 uint32 len, uint32 null_save_len, bool hasnull,
								 MemTuple mtup);
extern void memtuple_deform(MemTuple mtup, MemTupleBinding *pbind, Datum *datum, bool *isnull);

extern bool MemTupleHasExternal(MemTuple mtup, MemTupleBinding *pbind);

#endif /* MEMTUP_H */
