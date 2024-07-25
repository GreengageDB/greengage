/*-------------------------------------------------------------------------
 *
 * pg_attribute_encoding.h
 *	  some where to stash column level ENCODING () clauses
 *
 * GPDB_90_MERGE_FIXME: pg_attribute now has an attoptions field. We should
 * get rid of this table, and start using pg_attribute.attoptions instead.
 *
 * Portions Copyright (c) EMC, 2011
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_attribute_encoding.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ATTRIBUTE_ENCODING_H
#define PG_ATTRIBUTE_ENCODING_H

#include "catalog/genbki.h"
#include "catalog/pg_attribute_encoding_d.h"
#include "utils/rel.h"

/*
 * Shorthand for range of segfiles for a specific attnum.
 * For eg: filenum = 1 denotes a range of segfiles relfilenode.1 - relfilenode.128.
 * FileNumbers start at 1
 */
typedef int16 FileNumber;

#define InvalidFileNumber		0
#define MaxFileNumber			2 * MaxHeapAttributeNumber

/* ----------------
 *		pg_attribute_encoding definition.  cpp turns this into
 *		typedef struct FormData_pg_attribute_encoding
 * ----------------
 */
CATALOG(pg_attribute_encoding,6231,AttributeEncodingRelationId)
{
	Oid		attrelid;
	int16	attnum;
	int16   filenum;
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	int64 	lastrownums[1]; 	/* Last row number of each segfile when this attribute is added.
					   This is populated up to the highest numbered segfile and can
					   have a max length of MAX_AOREL_CONCURRENCY. */
	text	attoptions[1];	
#endif
} FormData_pg_attribute_encoding;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(attrelid REFERENCES pg_attribute(attrelid));

/* ----------------
 *		Form_pg_attribute_encoding corresponds to a pointer to a tuple with
 *		the format of pg_attribute_encoding relation.
 * ----------------
 */
typedef FormData_pg_attribute_encoding *Form_pg_attribute_encoding;


extern PGFunction *get_funcs_for_compression(char *compresstype);
extern StdRdOptions **RelationGetAttributeOptions(Relation rel);
extern List **RelationGetUntransformedAttributeOptions(Relation rel);

extern Datum transform_lastrownums(int64 *lastrownums);
extern void add_attribute_encoding_entry(Oid relid, AttrNumber attnum, FileNumber filenum, Datum lastrownums, Datum attoptions);
extern void update_attribute_encoding_entry(Oid relid, AttrNumber attnum, FileNumber newfilenum, Datum newlastrownums, Datum newattoptions);
extern void AddCOAttributeEncodings(Oid relid, List *attr_encodings);
extern void AddOrUpdateCOAttributeEncodings(Oid relid, List *attr_encodings);
extern void RemoveAttributeEncodingsByRelid(Oid relid);
extern void CloneAttributeEncodings(Oid oldrelid, Oid newrelid, AttrNumber max_attno);
extern void UpdateAttributeEncodings(Oid relid, List *new_attr_encodings);
extern void UpdateOrAddAttributeEncodingsAttoptionsOnly(Relation rel, List *new_attr_encodings);
extern void ClearAttributeEncodingLastrownums(Oid relid);
extern void ClearAttributeEncodingLastrownumsByAttnum(Oid relid, int attnum);
extern FileNumber GetFilenumForAttribute(Oid relid, AttrNumber attnum);
extern FileNumber GetFilenumForRewriteAttribute(Oid relid, AttrNumber attnum);
extern List *GetNextNAvailableFilenums(Oid relid, int n);
extern int64 *GetAttnumToLastrownumMapping(Oid relid, int natts);
extern bool *ExistValidLastrownums(Oid relid, int natts);
extern Datum *get_rel_attoptions(Oid relid, AttrNumber max_attno);
extern List * rel_get_column_encodings(Relation rel);

#endif   /* PG_ATTRIBUTE_ENCODING_H */
