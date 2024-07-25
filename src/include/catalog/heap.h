/*-------------------------------------------------------------------------
 *
 * heap.h
 *	  prototypes for functions in backend/catalog/heap.c
 *
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/heap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAP_H
#define HEAP_H

#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "parser/parse_node.h"


/* flag bits for CheckAttributeType/CheckAttributeNamesTypes */
#define CHKATYPE_ANYARRAY		0x01	/* allow ANYARRAY */
#define CHKATYPE_ANYRECORD		0x02	/* allow RECORD and RECORD[] */
#define CHKATYPE_IS_PARTKEY		0x04	/* attname is part key # not column */

typedef struct RawColumnDefault
{
	AttrNumber	attnum;			/* attribute to attach default to */
	Node	   *raw_default;	/* default value (untransformed parse tree) */
	bool		missingMode;	/* true if part of add column processing */
	bool		hasCookedMissingVal;
	Datum		missingVal;
	bool		missingIsNull;
	char		generated;		/* attgenerated setting */
} RawColumnDefault;

typedef struct CookedConstraint
{
	/*
	 * In PostgreSQL, this struct is only during CREATE TABLE processing, but
	 * in GPDB, we create these in the QD and dispatch pre-built
	 * CookedConstraints to the QE nodes, in the CreateStmt. That's why we
	 * need to have a node tag and copy/out/read function support for this
	 * in GPDB.
	 */
	NodeTag		type;
	ConstrType	contype;		/* CONSTR_DEFAULT or CONSTR_CHECK */
	Oid			conoid;			/* constr OID if created, otherwise Invalid */
	char	   *name;			/* name, or NULL if none */
	AttrNumber	attnum;			/* which attr (only for DEFAULT) */
	Node	   *expr;			/* transformed default or check expr */
	bool		skip_validation;	/* skip validation? (only for CHECK) */
	bool		is_local;		/* constraint has local (non-inherited) def */
	int			inhcount;		/* number of times constraint is inherited */
	bool		is_no_inherit;	/* constraint has local def and cannot be
								 * inherited */
	/*
	 * Remember to update copy/out/read functions if new fields are added
	 * here!
	 */
} CookedConstraint;

extern Relation heap_create(const char *relname,
							Oid relnamespace,
							Oid reltablespace,
							Oid relid,
							Oid relfilenode,
							Oid accessmtd,
							TupleDesc tupDesc,
							char relkind,
							char relpersistence,
							bool shared_relation,
							bool mapped_relation,
							bool allow_system_table_mods,
							TransactionId *relfrozenxid,
							MultiXactId *relminmxid);

extern Oid	heap_create_with_catalog(const char *relname,
									 Oid relnamespace,
									 Oid reltablespace,
									 Oid relid,
									 Oid reltypeid,
									 Oid reloftypeid,
									 Oid ownerid,
									 Oid accessmtd,
									 TupleDesc tupdesc,
									 List *cooked_constraints,
									 char relkind,
									 char relpersistence,
									 bool shared_relation,
									 bool mapped_relation,
									 OnCommitAction oncommit,
									 const struct GpPolicy *policy,    /* MPP */
									 Datum reloptions,
									 bool use_user_acl,
									 bool allow_system_table_mods,
									 bool is_internal,
									 Oid relrewrite,
									 ObjectAddress *typaddress,
									 bool valid_opts);

extern void heap_drop_with_catalog(Oid relid);

extern void heap_truncate(List *relids);

extern void heap_truncate_one_rel(Relation rel);

extern void heap_truncate_check_FKs(List *relations, bool tempTables);

extern List *heap_truncate_find_FKs(List *relationIds);

extern void InsertPgAttributeTuple(Relation pg_attribute_rel,
								   Form_pg_attribute new_attribute,
								   CatalogIndexState indstate);

extern void InsertPgClassTuple(Relation pg_class_desc,
							   Relation new_rel_desc,
							   Oid new_rel_oid,
							   Datum relacl,
							   Datum reloptions);

extern List *AddRelationNewConstraints(Relation rel,
									   List *newColDefaults,
									   List *newConstraints,
									   bool allow_merge,
									   bool is_local,
									   bool is_internal,
									   const char *queryString);
extern List *AddRelationConstraints(Relation rel,
						  List *rawColDefaults,
						  List *constraints);

extern void RelationClearMissing(Relation rel);
extern void RelationClearMissingByAttname(Relation rel, char *attname);
extern void SetAttrMissing(Oid relid, char *attname, char *value);

extern Oid	StoreAttrDefault(Relation rel, AttrNumber attnum,
							 Node *expr,
							 bool *cookedMissingVal,
							 Datum *missingval_p,
							 bool *missingIsNull_p,
							 bool is_internal,
							 bool add_column_mode);

extern Node *cookDefault(ParseState *pstate,
						 Node *raw_default,
						 Oid atttypid,
						 int32 atttypmod,
						 const char *attname,
						 char attgenerated);

extern void DeleteRelationTuple(Oid relid);
extern void DeleteAttributeTuples(Oid relid);
extern void DeleteSystemAttributeTuples(Oid relid);
extern void RemoveAttributeById(Oid relid, AttrNumber attnum);
extern void RemoveAttrDefault(Oid relid, AttrNumber attnum,
							  DropBehavior behavior, bool complain, bool internal);
extern void RemoveAttrDefaultById(Oid attrdefId);
extern void CopyStatistics(Oid fromrelid, Oid torelid);
extern void RemoveStatistics(Oid relid, AttrNumber attnum);

extern const FormData_pg_attribute *SystemAttributeDefinition(AttrNumber attno);

extern const FormData_pg_attribute *SystemAttributeByName(const char *attname);

extern void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind,
									 int flags);

extern void CheckAttributeType(const char *attname,
							   Oid atttypid, Oid attcollation,
							   List *containing_rowtypes,
							   int flags);

/* pg_partitioned_table catalog manipulation functions */
extern void StorePartitionKey(Relation rel,
							  char strategy,
							  int16 partnatts,
							  AttrNumber *partattrs,
							  List *partexprs,
							  Oid *partopclass,
							  Oid *partcollation);
extern void RemovePartitionKeyByRelId(Oid relid);
extern void StorePartitionBound(Relation rel, Relation parent,
								PartitionBoundSpec *bound);
extern void StorePartitionBoundSkipInvalidation(Relation rel, Relation parent,
												PartitionBoundSpec *bound);

/* MPP-6929: metadata tracking */
extern void MetaTrackAddObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackUpdObject(Oid		classid, 
							   Oid		objoid, 
							   Oid		relowner,
							   char*	actionname,
							   char*	subtype);
extern void MetaTrackDropObject(Oid		classid, 
								Oid		objoid);

#define MetaTrackValidRelkind(relkind) \
		(((relkind) == RELKIND_RELATION) \
		|| ((relkind) == RELKIND_INDEX) \
		|| ((relkind) == RELKIND_SEQUENCE) \
		|| ((relkind) == RELKIND_VIEW)) 

#endif							/* HEAP_H */
