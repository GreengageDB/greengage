/*-------------------------------------------------------------------------
 *
 * readfast.c
 *	  Binary Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * These routines must be exactly the inverse of the routines in
 * outfast.c.
 *
 * For most node types, these routines are identical to the text reader
 * functions, in readfuncs.c. To avoid code duplication and merge hazards
 * (readfast.c is a Greenplum addon), most read routines borrow the source
 * definition from readfuncs.c, we just compile it with different READ_*
 * macros.
 *
 * The way that works is that readfast.c defines all the necessary macros,
 * as well as COMPILING_BINARY_FUNCS, and then #includes readfuncs.c. For
 * those node types where the binary and text functions are different,
 * the function in readfuncs.c is put in a #ifndef COMPILING_BINARY_FUNCS
 * block, and readfast.c provides the binary version of the function.
 * outfast.c and outfuncs.c have a similar relationship.
 *
 * By this, CDB could link only readfast.o (#includes readfuncs.c) to get all
 * the fast version deserializing functions, outfast.o likewise.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "catalog/aocatalog.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"
#include "cdb/cdbgang.h"

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.
 */

#define READ_TEMP_LOCALS()

#define READ_LOCALS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/*
 * no difference between READ_LOCALS and READ_LOCALS_NO_FIELDS in readfast.c,
 * but define both for compatibility.
 */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	READ_LOCALS(nodeTypeName)

/* Allocate non-node variable */
#define ALLOCATE_LOCAL(local, typeName, size) \
	local = (typeName *)palloc0(sizeof(typeName) * size)

/* Read an integer field  */
#define READ_INT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int));  read_str_ptr+=sizeof(int)

/* Read an int8 field  */
#define READ_INT8_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int8));  read_str_ptr+=sizeof(int8)

/* Read an int16 field  */
#define READ_INT16_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int16));  read_str_ptr+=sizeof(int16)

/* Read an unsigned integer field) */
#define READ_UINT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(int)); read_str_ptr+=sizeof(int)

/* Read an uint64 field) */
#define READ_UINT64_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(uint64)); read_str_ptr+=sizeof(uint64)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(Oid)); read_str_ptr+=sizeof(Oid)

/* Read a long-integer field  */
#define READ_LONG_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(long)); read_str_ptr+=sizeof(long)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, 1); read_str_ptr++

/* Read an enumerated-type field that was written as a short integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	{ int16 ent; memcpy(&ent, read_str_ptr, sizeof(int16));  read_str_ptr+=sizeof(int16);local_node->fldname = (enumtype) ent; }

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	memcpy(&local_node->fldname, read_str_ptr, sizeof(double)); read_str_ptr+=sizeof(double)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	local_node->fldname = read_str_ptr[0] != 0;  Assert(read_str_ptr[0]==1 || read_str_ptr[0]==0); read_str_ptr++

/* Read a character-string variable */
#define READ_STRING_VAR(var) \
	{ int slen; char * nn = NULL; \
		memcpy(&slen, read_str_ptr, sizeof(int)); \
		read_str_ptr+=sizeof(int); \
		if (slen>0) { \
		    nn = palloc(slen+1); \
		    memcpy(nn,read_str_ptr,slen); \
		    read_str_ptr+=(slen); nn[slen]='\0'; \
		} \
		var = nn;  }

/* Read a character-string field */
#define READ_STRING_FIELD(fldname)  READ_STRING_VAR(local_node->fldname)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) READ_INT_FIELD(fldname)

/* Read a Node field */
#ifdef GP_SERIALIZATION_DEBUG
#define READ_NODE_FIELD(fldname) \
	do { \
		char *xexpected = CppAsString(fldname); \
		char got[100]; \
		\
		memcpy(got, read_str_ptr, strlen(xexpected) + 1); \
		read_str_ptr += strlen(xexpected) + 1; \
		if (strcmp(xexpected, got) != 0) \
			elog(ERROR, "deserialization lost sync: %s vs %02x%02x%02x", xexpected, (unsigned char) got[0], (unsigned char) got[1], (unsigned char) got[2]); \
		\
		local_node->fldname = readNodeBinary(); \
	} while(0)
#else
#define READ_NODE_FIELD(fldname) \
	local_node->fldname = readNodeBinary()
#endif

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	 local_node->fldname = _readBitmapset()

/* Read in a binary field */
#define READ_BINARY_FIELD(fldname, sz) \
	memcpy(&local_node->fldname, read_str_ptr, (sz)); read_str_ptr += (sz)

/* Read a bytea field */
#define READ_BYTEA_FIELD(fldname) \
	local_node->fldname = (bytea *) DatumGetPointer(readDatumBinary(false))

/* Read a dummy field */
#define READ_DUMMY_FIELD(fldname,fldvalue) \
	{ local_node->fldname = (0); /*read_str_ptr += sizeof(int*);*/ }

/* Routine exit */
#define READ_DONE() \
	return local_node

/* Read an integer array  */
#define READ_ARRAY_OF_TYPE(fldname, count, Type) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		local_node->fldname = (Type *)palloc((count) * sizeof(Type)); \
		for(i = 0; i < (count); i++) \
		{ \
			memcpy(&local_node->fldname[i], read_str_ptr, sizeof(Type)); read_str_ptr+=sizeof(Type); \
		} \
	}

/* Read a bool array  */
#define READ_BOOL_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		local_node->fldname = (bool *) palloc((count) * sizeof(bool)); \
		for(i = 0; i < (count); i++) \
		{ \
			local_node->fldname[i] = *read_str_ptr ? true : false; \
			read_str_ptr++; \
		} \
	}

/* Read an attribute number array  */
#define READ_ATTRNUMBER_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, AttrNumber)

/* Read an Oid array  */
#define READ_OID_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, Oid)

/* Read an int array  */
#define READ_INT_ARRAY(fldname, count) READ_ARRAY_OF_TYPE(fldname, count, int)


static void *readNodeBinary(void);

static Datum readDatumBinary(bool typbyval);
#define readDatum(x) readDatumBinary(x)

static Bitmapset *_readBitmapset(void);

/*
 * Current position in the message that we are processing. We can keep
 * this in a global variable because readNodeFromBinaryString() is not
 * re-entrant. This is similar to the current position that pg_strtok()
 * keeps, used by the normal stringToNode() function.
 */
static const char *read_str_ptr;

/*
 * For most structs, we reuse the definitions from readfuncs.c. See comment
 * in readfuncs.c.
 */
/*
 * GGDB binary node read functions.
 *
 * Formerly obtained by "#define COMPILING_BINARY_FUNCS" + "#include
 * \"readfuncs.c\"".  PG16 made readfuncs.c a thin stub that #includes the
 * generated readfuncs.funcs.c and can no longer be re-included under a macro,
 * so the binary variant of every shared read function is inlined here (faithful
 * snapshot).  readfast.c is now standalone; keep it in sync with node
 * definitions by hand (gen_node_support.pl does NOT produce the fast variant).
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"

#include "cdb/cdbgang.h"
#include "nodes/altertablenodes.h"
#include "utils/builtins.h"
#include "catalog/gp_distribution_policy.h"

/*
 * readfuncs.c is compiled normally into readfuncs.o, but it's also
 * #included from readfast.c. When #included, readfuncs.c defines
 * COMPILING_BINARY_FUNCS, and provides replacements READ_* macros. See
 * comments at top of readfast.c.
 */


/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(void)
{
	READ_LOCALS(NotifyStmt);

	READ_STRING_FIELD(conditionname);
	READ_STRING_FIELD(payload);

	READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(void)
{
	READ_LOCALS(DeclareCursorStmt);

	READ_STRING_FIELD(portalname);
	READ_INT_FIELD(options);
	READ_NODE_FIELD(query);

	READ_DONE();
}

/*
 * _readSingleRowErrorDesc
 */
static SingleRowErrorDesc *
_readSingleRowErrorDesc(void)
{
	READ_LOCALS(SingleRowErrorDesc);

	READ_INT_FIELD(rejectlimit);
	READ_BOOL_FIELD(is_limit_in_rows);
	READ_CHAR_FIELD(log_error_type);

	READ_DONE();
}

/*
 * _readWithCheckOption
 */
static WithCheckOption *
_readWithCheckOption(void)
{
	READ_LOCALS(WithCheckOption);

	READ_ENUM_FIELD(kind, WCOKind);
	READ_STRING_FIELD(relname);
	READ_STRING_FIELD(polname);
	READ_NODE_FIELD(qual);
	READ_BOOL_FIELD(cascaded);

	READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause *
_readSortGroupClause(void)
{
	READ_LOCALS(SortGroupClause);

	READ_UINT_FIELD(tleSortGroupRef);
	READ_OID_FIELD(eqop);
	READ_OID_FIELD(sortop);
	READ_BOOL_FIELD(nulls_first);
	READ_BOOL_FIELD(hashable);

	READ_DONE();
}

/*
 * _readGroupingSet
 */
static GroupingSet *
_readGroupingSet(void)
{
	READ_LOCALS(GroupingSet);

	READ_ENUM_FIELD(kind, GroupingSetKind);
	READ_NODE_FIELD(content);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause *
_readWindowClause(void)
{
	READ_LOCALS(WindowClause);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_NODE_FIELD(runCondition);
	READ_OID_FIELD(startInRangeFunc);
	READ_OID_FIELD(endInRangeFunc);
	READ_OID_FIELD(inRangeColl);
	READ_BOOL_FIELD(inRangeAsc);
	READ_BOOL_FIELD(inRangeNullsFirst);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(copiedOrder);

	READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(void)
{
	READ_LOCALS(RowMarkClause);

	READ_UINT_FIELD(rti);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	READ_BOOL_FIELD(pushedDown);

	READ_DONE();
}

/*
 * _readCTESearchClause
 */
static CTESearchClause *
_readCTESearchClause(void)
{
	READ_LOCALS(CTESearchClause);

	READ_NODE_FIELD(search_col_list);
	READ_BOOL_FIELD(search_breadth_first);
	READ_STRING_FIELD(search_seq_column);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCTECycleClause
 */
static CTECycleClause *
_readCTECycleClause(void)
{
	READ_LOCALS(CTECycleClause);

	READ_NODE_FIELD(cycle_col_list);
	READ_STRING_FIELD(cycle_mark_column);
	READ_NODE_FIELD(cycle_mark_value);
	READ_NODE_FIELD(cycle_mark_default);
	READ_STRING_FIELD(cycle_path_column);
	READ_LOCATION_FIELD(location);
	READ_OID_FIELD(cycle_mark_type);
	READ_INT_FIELD(cycle_mark_typmod);
	READ_OID_FIELD(cycle_mark_collation);
	READ_OID_FIELD(cycle_mark_neop);

	READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr *
_readCommonTableExpr(void)
{
	READ_LOCALS(CommonTableExpr);

	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	READ_ENUM_FIELD(ctematerialized, CTEMaterialize);
	READ_NODE_FIELD(ctequery);
	READ_NODE_FIELD(search_clause);
	READ_NODE_FIELD(cycle_clause);
	READ_LOCATION_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);
	READ_NODE_FIELD(ctecolcollations);

	READ_DONE();
}

static WithClause *
_readWithClause(void)
{
	READ_LOCALS(WithClause);

	READ_NODE_FIELD(ctes);
	READ_BOOL_FIELD(recursive);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readMergeWhenClause
 */
static MergeWhenClause *
_readMergeWhenClause(void)
{
	READ_LOCALS(MergeWhenClause);

	READ_BOOL_FIELD(matched);
	READ_ENUM_FIELD(commandType, CmdType);
	READ_NODE_FIELD(condition);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(values);
	READ_ENUM_FIELD(override, OverridingKind);

	READ_DONE();
}

/*
 * _readMergeAction
 */
static MergeAction *
_readMergeAction(void)
{
	READ_LOCALS(MergeAction);

	READ_BOOL_FIELD(matched);
	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(updateColnos);

	READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(void)
{
	READ_LOCALS(SetOperationStmt);

	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(colTypes);
	READ_NODE_FIELD(colTypmods);
	READ_NODE_FIELD(colCollations);
	READ_NODE_FIELD(groupClauses);

	READ_DONE();
}


/*
 *	Stuff from primnodes.h.
 */

static Alias *
_readAlias(void)
{
	READ_LOCALS(Alias);

	READ_STRING_FIELD(aliasname);
	READ_NODE_FIELD(colnames);

	READ_DONE();
}

static RangeVar *
_readRangeVar(void)
{
	READ_LOCALS(RangeVar);

	local_node->catalogname = NULL; /* not currently saved in output format */

	READ_STRING_FIELD(catalogname);
	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(relname);
	READ_BOOL_FIELD(inh);
	READ_CHAR_FIELD(relpersistence);
	READ_NODE_FIELD(alias);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readTableFunc
 */
static TableFunc *
_readTableFunc(void)
{
	READ_LOCALS(TableFunc);

	READ_NODE_FIELD(ns_uris);
	READ_NODE_FIELD(ns_names);
	READ_NODE_FIELD(docexpr);
	READ_NODE_FIELD(rowexpr);
	READ_NODE_FIELD(colnames);
	READ_NODE_FIELD(coltypes);
	READ_NODE_FIELD(coltypmods);
	READ_NODE_FIELD(colcollations);
	READ_NODE_FIELD(colexprs);
	READ_NODE_FIELD(coldefexprs);
	READ_BITMAPSET_FIELD(notnulls);
	READ_INT_FIELD(ordinalitycol);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static IntoClause *
_readIntoClause(void)
{
	READ_LOCALS(IntoClause);

	READ_NODE_FIELD(rel);
	READ_NODE_FIELD(colNames);
	READ_STRING_FIELD(accessMethod);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(onCommit, OnCommitAction);
	READ_STRING_FIELD(tableSpaceName);
	READ_NODE_FIELD(viewQuery);
	READ_BOOL_FIELD(skipData);
	READ_NODE_FIELD(distributedBy);

	READ_DONE();
}


static CopyIntoClause *
_readCopyIntoClause(void)
{
	READ_LOCALS(CopyIntoClause);

	READ_NODE_FIELD(attlist);
	READ_BOOL_FIELD(is_program);
	READ_STRING_FIELD(filename);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static RefreshClause *
_readRefreshClause(void)
{
	READ_LOCALS(RefreshClause);

	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(skipData);
	READ_NODE_FIELD(relation);

	READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(void)
{
	READ_LOCALS(Var);

	READ_INT_FIELD(varno);
	READ_INT_FIELD(varattno);
	READ_OID_FIELD(vartype);
	READ_INT_FIELD(vartypmod);
	READ_OID_FIELD(varcollid);
	READ_UINT_FIELD(varlevelsup);
	READ_UINT_FIELD(varnosyn);
	READ_INT_FIELD(varattnosyn);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}


/*
 * _readConstraint
 */
static Constraint *
_readConstraint(void)
{
	READ_LOCALS(Constraint);

	READ_ENUM_FIELD(contype, ConstrType);
	READ_STRING_FIELD(conname);			/* name, or NULL if unnamed */
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_LOCATION_FIELD(location);

	READ_BOOL_FIELD(is_no_inherit);
	READ_NODE_FIELD(raw_expr);
	READ_STRING_FIELD(cooked_expr);
	READ_CHAR_FIELD(generated_when);

	READ_NODE_FIELD(keys);
	READ_NODE_FIELD(including);

	READ_NODE_FIELD(exclusions);

	READ_NODE_FIELD(options);
	READ_STRING_FIELD(indexname);
	READ_STRING_FIELD(indexspace);
	READ_BOOL_FIELD(reset_default_tblspc);

	READ_STRING_FIELD(access_method);
	READ_NODE_FIELD(where_clause);

	READ_NODE_FIELD(pktable);
	READ_NODE_FIELD(fk_attrs);
	READ_NODE_FIELD(pk_attrs);
	READ_CHAR_FIELD(fk_matchtype);
	READ_CHAR_FIELD(fk_upd_action);
	READ_CHAR_FIELD(fk_del_action);
	READ_NODE_FIELD(old_conpfeqop);
	READ_OID_FIELD(old_pktable_oid);

	READ_BOOL_FIELD(nulls_not_distinct);
	READ_NODE_FIELD(fk_del_set_cols);

	READ_BOOL_FIELD(skip_validation);
	READ_BOOL_FIELD(initially_valid);

	READ_DONE();
}

static IndexStmt *
_readIndexStmt(void)
{
	READ_LOCALS(IndexStmt);

	READ_STRING_FIELD(idxname);
	READ_NODE_FIELD(relation);
	READ_OID_FIELD(relationOid);
	READ_STRING_FIELD(accessMethod);
	READ_STRING_FIELD(tableSpace);
	READ_NODE_FIELD(indexParams);
	READ_NODE_FIELD(indexIncludingParams);
	READ_NODE_FIELD(options);

	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(excludeOpNames);
	READ_STRING_FIELD(idxcomment);
	READ_OID_FIELD(indexOid);
	READ_OID_FIELD(oldNumber);
	READ_UINT_FIELD(oldCreateSubid);
	READ_UINT_FIELD(oldFirstRelfilelocatorSubid);
	READ_BOOL_FIELD(unique);
	READ_BOOL_FIELD(nulls_not_distinct);
	READ_BOOL_FIELD(primary);
	READ_BOOL_FIELD(isconstraint);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_BOOL_FIELD(transformed);
	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(if_not_exists);
	READ_BOOL_FIELD(reset_default_tblspc);

	READ_DONE();
}

static IndexElem *
_readIndexElem(void)
{
	READ_LOCALS(IndexElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);
	READ_STRING_FIELD(indexcolname);
	READ_NODE_FIELD(collation);
	READ_NODE_FIELD(opclass);
	READ_NODE_FIELD(opclassopts);
	READ_ENUM_FIELD(ordering, SortByDir);
	READ_ENUM_FIELD(nulls_ordering, SortByNulls);

	READ_DONE();
}

static StatsElem *
_readStatsElem(void)
{
	READ_LOCALS(StatsElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);

	READ_DONE();
}

static CreateStatsStmt *
_readCreateStatsStmt(void)
{
	READ_LOCALS(CreateStatsStmt);

	READ_NODE_FIELD(defnames);
	READ_NODE_FIELD(stat_types);
	READ_NODE_FIELD(exprs);
	READ_NODE_FIELD(relations);
	READ_STRING_FIELD(stxcomment);
	READ_BOOL_FIELD(transformed);
	READ_BOOL_FIELD(if_not_exists);

	READ_DONE();
}

static RangeSubselect *
_readRangeSubselect(void)
{
	READ_LOCALS(RangeSubselect);

	READ_BOOL_FIELD(lateral);
	READ_NODE_FIELD(subquery);
	READ_NODE_FIELD(alias);

	READ_DONE();
}

static InferClause *
_readInferClause(void)
{
	READ_LOCALS(InferClause);

	READ_NODE_FIELD(indexElems);
	READ_NODE_FIELD(whereClause);
	READ_STRING_FIELD(conname);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static OnConflictClause *
_readOnConflictClause(void)
{
	READ_LOCALS(OnConflictClause);

	READ_ENUM_FIELD(action, OnConflictAction);
	READ_NODE_FIELD(infer);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(whereClause);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static ReindexStmt *
_readReindexStmt(void)
{
	READ_LOCALS(ReindexStmt);

	READ_ENUM_FIELD(kind,ReindexObjectType);
	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(name);
	READ_OID_FIELD(relid);

	READ_DONE();
}

static ViewStmt *
_readViewStmt(void)
{
	READ_LOCALS(ViewStmt);

	READ_NODE_FIELD(view);
	READ_NODE_FIELD(aliases);
	READ_NODE_FIELD(query);
	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static RuleStmt *
_readRuleStmt(void)
{
	READ_LOCALS(RuleStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(rulename);
	READ_NODE_FIELD(whereClause);
	READ_ENUM_FIELD(event,CmdType);
	READ_BOOL_FIELD(instead);
	READ_NODE_FIELD(actions);
	READ_BOOL_FIELD(replace);

	READ_DONE();
}

static DropStmt *
_readDropStmt(void)
{
	READ_LOCALS(DropStmt);

	READ_NODE_FIELD(objects);
	READ_ENUM_FIELD(removeType,ObjectType);
	READ_ENUM_FIELD(behavior,DropBehavior);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(concurrent);

	/* Force 'missing_ok' in QEs */
	local_node->missing_ok=true;

	READ_DONE();
}

static TruncateStmt *
_readTruncateStmt(void)
{
	READ_LOCALS(TruncateStmt);

	READ_NODE_FIELD(relations);
	READ_ENUM_FIELD(behavior,DropBehavior);

	READ_DONE();
}

static ReplicaIdentityStmt *
_readReplicaIdentityStmt(void)
{
	READ_LOCALS(ReplicaIdentityStmt);

	READ_CHAR_FIELD(identity_type);
	READ_STRING_FIELD(name);

	READ_DONE();
}

static AlterDatabaseStmt *
_readAlterDatabaseStmt(void)
{
	READ_LOCALS(AlterDatabaseStmt);

	READ_STRING_FIELD(dbname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterTableStmt *
_readAlterTableStmt(void)
{
	READ_LOCALS(AlterTableStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(cmds);
	READ_ENUM_FIELD(objtype, ObjectType);
	READ_INT_FIELD(lockmode);
	READ_NODE_FIELD(wqueue);

	READ_DONE();
}

static AlterTableCmd *
_readAlterTableCmd(void)
{
	READ_LOCALS(AlterTableCmd);

	READ_ENUM_FIELD(subtype, AlterTableType);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(num);
	READ_NODE_FIELD(newowner);
	READ_NODE_FIELD(def);
	READ_NODE_FIELD(transform);
	READ_ENUM_FIELD(behavior, DropBehavior);
	READ_BOOL_FIELD(missing_ok);

	READ_INT_FIELD(backendId);
	READ_NODE_FIELD(policy);
	READ_NODE_FIELD(prepStmts);
	READ_NODE_FIELD(execStmts);
	READ_BOOL_FIELD(isNULL);

	READ_DONE();
}

static void
unwrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		String	   *val = (String *) lfirst(lc);

		lfirst(lc) = strVal(val);
		pfree(val);
	}
}

static AlteredTableInfo *
_readAlteredTableInfo(void)
{
	READ_LOCALS(AlteredTableInfo);

	READ_OID_FIELD(relid);
	READ_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
		READ_NODE_FIELD(subcmds[i]);

	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(afterStmts);
	READ_BOOL_FIELD(verify_new_notnull);
	READ_INT_FIELD(rewrite);
	READ_OID_FIELD(newAccessMethod);
	READ_BOOL_FIELD(dist_opfamily_changed);
	READ_OID_FIELD(new_opclass);
	READ_OID_FIELD(newTableSpace);
	READ_BOOL_FIELD(chgPersistence);
	READ_CHAR_FIELD(newrelpersistence);
	READ_NODE_FIELD(partition_constraint);
	READ_BOOL_FIELD(validate_default);
	READ_NODE_FIELD(changedConstraintOids);
	READ_NODE_FIELD(changedConstraintDefs);
	/* The QD sends changedConstraintDefs wrapped in Values. Unwrap them. */
	unwrapStringList(local_node->changedConstraintDefs);
	READ_NODE_FIELD(changedIndexOids);
	READ_NODE_FIELD(changedIndexDefs);
	unwrapStringList(local_node->changedIndexDefs);

	READ_DONE();
}

static NewConstraint *
_readNewConstraint(void)
{
	READ_LOCALS(NewConstraint);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(contype, ConstrType);
	READ_OID_FIELD(refrelid);
	READ_OID_FIELD(refindid);
	READ_OID_FIELD(conid);
	READ_NODE_FIELD(qual);
	/* can't serialize qualstate */

	READ_DONE();
}

static NewColumnValue *
_readNewColumnValue(void)
{
	READ_LOCALS(NewColumnValue);

	READ_INT_FIELD(attnum);
	READ_NODE_FIELD(expr);
	/* can't serialize exprstate */
	READ_BOOL_FIELD(is_generated);

	READ_DONE();
}

static CreateRoleStmt *
_readCreateRoleStmt(void)
{
	READ_LOCALS(CreateRoleStmt);

	READ_ENUM_FIELD(stmt_type, RoleStmtType);
	READ_STRING_FIELD(role);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DenyLoginInterval *
_readDenyLoginInterval(void)
{
	READ_LOCALS(DenyLoginInterval);

	READ_NODE_FIELD(start);
	READ_NODE_FIELD(end);

	READ_DONE();
}

static DenyLoginPoint *
_readDenyLoginPoint(void)
{
	READ_LOCALS(DenyLoginPoint);

	READ_NODE_FIELD(day);
	READ_NODE_FIELD(time);

	READ_DONE();
}

static DropRoleStmt *
_readDropRoleStmt(void)
{
	READ_LOCALS(DropRoleStmt);

	READ_NODE_FIELD(roles);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterRoleStmt *
_readAlterRoleStmt(void)
{
	READ_LOCALS(AlterRoleStmt);

	READ_NODE_FIELD(role);
	READ_NODE_FIELD(options);
	READ_INT_FIELD(action);

	READ_DONE();
}

static AlterRoleSetStmt *
_readAlterRoleSetStmt(void)
{
	READ_LOCALS(AlterRoleSetStmt);

	READ_NODE_FIELD(role);
	READ_NODE_FIELD(setstmt);

	READ_DONE();
}

static AlterSystemStmt *
_readAlterSystemStmt(void)
{
	READ_LOCALS(AlterSystemStmt);

	READ_NODE_FIELD(setstmt);

	READ_DONE();
}

static AlterObjectSchemaStmt *
_readAlterObjectSchemaStmt(void)
{
	READ_LOCALS(AlterObjectSchemaStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_STRING_FIELD(newschema);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(objectType,ObjectType);

	READ_DONE();
}

static AlterOwnerStmt *
_readAlterOwnerStmt(void)
{
	READ_LOCALS(AlterOwnerStmt);

	READ_ENUM_FIELD(objectType,ObjectType);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(newowner);

	READ_DONE();
}

static RenameStmt *
_readRenameStmt(void)
{
	READ_LOCALS(RenameStmt);

	READ_ENUM_FIELD(renameType, ObjectType);
	READ_ENUM_FIELD(relationType, ObjectType);
	READ_NODE_FIELD(relation);
	READ_OID_FIELD(objid);
	READ_NODE_FIELD(object);
	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(newname);
	READ_ENUM_FIELD(behavior,DropBehavior);

	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}


/*
 * _readFuncCall
 *
 * This parsenode is transformed during parse_analyze.
 * It not stored in views = no upgrade implication for changes
 */
static FuncCall *
_readFuncCall(void)
{
	READ_LOCALS(FuncCall);

	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(agg_order);
	READ_NODE_FIELD(agg_filter);
	READ_NODE_FIELD(over);
	READ_BOOL_FIELD(agg_within_group);
	READ_BOOL_FIELD(agg_star);
	READ_BOOL_FIELD(agg_distinct);
	READ_BOOL_FIELD(func_variadic);
	READ_ENUM_FIELD(funcformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}



/*
 * _readParam
 */
static Param *
_readParam(void)
{
	READ_LOCALS(Param);

	READ_ENUM_FIELD(paramkind, ParamKind);
	READ_INT_FIELD(paramid);
	READ_OID_FIELD(paramtype);
	READ_INT_FIELD(paramtypmod);
	READ_OID_FIELD(paramcollid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(void)
{
	READ_LOCALS(Aggref);

	READ_OID_FIELD(aggfnoid);
	READ_OID_FIELD(aggtype);
	READ_OID_FIELD(aggcollid);
	READ_OID_FIELD(inputcollid);
	READ_OID_FIELD(aggtranstype);
	READ_NODE_FIELD(aggargtypes);
	READ_NODE_FIELD(aggdirectargs);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggorder);
	READ_NODE_FIELD(aggdistinct);
	READ_NODE_FIELD(aggfilter);
	READ_BOOL_FIELD(aggstar);
	READ_BOOL_FIELD(aggvariadic);
	READ_CHAR_FIELD(aggkind);
	READ_UINT_FIELD(agglevelsup);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_INT_FIELD(aggno);
	READ_INT_FIELD(aggtransno);
	READ_LOCATION_FIELD(location);
	READ_INT_FIELD(agg_expr_id);

	READ_DONE();
}

/*
 * _readGroupingFunc
 */
static GroupingFunc *
_readGroupingFunc(void)
{
	READ_LOCALS(GroupingFunc);

	READ_NODE_FIELD(args);
	READ_NODE_FIELD(refs);
	READ_NODE_FIELD(cols);
	READ_UINT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readGroupId
 */
static GroupId *
_readGroupId(void)
{
	READ_LOCALS(GroupId);

	READ_INT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readGroupingSetId
 */
static GroupingSetId *
_readGroupingSetId(void)
{
	READ_LOCALS(GroupingSetId);

	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowFunc
 */
static WindowFunc *
_readWindowFunc(void)
{
	READ_LOCALS(WindowFunc);

	READ_OID_FIELD(winfnoid);
	READ_OID_FIELD(wintype);
	READ_OID_FIELD(wincollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggfilter);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(winstar);
	READ_BOOL_FIELD(winagg);
	READ_BOOL_FIELD(windistinct);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSubscriptingRef
 */
static SubscriptingRef *
_readSubscriptingRef(void)
{
	READ_LOCALS(SubscriptingRef);

	READ_OID_FIELD(refcontainertype);
	READ_OID_FIELD(refelemtype);
	READ_OID_FIELD(refrestype);
	READ_INT_FIELD(reftypmod);
	READ_OID_FIELD(refcollid);
	READ_NODE_FIELD(refupperindexpr);
	READ_NODE_FIELD(reflowerindexpr);
	READ_NODE_FIELD(refexpr);
	READ_NODE_FIELD(refassgnexpr);

	READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(void)
{
	READ_LOCALS(FuncExpr);

	READ_OID_FIELD(funcid);
	READ_OID_FIELD(funcresulttype);
	READ_BOOL_FIELD(funcretset);
	READ_BOOL_FIELD(funcvariadic);
	READ_ENUM_FIELD(funcformat, CoercionForm);
	READ_OID_FIELD(funccollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(is_tablefunc);  /* GPDB */
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr *
_readNamedArgExpr(void)
{
	READ_LOCALS(NamedArgExpr);

	READ_NODE_FIELD(arg);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(argnumber);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}


/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(void)
{
	READ_LOCALS(DistinctExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(void)
{
	READ_LOCALS(NullIfExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(void)
{
	READ_LOCALS(ScalarArrayOpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);
	READ_OID_FIELD(hashfuncid);
	READ_OID_FIELD(negfuncid);
	READ_BOOL_FIELD(useOr);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}


/*
 * _readSubLink
 */
static SubLink *
_readSubLink(void)
{
	READ_LOCALS(SubLink);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_INT_FIELD(subLinkId);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(operName);
	READ_NODE_FIELD(subselect);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(void)
{
	READ_LOCALS(FieldSelect);

	READ_NODE_FIELD(arg);
	READ_INT_FIELD(fieldnum);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);

	READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(void)
{
	READ_LOCALS(FieldStore);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(fieldnums);
	READ_OID_FIELD(resulttype);

	READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(void)
{
	READ_LOCALS(RelabelType);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(relabelformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO *
_readCoerceViaIO(void)
{
	READ_LOCALS(CoerceViaIO);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr *
_readArrayCoerceExpr(void)
{
	READ_LOCALS(ArrayCoerceExpr);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(elemexpr);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(void)
{
	READ_LOCALS(ConvertRowtypeExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_ENUM_FIELD(convertformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr *
_readCollateExpr(void)
{
	READ_LOCALS(CollateExpr);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(collOid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(void)
{
	READ_LOCALS(CaseExpr);

	READ_OID_FIELD(casetype);
	READ_OID_FIELD(casecollid);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(defresult);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(void)
{
	READ_LOCALS(CaseWhen);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(result);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(void)
{
	READ_LOCALS(CaseTestExpr);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);

	READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(void)
{
	READ_LOCALS(ArrayExpr);

	READ_OID_FIELD(array_typeid);
	READ_OID_FIELD(array_collid);
	READ_OID_FIELD(element_typeid);
	READ_NODE_FIELD(elements);
	READ_BOOL_FIELD(multidims);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readA_ArrayExpr
 */
static A_ArrayExpr *
_readA_ArrayExpr(void)
{
	READ_LOCALS(A_ArrayExpr);

	READ_NODE_FIELD(elements);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(void)
{
	READ_LOCALS(RowExpr);

	READ_NODE_FIELD(args);
	READ_OID_FIELD(row_typeid);
	READ_ENUM_FIELD(row_format, CoercionForm);
	READ_NODE_FIELD(colnames);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(void)
{
	READ_LOCALS(RowCompareExpr);

	READ_ENUM_FIELD(rctype, RowCompareType);
	READ_NODE_FIELD(opnos);
	READ_NODE_FIELD(opfamilies);
	READ_NODE_FIELD(inputcollids);
	READ_NODE_FIELD(largs);
	READ_NODE_FIELD(rargs);

	READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(void)
{
	READ_LOCALS(CoalesceExpr);

	READ_OID_FIELD(coalescetype);
	READ_OID_FIELD(coalescecollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(void)
{
	READ_LOCALS(MinMaxExpr);

	READ_OID_FIELD(minmaxtype);
	READ_OID_FIELD(minmaxcollid);
	READ_OID_FIELD(inputcollid);
	READ_ENUM_FIELD(op, MinMaxOp);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSQLValueFunction
 */
static SQLValueFunction *
_readSQLValueFunction(void)
{
	READ_LOCALS(SQLValueFunction);

	READ_ENUM_FIELD(op, SQLValueFunctionOp);
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr *
_readXmlExpr(void)
{
	READ_LOCALS(XmlExpr);

	READ_ENUM_FIELD(op, XmlExprOp);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(named_args);
	READ_NODE_FIELD(arg_names);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(xmloption, XmlOptionType);
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(void)
{
	READ_LOCALS(NullTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(nulltesttype, NullTestType);
	READ_BOOL_FIELD(argisrow);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(void)
{
	READ_LOCALS(BooleanTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(booltesttype, BoolTestType);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(void)
{
	READ_LOCALS(CoerceToDomain);

	READ_NODE_FIELD(arg);
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coercionformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(void)
{
	READ_LOCALS(CoerceToDomainValue);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(void)
{
	READ_LOCALS(SetToDefault);

	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(void)
{
	READ_LOCALS(CurrentOfExpr);

	READ_UINT_FIELD(cvarno);
	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(cursor_param);
	READ_OID_FIELD(target_relid);

	READ_DONE();
}

/*
 * _readNextValueExpr
 */
static NextValueExpr *
_readNextValueExpr(void)
{
	READ_LOCALS(NextValueExpr);

	READ_OID_FIELD(seqid);
	READ_OID_FIELD(typeId);

	READ_DONE();
}

/*
 * _readInferenceElem
 */
static InferenceElem *
_readInferenceElem(void)
{
	READ_LOCALS(InferenceElem);

	READ_NODE_FIELD(expr);
	READ_OID_FIELD(infercollid);
	READ_OID_FIELD(inferopclass);

	READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(void)
{
	READ_LOCALS(TargetEntry);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(resno);
	READ_STRING_FIELD(resname);
	READ_UINT_FIELD(ressortgroupref);
	READ_OID_FIELD(resorigtbl);
	READ_INT_FIELD(resorigcol);
	READ_BOOL_FIELD(resjunk);

	READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(void)
{
	READ_LOCALS(RangeTblRef);

	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(void)
{
	READ_LOCALS(JoinExpr);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(isNatural);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(usingClause);
	READ_NODE_FIELD(join_using_alias);
	READ_NODE_FIELD(quals);
	READ_NODE_FIELD(alias);
	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(void)
{
	READ_LOCALS(FromExpr);

	READ_NODE_FIELD(fromlist);
	READ_NODE_FIELD(quals);

	READ_DONE();
}

/*
 * _readOnConflictExpr
 */
static OnConflictExpr *
_readOnConflictExpr(void)
{
	READ_LOCALS(OnConflictExpr);

	READ_ENUM_FIELD(action, OnConflictAction);
	READ_NODE_FIELD(arbiterElems);
	READ_NODE_FIELD(arbiterWhere);
	READ_OID_FIELD(constraint);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictWhere);
	READ_INT_FIELD(exclRelIndex);
	READ_NODE_FIELD(exclRelTlist);

	READ_DONE();
}

/*
 * _readJsonFormat
 */
static JsonFormat *
_readJsonFormat(void)
{
	READ_LOCALS(JsonFormat);

	READ_ENUM_FIELD(format_type, JsonFormatType);
	READ_ENUM_FIELD(encoding, JsonEncoding);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readJsonReturning
 */
static JsonReturning *
_readJsonReturning(void)
{
	READ_LOCALS(JsonReturning);

	READ_NODE_FIELD(format);
	READ_OID_FIELD(typid);
	READ_INT_FIELD(typmod);

	READ_DONE();
}

/*
 * _readJsonValueExpr
 */
static JsonValueExpr *
_readJsonValueExpr(void)
{
	READ_LOCALS(JsonValueExpr);

	READ_NODE_FIELD(raw_expr);
	READ_NODE_FIELD(formatted_expr);
	READ_NODE_FIELD(format);

	READ_DONE();
}

/*
 * GPDB: readers for the untransformed SQL/JSON and publication parse nodes
 * that GPDB dispatches to the segments. See the matching _out* functions in
 * outfuncs.c for why these exist.
 */
static JsonOutput *
_readJsonOutput(void)
{
	READ_LOCALS(JsonOutput);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(returning);

	READ_DONE();
}

static PublicationObjSpec *
_readPublicationObjSpec(void)
{
	READ_LOCALS(PublicationObjSpec);

	READ_ENUM_FIELD(pubobjtype, PublicationObjSpecType);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(pubtable);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static PublicationTable *
_readPublicationTable(void)
{
	READ_LOCALS(PublicationTable);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(columns);

	READ_DONE();
}

/*
 * _readJsonConstructorExpr
 */
static JsonConstructorExpr *
_readJsonConstructorExpr(void)
{
	READ_LOCALS(JsonConstructorExpr);

	READ_ENUM_FIELD(type, JsonConstructorType);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(func);
	READ_NODE_FIELD(coercion);
	READ_NODE_FIELD(returning);
	READ_BOOL_FIELD(absent_on_null);
	READ_BOOL_FIELD(unique);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readJsonIsPredicate
 */
static JsonIsPredicate *
_readJsonIsPredicate()
{
	READ_LOCALS(JsonIsPredicate);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(format);
	READ_ENUM_FIELD(item_type, JsonValueType);
	READ_BOOL_FIELD(unique_keys);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 *	Stuff from pathnodes.h.
 *
 * Mostly we don't need to read planner nodes back in again, but some
 * of these also end up in plan trees.
 */

/*
 * _readAppendRelInfo
 */
static AppendRelInfo *
_readAppendRelInfo(void)
{
	READ_LOCALS(AppendRelInfo);

	READ_UINT_FIELD(parent_relid);
	READ_UINT_FIELD(child_relid);
	READ_OID_FIELD(parent_reltype);
	READ_OID_FIELD(child_reltype);
	READ_NODE_FIELD(translated_vars);
	READ_INT_FIELD(num_child_cols);
	READ_ATTRNUMBER_ARRAY(parent_colnos, local_node->num_child_cols);
	READ_OID_FIELD(parent_reloid);

	READ_DONE();
}

/*
 *	Stuff from parsenodes.h.
 */

static ColumnDef *
_readColumnDef(void)
{
	READ_LOCALS(ColumnDef);

	READ_STRING_FIELD(colname);
	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(compression);
	READ_INT_FIELD(inhcount);
	READ_BOOL_FIELD(is_local);
	READ_BOOL_FIELD(is_not_null);
	READ_BOOL_FIELD(is_from_type);
	READ_INT_FIELD(attnum);
	READ_INT_FIELD(storage);
	READ_NODE_FIELD(raw_default);
	READ_NODE_FIELD(cooked_default);

	READ_BOOL_FIELD(hasCookedMissingVal);
	READ_BOOL_FIELD(missingIsNull);
	if (local_node->hasCookedMissingVal && !local_node->missingIsNull)
		local_node->missingVal = readDatum(false);

	READ_CHAR_FIELD(identity);
	READ_NODE_FIELD(identitySequence);
	READ_CHAR_FIELD(generated);
	READ_NODE_FIELD(collClause);
	READ_OID_FIELD(collOid);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(fdwoptions);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static DistributionKeyElem *
_readDistributionKeyElem(void)
{
	READ_LOCALS(DistributionKeyElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(opclass);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static ColumnRef *
_readColumnRef(void)
{
	READ_LOCALS(ColumnRef);

	READ_NODE_FIELD(fields);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static TypeName *
_readTypeName(void)
{
	READ_LOCALS(TypeName);

	READ_NODE_FIELD(names);
	READ_OID_FIELD(typeOid);
	READ_BOOL_FIELD(setof);
	READ_BOOL_FIELD(pct_type);
	READ_NODE_FIELD(typmods);
	READ_INT_FIELD(typemod);
	READ_NODE_FIELD(arrayBounds);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static SortBy *
_readSortBy(void)
{
	READ_LOCALS(SortBy);

	READ_NODE_FIELD(node);
	READ_ENUM_FIELD(sortby_dir, SortByDir);
	READ_ENUM_FIELD(sortby_nulls, SortByNulls);
	READ_NODE_FIELD(useOp);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static TypeCast *
_readTypeCast(void)
{
	READ_LOCALS(TypeCast);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(typeName);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(void)
{
	READ_LOCALS(RangeTblEntry);

	/* put alias + eref first to make dump more legible */
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(eref);
	READ_ENUM_FIELD(rtekind, RTEKind);

	switch (local_node->rtekind)
	{
		case RTE_RELATION:
			READ_OID_FIELD(relid);
			READ_CHAR_FIELD(relkind);
			READ_INT_FIELD(rellockmode);
			READ_NODE_FIELD(tablesample);
			break;
		case RTE_SUBQUERY:
			READ_NODE_FIELD(subquery);
			READ_BOOL_FIELD(security_barrier);
			break;
		case RTE_JOIN:
			READ_ENUM_FIELD(jointype, JoinType);
			READ_INT_FIELD(joinmergedcols);
			READ_NODE_FIELD(joinaliasvars);
			READ_NODE_FIELD(joinleftcols);
			READ_NODE_FIELD(joinrightcols);
			READ_NODE_FIELD(join_using_alias);
			break;
		case RTE_FUNCTION:
			READ_NODE_FIELD(functions);
			READ_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNCTION:
			READ_NODE_FIELD(subquery);
			READ_NODE_FIELD(functions);
			READ_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			READ_NODE_FIELD(tablefunc);
			/* The RTE must have a copy of the column type info, if any */
			if (local_node->tablefunc)
			{
				TableFunc  *tf = local_node->tablefunc;

				local_node->coltypes = tf->coltypes;
				local_node->coltypmods = tf->coltypmods;
				local_node->colcollations = tf->colcollations;
			}
			break;
		case RTE_VALUES:
			READ_NODE_FIELD(values_lists);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			READ_STRING_FIELD(ctename);
			READ_UINT_FIELD(ctelevelsup);
			READ_BOOL_FIELD(self_reference);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			READ_STRING_FIELD(enrname);
			READ_FLOAT_FIELD(enrtuples);
			READ_OID_FIELD(relid);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_RESULT:
			/* no extra fields */
			break;
        case RTE_VOID:                                                  /*CDB*/
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) local_node->rtekind);
			break;
	}

	READ_UINT_FIELD(perminfoindex);
	READ_BOOL_FIELD(lateral);
	READ_BOOL_FIELD(inh);
	READ_BOOL_FIELD(inFromCl);
	READ_NODE_FIELD(securityQuals);

	READ_BOOL_FIELD(forceDistRandom);

	READ_DONE();
}

/*
 * _readRTEPermissionInfo
 */
static RTEPermissionInfo *
_readRTEPermissionInfo(void)
{
	READ_LOCALS(RTEPermissionInfo);

	READ_OID_FIELD(relid);
	READ_BOOL_FIELD(inh);
	READ_UINT64_FIELD(requiredPerms);
	READ_OID_FIELD(checkAsUser);
	READ_BITMAPSET_FIELD(selectedCols);
	READ_BITMAPSET_FIELD(insertedCols);
	READ_BITMAPSET_FIELD(updatedCols);

	READ_DONE();
}

/*
 * _readRangeTblFunction
 */
static RangeTblFunction *
_readRangeTblFunction(void)
{
	READ_LOCALS(RangeTblFunction);

	READ_NODE_FIELD(funcexpr);
	READ_INT_FIELD(funccolcount);
	READ_NODE_FIELD(funccolnames);
	READ_NODE_FIELD(funccoltypes);
	READ_NODE_FIELD(funccoltypmods);
	READ_NODE_FIELD(funccolcollations);
	/* funcuserdata is only serialized in binary out/read functions */
	READ_BYTEA_FIELD(funcuserdata);
	READ_BITMAPSET_FIELD(funcparams);

	READ_DONE();
}

/*
 * Greenplum Database additions for serialization support
 * These are currently not used (see outfastc ad readfast.c)
 */
#include "nodes/plannodes.h"

/*
 * _readTableSampleClause
 */
static TableSampleClause *
_readTableSampleClause(void)
{
	READ_LOCALS(TableSampleClause);

	READ_OID_FIELD(tsmhandler);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(repeatable);

	READ_DONE();
}

/*
 * _readDefElem
 */
static DefElem *
_readDefElem(void)
{
	READ_LOCALS(DefElem);

	READ_STRING_FIELD(defnamespace);
	READ_STRING_FIELD(defname);
	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(defaction, DefElemAction);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 *	Stuff from plannodes.h.
 */

/*
 * _readPlannedStmt
 */
static PlannedStmt *
_readPlannedStmt(void)
{
	READ_LOCALS(PlannedStmt);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(planGen, PlanGenerator);
	READ_UINT64_FIELD(queryId);
	READ_BOOL_FIELD(hasReturning);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(canSetTag);
	READ_BOOL_FIELD(transientPlan);
	READ_BOOL_FIELD(oneoffPlan);
	READ_OID_FIELD(simplyUpdatableRel);
	READ_BOOL_FIELD(dependsOnRole);
	READ_BOOL_FIELD(parallelModeNeeded);
	READ_INT_FIELD(jitFlags);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(permInfos);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(appendRelations);
	READ_NODE_FIELD(subplans);
	READ_BITMAPSET_FIELD(rewindPlanIDs);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(relationOids);
	/* invalItems not serialized in binary mode */
	READ_NODE_FIELD(paramExecTypes);
	READ_NODE_FIELD(utilityStmt);
	READ_LOCATION_FIELD(stmt_location);
	READ_INT_FIELD(stmt_len);

	READ_INT_ARRAY(subplan_sliceIds, list_length(local_node->subplans));

	READ_INT_FIELD(numSlices);
	local_node->slices = palloc(local_node->numSlices * sizeof(PlanSlice));
	for (int i = 0; i < local_node->numSlices; i++)
	{
		READ_INT_FIELD(slices[i].sliceIndex);
		READ_INT_FIELD(slices[i].parentIndex);
		READ_INT_FIELD(slices[i].gangType);
		READ_INT_FIELD(slices[i].numsegments);
		READ_INT_FIELD(slices[i].segindex);
		READ_BOOL_FIELD(slices[i].directDispatch.isDirectDispatch);
		READ_NODE_FIELD(slices[i].directDispatch.contentIds);
	}

	READ_BITMAPSET_FIELD(rewindPlanIDs);

	READ_NODE_FIELD(intoPolicy);

	READ_UINT64_FIELD(query_mem);

	READ_INT_FIELD(total_memory_coordinator);
	READ_INT_FIELD(nsegments_coordinator);

	READ_NODE_FIELD(intoClause);
	READ_NODE_FIELD(copyIntoClause);
	READ_NODE_FIELD(refreshClause);
	READ_INT_FIELD(metricsQueryType);

	READ_DONE();
}

/*
 * ReadCommonPlan
 *	Assign the basic stuff of all nodes that inherit from Plan
 */
static void
ReadCommonPlan(Plan *local_node)
{
	READ_TEMP_LOCALS();

	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(total_cost);
	READ_FLOAT_FIELD(plan_rows);
	READ_INT_FIELD(plan_width);
	READ_BOOL_FIELD(parallel_aware);
	READ_BOOL_FIELD(parallel_safe);
	READ_BOOL_FIELD(async_capable);
	READ_INT_FIELD(plan_node_id);
	READ_NODE_FIELD(targetlist);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(lefttree);
	READ_NODE_FIELD(righttree);
	READ_NODE_FIELD(initPlan);

	READ_BITMAPSET_FIELD(extParam);
	READ_BITMAPSET_FIELD(allParam);


	READ_UINT64_FIELD(operatorMemKB);
}

/*
 * _readPlan
 */
static Plan *
_readPlan(void)
{
	READ_LOCALS_NO_FIELDS(Plan);

	ReadCommonPlan(local_node);

	READ_DONE();
}

/*
 * _readResult
 */
static Result *
_readResult(void)
{
	READ_LOCALS(Result);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(resconstantqual);

	READ_INT_FIELD(numHashFilterCols);
	READ_ATTRNUMBER_ARRAY(hashFilterColIdx, local_node->numHashFilterCols);
	READ_OID_ARRAY(hashFilterFuncs, local_node->numHashFilterCols);

	READ_DONE();
}

/*
 * _readProjectSet
 */
static ProjectSet *
_readProjectSet(void)
{
	READ_LOCALS_NO_FIELDS(ProjectSet);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readModifyTable
 */
static ModifyTable *
_readModifyTable(void)
{
	READ_LOCALS(ModifyTable);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_BOOL_FIELD(canSetTag);
	READ_UINT_FIELD(nominalRelation);
	READ_UINT_FIELD(rootRelation);
	READ_BOOL_FIELD(partColsUpdated);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(updateColnosLists);
	READ_NODE_FIELD(withCheckOptionLists);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(fdwPrivLists);
	READ_BITMAPSET_FIELD(fdwDirectModifyPlans);
	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);
	READ_ENUM_FIELD(onConflictAction, OnConflictAction);
	READ_NODE_FIELD(arbiterIndexes);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictCols);
	READ_NODE_FIELD(onConflictWhere);
	READ_UINT_FIELD(exclRelRTI);
	READ_NODE_FIELD(exclRelTlist);
	READ_NODE_FIELD(isSplitUpdates);
	READ_BOOL_FIELD(forceTupleRouting);
	READ_NODE_FIELD(mergeActionLists);

	READ_DONE();
}

/*
 * _readAppend
 */
static Append *
_readAppend(void)
{
	READ_LOCALS(Append);

	ReadCommonPlan(&local_node->plan);

	READ_BITMAPSET_FIELD(apprelids);
	READ_NODE_FIELD(appendplans);
	READ_INT_FIELD(nasyncplans);
	READ_INT_FIELD(first_partial_plan);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readMergeAppend
 */
static MergeAppend *
_readMergeAppend(void)
{
	READ_LOCALS(MergeAppend);

	ReadCommonPlan(&local_node->plan);

	READ_BITMAPSET_FIELD(apprelids);
	READ_NODE_FIELD(mergeplans);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readRecursiveUnion
 */
static RecursiveUnion *
_readRecursiveUnion(void)
{
	READ_LOCALS(RecursiveUnion);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(wtParam);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
	READ_OID_ARRAY(dupOperators, local_node->numCols);
	READ_OID_ARRAY(dupCollations, local_node->numCols);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readBitmapAnd
 */
static BitmapAnd *
_readBitmapAnd(void)
{
	READ_LOCALS(BitmapAnd);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}

/*
 * _readBitmapOr
 */
static BitmapOr *
_readBitmapOr(void)
{
	READ_LOCALS(BitmapOr);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(bitmapplans);

	READ_DONE();
}

/*
 * ReadCommonScan
 *	Assign the basic stuff of all nodes that inherit from Scan
 */
static void
ReadCommonScan(Scan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(scanrelid);
}

/*
 * _readScan
 */
static Scan *
_readScan(void)
{
	READ_LOCALS_NO_FIELDS(Scan);

	ReadCommonScan(local_node);

	READ_DONE();
}

/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(void)
{
	READ_LOCALS_NO_FIELDS(SeqScan);

	ReadCommonScan(&local_node->scan);

	READ_DONE();
}

/*
 * _readSampleScan
 */
static SampleScan *
_readSampleScan(void)
{
	READ_LOCALS(SampleScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablesample);

	READ_DONE();
}

/*
 * _readIndexScan
 */
static void readIndexScanFields(IndexScan *local_node);

static IndexScan *
_readIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(IndexScan);

	readIndexScanFields(local_node);

	READ_DONE();
}

static DynamicIndexScan *
_readDynamicIndexScan(void)
{
	READ_LOCALS(DynamicIndexScan);

	/* DynamicIndexScan has some content from IndexScan. */
	readIndexScanFields(&local_node->indexscan);
	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);
	READ_DONE();
}

static void
readIndexScanFields(IndexScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indexorderbyorig);
	READ_NODE_FIELD(indexorderbyops);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);
}

/*
 * _readIndexOnlyScan
 */
static IndexOnlyScan *
_readIndexOnlyScan(void)
{
	READ_LOCALS(IndexOnlyScan);

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(recheckqual);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indextlist);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	READ_DONE();
}

static void
readBitmapIndexScanFields(BitmapIndexScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_OID_FIELD(indexid);
	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
}

/*
 * _readBitmapIndexScan
 */
static BitmapIndexScan *
_readBitmapIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(BitmapIndexScan);

	readBitmapIndexScanFields(local_node);

	READ_DONE();
}

static DynamicBitmapIndexScan *
_readDynamicBitmapIndexScan(void)
{
	READ_LOCALS_NO_FIELDS(DynamicBitmapIndexScan);

	/* DynamicBitmapIndexScan has some content from BitmapIndexScan. */
	readBitmapIndexScanFields(&local_node->biscan);

	READ_DONE();
}

static void
readBitmapHeapScanFields(BitmapHeapScan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(bitmapqualorig);
}

/*
 * _readBitmapHeapScan
 */
static BitmapHeapScan *
_readBitmapHeapScan(void)
{
	READ_LOCALS_NO_FIELDS(BitmapHeapScan);

	readBitmapHeapScanFields(local_node);

	READ_DONE();
}

static DynamicBitmapHeapScan *
_readDynamicBitmapHeapScan(void)
{
	READ_LOCALS(DynamicBitmapHeapScan);

	/* DynamicBitmapHeapScan has some content from BitmapHeapScan. */
	readBitmapHeapScanFields(&local_node->bitmapheapscan);

	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readTidScan
 */
static TidScan *
_readTidScan(void)
{
	READ_LOCALS(TidScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tidquals);

	READ_DONE();
}

/*
 * _readTidRangeScan
 */
static TidRangeScan *
_readTidRangeScan(void)
{
	READ_LOCALS(TidRangeScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tidrangequals);

	READ_DONE();
}

/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(void)
{
	READ_LOCALS(SubqueryScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(subplan);
	READ_ENUM_FIELD(scanstatus, SubqueryScanStatus);

	READ_DONE();
}

/*
 * _readTableFunctionScan
 */
static TableFunctionScan *
_readTableFunctionScan(void)
{
	READ_LOCALS(TableFunctionScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(function);

	READ_DONE();
}

/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(void)
{
	READ_LOCALS(FunctionScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(functions);
	READ_BOOL_FIELD(funcordinality);
	READ_NODE_FIELD(param);
	READ_BOOL_FIELD(resultInTupleStore);
	READ_INT_FIELD(initplanId);

	READ_DONE();
}

/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(void)
{
	READ_LOCALS(ValuesScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(values_lists);

	READ_DONE();
}

/*
 * _readTableFuncScan
 */
static TableFuncScan *
_readTableFuncScan(void)
{
	READ_LOCALS(TableFuncScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablefunc);

	READ_DONE();
}

/*
 * _readCteScan
 */
static CteScan *
_readCteScan(void)
{
	READ_LOCALS(CteScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(ctePlanId);
	READ_INT_FIELD(cteParam);

	READ_DONE();
}

/*
 * _readNamedTuplestoreScan
 */
static NamedTuplestoreScan *
_readNamedTuplestoreScan(void)
{
	READ_LOCALS(NamedTuplestoreScan);

	ReadCommonScan(&local_node->scan);

	READ_STRING_FIELD(enrname);

	READ_DONE();
}

/*
 * _readWorkTableScan
 */
static WorkTableScan *
_readWorkTableScan(void)
{
	READ_LOCALS(WorkTableScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(wtParam);

	READ_DONE();
}

/*
 * _readForeignScan
 */
static ForeignScan *
_readForeignScan(void)
{
	READ_LOCALS(ForeignScan);

	ReadCommonScan(&local_node->scan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_UINT_FIELD(resultRelation);
	READ_OID_FIELD(fs_server);
	READ_NODE_FIELD(fdw_exprs);
	READ_NODE_FIELD(fdw_private);
	READ_NODE_FIELD(fdw_scan_tlist);
	READ_NODE_FIELD(fdw_recheck_quals);
	READ_BITMAPSET_FIELD(fs_relids);
	READ_BOOL_FIELD(fsSystemCol);

	READ_DONE();
}


/*
 * ReadCommonJoin
 *	Assign the basic stuff of all nodes that inherit from Join
 */
static void
ReadCommonJoin(Join *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(prefetch_inner);
	READ_BOOL_FIELD(prefetch_joinqual);
	READ_BOOL_FIELD(prefetch_qual);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(inner_unique);
	READ_NODE_FIELD(joinqual);
}

/*
 * _readJoin
 */
static Join *
_readJoin(void)
{
	READ_LOCALS_NO_FIELDS(Join);

	ReadCommonJoin(local_node);

	READ_DONE();
}

/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(void)
{
	READ_LOCALS(NestLoop);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(nestParams);

	READ_BOOL_FIELD(shared_outer);
	READ_BOOL_FIELD(singleton_outer); /*CDB-OLAP*/

	READ_DONE();
}

/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(void)
{
	int			numCols;

	READ_LOCALS(MergeJoin);

	ReadCommonJoin(&local_node->join);

	READ_BOOL_FIELD(skip_mark_restore);
	READ_NODE_FIELD(mergeclauses);

	numCols = list_length(local_node->mergeclauses);

	READ_OID_ARRAY(mergeFamilies, numCols);
	READ_OID_ARRAY(mergeCollations, numCols);
	READ_INT_ARRAY(mergeStrategies, numCols);
	READ_BOOL_ARRAY(mergeNullsFirst, numCols);
	READ_BOOL_FIELD(unique_outer);

	READ_DONE();
}

/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(void)
{
	READ_LOCALS(HashJoin);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(hashclauses);
	READ_NODE_FIELD(hashqualclauses);
	READ_NODE_FIELD(hashoperators);
	READ_NODE_FIELD(hashcollations);
	READ_NODE_FIELD(hashkeys);

	READ_DONE();
}

/*
 * _readMaterial
 */
static Material *
_readMaterial(void)
{
	READ_LOCALS(Material);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(cdb_strict);
	READ_BOOL_FIELD(cdb_shield_child_from_rescans);

	READ_DONE();
}

/*
 * _readMemoize
 */
static Memoize *
_readMemoize(void)
{
	READ_LOCALS(Memoize);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numKeys);
	READ_OID_ARRAY(hashOperators, local_node->numKeys);
	READ_OID_ARRAY(collations, local_node->numKeys);
	READ_NODE_FIELD(param_exprs);
	READ_BOOL_FIELD(singlerow);
	READ_BOOL_FIELD(binary_mode);
	READ_UINT_FIELD(est_entries);
	READ_BITMAPSET_FIELD(keyparamids);

	READ_DONE();
}

/*
 * ReadCommonSort
 *	Assign the basic stuff of all nodes that inherit from Sort
 */
static void
ReadCommonSort(Sort *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
}

/*
 * _readSort
 */
static Sort *
_readSort(void)
{
	READ_LOCALS_NO_FIELDS(Sort);

	ReadCommonSort(local_node);

	READ_DONE();
}

/*
 * _readIncrementalSort
 */
static IncrementalSort *
_readIncrementalSort(void)
{
	READ_LOCALS(IncrementalSort);

	ReadCommonSort(&local_node->sort);

	READ_INT_FIELD(nPresortedCols);

	READ_DONE();
}

/*
 * _readAgg
 */
static Agg *
_readAgg(void)
{
	READ_LOCALS(Agg);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(aggstrategy, AggStrategy);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);
	READ_OID_ARRAY(grpOperators, local_node->numCols);
	READ_OID_ARRAY(grpCollations, local_node->numCols);
	READ_LONG_FIELD(numGroups);
	READ_UINT64_FIELD(transitionSpace);
	READ_BITMAPSET_FIELD(aggParams);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(chain);
	READ_BOOL_FIELD(streaming);
	READ_UINT_FIELD(agg_expr_id);

	READ_DONE();
}

static TupleSplit *
_readTupleSplit(void)
{
	READ_LOCALS(TupleSplit);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);

	READ_NODE_FIELD(dqa_expr_lst);

	READ_DONE();
}

static DQAExpr*
_readDQAExpr(void)
{
    READ_LOCALS(DQAExpr);

    READ_INT_FIELD(agg_expr_id);
    READ_BITMAPSET_FIELD(agg_args_id_bms);
    READ_NODE_FIELD(agg_filter);

    READ_DONE();
}

/*
 * _readWindowAgg
 */
static WindowAgg *
_readWindowAgg(void)
{
	READ_LOCALS(WindowAgg);

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(winref);
	READ_INT_FIELD(partNumCols);
	READ_ATTRNUMBER_ARRAY(partColIdx, local_node->partNumCols);
	READ_OID_ARRAY(partOperators, local_node->partNumCols);
	READ_OID_ARRAY(partCollations, local_node->partNumCols);
	READ_INT_FIELD(ordNumCols);
	READ_ATTRNUMBER_ARRAY(ordColIdx, local_node->ordNumCols);
	READ_OID_ARRAY(ordOperators, local_node->ordNumCols);
	READ_OID_ARRAY(ordCollations, local_node->ordNumCols);
	READ_INT_FIELD(firstOrderCol);
	READ_OID_FIELD(firstOrderCmpOperator);
	READ_BOOL_FIELD(firstOrderNullsFirst);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_NODE_FIELD(runCondition);
	READ_NODE_FIELD(runConditionOrig);
	READ_OID_FIELD(startInRangeFunc);
	READ_OID_FIELD(endInRangeFunc);
	READ_OID_FIELD(inRangeColl);
	READ_BOOL_FIELD(inRangeAsc);
	READ_BOOL_FIELD(inRangeNullsFirst);
	READ_BOOL_FIELD(topWindow);

	READ_DONE();
}

/*
 * _readUnique
 */
static Unique *
_readUnique(void)
{
	READ_LOCALS(Unique);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->numCols);
	READ_OID_ARRAY(uniqOperators, local_node->numCols);
	READ_OID_ARRAY(uniqCollations, local_node->numCols);

	READ_DONE();
}

/*
 * _readGather
 */
static Gather *
_readGather(void)
{
	READ_LOCALS(Gather);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_BOOL_FIELD(single_copy);
	READ_BOOL_FIELD(invisible);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readGatherMerge
 */
static GatherMerge *
_readGatherMerge(void)
{
	READ_LOCALS(GatherMerge);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readHash
 */
static Hash *
_readHash(void)
{
	READ_LOCALS(Hash);

	ReadCommonPlan(&local_node->plan);

    READ_BOOL_FIELD(rescannable);           /*CDB*/
	READ_NODE_FIELD(hashkeys);
	READ_OID_FIELD(skewTable);
	READ_INT_FIELD(skewColumn);
	READ_BOOL_FIELD(skewInherit);
	READ_FLOAT_FIELD(rows_total);

	READ_DONE();
}

/*
 * _readSetOp
 */
static SetOp *
_readSetOp(void)
{
	READ_LOCALS(SetOp);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(cmd, SetOpCmd);
	READ_ENUM_FIELD(strategy, SetOpStrategy);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);
	READ_OID_ARRAY(dupOperators, local_node->numCols);
	READ_OID_ARRAY(dupCollations, local_node->numCols);
	READ_INT_FIELD(flagColIdx);
	READ_INT_FIELD(firstFlag);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readLockRows
 */
static LockRows *
_readLockRows(void)
{
	READ_LOCALS(LockRows);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);

	READ_DONE();
}

/*
 * _readLimit
 */
static Limit *
_readLimit(void)
{
	READ_LOCALS(Limit);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_ENUM_FIELD(limitOption, LimitOption);
	READ_INT_FIELD(uniqNumCols);
	READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->uniqNumCols);
	READ_OID_ARRAY(uniqOperators, local_node->uniqNumCols);
	READ_OID_ARRAY(uniqCollations, local_node->uniqNumCols);

	READ_DONE();
}

/*
 * _readNestLoopParam
 */
static NestLoopParam *
_readNestLoopParam(void)
{
	READ_LOCALS(NestLoopParam);

	READ_INT_FIELD(paramno);
	READ_NODE_FIELD(paramval);

	READ_DONE();
}

/*
 * _readPlanRowMark
 */
static PlanRowMark *
_readPlanRowMark(void)
{
	READ_LOCALS(PlanRowMark);

	READ_UINT_FIELD(rti);
	READ_UINT_FIELD(prti);
	READ_UINT_FIELD(rowmarkId);
	READ_ENUM_FIELD(markType, RowMarkType);
	READ_INT_FIELD(allMarkTypes);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	READ_BOOL_FIELD(isParent);
	READ_BOOL_FIELD(canOptSelectLockingClause);

	READ_DONE();
}

static PartitionPruneInfo *
_readPartitionPruneInfo(void)
{
	READ_LOCALS(PartitionPruneInfo);

	READ_NODE_FIELD(prune_infos);
	READ_BITMAPSET_FIELD(other_subplans);

	READ_DONE();
}

static PartitionedRelPruneInfo *
_readPartitionedRelPruneInfo(void)
{
	READ_LOCALS(PartitionedRelPruneInfo);

	READ_UINT_FIELD(rtindex);
	READ_BITMAPSET_FIELD(present_parts);
	READ_INT_FIELD(nparts);
	READ_INT_ARRAY(subplan_map, local_node->nparts);
	READ_INT_ARRAY(subpart_map, local_node->nparts);
	READ_OID_ARRAY(relid_map, local_node->nparts);
	READ_NODE_FIELD(initial_pruning_steps);
	READ_NODE_FIELD(exec_pruning_steps);
	READ_BITMAPSET_FIELD(execparamids);

	READ_DONE();
}

static PartitionPruneStepOp *
_readPartitionPruneStepOp(void)
{
	READ_LOCALS(PartitionPruneStepOp);

	READ_INT_FIELD(step.step_id);
	READ_INT_FIELD(opstrategy);
	READ_NODE_FIELD(exprs);
	READ_NODE_FIELD(cmpfns);
	READ_BITMAPSET_FIELD(nullkeys);

	READ_DONE();
}

static PartitionPruneStepCombine *
_readPartitionPruneStepCombine(void)
{
	READ_LOCALS(PartitionPruneStepCombine);

	READ_INT_FIELD(step.step_id);
	READ_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
	READ_NODE_FIELD(source_stepids);

	READ_DONE();
}

/*
 * _readPlanInvalItem
 */
static PlanInvalItem *
_readPlanInvalItem(void)
{
	READ_LOCALS(PlanInvalItem);

	READ_INT_FIELD(cacheId);
	READ_UINT_FIELD(hashValue);

	READ_DONE();
}

/*
 * _readSubPlan
 */
static SubPlan *
_readSubPlan(void)
{
	READ_LOCALS(SubPlan);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(paramIds);
	READ_INT_FIELD(plan_id);
	READ_STRING_FIELD(plan_name);
	READ_OID_FIELD(firstColType);
	READ_INT_FIELD(firstColTypmod);
	READ_OID_FIELD(firstColCollation);
	READ_BOOL_FIELD(useHashTable);
	READ_BOOL_FIELD(unknownEqFalse);
    READ_BOOL_FIELD(parallel_safe);
	READ_BOOL_FIELD(is_initplan); /*CDB*/
	READ_BOOL_FIELD(is_multirow); /*CDB*/
	READ_NODE_FIELD(setParam);
	READ_NODE_FIELD(parParam);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(extParam);
	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(per_call_cost);

	READ_DONE();
}

/*
 * _readAlternativeSubPlan
 */
static AlternativeSubPlan *
_readAlternativeSubPlan(void)
{
	READ_LOCALS(AlternativeSubPlan);

	READ_NODE_FIELD(subplans);

	READ_DONE();
}

static RestrictInfo *
_readRestrictInfo(void)
{
	READ_LOCALS(RestrictInfo);

	/* NB: this isn't a complete set of fields */
	READ_NODE_FIELD(clause);
	READ_BOOL_FIELD(is_pushed_down);
	READ_BOOL_FIELD(can_join);
	READ_BOOL_FIELD(pseudoconstant);
	READ_BOOL_FIELD(has_clone);
	READ_BOOL_FIELD(is_clone);
	READ_BOOL_FIELD(leakproof);
	READ_ENUM_FIELD(has_volatile, VolatileFunctionStatus);
	READ_UINT_FIELD(security_level);
	READ_BOOL_FIELD(contain_outer_query_references);
	READ_BITMAPSET_FIELD(clause_relids);
	READ_BITMAPSET_FIELD(required_relids);
	READ_BITMAPSET_FIELD(outer_relids);
	READ_BITMAPSET_FIELD(incompatible_relids);
	READ_BITMAPSET_FIELD(left_relids);
	READ_BITMAPSET_FIELD(right_relids);
	READ_NODE_FIELD(orclause);

	READ_FLOAT_FIELD(norm_selec);
	READ_FLOAT_FIELD(outer_selec);
	READ_NODE_FIELD(mergeopfamilies);

	READ_NODE_FIELD(left_em);
	READ_NODE_FIELD(right_em);
	READ_BOOL_FIELD(outer_is_left);
	READ_OID_FIELD(hashjoinoperator);
	READ_OID_FIELD(left_hasheqoperator);
	READ_OID_FIELD(right_hasheqoperator);

	READ_DONE();
}


static SegfileMapNode *
_readSegfileMapNode(void)
{
	READ_LOCALS(SegfileMapNode);

	READ_OID_FIELD(relid);
	READ_INT_FIELD(segno);

	READ_DONE();
}

static ExtTableTypeDesc *
_readExtTableTypeDesc(void)
{
	READ_LOCALS(ExtTableTypeDesc);

	READ_ENUM_FIELD(exttabletype, ExtTableType);
	READ_NODE_FIELD(location_list);
	READ_NODE_FIELD(on_clause);
	READ_STRING_FIELD(command_string);

	READ_DONE();
}

static CreateExternalStmt *
_readCreateExternalStmt(void)
{
	READ_LOCALS(CreateExternalStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_NODE_FIELD(exttypedesc);
	READ_STRING_FIELD(format);
	READ_NODE_FIELD(formatOpts);
	READ_BOOL_FIELD(isweb);
	READ_BOOL_FIELD(iswritable);
	READ_NODE_FIELD(sreh);
	READ_NODE_FIELD(extOptions);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(distributedBy);

	READ_DONE();
}

static CreateSchemaStmt *
_readCreateSchemaStmt(void)
{
	READ_LOCALS(CreateSchemaStmt);

	READ_STRING_FIELD(schemaname);
	READ_NODE_FIELD(authrole);
	local_node->schemaElts = 0;
	READ_BOOL_FIELD(istemp);

	READ_DONE();
}


static CreatePLangStmt *
_readCreatePLangStmt(void)
{
	READ_LOCALS(CreatePLangStmt);

	READ_BOOL_FIELD(replace);
	READ_STRING_FIELD(plname);
	READ_NODE_FIELD(plhandler);
	READ_NODE_FIELD(plinline);
	READ_NODE_FIELD(plvalidator);
	READ_BOOL_FIELD(pltrusted);

	READ_DONE();
}

static CreateSeqStmt *
_readCreateSeqStmt(void)
{
	READ_LOCALS(CreateSeqStmt);
	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);
	READ_OID_FIELD(ownerId);
	READ_BOOL_FIELD(for_identity);
	READ_BOOL_FIELD(if_not_exists);

	READ_DONE();
}

static AlterSeqStmt *
_readAlterSeqStmt(void)
{
	READ_LOCALS(AlterSeqStmt);

	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);
	READ_BOOL_FIELD(for_identity);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static ClusterStmt *
_readClusterStmt(void)
{
	READ_LOCALS(ClusterStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(indexname);

	READ_DONE();
}

static CreatedbStmt *
_readCreatedbStmt(void)
{
	READ_LOCALS(CreatedbStmt);

	READ_STRING_FIELD(dbname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropdbStmt *
_readDropdbStmt(void)
{
	READ_LOCALS(DropdbStmt);

	READ_STRING_FIELD(dbname);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static CreateDomainStmt *
_readCreateDomainStmt(void)
{
	READ_LOCALS(CreateDomainStmt);

	READ_NODE_FIELD(domainname);
	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(collClause);
	READ_NODE_FIELD(constraints);

	READ_DONE();
}


static ReturnStmt *
_readReturnStmt(void)
{
	READ_LOCALS(ReturnStmt);

	READ_NODE_FIELD(returnval);

	READ_DONE();
}

static RawStmt *
_readRawStmt(void)
{
	READ_LOCALS(RawStmt);

	READ_NODE_FIELD(stmt);
	READ_LOCATION_FIELD(stmt_location);
	READ_INT_FIELD(stmt_len);

	READ_DONE();
}

static CreateFunctionStmt *
_readCreateFunctionStmt(void)
{
	READ_LOCALS(CreateFunctionStmt);

	READ_BOOL_FIELD(is_procedure);
	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(parameters);
	READ_NODE_FIELD(returnType);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(sql_body);

	READ_DONE();
}

static FunctionParameter *
_readFunctionParameter(void)
{
	READ_LOCALS(FunctionParameter);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(argType);
	READ_ENUM_FIELD(mode, FunctionParameterMode);
	READ_NODE_FIELD(defexpr);

	READ_DONE();
}

static AlterFunctionStmt *
_readAlterFunctionStmt(void)
{
	READ_LOCALS(AlterFunctionStmt);
	READ_ENUM_FIELD(objtype,ObjectType);
	READ_NODE_FIELD(func);
	READ_NODE_FIELD(actions);

	READ_DONE();
}

static DefineStmt *
_readDefineStmt(void)
{
	READ_LOCALS(DefineStmt);
	READ_ENUM_FIELD(kind, ObjectType);
	READ_BOOL_FIELD(oldstyle);
	READ_NODE_FIELD(defnames);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(definition);
	READ_BOOL_FIELD(if_not_exists);
	READ_BOOL_FIELD(replace);
	READ_BOOL_FIELD(trusted);   /* CDB */

	READ_DONE();
}

static CompositeTypeStmt *
_readCompositeTypeStmt(void)
{
	READ_LOCALS(CompositeTypeStmt);

	READ_NODE_FIELD(typevar);
	READ_NODE_FIELD(coldeflist);

	READ_DONE();
}

static CreateEnumStmt *
_readCreateEnumStmt(void)
{
	READ_LOCALS(CreateEnumStmt);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(vals);

	READ_DONE();
}

static CreateCastStmt *
_readCreateCastStmt(void)
{
	READ_LOCALS(CreateCastStmt);

	READ_NODE_FIELD(sourcetype);
	READ_NODE_FIELD(targettype);
	READ_NODE_FIELD(func);
	READ_ENUM_FIELD(context, CoercionContext);
	READ_BOOL_FIELD(inout);

	READ_DONE();
}

static CreateOpClassStmt *
_readCreateOpClassStmt(void)
{
	READ_LOCALS(CreateOpClassStmt);

	READ_NODE_FIELD(opclassname);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);
	READ_NODE_FIELD(datatype);
	READ_NODE_FIELD(items);
	READ_BOOL_FIELD(isDefault);

	READ_DONE();
}

static CreateOpClassItem *
_readCreateOpClassItem(void)
{
	READ_LOCALS(CreateOpClassItem);
	READ_INT_FIELD(itemtype);
	READ_NODE_FIELD(name);
	READ_INT_FIELD(number);
	READ_NODE_FIELD(order_family);
	READ_NODE_FIELD(class_args);
	READ_NODE_FIELD(storedtype);

	READ_DONE();
}

static CreateOpFamilyStmt *
_readCreateOpFamilyStmt(void)
{
	READ_LOCALS(CreateOpFamilyStmt);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);

	READ_DONE();
}

static AlterOpFamilyStmt *
_readAlterOpFamilyStmt(void)
{
	READ_LOCALS(AlterOpFamilyStmt);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);
	READ_BOOL_FIELD(isDrop);
	READ_NODE_FIELD(items);

	READ_DONE();
}

static CreateConversionStmt *
_readCreateConversionStmt(void)
{
	READ_LOCALS(CreateConversionStmt);

	READ_NODE_FIELD(conversion_name);
	READ_STRING_FIELD(for_encoding_name);
	READ_STRING_FIELD(to_encoding_name);
	READ_NODE_FIELD(func_name);
	READ_BOOL_FIELD(def);

	READ_DONE();
}

static GrantStmt *
_readGrantStmt(void)
{
	READ_LOCALS(GrantStmt);

	READ_BOOL_FIELD(is_grant);
	READ_ENUM_FIELD(targtype,GrantTargetType);
	READ_ENUM_FIELD(objtype,ObjectType);
	READ_NODE_FIELD(objects);
	READ_NODE_FIELD(privileges);
	READ_NODE_FIELD(grantees);
	READ_BOOL_FIELD(grant_option);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static ObjectWithArgs *
_readObjectWithArgs(void)
{
	READ_LOCALS(ObjectWithArgs);

	READ_NODE_FIELD(objname);
	READ_NODE_FIELD(objargs);
	READ_BOOL_FIELD(args_unspecified);

	READ_DONE();
}


static LockStmt *
_readLockStmt(void)
{
	READ_LOCALS(LockStmt);

	READ_NODE_FIELD(relations);
	READ_INT_FIELD(mode);
	READ_BOOL_FIELD(nowait);

	READ_DONE();
}

static ConstraintsSetStmt *
_readConstraintsSetStmt(void)
{
	READ_LOCALS(ConstraintsSetStmt);

	READ_NODE_FIELD(constraints);
	READ_BOOL_FIELD(deferred);

	READ_DONE();
}

/*
 * _readVacuumStmt
 */
static VacuumStmt *
_readVacuumStmt(void)
{
	READ_LOCALS(VacuumStmt);

	READ_NODE_FIELD(options);
	READ_NODE_FIELD(rels);
	READ_BOOL_FIELD(is_vacuumcmd);

	READ_DONE();
}

static VacuumRelation *
_readVacuumRelation(void)
{
	READ_LOCALS(VacuumRelation);

	READ_NODE_FIELD(relation);
	READ_OID_FIELD(oid);
	READ_NODE_FIELD(va_cols);

	READ_DONE();
}

static CreatePublicationStmt *
_readCreatePublicationStmt()
{
	READ_LOCALS(CreatePublicationStmt);

	READ_STRING_FIELD(pubname);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(pubobjects);
	READ_BOOL_FIELD(for_all_tables);

	READ_DONE();
}

static AlterPublicationStmt *
_readAlterPublicationStmt()
{
	READ_LOCALS(AlterPublicationStmt);

	READ_STRING_FIELD(pubname);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(pubobjects);
	READ_BOOL_FIELD(for_all_tables);
	READ_ENUM_FIELD(action, AlterPublicationAction);

	READ_DONE();
}

static CreateSubscriptionStmt *
_readCreateSubscriptionStmt()
{
	READ_LOCALS(CreateSubscriptionStmt);

	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(conninfo);
	READ_NODE_FIELD(publication);
	READ_NODE_FIELD(options);

	/*
	 * conninfo can be an empty string, but the serialization
	 * doesn't distinguish an empty string from NULL. The
	 * code that executes the command in't prepared for a NULL.
	 */
	if (local_node->conninfo == NULL)
		local_node->conninfo = pstrdup("");

	READ_DONE();
}

static DropSubscriptionStmt *
_readDropSubscriptionStmt()
{
	READ_LOCALS(DropSubscriptionStmt);

	READ_STRING_FIELD(subname);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static AlterSubscriptionStmt *
_readAlterSubscriptionStmt()
{
	READ_LOCALS(AlterSubscriptionStmt);

	READ_ENUM_FIELD(kind, AlterSubscriptionType);
	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(conninfo);
	READ_NODE_FIELD(publication);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreatePolicyStmt *
_readCreatePolicyStmt()
{
	READ_LOCALS(CreatePolicyStmt);

	READ_STRING_FIELD(policy_name);
	READ_NODE_FIELD(table);
	READ_STRING_FIELD(cmd_name);
	READ_BOOL_FIELD(permissive);
	READ_NODE_FIELD(roles);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(with_check);

	READ_DONE();
}

static AlterPolicyStmt *
_readAlterPolicyStmt()
{
	READ_LOCALS(AlterPolicyStmt);

	READ_STRING_FIELD(policy_name);
	READ_NODE_FIELD(table);
	READ_NODE_FIELD(roles);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(with_check);

	READ_DONE();
}

static CreateTransformStmt *
_readCreateTransformStmt()
{
	READ_LOCALS(CreateTransformStmt);

	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(type_name);
	READ_STRING_FIELD(lang);
	READ_NODE_FIELD(fromsql);
	READ_NODE_FIELD(tosql);

	READ_DONE();
}

static CdbProcess *
_readCdbProcess(void)
{
	READ_LOCALS(CdbProcess);

	READ_STRING_FIELD(listenerAddr);
	READ_INT_FIELD(listenerPort);
	READ_INT_FIELD(pid);
	READ_INT_FIELD(contentid);
	READ_INT_FIELD(dbid);

	READ_DONE();
}

static SliceTable *
_readSliceTable(void)
{
	READ_LOCALS(SliceTable);

	READ_INT_FIELD(localSlice);
	READ_INT_FIELD(numSlices);
	local_node->slices = palloc0(local_node->numSlices * sizeof(ExecSlice));
	for (int i = 0; i < local_node->numSlices; i++)
	{
		READ_INT_FIELD(slices[i].sliceIndex);
		READ_INT_FIELD(slices[i].rootIndex);
		READ_INT_FIELD(slices[i].parentIndex);
		READ_INT_FIELD(slices[i].planNumSegments);
		READ_NODE_FIELD(slices[i].children); /* List of int index */
		READ_ENUM_FIELD(slices[i].gangType, GangType);
		READ_NODE_FIELD(slices[i].segments); /* List of int index */
		READ_DUMMY_FIELD(slices[i].primaryGang, NULL);
		READ_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		READ_BITMAPSET_FIELD(slices[i].processesMap);
	}
	READ_BOOL_FIELD(hasMotions);

	READ_INT_FIELD(instrument_options);
	READ_INT_FIELD(ic_instance_id);

	READ_DONE();
}

static CursorPosInfo *
_readCursorPosInfo(void)
{
	READ_LOCALS(CursorPosInfo);

	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(gp_segment_id);
	READ_UINT_FIELD(ctid.ip_blkid.bi_hi);
	READ_UINT_FIELD(ctid.ip_blkid.bi_lo);
	READ_UINT_FIELD(ctid.ip_posid);
	READ_OID_FIELD(table_oid);

	READ_DONE();
}

static VariableSetStmt *
_readVariableSetStmt(void)
{
	READ_LOCALS(VariableSetStmt);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(kind, VariableSetKind);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(is_local);

	READ_DONE();
}

static TableValueExpr *
_readTableValueExpr(void)
{
	READ_LOCALS(TableValueExpr);

	READ_NODE_FIELD(subquery);

	READ_DONE();
}

static AlterTypeStmtSetDefaultEnc *
_readAlterTypeStmtSetDefaultEnc(void)
{
	READ_LOCALS(AlterTypeStmtSetDefaultEnc);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}

static AlterTypeStmt *
_readAlterTypeStmt(void)
{
	READ_LOCALS(AlterTypeStmt);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static PartitionElem *
_readPartitionElem(void)
{
	READ_LOCALS(PartitionElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(collation);
	READ_NODE_FIELD(opclass);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static PartitionSpec *
_readPartitionSpec(void)
{
	READ_LOCALS(PartitionSpec);

	READ_CHAR_FIELD(strategy);
	READ_NODE_FIELD(partParams);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readPartitionBoundSpec
 */
static PartitionBoundSpec *
_readPartitionBoundSpec(void)
{
	READ_LOCALS(PartitionBoundSpec);

	READ_CHAR_FIELD(strategy);
	READ_BOOL_FIELD(is_default);
	READ_INT_FIELD(modulus);
	READ_INT_FIELD(remainder);
	READ_NODE_FIELD(listdatums);
	READ_NODE_FIELD(lowerdatums);
	READ_NODE_FIELD(upperdatums);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readPartitionRangeDatum
 */
static PartitionRangeDatum *
_readPartitionRangeDatum(void)
{
	READ_LOCALS(PartitionRangeDatum);

	READ_ENUM_FIELD(kind, PartitionRangeDatumKind);
	READ_NODE_FIELD(value);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static PartitionCmd *
_readPartitionCmd(void)
{
	READ_LOCALS(PartitionCmd);

	READ_NODE_FIELD(name);
	READ_NODE_FIELD(bound);

	READ_DONE();
}


/*
 * For some structs, we have to provide a read functions because it differs
 * from the text version (or the text version doesn't exist at all).
 */

/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType); Assert(local_node->commandType <= CMD_NOTHING);
	READ_ENUM_FIELD(querySource, QuerySource); 	Assert(local_node->querySource <= QSRC_PLANNER);
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindowFuncs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_BOOL_FIELD(hasDynamicFunctions);
	READ_BOOL_FIELD(hasFuncsWithExecRestrictions);
	READ_BOOL_FIELD(hasDistinctOn);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(hasForUpdate);
	READ_BOOL_FIELD(hasRowSecurity);
	READ_BOOL_FIELD(canOptSelectLockingClause);
	READ_NODE_FIELD(cteList);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(rteperminfos);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(withCheckOptions);
	READ_NODE_FIELD(onConflict);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(scatterClause);
	READ_BOOL_FIELD(isTableValueSelect);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(constraintDeps);
	READ_BOOL_FIELD(parentStmtType);

	/* policy not serialized */

	READ_DONE();
}

static DMLActionExpr *
_readDMLActionExpr(void)
{
	READ_LOCALS(DMLActionExpr);

	READ_DONE();
}

/*
 *	Stuff from primnodes.h.
 */

/*
 * _readConst
 */
static Const *
_readConst(void)
{
	READ_LOCALS(Const);

	READ_OID_FIELD(consttype);
	READ_INT_FIELD(consttypmod);
	READ_OID_FIELD(constcollid);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);
	READ_LOCATION_FIELD(location);
	if (local_node->constisnull)
		local_node->constvalue = 0;
	else
		local_node->constvalue = readDatumBinary(local_node->constbyval);

	READ_DONE();
}

static ResTarget *
_readResTarget(void)
{
	READ_LOCALS(ResTarget);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(indirection);
	READ_NODE_FIELD(val);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static MultiAssignRef *
_readMultiAssignRef(void)
{
	READ_LOCALS(MultiAssignRef);

	READ_NODE_FIELD(source);
	READ_INT_FIELD(colno);
	READ_INT_FIELD(ncolumns);

	READ_DONE();
}

static DropOwnedStmt *
_readDropOwnedStmt(void)
{
	READ_LOCALS(DropOwnedStmt);

	READ_NODE_FIELD(roles);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static ReassignOwnedStmt *
_readReassignOwnedStmt(void)
{
	READ_LOCALS(ReassignOwnedStmt);

	READ_NODE_FIELD(roles);
	READ_NODE_FIELD(newrole);

	READ_DONE();
}

static AlterObjectDependsStmt *
_readAlterObjectDependsStmt(void)
{
	READ_LOCALS(AlterObjectDependsStmt);

	READ_ENUM_FIELD(objectType,ObjectType);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(extname);

	READ_DONE();
}

static SelectStmt *
_readSelectStmt(void)
{
	READ_LOCALS(SelectStmt);

	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(intoClause);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(fromClause);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(groupClause);
	READ_BOOL_FIELD(groupDistinct);
	READ_NODE_FIELD(havingClause);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(valuesLists);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(scatterClause);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	READ_ENUM_FIELD(limitOption, LimitOption);
	READ_NODE_FIELD(lockingClause);
	READ_NODE_FIELD(withClause);
	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_BOOL_FIELD(disableLockingOptimization);
	READ_DONE();
}

static ParamRef *
_readParamRef(void)
{
	READ_LOCALS(ParamRef);

	READ_INT_FIELD(number);
	READ_LOCATION_FIELD(location);
	READ_DONE();
}

static InsertStmt *
_readInsertStmt(void)
{
	READ_LOCALS(InsertStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(cols);
	READ_NODE_FIELD(selectStmt);
	READ_NODE_FIELD(onConflictClause);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(withClause);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_DONE();
}

static DeleteStmt *
_readDeleteStmt(void)
{
	READ_LOCALS(DeleteStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(usingClause);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(withClause);
	READ_DONE();
}

static UpdateStmt *
_readUpdateStmt(void)
{
	READ_LOCALS(UpdateStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(fromClause);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(withClause);
	READ_DONE();
}

static A_Const *
_readAConst(void)
{
	READ_LOCALS(A_Const);
	bool isnull;

	memcpy(&isnull, read_str_ptr, sizeof(bool)); read_str_ptr+=sizeof(bool);
	local_node->isnull = isnull;
	if (!isnull)
	{
		int16 vt;
		memcpy(&vt, read_str_ptr, sizeof(int16)); read_str_ptr+=sizeof(int16);
		local_node->val.node.type = (NodeTag) vt;
		switch ((NodeTag) vt)
		{
			case T_Integer:
			{
				long ival;
				memcpy(&ival, read_str_ptr, sizeof(long)); read_str_ptr+=sizeof(long);
				local_node->val.ival.ival = (int) ival;
				break;
			}
			case T_Float:
			case T_String:
			case T_BitString:
			{
				int slen; char *nn;
				memcpy(&slen, read_str_ptr, sizeof(int)); read_str_ptr+=sizeof(int);
				nn = palloc(slen+1);
				if (slen > 0) memcpy(nn, read_str_ptr, slen);
				read_str_ptr+=slen; nn[slen]='\0';
				local_node->val.sval.sval = nn;
				break;
			}
			case T_Boolean:
			{
				bool bval;
				memcpy(&bval, read_str_ptr, sizeof(bool)); read_str_ptr+=sizeof(bool);
				local_node->val.boolval.boolval = bval;
				break;
			}
			default:
				break;
		}
	}
	READ_LOCATION_FIELD(location);   /*CDB*/
	READ_DONE();
}

static A_Star *
_readA_Star(void)
{
	READ_LOCALS(A_Star);
	READ_DONE();
}


static A_Indices *
_readA_Indices(void)
{
	READ_LOCALS(A_Indices);
	READ_BOOL_FIELD(is_slice);
	READ_NODE_FIELD(lidx);
	READ_NODE_FIELD(uidx);
	READ_DONE();
}

static A_Indirection *
_readA_Indirection(void)
{
	READ_LOCALS(A_Indirection);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(indirection);
	READ_DONE();
}

static RoleSpec *
_readRoleSpec(void)
{
	READ_LOCALS(RoleSpec);

	READ_ENUM_FIELD(roletype, RoleSpecType);
	READ_STRING_FIELD(rolename);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static A_Expr *
_readAExpr(void)
{
	READ_LOCALS(A_Expr);

	READ_ENUM_FIELD(kind, A_Expr_Kind);

	Assert(local_node->kind <= AEXPR_NOT_BETWEEN_SYM);

	switch (local_node->kind)
	{
		case AEXPR_OP:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			READ_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_OF:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_IN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:

			READ_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:

			READ_NODE_FIELD(name);
			break;
		default:
			elog(ERROR,"Unable to understand A_Expr node ");
			break;
	}

	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{
	READ_LOCALS(OpExpr);

	READ_OID_FIELD(opno);
	READ_OID_FIELD(opfuncid);

	/*
	 * The opfuncid is stored in the textual format primarily for debugging
	 * and documentation reasons.  We want to always read it as zero to force
	 * it to be re-looked-up in the pg_operator entry.	This ensures that
	 * stored rules don't have hidden dependencies on operators' functions.
	 * (We don't currently support an ALTER OPERATOR command, but might
	 * someday.)
	 */
/*	local_node->opfuncid = InvalidOid; */

	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
	READ_OID_FIELD(opcollid);
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	READ_ENUM_FIELD(boolop, BoolExprType);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 *	Stuff from parsenodes.h.
 */


/*
 * _readCollateClause
 */
static CollateClause *
_readCollateClause(void)
{
	READ_LOCALS(CollateClause);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(collname);
	READ_INT_FIELD(location);

	READ_DONE();
}

static ExtensibleNode *
_readExtensibleNode(void)
{
	const ExtensibleNodeMethods *methods;
	ExtensibleNode *local_node;
	const char *extnodename;

	char *str;
	const char *save_strtok = NULL;
	const char *save_begin = NULL;
	const char ** save_strtok_ptr = &save_strtok;
	const char ** save_begin_ptr = &save_begin;

	READ_STRING_VAR(extnodename);
	if (!extnodename)
		elog(ERROR, "extnodename has to be supplied");
	methods = GetExtensibleNodeMethods(extnodename, false);

	local_node = (ExtensibleNode *) newNode(methods->node_size,
											T_ExtensibleNode);
	local_node->extnodename = extnodename;

	READ_STRING_VAR(str);

	/*
	 * deserialize the private fields
	 */

	/* set the states for pg_strtok(), let methods->nodeRead() to process str */
	save_strtok_states(save_strtok_ptr, save_begin_ptr);
	set_strtok_states(str, str);

	/* do reading */
	methods->nodeRead(local_node);

	/* set the states for pg_strtok() back */
	set_strtok_states(save_strtok, save_begin);

	READ_DONE();
}

/*
 * Greenplum Database additions for serialization support
 */
#include "nodes/plannodes.h"

static CreateExtensionStmt *
_readCreateExtensionStmt(void)
{
	READ_LOCALS(CreateExtensionStmt);
	READ_STRING_FIELD(extname);
	READ_BOOL_FIELD(if_not_exists);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(create_ext_state, CreateExtensionState);

	READ_DONE();
}

static void
_readCreateStmt_common(CreateStmt *local_node)
{
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_NODE_FIELD(inhRelations);
	READ_NODE_FIELD(partspec);
	READ_NODE_FIELD(partbound);
	READ_NODE_FIELD(ofTypename);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(oncommit,OnCommitAction);
	READ_STRING_FIELD(tablespacename);
	READ_STRING_FIELD(accessMethod);
	READ_BOOL_FIELD(if_not_exists);
	READ_BOOL_FIELD(gp_style_alter_part);

	READ_NODE_FIELD(distributedBy);
	READ_NODE_FIELD(partitionBy);
	READ_CHAR_FIELD(relKind);
	READ_OID_FIELD(ownerid);
	READ_BOOL_FIELD(buildAoBlkdir);
	READ_NODE_FIELD(attr_encodings);
	READ_BOOL_FIELD(isCtas);
	READ_NODE_FIELD(intoQuery);
	READ_NODE_FIELD(intoPolicy);

	READ_NODE_FIELD(part_idx_oids);
	READ_NODE_FIELD(part_idx_names);

	/*
	 * Some extra checks to make sure we didn't get lost
	 * during serialization/deserialization
	 */
	Assert(local_node->relKind == RELKIND_RELATION ||
		   local_node->relKind == RELKIND_PARTITIONED_TABLE ||
		   local_node->relKind == RELKIND_INDEX ||
		   local_node->relKind == RELKIND_SEQUENCE ||
		   local_node->relKind == RELKIND_TOASTVALUE ||
		   local_node->relKind == RELKIND_VIEW ||
		   local_node->relKind == RELKIND_COMPOSITE_TYPE ||
		   local_node->relKind == RELKIND_FOREIGN_TABLE ||
		   local_node->relKind == RELKIND_MATVIEW ||
		   IsAppendonlyMetadataRelkind(local_node->relKind));
	Assert(local_node->oncommit <= ONCOMMIT_DROP);
}

static CreateStmt *
_readCreateStmt(void)
{
	READ_LOCALS(CreateStmt);

	_readCreateStmt_common(local_node);

	READ_DONE();
}

static CreateRangeStmt *
_readCreateRangeStmt(void)
{
	READ_LOCALS(CreateRangeStmt);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(params);

	READ_DONE();
}

static CreateForeignTableStmt *
_readCreateForeignTableStmt(void)
{
	READ_LOCALS(CreateForeignTableStmt);

	_readCreateStmt_common(&local_node->base);

	READ_STRING_FIELD(servername);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(distributedBy);

	READ_DONE();
}

static ColumnReferenceStorageDirective *
_readColumnReferenceStorageDirective(void)
{
	READ_LOCALS(ColumnReferenceStorageDirective);

	READ_STRING_FIELD(column);
	READ_BOOL_FIELD(deflt);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}

static AlterDomainStmt *
_readAlterDomainStmt(void)
{
	READ_LOCALS(AlterDomainStmt);

	READ_CHAR_FIELD(subtype);
	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(def);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterDefaultPrivilegesStmt *
_readAlterDefaultPrivilegesStmt(void)
{
	READ_LOCALS(AlterDefaultPrivilegesStmt);

	READ_NODE_FIELD(options);
	READ_NODE_FIELD(action);

	READ_DONE();
}

static CopyStmt *
_readCopyStmt(void)
{
	READ_LOCALS(CopyStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(attlist);
	READ_BOOL_FIELD(is_from);
	READ_BOOL_FIELD(is_program);
	READ_STRING_FIELD(filename);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(sreh);

	READ_DONE();
}

static GrantRoleStmt *
_readGrantRoleStmt(void)
{
	READ_LOCALS(GrantRoleStmt);
	READ_NODE_FIELD(granted_roles);
	READ_NODE_FIELD(grantee_roles);
	READ_BOOL_FIELD(is_grant);
	READ_NODE_FIELD(opt);
	READ_NODE_FIELD(grantor);
	READ_ENUM_FIELD(behavior, DropBehavior); Assert(local_node->behavior <= DROP_CASCADE);

	READ_DONE();
}

static QueryDispatchDesc *
_readQueryDispatchDesc(void)
{
	READ_LOCALS(QueryDispatchDesc);

	READ_NODE_FIELD(intoCreateStmt);
	READ_NODE_FIELD(paramInfo);
	READ_NODE_FIELD(oidAssignments);
	READ_NODE_FIELD(sliceTable);
	READ_NODE_FIELD(cursorPositions);
	READ_STRING_FIELD(parallelCursorName);
	READ_BOOL_FIELD(useChangedAOOpts);
	READ_INT_FIELD(secContext);
	READ_DONE();
}

static SerializedParams *
_readSerializedParams(void)
{
	READ_LOCALS(SerializedParams);

	READ_INT_FIELD(nExternParams);
	local_node->externParams = palloc0(local_node->nExternParams * sizeof(SerializedParamExternData));
	for (int i = 0; i < local_node->nExternParams; i++)
	{
		READ_BOOL_FIELD(externParams[i].isnull);
		READ_INT_FIELD(externParams[i].pflags);
		READ_OID_FIELD(externParams[i].ptype);
		READ_INT_FIELD(externParams[i].plen);
		READ_BOOL_FIELD(externParams[i].pbyval);

		if (!local_node->externParams[i].isnull)
			local_node->externParams[i].value = readDatum(local_node->externParams[i].pbyval);
	}

	READ_INT_FIELD(nExecParams);
	local_node->execParams = palloc0(local_node->nExecParams * sizeof(SerializedParamExecData));
	for (int i = 0; i < local_node->nExecParams; i++)
	{
		READ_BOOL_FIELD(execParams[i].isnull);
		READ_BOOL_FIELD(execParams[i].isvalid);
		READ_INT_FIELD(execParams[i].plen);
		READ_BOOL_FIELD(execParams[i].pbyval);

		if (local_node->execParams[i].isvalid && !local_node->execParams[i].isnull)
			local_node->execParams[i].value = readDatum(local_node->execParams[i].pbyval);
		READ_BOOL_FIELD(execParams[i].pbyval);
	}

	READ_NODE_FIELD(transientTypes);

	READ_DONE();
}

static OidAssignment *
_readOidAssignment(void)
{
	READ_LOCALS(OidAssignment);

	READ_OID_FIELD(catalog);
	READ_STRING_FIELD(objname);
	READ_OID_FIELD(namespaceOid);
	READ_OID_FIELD(keyOid1);
	READ_OID_FIELD(keyOid2);
	READ_OID_FIELD(oid);
	READ_DONE();
}

static Sequence *
_readSequence(void)
{
	READ_LOCALS(Sequence);
	ReadCommonPlan(&local_node->plan);
	READ_NODE_FIELD(subplans);
	READ_DONE();
}

static DynamicSeqScan *
_readDynamicSeqScan(void)
{
	READ_LOCALS(DynamicSeqScan);

	ReadCommonScan(&local_node->seqscan);
	READ_NODE_FIELD(partOids);
	READ_NODE_FIELD(part_prune_info);
	READ_NODE_FIELD(join_prune_paramids);

	READ_DONE();
}

/*
 * _readExternalScanInfo
 */
static ExternalScanInfo *
_readExternalScanInfo(void)
{
	READ_LOCALS(ExternalScanInfo);

	READ_NODE_FIELD(uriList);
	READ_CHAR_FIELD(fmtType);
	READ_BOOL_FIELD(isMasterOnly);
	READ_INT_FIELD(rejLimit);
	READ_BOOL_FIELD(rejLimitInRows);
	READ_CHAR_FIELD(logErrors);
	READ_INT_FIELD(encoding);
	READ_INT_FIELD(scancounter);
	READ_NODE_FIELD(extOptions);

	READ_DONE();
}

static CustomScan *
_readCustomScan(void)
{
	char	   *custom_name;
	const CustomScanMethods *methods;
	
	READ_LOCALS(CustomScan);

	ReadCommonScan(&local_node->scan);

	READ_UINT_FIELD(flags);
	READ_NODE_FIELD(custom_plans);
	READ_NODE_FIELD(custom_exprs);
	READ_NODE_FIELD(custom_private);
	READ_NODE_FIELD(custom_scan_tlist);
	READ_BITMAPSET_FIELD(custom_relids);
	READ_STRING_VAR(custom_name);
	/* find custom scan methods from hash table. */
	methods = GetCustomScanMethods(custom_name, false);
	local_node->methods = methods;

	READ_DONE();
}

/*
 * _readShareInputScan
 */
static ShareInputScan *
_readShareInputScan(void)
{
	READ_LOCALS(ShareInputScan);

	READ_BOOL_FIELD(cross_slice);
	READ_INT_FIELD(share_id);
	READ_INT_FIELD(producer_slice_id);
	READ_INT_FIELD(this_slice_id);
	READ_INT_FIELD(nconsumers);

	ReadCommonPlan(&local_node->scan.plan);

	READ_DONE();
}

/*
 * _readMotion
 */
static Motion *
_readMotion(void)
{
	READ_LOCALS(Motion);

	READ_INT_FIELD(motionID);
	READ_ENUM_FIELD(motionType, MotionType);

	Assert(local_node->motionType == MOTIONTYPE_GATHER ||
		   local_node->motionType == MOTIONTYPE_GATHER_SINGLE ||
		   local_node->motionType == MOTIONTYPE_HASH ||
		   local_node->motionType == MOTIONTYPE_BROADCAST ||
		   local_node->motionType == MOTIONTYPE_EXPLICIT);

	READ_BOOL_FIELD(sendSorted);

	READ_NODE_FIELD(hashExprs);
	READ_OID_ARRAY(hashFuncs, list_length(local_node->hashExprs));

	READ_INT_FIELD(numSortCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numSortCols);
	READ_OID_ARRAY(sortOperators, local_node->numSortCols);
	READ_OID_ARRAY(collations, local_node->numSortCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numSortCols);

	READ_INT_FIELD(segidColIdx);
	READ_INT_FIELD(numHashSegments);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readSplitUpdate
 */
static SplitUpdate *
_readSplitUpdate(void)
{
	READ_LOCALS(SplitUpdate);

	READ_INT_FIELD(actionColIdx);
	READ_NODE_FIELD(insertColIdx);
	READ_NODE_FIELD(deleteColIdx);

	READ_INT_FIELD(numHashSegments);
	READ_INT_FIELD(numHashAttrs);
	READ_ATTRNUMBER_ARRAY(hashAttnos, local_node->numHashAttrs);
	READ_OID_ARRAY(hashFuncs, local_node->numHashAttrs);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readAssertOp
 */
static AssertOp *
_readAssertOp(void)
{
	READ_LOCALS(AssertOp);

	READ_NODE_FIELD(errmessage);
	READ_INT_FIELD(errcode);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readPartitionSelector
 */
static PartitionSelector *
_readPartitionSelector(void)
{
	READ_LOCALS(PartitionSelector);

	READ_INT_FIELD(paramid);
	READ_NODE_FIELD(part_prune_info);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *bms = NULL;
	int			nwords;
	int			i;

	memcpy(&nwords, read_str_ptr, sizeof(int)); read_str_ptr+=sizeof(int);
	if (nwords==0)
		return bms;

	bms = palloc(offsetof(Bitmapset, words)+nwords*sizeof(bitmapword));
	bms->nwords = nwords;
	for (i = 0; i < nwords; i++)
	{
		memcpy(&bms->words[i], read_str_ptr, sizeof(bitmapword)); read_str_ptr+=sizeof(bitmapword);
	}

	return bms;
}

static CreateTrigStmt *
_readCreateTrigStmt(void)
{
	READ_LOCALS(CreateTrigStmt);

	READ_STRING_FIELD(trigname);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(row);
	READ_INT_FIELD(timing);
	READ_INT_FIELD(events);
	READ_NODE_FIELD(columns);
	READ_NODE_FIELD(whenClause);
	READ_BOOL_FIELD(isconstraint);
	READ_NODE_FIELD(transitionRels);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_NODE_FIELD(constrrel);

	READ_DONE();
}

static TriggerTransition *
_readTriggerTransition()
{
	READ_LOCALS(TriggerTransition);

	READ_STRING_FIELD(name);
	READ_BOOL_FIELD(isNew);
	READ_BOOL_FIELD(isTable);

	READ_DONE();
}

static CreateTableSpaceStmt *
_readCreateTableSpaceStmt(void)
{
	READ_LOCALS(CreateTableSpaceStmt);

	READ_STRING_FIELD(tablespacename);
	READ_NODE_FIELD(owner);
	READ_STRING_FIELD(location);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreateAmStmt *
_readCreateAmStmt()
{
	READ_LOCALS(CreateAmStmt);

	READ_STRING_FIELD(amname);
	READ_NODE_FIELD(handler_name);
	READ_INT_FIELD(amtype);

	READ_DONE();
}

static AlterTableMoveAllStmt *
_readAlterTableMoveAllStmt(void)
{
	READ_LOCALS(AlterTableMoveAllStmt);

	READ_STRING_FIELD(orig_tablespacename);
	READ_ENUM_FIELD(objtype, ObjectType);
	READ_NODE_FIELD(roles);
	READ_STRING_FIELD(new_tablespacename);
	READ_BOOL_FIELD(nowait);

	READ_DONE();
}

static AlterTableSpaceOptionsStmt *
_readAlterTableSpaceOptionsStmt(void)
{
	READ_LOCALS(AlterTableSpaceOptionsStmt);

	READ_STRING_FIELD(tablespacename);
	READ_NODE_FIELD(options);
	READ_BOOL_FIELD(isReset);

	READ_DONE();
}

static DropTableSpaceStmt *
_readDropTableSpaceStmt(void)
{
	READ_LOCALS(DropTableSpaceStmt);

	READ_STRING_FIELD(tablespacename);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}


static CreateQueueStmt *
_readCreateQueueStmt(void)
{
	READ_LOCALS(CreateQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);

	READ_DONE();
}
static AlterQueueStmt *
_readAlterQueueStmt(void)
{
	READ_LOCALS(AlterQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);

	READ_DONE();
}
static DropQueueStmt *
_readDropQueueStmt(void)
{
	READ_LOCALS(DropQueueStmt);

	READ_STRING_FIELD(queue);

	READ_DONE();
}

static CreateResourceGroupStmt *
_readCreateResourceGroupStmt(void)
{
	READ_LOCALS(CreateResourceGroupStmt);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropResourceGroupStmt *
_readDropResourceGroupStmt(void)
{
	READ_LOCALS(DropResourceGroupStmt);

	READ_STRING_FIELD(name);

	READ_DONE();
}

static AlterResourceGroupStmt *
_readAlterResourceGroupStmt(void)
{
	READ_LOCALS(AlterResourceGroupStmt);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CommentStmt *
_readCommentStmt(void)
{
	READ_LOCALS(CommentStmt);

	READ_ENUM_FIELD(objtype, ObjectType);
	READ_NODE_FIELD(object);
	READ_STRING_FIELD(comment);

	READ_DONE();
}

static TupleDescNode *
_readTupleDescNode(void)
{
	READ_LOCALS(TupleDescNode);

	READ_INT_FIELD(natts);

	local_node->tuple = CreateTemplateTupleDesc(local_node->natts);

	READ_INT_FIELD(tuple->natts);
	if (local_node->tuple->natts > 0)
	{
		int i = 0;
		for (; i < local_node->tuple->natts; i++)
		{
			memcpy(&local_node->tuple->attrs[i], read_str_ptr, ATTRIBUTE_FIXED_PART_SIZE);
			read_str_ptr+=ATTRIBUTE_FIXED_PART_SIZE;
		}
	}

	READ_OID_FIELD(tuple->tdtypeid);
	READ_INT_FIELD(tuple->tdtypmod);
	READ_INT_FIELD(tuple->tdrefcount);

	// Transient type don't have constraint.
	local_node->tuple->constr = NULL;

	Assert(local_node->tuple->tdtypeid == RECORDOID);

	READ_DONE();
}

static AlterExtensionStmt *
_readAlterExtensionStmt(void)
{
	READ_LOCALS(AlterExtensionStmt);
	READ_STRING_FIELD(extname);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(update_ext_state, UpdateExtensionState);
	READ_DONE();
}

static AlterExtensionContentsStmt *
_readAlterExtensionContentsStmt(void)
{
	READ_LOCALS(AlterExtensionContentsStmt);

	READ_STRING_FIELD(extname);
	READ_INT_FIELD(action);
	READ_ENUM_FIELD(objtype, ObjectType);
	READ_NODE_FIELD(object);

	READ_DONE();
}

static AlterTSConfigurationStmt *
_readAlterTSConfigurationStmt(void)
{
	READ_LOCALS(AlterTSConfigurationStmt);

	READ_NODE_FIELD(cfgname);
	READ_NODE_FIELD(tokentype);
	READ_NODE_FIELD(dicts);
	READ_BOOL_FIELD(override);
	READ_BOOL_FIELD(replace);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterTSDictionaryStmt *
_readAlterTSDictionaryStmt(void)
{
	READ_LOCALS(AlterTSDictionaryStmt);

	READ_NODE_FIELD(dictname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CookedConstraint *
_readCookedConstraint(void)
{
	READ_LOCALS(CookedConstraint);

	READ_ENUM_FIELD(contype,ConstrType);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(attnum);
	READ_NODE_FIELD(expr);
	READ_BOOL_FIELD(is_local);
	READ_INT_FIELD(inhcount);
	READ_BOOL_FIELD(is_no_inherit);

	READ_DONE();
}

static AlterEnumStmt *
_readAlterEnumStmt(void)
{
	READ_LOCALS(AlterEnumStmt);

	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(oldVal);
	READ_STRING_FIELD(newVal);
	READ_STRING_FIELD(newValNeighbor);
	READ_BOOL_FIELD(newValIsAfter);
	READ_BOOL_FIELD(skipIfNewValExists);

	READ_DONE();
}

static CreateFdwStmt *
_readCreateFdwStmt(void)
{
	READ_LOCALS(CreateFdwStmt);

	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(func_options);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DistributedBy*
_readDistributedBy(void)
{
	READ_LOCALS(DistributedBy);

	READ_ENUM_FIELD(ptype, GpPolicyType);
	READ_INT_FIELD(numsegments);
	READ_NODE_FIELD(keyCols);

	READ_DONE();
}

static ImportForeignSchemaStmt*
_readImportForeignSchemaStmt(void)
{
	READ_LOCALS(ImportForeignSchemaStmt);

	READ_STRING_FIELD(server_name);
	READ_STRING_FIELD(remote_schema);
	READ_STRING_FIELD(local_schema);
	READ_ENUM_FIELD(list_type, ImportForeignSchemaType);
	READ_NODE_FIELD(table_list);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterFdwStmt *
_readAlterFdwStmt(void)
{
	READ_LOCALS(AlterFdwStmt);

	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(func_options);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreateForeignServerStmt *
_readCreateForeignServerStmt(void)
{
	READ_LOCALS(CreateForeignServerStmt);

	READ_STRING_FIELD(servername);
	READ_STRING_FIELD(servertype);
	READ_STRING_FIELD(version);
	READ_STRING_FIELD(fdwname);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterForeignServerStmt *
_readAlterForeignServerStmt(void)
{
	READ_LOCALS(AlterForeignServerStmt);

	READ_STRING_FIELD(servername);
	READ_STRING_FIELD(version);
	READ_NODE_FIELD(options);
	READ_BOOL_FIELD(has_version);

	READ_DONE();
}

static CreateUserMappingStmt *
_readCreateUserMappingStmt(void)
{
	READ_LOCALS(CreateUserMappingStmt);

	READ_NODE_FIELD(user);
	READ_STRING_FIELD(servername);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterUserMappingStmt *
_readAlterUserMappingStmt(void)
{
	READ_LOCALS(AlterUserMappingStmt);

	READ_NODE_FIELD(user);
	READ_STRING_FIELD(servername);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropUserMappingStmt *
_readDropUserMappingStmt(void)
{
	READ_LOCALS(DropUserMappingStmt);

	READ_NODE_FIELD(user);
	READ_STRING_FIELD(servername);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AccessPriv *
_readAccessPriv(void)
{
	READ_LOCALS(AccessPriv);

	READ_STRING_FIELD(priv_name);
	READ_NODE_FIELD(cols);

	READ_DONE();
}

static LockingClause *
_readLockingClause(void)
{
	READ_LOCALS(LockingClause);

	READ_NODE_FIELD(lockedRels);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);

	READ_DONE();
}

static WindowDef *
_readWindowDef(void)
{
	READ_LOCALS(WindowDef);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static RangeFunction *
_readRangeFunction(void)
{
	READ_LOCALS(RangeFunction);

	READ_BOOL_FIELD(lateral);
	READ_BOOL_FIELD(ordinality);
	READ_BOOL_FIELD(is_rowsfrom);
	READ_NODE_FIELD(functions);
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(coldeflist);

	READ_DONE();
}

static XmlSerialize *
_readXmlSerialize(void)
{
	READ_LOCALS(XmlSerialize);

	READ_ENUM_FIELD(xmloption, XmlOptionType);
	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(typeName);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static TableLikeClause *
_readTableLikeClause(void)
{
	READ_LOCALS(TableLikeClause);

	READ_NODE_FIELD(relation);
	READ_UINT_FIELD(options);
	READ_OID_FIELD(relationOid);

	READ_DONE();
}

static AggExprId *
_readAggExprId(void)
{
	READ_LOCALS(AggExprId);
	READ_DONE();
}

static RowIdExpr *
_readRowIdExpr(void)
{
	READ_LOCALS(RowIdExpr);
	READ_INT_FIELD(rowidexpr_id);
	READ_DONE();
}

static Node *
_readValue(NodeTag nt)
{
	Node * result = NULL;
	if (nt == T_Integer)
	{
		long ival;
		memcpy(&ival, read_str_ptr, sizeof(long)); read_str_ptr+=sizeof(long);
		result = (Node *) makeInteger((int) ival);
	}
	else if (nt == T_Boolean)
	{
		bool bval;
		memcpy(&bval, read_str_ptr, sizeof(bool)); read_str_ptr+=sizeof(bool);
		result = (Node *) makeBoolean(bval);
	}
	else
	{
		int slen;
		char * nn = NULL;
		memcpy(&slen, read_str_ptr, sizeof(int));
		read_str_ptr+=sizeof(int);
		if (slen > 0 || nt == T_String)
		{
		    nn = palloc(slen + 1);
			if (slen > 0)
			    memcpy(nn, read_str_ptr, slen);
		    read_str_ptr += (slen);
			nn[slen] = '\0';
		}
		if (nt == T_Float)
			result = (Node *) makeFloat(nn);
		else if (nt == T_String)
			result = (Node *) makeString(nn);
		else if (nt == T_BitString)
			result = (Node *) makeBitString(nn);
		else
			elog(ERROR, "unknown Value node type %i", nt);
	}
	return result;
}

/*
 * _readGpPolicy
 */
static GpPolicy *
_readGpPolicy(void)
{
	READ_LOCALS(GpPolicy);

	READ_ENUM_FIELD(ptype, GpPolicyType);

	READ_INT_FIELD(numsegments);

	READ_INT_FIELD(nattrs);
	READ_ATTRNUMBER_ARRAY(attrs, local_node->nattrs);
	READ_OID_ARRAY(opclasses, local_node->nattrs);

	READ_DONE();
}


static void *
readNodeBinary(void)
{
	void	   *return_value;
	NodeTag 	nt;
	int16       ntt;

	memcpy(&ntt, read_str_ptr,sizeof(int16));
	read_str_ptr+=sizeof(int16);
	nt = (NodeTag) ntt;

	if (nt==0)
		return NULL;

	if (nt == T_List || nt == T_IntList || nt == T_OidList)
	{
		List	   *l = NIL;
		int listsize = 0;
		int i;

		memcpy(&listsize,read_str_ptr,sizeof(int));
		read_str_ptr+=sizeof(int);

		if (nt == T_IntList)
		{
			int val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,read_str_ptr,sizeof(int));
				read_str_ptr+=sizeof(int);
				l = lappend_int(l, val);
			}
		}
		else if (nt == T_OidList)
		{
			Oid val;
			for (i = 0; i < listsize; i++)
			{
				memcpy(&val,read_str_ptr,sizeof(Oid));
				read_str_ptr+=sizeof(Oid);
				l = lappend_oid(l, val);
			}
		}
		else
		{

			for (i = 0; i < listsize; i++)
			{
				l = lappend(l, readNodeBinary());
			}
		}
		Assert(l->length==listsize);

		return l;
	}

	if (nt == T_Integer || nt == T_Float || nt == T_String ||
	   	nt == T_BitString || nt == T_Boolean)
	{
		return _readValue(nt);
	}

	switch(nt)
	{
			case T_PlannedStmt:
				return_value = _readPlannedStmt();
				break;
			case T_QueryDispatchDesc:
				return_value = _readQueryDispatchDesc();
				break;
			case T_OidAssignment:
				return_value = _readOidAssignment();
				break;
			case T_Plan:
				return_value = _readPlan();
				break;
			case T_Result:
				return_value = _readResult();
				break;
			case T_ProjectSet:
				return_value = _readProjectSet();
				break;
			case T_Append:
				return_value = _readAppend();
				break;
			case T_MergeAppend:
				return_value = _readMergeAppend();
				break;
			case T_Sequence:
				return_value = _readSequence();
				break;
			case T_RecursiveUnion:
				return_value = _readRecursiveUnion();
				break;
			case T_BitmapAnd:
				return_value = _readBitmapAnd();
				break;
			case T_BitmapOr:
				return_value = _readBitmapOr();
				break;
			case T_Gather:
				return_value = _readGather();
				break;
			case T_GatherMerge:
				return_value = _readGatherMerge();
				break;
			case T_Scan:
				return_value = _readScan();
				break;
			case T_SeqScan:
				return_value = _readSeqScan();
				break;
			case T_DynamicSeqScan:
				return_value = _readDynamicSeqScan();
				break;
			case T_ExternalScanInfo:
				return_value = _readExternalScanInfo();
				break;
			case T_IndexScan:
				return_value = _readIndexScan();
				break;
			case T_IndexOnlyScan:
				return_value = _readIndexOnlyScan();
				break;
			case T_DynamicIndexScan:
				return_value = _readDynamicIndexScan();
				break;
			case T_BitmapIndexScan:
				return_value = _readBitmapIndexScan();
				break;
			case T_DynamicBitmapIndexScan:
				return_value = _readDynamicBitmapIndexScan();
				break;
			case T_BitmapHeapScan:
				return_value = _readBitmapHeapScan();
				break;
			case T_DynamicBitmapHeapScan:
				return_value = _readDynamicBitmapHeapScan();
				break;
			case T_CteScan:
				return_value = _readCteScan();
				break;
			case T_NamedTuplestoreScan:
				return_value = _readNamedTuplestoreScan();
				break;
			case T_WorkTableScan:
				return_value = _readWorkTableScan();
				break;
			case T_TidScan:
				return_value = _readTidScan();
				break;
			case T_SubqueryScan:
				return_value = _readSubqueryScan();
				break;
			case T_FunctionScan:
				return_value = _readFunctionScan();
				break;
			case T_TableFuncScan:
				return_value = _readTableFuncScan();
				break;
			case T_ValuesScan:
				return_value = _readValuesScan();
				break;
			case T_ForeignScan:
				return_value = _readForeignScan();
				break;
			case T_CustomScan:
				return_value = _readCustomScan();
				break;
			case T_SampleScan:
				return_value = _readSampleScan();
				break;
			case T_Join:
				return_value = _readJoin();
				break;
			case T_NestLoop:
				return_value = _readNestLoop();
				break;
			case T_MergeJoin:
				return_value = _readMergeJoin();
				break;
			case T_HashJoin:
				return_value = _readHashJoin();
				break;
			case T_Agg:
				return_value = _readAgg();
				break;
			case T_TupleSplit:
				return_value = _readTupleSplit();
				break;
			case T_DQAExpr:
				return_value = _readDQAExpr();
				break;
			case T_WindowAgg:
				return_value = _readWindowAgg();
				break;
			case T_TableFunctionScan:
				return_value = _readTableFunctionScan();
				break;
			case T_Material:
				return_value = _readMaterial();
				break;
			case T_ShareInputScan:
				return_value = _readShareInputScan();
				break;
			case T_Sort:
				return_value = _readSort();
				break;
			case T_Unique:
				return_value = _readUnique();
				break;
			case T_SetOp:
				return_value = _readSetOp();
				break;
			case T_Limit:
				return_value = _readLimit();
				break;
			case T_NestLoopParam:
				return_value = _readNestLoopParam();
				break;
			case T_PlanRowMark:
				return_value = _readPlanRowMark();
				break;
			case T_PartitionPruneInfo:
				return_value = _readPartitionPruneInfo();
				break;
			case T_PartitionedRelPruneInfo:
				return_value = _readPartitionedRelPruneInfo();
				break;
			case T_PartitionPruneStepOp:
				return_value = _readPartitionPruneStepOp();
				break;
			case T_PartitionPruneStepCombine:
				return_value = _readPartitionPruneStepCombine();
				break;
			case T_PlanInvalItem:
				return_value = _readPlanInvalItem();
				break;
			case T_Hash:
				return_value = _readHash();
				break;
			case T_Motion:
				return_value = _readMotion();
				break;
			case T_SplitUpdate:
				return_value = _readSplitUpdate();
				break;
			case T_AssertOp:
				return_value = _readAssertOp();
				break;
			case T_PartitionSelector:
				return_value = _readPartitionSelector();
				break;
			case T_Alias:
				return_value = _readAlias();
				break;
			case T_RangeVar:
				return_value = _readRangeVar();
				break;
			case T_TableFunc:
				return_value = _readTableFunc();
				break;
			case T_IntoClause:
				return_value = _readIntoClause();
				break;
			case T_CopyIntoClause:
				return_value = _readCopyIntoClause();
				break;
			case T_RefreshClause:
				return_value = _readRefreshClause();
				break;
			case T_Var:
				return_value = _readVar();
				break;
			case T_Const:
				return_value = _readConst();
				break;
			case T_Param:
				return_value = _readParam();
				break;
			case T_Aggref:
				return_value = _readAggref();
				break;
			case T_GroupingFunc:
				return_value = _readGroupingFunc();
				break;
			case T_GroupId:
				return_value = _readGroupId();
				break;
			case T_GroupingSetId:
				return_value = _readGroupingSetId();
				break;
			case T_WindowFunc:
				return_value = _readWindowFunc();
				break;
			case T_SubscriptingRef:
				return_value = _readSubscriptingRef();
				break;
			case T_FuncExpr:
				return_value = _readFuncExpr();
				break;
			case T_NamedArgExpr:
				return_value = _readNamedArgExpr();
				break;
			case T_OpExpr:
				return_value = _readOpExpr();
				break;
			case T_DistinctExpr:
				return_value = _readDistinctExpr();
				break;
			case T_ScalarArrayOpExpr:
				return_value = _readScalarArrayOpExpr();
				break;
			case T_BoolExpr:
				return_value = _readBoolExpr();
				break;
			case T_SubLink:
				return_value = _readSubLink();
				break;
			case T_SubPlan:
				return_value = _readSubPlan();
				break;
			case T_AlternativeSubPlan:
				return_value = _readAlternativeSubPlan();
				break;
			case T_FieldSelect:
				return_value = _readFieldSelect();
				break;
			case T_FieldStore:
				return_value = _readFieldStore();
				break;
			case T_RelabelType:
				return_value = _readRelabelType();
				break;
			case T_CoerceViaIO:
				return_value = _readCoerceViaIO();
				break;
			case T_ArrayCoerceExpr:
				return_value = _readArrayCoerceExpr();
				break;
			case T_ConvertRowtypeExpr:
				return_value = _readConvertRowtypeExpr();
				break;
			case T_CollateExpr:
				return_value = _readCollateExpr();
				break;
			case T_CaseExpr:
				return_value = _readCaseExpr();
				break;
			case T_CaseWhen:
				return_value = _readCaseWhen();
				break;
			case T_CaseTestExpr:
				return_value = _readCaseTestExpr();
				break;
			case T_ArrayExpr:
				return_value = _readArrayExpr();
				break;
			case T_A_ArrayExpr:
				return_value = _readA_ArrayExpr();
				break;
			case T_RowExpr:
				return_value = _readRowExpr();
				break;
			case T_RowCompareExpr:
				return_value = _readRowCompareExpr();
				break;
			case T_CoalesceExpr:
				return_value = _readCoalesceExpr();
				break;
			case T_MinMaxExpr:
				return_value = _readMinMaxExpr();
				break;
			case T_NullIfExpr:
				return_value = _readNullIfExpr();
				break;
			case T_NullTest:
				return_value = _readNullTest();
				break;
			case T_BooleanTest:
				return_value = _readBooleanTest();
				break;
			case T_SQLValueFunction:
				return_value = _readSQLValueFunction();
				break;
			case T_XmlExpr:
				return_value = _readXmlExpr();
				break;
			case T_CoerceToDomain:
				return_value = _readCoerceToDomain();
				break;
			case T_CoerceToDomainValue:
				return_value = _readCoerceToDomainValue();
				break;
			case T_SetToDefault:
				return_value = _readSetToDefault();
				break;
			case T_CurrentOfExpr:
				return_value = _readCurrentOfExpr();
				break;
			case T_NextValueExpr:
				return_value = _readNextValueExpr();
				break;
			case T_InferenceElem:
				return_value = _readInferenceElem();
				break;
			case T_TargetEntry:
				return_value = _readTargetEntry();
				break;
			case T_RangeTblRef:
				return_value = _readRangeTblRef();
				break;
			case T_RangeTblFunction:
				return_value = _readRangeTblFunction();
				break;
			case T_TableSampleClause:
				return_value = _readTableSampleClause();
				break;
			case T_JoinExpr:
				return_value = _readJoinExpr();
				break;
			case T_FromExpr:
				return_value = _readFromExpr();
				break;
			case T_OnConflictExpr:
				return_value = _readOnConflictExpr();
				break;
			case T_GrantStmt:
				return_value = _readGrantStmt();
				break;
			case T_AccessPriv:
				return_value = _readAccessPriv();
				break;
			case T_ObjectWithArgs:
				return_value = _readObjectWithArgs();
				break;
			case T_GrantRoleStmt:
				return_value = _readGrantRoleStmt();
				break;
			case T_LockStmt:
				return_value = _readLockStmt();
				break;

			case T_PartitionSpec:
				return_value = _readPartitionSpec();
				break;
			case T_PartitionElem:
				return_value = _readPartitionElem();
				break;
			case T_PartitionRangeDatum:
				return_value = _readPartitionRangeDatum();
				break;
			case T_PartitionCmd:
				return_value = _readPartitionCmd();
				break;
			case T_DistributionKeyElem:
				return_value = _readDistributionKeyElem();
				break;
			case T_PartitionBoundSpec:
				return_value = _readPartitionBoundSpec();
				break;
			case T_RestrictInfo:
				return_value = _readRestrictInfo();
				break;
			case T_AppendRelInfo:
				return_value = _readAppendRelInfo();
				break;
			case T_MergeAction:
				return_value = _readMergeAction();
				break;

			/* GPDB: PG15 SQL/JSON nodes (bodies from readfuncs.c) */
			case T_JsonFormat:
				return_value = _readJsonFormat();
				break;
			case T_JsonReturning:
				return_value = _readJsonReturning();
				break;
			case T_JsonValueExpr:
				return_value = _readJsonValueExpr();
				break;
			case T_JsonConstructorExpr:
				return_value = _readJsonConstructorExpr();
				break;
			case T_JsonIsPredicate:
				return_value = _readJsonIsPredicate();
				break;
			case T_JsonOutput:
				return_value = _readJsonOutput();
				break;
			case T_PublicationObjSpec:
				return_value = _readPublicationObjSpec();
				break;
			case T_PublicationTable:
				return_value = _readPublicationTable();
				break;

			case T_ExtensibleNode:
				return_value = _readExtensibleNode();
				break;
			case T_CreateStmt:
				return_value = _readCreateStmt();
				break;
			case T_CreateForeignTableStmt:
				return_value = _readCreateForeignTableStmt();
				break;
			case T_ColumnReferenceStorageDirective:
				return_value = _readColumnReferenceStorageDirective();
				break;
			case T_SegfileMapNode:
				return_value = _readSegfileMapNode();
				break;
			case T_ExtTableTypeDesc:
				return_value = _readExtTableTypeDesc();
				break;
			case T_CreateExternalStmt:
				return_value = _readCreateExternalStmt();
				break;
			case T_CreateExtensionStmt:
				return_value = _readCreateExtensionStmt();
				break;
			case T_IndexStmt:
				return_value = _readIndexStmt();
				break;
			case T_ReindexStmt:
				return_value = _readReindexStmt();
				break;

			case T_ConstraintsSetStmt:
				return_value = _readConstraintsSetStmt();
				break;

			case T_CreateFunctionStmt:
				return_value = _readCreateFunctionStmt();
				break;
			case T_ReturnStmt:
				return_value = _readReturnStmt();
				break;
			case T_RawStmt:
				return_value = _readRawStmt();
				break;
			case T_FunctionParameter:
				return_value = _readFunctionParameter();
				break;
			case T_AlterFunctionStmt:
				return_value = _readAlterFunctionStmt();
				break;

			case T_DefineStmt:
				return_value = _readDefineStmt();
				break;

			case T_CompositeTypeStmt:
				return_value = _readCompositeTypeStmt();
				break;
			case T_CreateEnumStmt:
				return_value = _readCreateEnumStmt();
				break;
			case T_CreateRangeStmt:
				return_value = _readCreateRangeStmt();
				break;
			case T_AlterEnumStmt:
				return_value = _readAlterEnumStmt();
				break;
			case T_CreateCastStmt:
				return_value = _readCreateCastStmt();
				break;
			case T_CreateOpClassStmt:
				return_value = _readCreateOpClassStmt();
				break;
			case T_CreateOpClassItem:
				return_value = _readCreateOpClassItem();
				break;
			case T_CreateOpFamilyStmt:
				return_value = _readCreateOpFamilyStmt();
				break;
			case T_AlterOpFamilyStmt:
				return_value = _readAlterOpFamilyStmt();
				break;
			case T_CreateConversionStmt:
				return_value = _readCreateConversionStmt();
				break;
			case T_ViewStmt:
				return_value = _readViewStmt();
				break;
			case T_RuleStmt:
				return_value = _readRuleStmt();
				break;
			case T_DropStmt:
				return_value = _readDropStmt();
				break;

			case T_DropOwnedStmt:
				return_value = _readDropOwnedStmt();
				break;
			case T_ReassignOwnedStmt:
				return_value = _readReassignOwnedStmt();
				break;

			case T_TruncateStmt:
				return_value = _readTruncateStmt();
				break;

			case T_ReplicaIdentityStmt:
				return_value = _readReplicaIdentityStmt();
				break;
			case T_AlterDatabaseStmt:
				return_value = _readAlterDatabaseStmt();
			break;
			case T_AlterTableStmt:
				return_value = _readAlterTableStmt();
				break;
			case T_AlterTableCmd:
				return_value = _readAlterTableCmd();
				break;
			case T_AlteredTableInfo:
				return_value = _readAlteredTableInfo();
				break;
			case T_NewConstraint:
				return_value = _readNewConstraint();
				break;
			case T_NewColumnValue:
				return_value = _readNewColumnValue();
				break;

			case T_CreateRoleStmt:
				return_value = _readCreateRoleStmt();
				break;
			case T_DropRoleStmt:
				return_value = _readDropRoleStmt();
				break;
			case T_AlterRoleStmt:
				return_value = _readAlterRoleStmt();
				break;
			case T_AlterRoleSetStmt:
				return_value = _readAlterRoleSetStmt();
				break;

			case T_AlterObjectDependsStmt:
				return_value = _readAlterObjectDependsStmt();
				break;

			case T_AlterSystemStmt:
				return_value = _readAlterSystemStmt();
				break;

			case T_AlterObjectSchemaStmt:
				return_value = _readAlterObjectSchemaStmt();
				break;

			case T_AlterOwnerStmt:
				return_value = _readAlterOwnerStmt();
				break;

			case T_RenameStmt:
				return_value = _readRenameStmt();
				break;

			case T_CreateSeqStmt:
				return_value = _readCreateSeqStmt();
				break;
			case T_AlterSeqStmt:
				return_value = _readAlterSeqStmt();
				break;
			case T_ClusterStmt:
				return_value = _readClusterStmt();
				break;
			case T_CreatedbStmt:
				return_value = _readCreatedbStmt();
				break;
			case T_DropdbStmt:
				return_value = _readDropdbStmt();
				break;
			case T_CreateDomainStmt:
				return_value = _readCreateDomainStmt();
				break;
			case T_AlterDomainStmt:
				return_value = _readAlterDomainStmt();
				break;
			case T_AlterDefaultPrivilegesStmt:
				return_value = _readAlterDefaultPrivilegesStmt();
				break;

			case T_NotifyStmt:
				return_value = _readNotifyStmt();
				break;
			case T_DeclareCursorStmt:
				return_value = _readDeclareCursorStmt();
				break;

			case T_SingleRowErrorDesc:
				return_value = _readSingleRowErrorDesc();
				break;
			case T_CopyStmt:
				return_value = _readCopyStmt();
				break;
			case T_SelectStmt:
				return_value = _readSelectStmt();
				break;
			case T_InsertStmt:
				return_value = _readInsertStmt();
				break;
			case T_ParamRef:
				return_value = _readParamRef();
				break;
			case T_DeleteStmt:
				return_value = _readDeleteStmt();
				break;
			case T_UpdateStmt:
				return_value = _readUpdateStmt();
				break;
			case T_ColumnDef:
				return_value = _readColumnDef();
				break;
			case T_TypeName:
				return_value = _readTypeName();
				break;
			case T_SortBy:
				return_value = _readSortBy();
				break;
			case T_TypeCast:
				return_value = _readTypeCast();
				break;
			case T_CollateClause:
				return_value = _readCollateClause();
				break;
			case T_IndexElem:
				return_value = _readIndexElem();
				break;
			case T_StatsElem:
				return_value = _readStatsElem();
				break;
			case T_CreateStatsStmt:
				return_value = _readCreateStatsStmt();
				break;
			case T_Query:
				return_value = _readQuery();
				break;
			case T_WithCheckOption:
				return_value = _readWithCheckOption();
				break;
			case T_SortGroupClause:
				return_value = _readSortGroupClause();
				break;
			case T_DMLActionExpr:
				return_value = _readDMLActionExpr();
				break;
			case T_GroupingSet:
				return_value = _readGroupingSet();
				break;
			case T_WindowClause:
				return_value = _readWindowClause();
				break;
			case T_RowMarkClause:
				return_value = _readRowMarkClause();
				break;
			case T_WithClause:
				return_value = _readWithClause();
				break;
			case T_CTESearchClause:
				return_value = _readCTESearchClause();
				break;
			case T_CTECycleClause:
				return_value = _readCTECycleClause();
				break;
			case T_CommonTableExpr:
				return_value = _readCommonTableExpr();
				break;
			case T_RoleSpec:
				return_value = _readRoleSpec();
				break;
			case T_SetOperationStmt:
				return_value = _readSetOperationStmt();
				break;
			case T_RangeTblEntry:
				return_value = _readRangeTblEntry();
				break;
			case T_RTEPermissionInfo:
				return_value = _readRTEPermissionInfo();
				break;
			case T_A_Expr:
				return_value = _readAExpr();
				break;
			case T_ColumnRef:
				return_value = _readColumnRef();
				break;
			case T_A_Const:
				return_value = _readAConst();
				break;
			case T_A_Star:
				return_value = _readA_Star();
				break;
			case T_A_Indices:
				return_value = _readA_Indices();
				break;
			case T_A_Indirection:
				return_value = _readA_Indirection();
				break;
			case T_ResTarget:
				return_value = _readResTarget();
				break;
			case T_MultiAssignRef:
				return_value = _readMultiAssignRef();
				break;
			case T_Constraint:
				return_value = _readConstraint();
				break;
			case T_FuncCall:
				return_value = _readFuncCall();
				break;
			case T_DefElem:
				return_value = _readDefElem();
				break;
			case T_CreateSchemaStmt:
				return_value = _readCreateSchemaStmt();
				break;
			case T_CreatePLangStmt:
				return_value = _readCreatePLangStmt();
				break;
			case T_VacuumStmt:
				return_value = _readVacuumStmt();
				break;
			case T_VacuumRelation:
				return_value = _readVacuumRelation();
				break;
			case T_CdbProcess:
				return_value = _readCdbProcess();
				break;
			case T_SliceTable:
				return_value = _readSliceTable();
				break;
			case T_CursorPosInfo:
				return_value = _readCursorPosInfo();
				break;
			case T_VariableSetStmt:
				return_value = _readVariableSetStmt();
				break;
			case T_CreateTrigStmt:
				return_value = _readCreateTrigStmt();
				break;
			case T_TriggerTransition:
				return_value = _readTriggerTransition();
				break;

			case T_CreateTableSpaceStmt:
				return_value = _readCreateTableSpaceStmt();
				break;
			case T_AlterTableSpaceOptionsStmt:
				return_value = _readAlterTableSpaceOptionsStmt();
				break;
			case T_DropTableSpaceStmt:
				return_value = _readDropTableSpaceStmt();
				break;

			case T_CreateQueueStmt:
				return_value = _readCreateQueueStmt();
				break;
			case T_AlterQueueStmt:
				return_value = _readAlterQueueStmt();
				break;
			case T_DropQueueStmt:
				return_value = _readDropQueueStmt();
				break;

			case T_CreateResourceGroupStmt:
				return_value = _readCreateResourceGroupStmt();
				break;
			case T_DropResourceGroupStmt:
				return_value = _readDropResourceGroupStmt();
				break;
			case T_AlterResourceGroupStmt:
				return_value = _readAlterResourceGroupStmt();
				break;

			case T_CommentStmt:
				return_value = _readCommentStmt();
				break;
			case T_DenyLoginInterval:
				return_value = _readDenyLoginInterval();
				break;
			case T_DenyLoginPoint:
				return_value = _readDenyLoginPoint();
				break;

			case T_TableValueExpr:
				return_value = _readTableValueExpr();
				break;

			case T_AlterTypeStmtSetDefaultEnc:
				return_value = _readAlterTypeStmtSetDefaultEnc();
				break;
			case T_AlterTypeStmt:
				return_value = _readAlterTypeStmt();
				break;
			case T_AlterExtensionStmt:
				return_value = _readAlterExtensionStmt();
				break;
			case T_AlterExtensionContentsStmt:
				return_value = _readAlterExtensionContentsStmt();
				break;

			case T_TupleDescNode:
				return_value = _readTupleDescNode();
				break;
			case T_SerializedParams:
				return_value = _readSerializedParams();
				break;

			case T_AlterTSConfigurationStmt:
				return_value = _readAlterTSConfigurationStmt();
				break;
			case T_AlterTSDictionaryStmt:
				return_value = _readAlterTSDictionaryStmt();
				break;

			case T_CookedConstraint:
				return_value = _readCookedConstraint();
				break;

			case T_DropUserMappingStmt:
				return_value = _readDropUserMappingStmt();
				break;
			case T_AlterUserMappingStmt:
				return_value = _readAlterUserMappingStmt();
				break;
			case T_CreateUserMappingStmt:
				return_value = _readCreateUserMappingStmt();
				break;
			case T_AlterForeignServerStmt:
				return_value = _readAlterForeignServerStmt();
				break;
			case T_CreateForeignServerStmt:
				return_value = _readCreateForeignServerStmt();
				break;
			case T_AlterFdwStmt:
				return_value = _readAlterFdwStmt();
				break;
			case T_CreateFdwStmt:
				return_value = _readCreateFdwStmt();
				break;
			case T_ModifyTable:
				return_value = _readModifyTable();
				break;
			case T_LockRows:
				return_value = _readLockRows();
				break;
			case T_GpPolicy:
				return_value = _readGpPolicy();
				break;
			case T_DistributedBy:
				return_value = _readDistributedBy();
				break;
			case T_ImportForeignSchemaStmt:
				return_value = _readImportForeignSchemaStmt();
				break;
			case T_AlterTableMoveAllStmt:
				return_value = _readAlterTableMoveAllStmt();
				break;

			case T_CreatePublicationStmt:
				return_value = _readCreatePublicationStmt();
				break;
			case T_AlterPublicationStmt:
				return_value = _readAlterPublicationStmt();
				break;
			case T_CreateSubscriptionStmt:
				return_value = _readCreateSubscriptionStmt();
				break;
			case T_DropSubscriptionStmt:
				return_value = _readDropSubscriptionStmt();
				break;
			case T_AlterSubscriptionStmt:
				return_value = _readAlterSubscriptionStmt();
				break;

			case T_CreatePolicyStmt:
				return_value = _readCreatePolicyStmt();
				break;
			case T_AlterPolicyStmt:
				return_value = _readAlterPolicyStmt();
				break;
			case T_CreateTransformStmt:
				return_value = _readCreateTransformStmt();
				break;
			case T_CreateAmStmt:
				return_value = _readCreateAmStmt();
				break;
			case T_WindowDef:
				return_value = _readWindowDef();
				break;
			case T_RangeSubselect:
				return_value = _readRangeSubselect();
				break;
			case T_InferClause:
				return_value = _readInferClause();
				break;
			case T_OnConflictClause:
				return_value = _readOnConflictClause();
				break;
			case T_RangeFunction:
				return_value = _readRangeFunction();
				break;
			case T_XmlSerialize:
				return_value = _readXmlSerialize();
				break;
			case T_TableLikeClause:
				return_value = _readTableLikeClause();
				break;
			case T_LockingClause:
				return_value = _readLockingClause();
				break;
			case T_AggExprId:
				return_value = _readAggExprId();
				break;
			case T_RowIdExpr:
				return_value = _readRowIdExpr();
				break;
			default:
				return_value = NULL; /* keep the compiler silent */
				elog(ERROR, "could not deserialize unrecognized node type: %d",
						 (int) nt);
				break;
	}

	return (Node *)return_value;
}

Node *
readNodeFromBinaryString(const char *str_arg, int len pg_attribute_unused())
{
	Node	   *node;
	int16		tg;

	read_str_ptr = str_arg;

	node = readNodeBinary();

	memcpy(&tg, read_str_ptr, sizeof(int16));
	if (tg != (int16)0xDEAD)
		elog(ERROR,"Deserialization lost sync.");

	return node;

}
/*
 * readDatumBinary
 *
 * Like readDatum() in readfuncs.c.
 */
static Datum
readDatumBinary(bool typbyval)
{
	Size		length;

	Datum		res;
	char	   *s;

	if (typbyval)
	{
		memcpy(&res, read_str_ptr, sizeof(Datum)); read_str_ptr+=sizeof(Datum);
	}
	else
	{
		memcpy(&length, read_str_ptr, sizeof(Size)); read_str_ptr+=sizeof(Size);
	  	if (length <= 0)
			res = 0;
		else
		{
			s = (char *) palloc(length+1);
			memcpy(s, read_str_ptr, length); read_str_ptr+=length;
			s[length]='\0';
			res = PointerGetDatum(s);
		}

	}

	return res;
}
