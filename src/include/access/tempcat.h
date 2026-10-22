/*-------------------------------------------------------------------------
 *
 * tempcat.h
 *	  In-memory catalog for temporary table metadata.
 *
 * When gp_enable_temp_memory_catalog is on, temporary table metadata is
 * kept in backend-private memory instead of on-disk pg_catalog tables.
 * This header declares the public API: catalog DML redirects, scan
 * helpers, transaction callbacks, and the BEGIN/END_TEMP_TABLE_SCOPE
 * macros that DDL code uses to activate the in-memory path.
 *
 * Internal to the server. Not part of any public or extension API.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tempcat.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef _TEMP_CAT_H
#define _TEMP_CAT_H

#include "postgres_ext.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/sdir.h"
#include "access/genam.h"
#include "catalog/catalog.h"
#include "catalog/gp_fastsequence_d.h"
#include "catalog/indexing.h"
#include "catalog/pg_depend_d.h"
#include "catalog/pg_inherits_d.h"
#include "catalog/pg_shdepend_d.h"
#include "catalog/objectaddress.h"
#include "storage/itemptr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * Flag stored in ItemPointerData.ip_posid to mark tuple as virtual. We can
 * safely store a flag in higher bits of ip_posid since it's maximum value is
 * very limited. See MaxHeapTuplesPerPage.
 */
#define TEMPCAT_ITEM_POINTER_BIT 0x0800

/* Determine whether ItemPointer is virtual */
#define IsTempcatItemPointer(ptr) \
	( ((ptr)->ip_posid & TEMPCAT_ITEM_POINTER_BIT) != 0 )
	
typedef struct TempCatScanData TempCatScanData;

/* Heap operations for virtual tuples */
extern void      tempcat_insert(Relation relation, HeapTuple htup);
extern void      tempcat_delete(Relation relation, ItemPointer ptr);
extern void      tempcat_update(Relation relation, ItemPointer ptr, HeapTuple htup);
extern void      tempcat_update_inplace(Relation relation, HeapTuple htup);

/* Scan operations for virtual tuples */
extern TempCatScanData* tempcat_beginscan(Relation rel, int nkeys, ScanKey key);
extern void      tempcat_endscan(TempCatScanData* scan);
extern void      tempcat_rescan(TempCatScanData* scan, ScanKey keys, int nkeys);
extern HeapTuple tempcat_getnext(TempCatScanData* scan, BufferHeapTupleTableSlot* bslot);
extern bool      tempcat_is_fetched(TempCatScanData* scan);
extern bool      tempcat_relation_has_entries(Relation rel);

/* Transaction support */
extern void      tempcat_begin_transaction(void);
extern void      tempcat_end_transaction(void);
extern void      tempcat_abort_transaction(void);
extern void      tempcat_prepare_transaction(void);
extern void      tempcat_finish_prepared_transaction(bool isCommit);
extern void      tempcat_begin_subtransaction(void);
extern void      tempcat_commit_subtransaction(void);
extern void      tempcat_abort_subtransaction(void);

/*
 * Temp table scope.
 *
 * Wraps a DDL code path that targets a temporary table. When the
 * enable_temp_memory_catalog GUC is on and isTemp is true, catalog DML
 * inside the scope (CatalogTupleInsert, etc.) is redirected to the
 * in-memory virtual catalog (tempcat) instead of writing to pg_catalog.
 *
 * BEGIN_TEMP_TABLE_SCOPE(isTemp) / END_TEMP_TABLE_SCOPE() save and
 * restore the scope flag, so nesting is safe.
 *
 * On the error path END_TEMP_TABLE_SCOPE() is never reached, and the flag is restored by the
 * transaction machinery in xact.c instead.  AbortSubTransaction() restores
 * the value saved by PushTransaction(), and AbortTransaction() /
 * CommitTransaction() reset it to false at the top level.  Between them
 * every error exit is covered, since catching an error and continuing in
 * the same transaction is only legal via a subtransaction.
 */
extern bool enable_temp_memory_catalog;
extern bool temp_table_scope;

#define BEGIN_TEMP_TABLE_SCOPE(isTemp) \
	do { \
		const bool _temp_scope_save = temp_table_scope; \
		if (enable_temp_memory_catalog && (isTemp)) \
			temp_table_scope = true; \
		{

#define END_TEMP_TABLE_SCOPE() \
		} \
		temp_table_scope = _temp_scope_save; \
	} while (0)

/* Raw scope flag.  Catalog DML sites must use IsTempTableScopeFor() instead. */
#define IsTempTableScope()  (temp_table_scope)

/*
 * Should a write to `rel` be redirected into the in-memory catalog?
 *
 * Being inside a temp table scope is not sufficient.  The CatalogTuple*
 * wrappers surprisingly are also used to write ordinary relations that are not catalogs at
 * all.
 *
 * gp_fastsequence is always excluded: its last_sequence must never roll
 * back, even when the transaction that consumed the numbers aborts,
 * because AO row numbers are embedded in index TIDs and reusing them
 * yields wrong index scan results (see the invariant comment atop
 * gp_fastsequence.c).
 *
 * pg_depend and pg_shdepend rows of temporary objects are routed into the
 * in-memory catalog with every other row of the object, but their edges
 * are additionally published to the shared-memory registry
 * (tempcat_registry.c) at commit, so that other sessions' drops and
 * alters of the referenced shared objects are still blocked - see the
 * consult sites in performDeletion(), shdepDropOwned(),
 * shdepReassignOwned() and the domain/type walkers.
 */
#define IsTempTableScopeFor(rel)  \
	(temp_table_scope && IsCatalogRelation(rel) && \
	 RelationGetRelid(rel) != FastSequenceRelationId)

/* Dirty tracking for tempcat versioning */
extern bool tempcat_is_dirty(void);
extern void tempcat_clear_dirty(void);

/*
 * Does the in-memory catalog still hold temporary table metadata?
 * (pg_temp_N namespace rows don't count, see tempcat.c)
 */
extern bool tempcat_is_empty(void);

/*
 * Serialize the current tempcat snapshot state into a byte buffer for
 * transmission via shared memory (DSM segment).
 *
 * *len is set to the size of the serialized data.
 * Returns a palloc'd buffer that the caller must pfree.
 */
extern void tempcat_serialize(int *len, char **data);

/*
 * Deserialize tempcat state from a byte buffer, populating the in-memory
 * catalog.
 *
 * Typically called on reader QEs to receive state serialized by the writer
 * QE on the same segment via SharedLocalSnapshotSlot.
 */
extern void tempcat_deserialize(int len, const char *data);

/*
 * Shared-memory registry of dependency edges that temporary objects hold
 * on shared objects (their pg_depend/pg_shdepend rows are virtual, so the
 * cluster needs another place where every session can see the edges).
 * See tempcat_registry.c.
 */
typedef struct TempcatDepEdge
{
	Oid			dbid;			/* MyDatabaseId; InvalidOid for pg_shdepend */
	int			pgprocno;		/* owner backend identity, for liveness */
	int			pid;
	Oid			classid;		/* depender (the temporary object's row) */
	Oid			objid;
	int32		objsubid;
	Oid			refclassid;		/* referenced shared object */
	Oid			refobjid;
	int32		refobjsubid;	/* its sub-object, when it has one */
	char		deptype;			/* DEPENDENCY_* / SHARED_DEPENDENCY_* */
} TempcatDepEdge;

/* postmaster: create the registry in shared memory (ipci.c) */
extern void tempcat_registry_init(void);
extern Size tempcat_registry_shmem_size(void);

/*
 * Replace this backend's published edges and OID reservations with the
 * given sets (regenerated from the committed snapshot).
 */
extern void tempcat_registry_publish(int nedges, const TempcatDepEdge *edges,
									 int noids, const Oid *oids);

/* delete this backend's published edges (session end) */
extern void tempcat_registry_unpublish(void);

/* before_shmem_exit wrapper for tempcat_registry_unpublish() */
extern void tempcat_registry_unpublish_on_exit(int code, Datum arg);

/*
 * True when some other live session's temporary objects depend on the
 * given shared object.  deptype 0 matches any.  On true, *depender is set
 * to the dependent temporary object's address, for the error message.
 */
extern bool tempcat_registry_has_dependents(const ObjectAddress *referenced,
											char deptype,
											ObjectAddress *depender);

/*
 * OID reservation for virtual catalog rows, consulted by the OID
 * allocator's collision loop (GetNewOidWithIndex): catalog indexes cannot
 * see virtual rows, so the wrap-around check would otherwise miss them.
 */
extern bool tempcat_registry_oid_taken(Oid oid);
extern void tempcat_registry_reserve_oid(Oid oid);

/*
 * Glue for the OID allocator: reserve the OID and mark this session's
 * registry state as pending, so the next commit or abort regenerates it
 * from the committed snapshot.
 */
extern void tempcat_note_reserved_oid(Oid oid);

#endif   /* _TEMP_CAT_H */
