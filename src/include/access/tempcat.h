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
#include "catalog/indexing.h"
#include "storage/itemptr.h"
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

/* Transaction support */
extern void      tempcat_begin_transaction(void);
extern void      tempcat_end_transaction(void);
extern void      tempcat_abort_transaction(void);
extern void      tempcat_define_savepoint(const char *name);
extern void      tempcat_release_savepoint(const char *name);
extern void      tempcat_rollback_to_savepoint(const char *name);
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
 * restore the scope flag, so nesting is safe.  The macros also install
 * a sigsetjmp catch block so that temp_table_scope is restored on
 * error (e.g. when an exception is caught by a PL/pgSQL EXCEPTION
 * handler inside an implicit subtransaction).  When isTemp is false
 * (or the GUC is off), no try/catch overhead is incurred.
 * On transaction abort, AbortTransaction() in xact.c resets it to
 * false as an additional safety net.
 */
extern bool enable_temp_memory_catalog;
extern bool temp_table_scope;

#define BEGIN_TEMP_TABLE_SCOPE(isTemp) \
	do { \
		const bool _temp_scope_do = (enable_temp_memory_catalog && (isTemp) && \
									 !temp_table_scope); \
		bool _temp_scope_throw = false; \
		const bool _temp_scope_save = temp_table_scope; \
		sigjmp_buf *_temp_scope_save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *_temp_scope_save_error_stack; \
		sigjmp_buf _temp_scope_sigjmp_buf; \
		if (_temp_scope_do) \
		{ \
			_temp_scope_save_error_stack = error_context_stack; \
			if (sigsetjmp(_temp_scope_sigjmp_buf, 0) == 0) \
			{ \
				PG_exception_stack = &_temp_scope_sigjmp_buf; \
				temp_table_scope = true; \
			} \
			else \
				_temp_scope_throw = true; \
		} \
		if (!_temp_scope_throw) \
		{

#define END_TEMP_TABLE_SCOPE() \
		} \
		PG_exception_stack = _temp_scope_save_exception_stack; \
		if (_temp_scope_do) \
		{ \
			error_context_stack = _temp_scope_save_error_stack; \
			temp_table_scope = _temp_scope_save; \
			if (_temp_scope_throw) \
				PG_RE_THROW(); \
		} \
	} while (0)

#define IsTempTableScope()  (temp_table_scope)

/* Dirty tracking for tempcat versioning */
extern bool tempcat_is_dirty(void);
extern void tempcat_clear_dirty(void);

/*
 * Serialize the current tempcat snapshot state into a byte buffer for
 * transmission via shared memory (DSM segment).
 *
 * *len is set to the size of the serialized data.
 * Returns a palloc'd buffer that the caller must pfree.
 */
extern void tempcat_serialize(int *len, char **data);

/*
 * Deserialize tempcat state from a byte buffer, populating the
 * in-memory catalog.
 *
 * Typically called on reader QEs to receive state serialized by
 * the writer QE on the same segment via SharedLocalSnapshotSlot.
 */
extern void tempcat_deserialize(int len, const char *data);

#endif   /* _TEMP_CAT_H */
