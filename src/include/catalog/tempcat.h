/*-------------------------------------------------------------------------
 *
 * tempcat.h
 *	  In-memory catalog rows for temporary objects.
 *
 * Objects created in a temporary namespace that was itself created with
 * gp_enable_temp_memory_catalog enabled get OIDs from a reserved range
 * (FirstTempcatObjectId..LastTempcatObjectId).  Catalog rows whose owner
 * column holds such an OID are kept in a per-session dynamic shared memory
 * area instead of the on-disk catalog, so creating and dropping temporary
 * tables leaves no dead tuples behind.  See tempcat.c for details.
 *
 * src/include/catalog/tempcat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEMPCAT_H
#define TEMPCAT_H

#include "access/htup.h"
#include "access/skey.h"
#include "storage/itemptr.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 * OIDs reserved for temporary objects whose catalog rows live in tempcat.
 * The ordinary OID counter skips this range, but ordinary objects created
 * before it was reserved (or restored by pg_upgrade) may still use it, so
 * being in the range alone does not make an object temporary; see
 * tempcat_owns_oid().
 */
#define FirstTempcatObjectId	((Oid) 0xF0000000)
#define LastTempcatObjectId		((Oid) 0xFFFFFFFE)

#define IsTempcatOid(oid) \
	((Oid) (oid) >= FirstTempcatObjectId && (Oid) (oid) <= LastTempcatObjectId)

/*
 * Virtual tuples get TIDs with this bit set in the offset number.  Real
 * offsets never reach it (MaxOffsetNumber is far below 0x8000).
 */
#define TEMPCAT_TID_BIT		0x8000

#define IsTempcatTid(tid)	(((tid)->ip_posid & TEMPCAT_TID_BIT) != 0)

extern bool gp_enable_temp_memory_catalog;
extern int	gp_temp_memory_catalog_max_size;
extern bool gp_temp_memory_catalog_disk_only;
extern bool gp_temp_memory_catalog_hide_others;

typedef struct TempcatScanData *TempcatScan;

struct ObjectAddress;
struct TupleTableSlot;

/* OID assignment */
extern bool tempcat_want_temp_oid(Oid catalog, const char *objname,
								  Oid namespaceOid, Oid keyOid1, Oid keyOid2);
extern Oid	tempcat_allocate_oid(Relation relation, Oid indexId,
								 AttrNumber oidcolumn);
extern void tempcat_note_preassigned_oid(Oid catalog, const char *objname,
										 Oid namespaceOid, Oid keyOid1,
										 Oid keyOid2, Oid oid);
extern bool tempcat_owns_oid(Oid catalog, Oid oid);
extern void tempcat_note_temp_oid(Oid catalog, Oid oid);

/* Catalog DML */
extern bool tempcat_route_insert(Relation rel, HeapTuple tup);
extern bool tempcat_insert(Relation rel, HeapTuple tup);
extern bool tempcat_update(Relation rel, ItemPointer otid, HeapTuple newtup);
extern void tempcat_delete(Relation rel, ItemPointer tid);
extern void tempcat_inplace_update(Relation rel, HeapTuple tup);
extern void tempcat_freeze(Relation rel, HeapTuple tup);

/* Catalog scans */
extern TempcatScan tempcat_beginscan(Relation rel, Relation irel,
									 Snapshot snapshot, int nkeys, ScanKey key);
extern HeapTuple tempcat_getnext(TempcatScan scan,
								 HeapTuple (*fetch_disk) (void *), void *arg);
extern HeapTuple tempcat_getnext_dir(TempcatScan scan, bool forward,
									 HeapTuple (*fetch_disk) (void *), void *arg);
extern bool tempcat_scan_on_virtual(TempcatScan scan);
extern void tempcat_endscan(TempcatScan scan);

/* SQL-level catalog scans */
extern bool tempcat_restrict_catalog_index_scans(Oid relid);
extern TempcatScan tempcat_begin_sql_scan(Relation rel, Snapshot snapshot);
extern HeapTuple tempcat_next_virtual(TempcatScan scan);
extern HeapTuple tempcat_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot);
extern TempcatScan tempcat_begin_index_sql_scan(Relation rel, Relation irel,
												Snapshot snapshot,
												ScanKey indexkeys, int nindexkeys);
extern void tempcat_scan_filter(TempcatScan scan,
								bool (*keep) (HeapTuple, void *), void *arg);
extern HeapTuple tempcat_scan_peek(TempcatScan scan, bool forward);
extern void tempcat_scan_advance(TempcatScan scan);
extern int	tempcat_scan_compare(TempcatScan scan, HeapTuple a, HeapTuple b);

/* Objects of other sessions */
extern bool tempcat_object_missing(const struct ObjectAddress *object);
extern bool tempcat_role_used_elsewhere(Oid roleid);
extern bool tempcat_hide_disk_row(Relation rel, HeapTuple tup);
extern bool tempcat_hide_disk_slot(Relation rel, struct TupleTableSlot *slot);

/* Object restrictions */
extern void tempcat_check_namespace_object(Oid catalog, Oid namespaceOid);
extern void tempcat_check_unsupported(Oid catalog, Oid objectId, const char *what);

/* VACUUM support */
extern void tempcat_autovacuum_sweep(void);
extern void tempcat_sweep_before_drop(void);
extern void tempcat_fold_frozen_horizon(TransactionId *frozenXid,
										MultiXactId *minMulti);

/* Shared memory */
extern Size TempcatShmemSize(void);
extern void TempcatShmemInit(void);

/* Transaction support */
extern void tempcat_start_transaction(void);
extern void tempcat_abort_subtransaction(void);
extern void tempcat_check_prepare(void);

#endif							/* TEMPCAT_H */
