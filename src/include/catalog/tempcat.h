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
 * Portions Copyright (c) 2026, Greengage Development Group
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
 * OIDs reserved for temporary objects whose catalog rows live in memory.
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
 * Rows in memory get TIDs with this bit set in the offset number.  Real
 * offsets never reach it (MaxOffsetNumber is far below 0x8000).
 */
#define TEMPCAT_TID_BIT		0x8000

#define IsTempcatTid(tid)	(((tid)->ip_posid & TEMPCAT_TID_BIT) != 0)

/* GUCs */
extern bool gp_enable_temp_memory_catalog;
extern int	gp_temp_memory_catalog_max_size;
extern int	gp_temp_memory_catalog_max_file_size;

typedef struct TempcatScanData *TempcatScan;

struct ObjectAddress;
struct TM_FailureData;

/*
 * What a session knows of an object that on-disk rows refer to (pg_depend,
 * pg_shdepend): an object of its own or an ordinary one, a temporary object
 * that another live session keeps in memory, or one of a session that is
 * gone, whose rows are leftovers.
 */
typedef enum TempcatForeign
{
	TCF_NONE,
	TCF_LIVE,
	TCF_GONE
} TempcatForeign;

/* OID assignment */
extern bool tempcat_want_temp_oid(Oid catalog, const char *objname,
								  Oid namespaceOid, Oid keyOid1, Oid keyOid2);
extern Oid	tempcat_allocate_oid(Relation relation, Oid indexId,
								 AttrNumber oidcolumn);
extern void tempcat_note_preassigned_oid(Relation relation, Oid indexId,
										 AttrNumber oidcolumn,
										 const char *objname,
										 Oid namespaceOid, Oid keyOid1,
										 Oid keyOid2, bool temp, Oid oid);
extern bool tempcat_owns_oid(Oid catalog, Oid oid);
extern void tempcat_check_namespace_object(Oid catalog, Oid namespaceOid);

/* Catalog DML */
extern bool tempcat_route_insert(Relation rel, HeapTuple tup);
extern void tempcat_insert(Relation rel, HeapTuple tup);
extern void tempcat_update(Relation rel, ItemPointer otid, HeapTuple newtup);
extern void tempcat_delete(Relation rel, ItemPointer tid);
extern void tempcat_inplace_update(Relation rel, HeapTuple tup);
extern void tempcat_freeze(Relation rel, HeapTuple tup);
extern int	tempcat_self_modified(Relation rel, ItemPointer tid,
								  struct TM_FailureData *tmfd);
extern void tempcat_sql_check(Relation rel, HeapTuple tup);
extern void tempcat_sql_insert(Relation rel, HeapTuple tup);

/* Catalog scans */
extern TempcatScan tempcat_beginscan(Relation rel, Relation irel,
									 Snapshot snapshot, int nkeys, ScanKey key);
extern HeapTuple tempcat_getnext(TempcatScan scan,
								 HeapTuple (*fetch_disk) (void *), void *arg);
extern HeapTuple tempcat_getnext_dir(TempcatScan scan, bool forward,
									 HeapTuple (*fetch_disk) (void *), void *arg);
extern HeapTuple tempcat_next_virtual(TempcatScan scan);
extern bool tempcat_has_rows(Relation rel);
extern bool tempcat_scan_on_virtual(TempcatScan scan);
extern void tempcat_endscan(TempcatScan scan);
extern HeapTuple tempcat_fetch_tid(Relation rel, ItemPointer tid,
								   Snapshot snapshot);
extern bool tempcat_plan_catalog_index_scans(Oid relid);
extern void tempcat_check_index_scan(Relation rel, Snapshot snapshot);
extern void tempcat_check_sample_scan(Relation rel, Snapshot snapshot);

/* Objects of other sessions */
extern TempcatForeign tempcat_foreign_object(Oid classId, Oid objectId);
extern void tempcat_forget_leftover(Relation rel, HeapTuple tup);
extern bool tempcat_role_used_elsewhere(Oid roleid);
extern bool tempcat_oid_maybe_live(Oid oid, Oid dbid);
extern bool tempcat_check_type_user(Oid classId, Oid objectId,
									const char *typeName);

/* Object restrictions */
extern void tempcat_check_inheritance(Oid relid, Oid parentid);
extern void tempcat_check_typed_table(Oid relid, Oid typid);
extern void tempcat_check_operator_link(Oid namespaceOid, Oid otherOprOid);
extern void tempcat_check_dependency(const struct ObjectAddress *depender,
									 const struct ObjectAddress *referenced);
extern void tempcat_check_unsupported(Oid catalog, Oid objectId,
									  const char *what);

/* VACUUM support */
extern void tempcat_fold_frozen_horizon(TransactionId *frozenXid,
										MultiXactId *minMulti);

/* Shared memory */
extern Size TempcatShmemSize(void);
extern void TempcatShmemInit(void);

/* Transaction support */
extern void tempcat_start_transaction(void);
extern void tempcat_idle(void);
extern void tempcat_abort_subtransaction(void);
extern void tempcat_check_prepare(void);
extern bool tempcat_xact_changed(void);
extern void tempcat_note_cursor_snapshot(CommandId cid);

#endif							/* TEMPCAT_H */
