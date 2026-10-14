/*-------------------------------------------------------------------------
 *
 * tempcat.c
 *	  In-memory catalog rows for temporary objects.
 *
 * Every temporary table leaves dead tuples in pg_class, pg_attribute,
 * pg_type, pg_depend and friends, on the coordinator and on every segment.
 * Applications that create and drop many temporary tables therefore bloat
 * the shared catalogs and keep autovacuum busy.  This module keeps the
 * catalog rows of temporary objects out of the on-disk catalogs.
 *
 * Which rows are virtual is decided by the data, not by the code path that
 * writes them:
 *
 * - When gp_enable_temp_memory_catalog is on at the moment the session's
 *   temporary namespace is created, the namespace gets an OID from the
 *   reserved range FirstTempcatObjectId..LastTempcatObjectId.  Relations and
 *   types created in such a namespace, and the attribute defaults,
 *   constraints, rules and triggers of such relations, get OIDs from the
 *   same range (see tempcat_want_temp_oid(), called from oid_dispatch.c).
 *   The coordinator assigns the OIDs and dispatches them to the segments as
 *   usual, so all nodes agree on them.  The range is split into one slice
 *   per PGPROC, so OIDs of concurrent sessions never collide and lock tags
 *   stay unique.
 *
 * - A row inserted into one of the catalogs listed in tempcat_catalogs[] is
 *   virtual if its owner column (pg_attribute.attrelid, pg_index.indrelid,
 *   ...) holds an OID that this session assigned to a temporary object.
 *   Ownership is tracked per (catalog, OID) in a backend-local set, filled
 *   when the OID is allocated (QD) or received from the QD (QE), so ordinary
 *   objects that happen to have OIDs in the reserved range (created before
 *   the range was reserved, or carried over by pg_upgrade) are never
 *   affected.  Rows that also reference another object (pg_depend,
 *   pg_inherits) are virtual only if that object is the session's
 *   temporary object too, so dependencies of temporary objects on ordinary
 *   objects stay on disk and keep protecting those objects from DROP in
 *   other sessions.
 *
 * Virtual rows are stored in a dynamic shared memory area owned by the
 * session's writer process (the QD backend, or the writer QE on a segment).
 * Its handle is published in the writer's SharedSnapshotSlot, so reader
 * gangs and entry-db readers of the same session attach to it and see the
 * same rows, including uncommitted ones, without any serialization.
 *
 * Every row carries xmin/xmax/cmin/cmax and is checked with the usual MVCC
 * rules, so subtransactions, aborts and two-phase commit behave like on-disk
 * rows: an aborted insert simply never becomes visible.  Rows whose fate is
 * known are cleaned up by the writer when it goes idle, or at the start of
 * its next transaction.
 * Frozen inserts and in-place updates (gp_fastsequence, relpages, ...) stay
 * non-transactional, like on disk.
 *
 * Lookups go through hash tables keyed by up to two columns per catalog
 * (typically the owner OID and the object's own OID or name), so the cost
 * does not grow with the number of temporary tables in the session.
 *
 * Portions Copyright (c) 2026, Greengage Development Group
 *
 * IDENTIFICATION
 *	  src/backend/catalog/tempcat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/statvfs.h>

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/htup_details.h"
#include "access/tuptoaster.h"
#include "access/multixact.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/valid.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/gp_partition_template.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_range.h"
#include "catalog/pg_proc_callback.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_index.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_type_encoding.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/tempcat.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/dsm_impl.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/dsa.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/faultinjector.h"
#include "utils/sharedsnapshot.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/vmem_tracker.h"
#include "utils/snapmgr.h"

/* GUCs */
bool		gp_enable_temp_memory_catalog = false;
int			gp_temp_memory_catalog_max_size = 16384;	/* kB, per writer process */
bool		gp_temp_memory_catalog_disk_only = false;	/* hide virtual rows from SQL */
bool		gp_temp_memory_catalog_hide_others = false; /* see tempcat_hide_disk_row() */

/*
 * Catalogs that can hold virtual rows.
 */
typedef enum TempcatKeyType
{
	TCK_NONE,
	TCK_OID,
	TCK_NAME
} TempcatKeyType;

#define TEMPCAT_NKEYS 2

typedef struct TempcatCatalogDef
{
	Oid			relid;

	/*
	 * A row is virtual if its owner column holds an OID that this session
	 * assigned to a temporary object of catalog 'ownerclass' (or of the
	 * catalog named by column 'ownerclassattr'), and, if 'owner2' is set,
	 * the same holds for the second owner column.
	 */
	AttrNumber	owner;
	Oid			ownerclass;
	AttrNumber	ownerclassattr;
	AttrNumber	owner2;
	Oid			owner2class;
	AttrNumber	owner2classattr;

	AttrNumber	keyattr[TEMPCAT_NKEYS]; /* hashed lookup columns */
	TempcatKeyType keytype[TEMPCAT_NKEYS];
} TempcatCatalogDef;

#define REL RelationRelationId

static const TempcatCatalogDef tempcat_catalogs[] =
{
	{RelationRelationId, Anum_pg_class_oid, REL, 0, 0, 0, 0,
	{Anum_pg_class_oid, Anum_pg_class_relname}, {TCK_OID, TCK_NAME}},
	{AttributeRelationId, Anum_pg_attribute_attrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attribute_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{TypeRelationId, Anum_pg_type_oid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_type_oid, Anum_pg_type_typname}, {TCK_OID, TCK_NAME}},
	{NamespaceRelationId, Anum_pg_namespace_oid, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_namespace_oid, Anum_pg_namespace_nspname}, {TCK_OID, TCK_NAME}},
	{IndexRelationId, Anum_pg_index_indrelid, REL, 0, 0, 0, 0,
	{Anum_pg_index_indrelid, Anum_pg_index_indexrelid}, {TCK_OID, TCK_OID}},
	{AttrDefaultRelationId, Anum_pg_attrdef_adrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attrdef_adrelid, Anum_pg_attrdef_oid}, {TCK_OID, TCK_OID}},
	/* conrelid, or contypid for domain constraints; see tempcat_route_insert() */
	{ConstraintRelationId, Anum_pg_constraint_conrelid, REL, 0, 0, 0, 0,
	{Anum_pg_constraint_conrelid, Anum_pg_constraint_oid}, {TCK_OID, TCK_OID}},
	{RewriteRelationId, Anum_pg_rewrite_ev_class, REL, 0, 0, 0, 0,
	{Anum_pg_rewrite_ev_class, Anum_pg_rewrite_oid}, {TCK_OID, TCK_OID}},
	{TriggerRelationId, Anum_pg_trigger_tgrelid, REL, 0, 0, 0, 0,
	{Anum_pg_trigger_tgrelid, Anum_pg_trigger_oid}, {TCK_OID, TCK_OID}},
	{StatisticRelationId, Anum_pg_statistic_starelid, REL, 0, 0, 0, 0,
	{Anum_pg_statistic_starelid, 0}, {TCK_OID, TCK_NONE}},
	{DescriptionRelationId, Anum_pg_description_objoid, InvalidOid,
		Anum_pg_description_classoid, 0, 0, 0,
	{Anum_pg_description_objoid, 0}, {TCK_OID, TCK_NONE}},
	{DependRelationId, Anum_pg_depend_objid, InvalidOid, Anum_pg_depend_classid,
		Anum_pg_depend_refobjid, InvalidOid, Anum_pg_depend_refclassid,
	{Anum_pg_depend_objid, Anum_pg_depend_refobjid}, {TCK_OID, TCK_OID}},
	{InheritsRelationId, Anum_pg_inherits_inhrelid, REL, 0,
		Anum_pg_inherits_inhparent, REL, 0,
	{Anum_pg_inherits_inhrelid, Anum_pg_inherits_inhparent}, {TCK_OID, TCK_OID}},
	{PartitionedRelationId, Anum_pg_partitioned_table_partrelid, REL, 0, 0, 0, 0,
	{Anum_pg_partitioned_table_partrelid, 0}, {TCK_OID, TCK_NONE}},
	{SequenceRelationId, Anum_pg_sequence_seqrelid, REL, 0, 0, 0, 0,
	{Anum_pg_sequence_seqrelid, 0}, {TCK_OID, TCK_NONE}},
	{GpPolicyRelationId, Anum_gp_distribution_policy_localoid, REL, 0, 0, 0, 0,
	{Anum_gp_distribution_policy_localoid, 0}, {TCK_OID, TCK_NONE}},
	{AppendOnlyRelationId, Anum_pg_appendonly_relid, REL, 0, 0, 0, 0,
	{Anum_pg_appendonly_relid, 0}, {TCK_OID, TCK_NONE}},
	{FastSequenceRelationId, Anum_gp_fastsequence_objid, REL, 0, 0, 0, 0,
	{Anum_gp_fastsequence_objid, 0}, {TCK_OID, TCK_NONE}},
	{AttributeEncodingRelationId, Anum_pg_attribute_encoding_attrelid, REL, 0, 0, 0, 0,
	{Anum_pg_attribute_encoding_attrelid, 0}, {TCK_OID, TCK_NONE}},
	{PartitionTemplateRelationId, Anum_gp_partition_template_relid, REL, 0, 0, 0, 0,
	{Anum_gp_partition_template_relid, 0}, {TCK_OID, TCK_NONE}},
	{PolicyRelationId, Anum_pg_policy_polrelid, REL, 0, 0, 0, 0,
	{Anum_pg_policy_polrelid, Anum_pg_policy_oid}, {TCK_OID, TCK_OID}},
	/* foreign and external tables */
	{ForeignTableRelationId, Anum_pg_foreign_table_ftrelid, REL, 0, 0, 0, 0,
	{Anum_pg_foreign_table_ftrelid, 0}, {TCK_OID, TCK_NONE}},
	{SecLabelRelationId, Anum_pg_seclabel_objoid, InvalidOid,
		Anum_pg_seclabel_classoid, 0, 0, 0,
	{Anum_pg_seclabel_objoid, 0}, {TCK_OID, TCK_NONE}},
	/* GPDB's DDL tracking (MetaTrackAddObject() and friends) */
	{StatLastOpRelationId, Anum_pg_stat_last_operation_objid, InvalidOid,
		Anum_pg_stat_last_operation_classid, 0, 0, 0,
	{Anum_pg_stat_last_operation_objid, 0}, {TCK_OID, TCK_NONE}},
	/* objects in a temporary namespace kept in memory, and their parts */
	{ProcedureRelationId, Anum_pg_proc_pronamespace, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_proc_oid, Anum_pg_proc_proname}, {TCK_OID, TCK_NAME}},
	{AggregateRelationId, Anum_pg_aggregate_aggfnoid, ProcedureRelationId, 0, 0, 0, 0,
	{Anum_pg_aggregate_aggfnoid, 0}, {TCK_OID, TCK_NONE}},
	{ProcCallbackRelationId, Anum_pg_proc_callback_profnoid, ProcedureRelationId, 0, 0, 0, 0,
	{Anum_pg_proc_callback_profnoid, 0}, {TCK_OID, TCK_NONE}},
	{OperatorRelationId, Anum_pg_operator_oprnamespace, NamespaceRelationId, 0, 0, 0, 0,
	{Anum_pg_operator_oid, Anum_pg_operator_oprname}, {TCK_OID, TCK_NAME}},
	{EnumRelationId, Anum_pg_enum_enumtypid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_enum_enumtypid, Anum_pg_enum_oid}, {TCK_OID, TCK_OID}},
	{RangeRelationId, Anum_pg_range_rngtypid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_range_rngtypid, 0}, {TCK_OID, TCK_NONE}},
	{TypeEncodingRelationId, Anum_pg_type_encoding_typid, TypeRelationId, 0, 0, 0, 0,
	{Anum_pg_type_encoding_typid, 0}, {TCK_OID, TCK_NONE}},
	{StatisticExtRelationId, Anum_pg_statistic_ext_stxrelid, REL, 0, 0, 0, 0,
	{Anum_pg_statistic_ext_stxrelid, Anum_pg_statistic_ext_oid}, {TCK_OID, TCK_OID}},
	{StatisticExtDataRelationId, Anum_pg_statistic_ext_data_stxoid, StatisticExtRelationId, 0, 0, 0, 0,
	{Anum_pg_statistic_ext_data_stxoid, 0}, {TCK_OID, TCK_NONE}},
	/* owner and ACL rows only; see tempcat_route_insert() */
	{SharedDependRelationId, Anum_pg_shdepend_objid, InvalidOid,
		Anum_pg_shdepend_classid, 0, 0, 0,
	{Anum_pg_shdepend_objid, Anum_pg_shdepend_refobjid}, {TCK_OID, TCK_OID}},
};

#undef REL

#define TEMPCAT_NCATALOGS lengthof(tempcat_catalogs)

/*
 * Shared structures.  Everything below lives in the session's DSA area and
 * refers to other objects by dsa_pointer.  The writer modifies them under
 * the exclusive lock; readers take the lock in shared mode.
 */
typedef struct TempcatHash
{
	uint32		nbuckets;		/* power of two, or 0 if not allocated */
	uint32		nentries;
	dsa_pointer buckets;		/* dsa_pointer[nbuckets] */
} TempcatHash;

typedef struct TempcatHashNode
{
	dsa_pointer next;
	dsa_pointer entry;
	uint32		hash;
} TempcatHashNode;

typedef struct TempcatCatalog
{
	int32		nentries;		/* entries of any visibility */
	dsa_pointer head;			/* list of all entries */
	TempcatHash keys[TEMPCAT_NKEYS];
} TempcatCatalog;

typedef struct TempcatRoot
{
	LWLock		lock;
	int			writer_pgprocno;	/* the writer's, see tempcat_writer_state() */
	uint64		next_tid;
	TempcatHash tids;
	TempcatCatalog catalogs[TEMPCAT_NCATALOGS];
} TempcatRoot;

/* xmin is known to be committed (or frozen) */
#define TCE_XMIN_COMMITTED	0x0001
/* entry is on the writer's pending list */
#define TCE_PENDING			0x0002

typedef struct TempcatEntry
{
	dsa_pointer prev;
	dsa_pointer next;
	TransactionId xmin;
	TransactionId xmax;
	CommandId	cmin;
	CommandId	cmax;
	uint32		keyhash[TEMPCAT_NKEYS];
	uint32		tidhash;
	uint16		flags;
	int16		catidx;
	ItemPointerData tid;
	uint32		len;			/* length of the tuple data that follows */
} TempcatEntry;

#define TCE_DATA(e) \
	((HeapTupleHeader) ((char *) (e) + MAXALIGN(sizeof(TempcatEntry))))

#define TC_ADDR(dp)	dsa_get_address(tc_area, (dp))

/*
 * Process-local state.
 */
static dsa_area *tc_area = NULL;
static TempcatRoot *tc_root = NULL;

/*
 * Writer only: entries inserted or deleted by transactions whose outcome
 * has not been applied yet.
 */
static dsa_pointer *tc_pending = NULL;
static int	tc_npending = 0;
static int	tc_maxpending = 0;

/* Rows deleted so far, and the last walk of tempcat_reclaim_dead() */
static uint64 tc_ndeletions = 0;
static bool tc_reclaim_failed = false;	/* it freed nothing */
static TransactionId tc_reclaim_xid = InvalidTransactionId;
static CommandId tc_reclaim_cid = InvalidCommandId;
static uint64 tc_reclaim_ndeletions = 0;
static bool tc_reclaim_waiting = false;	/* rows left for a later command */

struct TempcatScanData
{
	HeapTuple  *tuples;
	int			ntuples;
	int			next;
	bool		on_virtual;		/* last tuple returned was virtual */

	/*
	 * For scans through an index: virtual rows are sorted by the index key
	 * and merged with the on-disk rows, so that callers relying on index
	 * order (e.g. catcache lists of pg_attribute by attnum) see it.
	 */
	Relation	irel;
	TupleDesc	heapdesc;
	int			nidxkeys;
	FmgrInfo  **procs;
	HeapTuple	disk_tuple;		/* fetched on-disk row not returned yet */
	bool		disk_done;
	int			direction;		/* 0 until the first row, then 1 or -1 */
};

typedef enum TempcatXidStatus
{
	TCX_IN_PROGRESS,
	TCX_COMMITTED,
	TCX_ABORTED
} TempcatXidStatus;

static int	tempcat_tranche_id(void);

/*
 * Reader processes (reader gangs, entry-db readers, and parallel retrieve
 * cursor connections) only look at the writer's area; everything else owns
 * its own.
 */
static inline bool
tempcat_is_reader(void)
{
	return (Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer) ||
		am_cursor_retrieve_handler;
}

/*
 * The index of each covered catalog in tempcat_catalogs[], plus one, by the
 * catalog's OID (0 for others), built at the first lookup: catalog scans
 * look it up all the time, also with the feature off.  Catalogs have OIDs
 * below FirstGenbkiObjectId.
 */
static uint8 tc_catidx_by_relid[FirstGenbkiObjectId];
static bool tc_catidx_ready = false;

StaticAssertDecl(lengthof(tempcat_catalogs) < PG_UINT8_MAX,
				 "too many catalogs for tc_catidx_by_relid");

static int
tempcat_catalog_index(Oid relid)
{
	if (unlikely(!tc_catidx_ready))
	{
		int			i;

		for (i = 0; i < TEMPCAT_NCATALOGS; i++)
		{
			Assert(tempcat_catalogs[i].relid < FirstGenbkiObjectId);
			tc_catidx_by_relid[tempcat_catalogs[i].relid] = i + 1;
		}
		tc_catidx_ready = true;
	}
	if (relid >= FirstGenbkiObjectId)
		return -1;
	return (int) tc_catidx_by_relid[relid] - 1;
}

/*
 * Space for dynamic shared memory.
 *
 * The session's area grows by creating DSM segments.  With the default
 * dynamic_shared_memory_type = posix they live in /dev/shm, which can be
 * small (64 MB in a default Docker container).  Allocations are made with
 * DSA_ALLOC_NO_OOM, so failing to create a segment for lack of space just
 * makes the row go to the on-disk catalog (see DSM_CREATE_NULL_IF_NOSPACE).
 * Other users of DSM (e.g. combo CIDs shared with reader gangs) are not that
 * tolerant, so the area does not grow into the last quarter of the space
 * (dsa_set_min_free_space()), nor into the last quarter of the slots of the
 * DSM control segment, which every segment takes one of
 * (dsa_set_min_free_slots()).  Both are checked before creating the area,
 * and failing to create its first segment is no error either
 * (dsa_try_create()).
 */
#define TEMPCAT_DSA_FIRST_SEGMENT	(1024 * 1024)
#define TEMPCAT_DSM_CHECK_ATTEMPTS	100

static bool tc_dsm_low = false;		/* could not create the area */
static int	tc_attempts_since_check = 0;

/* Leave this fraction of the DSM space to its other users. */
#define TEMPCAT_DSM_HEADROOM_DIV	4

/* Would creating 'needed' bytes of segments eat into the headroom? */
static bool
tempcat_dsm_space_low(size_t needed)
{
	Size		free_bytes;
	Size		total_bytes;

#ifdef FAULT_INJECTOR
	/* Test hook: pretend that DSM space is short. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_dsm_space_low") == FaultInjectorTypeSkip)
		return true;
#endif
	return dsm_impl_free_space(&free_bytes, &total_bytes) &&
		free_bytes < needed + total_bytes / TEMPCAT_DSM_HEADROOM_DIV;
}

/* Would creating a segment eat into the slots left to other users? */
static bool
tempcat_dsm_slots_low(void)
{
	uint32		nfree;
	uint32		nmax;

	return dsm_free_slots(&nfree, &nmax) &&
		nfree <= nmax / TEMPCAT_DSM_HEADROOM_DIV;
}

static inline size_t
tempcat_max_area_bytes(void)
{
	return (size_t) gp_temp_memory_catalog_max_size * 1024;
}

/*
 * Attach to the session's area, creating it if 'create' and we are the
 * writer.  Returns false if there is no area (yet), or if it could not be
 * created because DSM space is short.
 */
static bool
tempcat_attach(bool create)
{
	static bool tranche_registered = false;
	static LocalTransactionId no_area_lxid = InvalidLocalTransactionId;
	MemoryContext oldcxt;
	dsa_pointer rootptr;
	dsa_area   *area;

	if (tc_area != NULL)
		return true;

	/*
	 * Once per process, also where no area is to come: pg_stat_activity
	 * names other processes' waits for the lock by this process's tranches.
	 */
	if (unlikely(!tranche_registered))
	{
		LWLockRegisterTranche(tempcat_tranche_id(), "tempcat");
		tranche_registered = true;
	}

	if (am_cursor_retrieve_handler)
	{
		/*
		 * A retrieve connection outputs the rows of another session's
		 * parallel retrieve cursor and must see that session's temporary
		 * types.  It runs its own transactions, so it sees their committed
		 * catalog rows only, like on disk.
		 */
		dsa_handle	handle;
		int			sessionId = RetrieveSessionId();

		/*
		 * (free slots have session ID -1, like an unauthenticated session;
		 * and do not search the slots, under their lock, at every catalog
		 * scan of a transaction that found no area: it sees committed rows
		 * only, which the next transaction looks for again)
		 */
		if (no_area_lxid == MyProc->lxid && MyProc->lxid != InvalidLocalTransactionId)
			return false;
		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr))
		{
			no_area_lxid = MyProc->lxid;
			return false;
		}

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		tc_area = dsa_attach(handle);
		dsa_pin_mapping(tc_area);
		MemoryContextSwitchTo(oldcxt);

		tc_root = dsa_get_address(tc_area, rootptr);
		return true;
	}

	if (tempcat_is_reader())
	{
		dsa_handle	handle;

		if (SharedLocalSnapshotSlot == NULL)
			return false;
		handle = SharedLocalSnapshotSlot->tempcat_handle;
		if (handle == DSM_HANDLE_INVALID)
			return false;
		pg_read_barrier();
		rootptr = SharedLocalSnapshotSlot->tempcat_root;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		tc_area = dsa_attach(handle);
		dsa_pin_mapping(tc_area);
		MemoryContextSwitchTo(oldcxt);

		tc_root = dsa_get_address(tc_area, rootptr);
		return true;
	}

	if (!create)
		return false;

	/* Its first segment must fit; recheck only every so often. */
	if (tc_dsm_low && ++tc_attempts_since_check < TEMPCAT_DSM_CHECK_ATTEMPTS)
		return false;
	tc_attempts_since_check = 0;
	tc_dsm_low = tempcat_dsm_space_low(2 * TEMPCAT_DSA_FIRST_SEGMENT) ||
		tempcat_dsm_slots_low();
	if (tc_dsm_low)
		return false;

	/*
	 * The area's memory is the session's, though shared: charge it to the
	 * session's vmem (gp_vmem_protect_limit, resource groups), its first
	 * segment here and later ones as the area grows (dsa_set_vmem_tracked()).
	 * Without vmem to spare, rows go to the on-disk catalog.  Charge it before
	 * creating the area, so that a refusal costs nothing; dsa_try_create()
	 * can still fail with an error (e.g. out of local memory), which must
	 * not leave vmem charged.
	 */
	if (VmemTracker_ReserveVmem(TEMPCAT_DSA_FIRST_SEGMENT) != MemoryAllocation_Success)
		return false;
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	PG_TRY();
	{
		area = dsa_try_create(tempcat_tranche_id());
	}
	PG_CATCH();
	{
		VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcxt);
	if (area == NULL)
	{
		/* (the space or the slots ran out since the check) */
		VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
		tc_dsm_low = true;
		return false;
	}
	dsa_pin_mapping(area);
	dsa_set_vmem_tracked(area);
	tc_area = area;

	rootptr = dsa_allocate0(tc_area, sizeof(TempcatRoot));
	tc_root = dsa_get_address(tc_area, rootptr);
	LWLockInitialize(&tc_root->lock, tempcat_tranche_id());
	tc_root->writer_pgprocno = MyProc->pgprocno;

	/* Rows that do not fit go to the on-disk catalog instead. */
	dsa_set_size_limit(tc_area, tempcat_max_area_bytes());
	{
		Size		free_bytes;
		Size		total_bytes;

		uint32		nfree;
		uint32		nmax;

		if (dsm_impl_free_space(&free_bytes, &total_bytes))
			dsa_set_min_free_space(tc_area, total_bytes / TEMPCAT_DSM_HEADROOM_DIV);
		if (dsm_free_slots(&nfree, &nmax))
			dsa_set_min_free_slots(tc_area, nmax / TEMPCAT_DSM_HEADROOM_DIV);
	}

	/* Publish the area to the readers of this session. */
	if (SharedLocalSnapshotSlot != NULL)
	{
		SharedLocalSnapshotSlot->tempcat_root = rootptr;
		pg_write_barrier();
		SharedLocalSnapshotSlot->tempcat_handle = dsa_get_handle(tc_area);
	}

	return true;
}

/* ----------------------------------------------------------------
 * OID assignment
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * Shared memory
 *
 * Per PGPROC: the next OID of its slice of the reserved range, and the
 * oldest relfrozenxid/relminmxid of the in-memory temporary tables of the
 * session using it.  The latter stand in for the pg_class rows that
 * vac_update_datfrozenxid() cannot see.  Also the databases already swept
 * for leftovers of crashed sessions since the node started.
 * ----------------------------------------------------------------
 */
#define TEMPCAT_MAX_SWEPT_DBS 128
#define TEMPCAT_MAX_ROLES	8

typedef struct TempcatProcState
{
	Oid			nextOid;
	Oid			databaseId;		/* database of the published horizon */
	pg_atomic_uint32 frozenXid; /* or InvalidTransactionId */
	pg_atomic_uint32 minMulti;	/* or InvalidMultiXactId */

	/*
	 * Roles that in-memory pg_shdepend rows of the session refer to (owners
	 * and grantees of its temporary objects), standing in for the rows
	 * DROP ROLE cannot see.  Changed under TempcatShared->mutex.
	 */
	int			nroles;
	Oid			roles[TEMPCAT_MAX_ROLES];

	/*
	 * Bounds of the reserved OIDs the session assigned to (QD) or received
	 * for (QE) its temporary objects, or InvalidOid; for other processes
	 * (see tempcat_bounds_cover()).  Written by the session only.  The OIDs
	 * of a session come in order from its slice of the range, which starts
	 * anywhere in the slice and wraps around at its end: those taken after
	 * a wrap get bounds of their own (wrapMin, wrapMax), rather than make
	 * the bounds span the whole slice.
	 */
	pg_atomic_uint32 ownedMin;
	pg_atomic_uint32 ownedMax;
	pg_atomic_uint32 wrapMin;
	pg_atomic_uint32 wrapMax;
} TempcatProcState;

/*
 * An OID that far below the bounds of a session, at most, widens them
 * rather than starting the second ones: OIDs of one command can come out of
 * order on a QE.
 */
#define TEMPCAT_BOUNDS_GAP		10000

/*
 * A live session that a sweep left a left temporary namespace to, as it
 * would take its name; see tempcat_note_claim().
 */
typedef struct TempcatClaim
{
	Oid			databaseId;
	int			pgprocno;
	int			sessionId;
} TempcatClaim;

#define TEMPCAT_MAX_CLAIMS 16

typedef struct TempcatSharedData
{
	int			tranche_id;		/* LWLock tranche of the session areas */
	slock_t		mutex;			/* protects the swept list */
	int			nswept;
	int			nextswept;		/* entry to replace when the list is full */
	Oid			swept[TEMPCAT_MAX_SWEPT_DBS];
	uint64		nunmarks;		/* tempcat_unmark_swept() calls so far */
	int			nclaims;		/* also protected by mutex */
	TempcatClaim claims[TEMPCAT_MAX_CLAIMS];
	int			nprocs;
	TempcatProcState procs[FLEXIBLE_ARRAY_MEMBER];
} TempcatSharedData;

static TempcatSharedData *TempcatShared = NULL;

/* Lock tag sub-ID serializing the leftover sweep of a database */
#define TEMPCAT_SWEEP_LOCK_SUBID	0x7EC

static inline int
tempcat_total_procs(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;
}

Size
TempcatShmemSize(void)
{
	return add_size(offsetof(TempcatSharedData, procs),
					mul_size(tempcat_total_procs(), sizeof(TempcatProcState)));
}

void
TempcatShmemInit(void)
{
	bool		found;
	int			i;

	TempcatShared = ShmemInitStruct("Temporary catalog data",
									TempcatShmemSize(), &found);
	if (found)
		return;

	/* A dynamic tranche, so that the builtin tranche IDs stay unchanged. */
	TempcatShared->tranche_id = LWLockNewTrancheId();
	SpinLockInit(&TempcatShared->mutex);
	TempcatShared->nswept = 0;
	TempcatShared->nextswept = 0;
	TempcatShared->nunmarks = 0;
	TempcatShared->nclaims = 0;
	TempcatShared->nprocs = tempcat_total_procs();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		st->nextOid = InvalidOid;
		st->databaseId = InvalidOid;
		st->nroles = 0;
		pg_atomic_init_u32(&st->frozenXid, InvalidTransactionId);
		pg_atomic_init_u32(&st->minMulti, InvalidMultiXactId);
		pg_atomic_init_u32(&st->ownedMin, InvalidOid);
		pg_atomic_init_u32(&st->ownedMax, InvalidOid);
		pg_atomic_init_u32(&st->wrapMin, InvalidOid);
		pg_atomic_init_u32(&st->wrapMax, InvalidOid);
	}
}

/* LWLock tranche of the session areas */
static int
tempcat_tranche_id(void)
{
	return TempcatShared->tranche_id;
}

static inline TempcatProcState *
tempcat_my_state(void)
{
	Assert(MyProc->pgprocno < TempcatShared->nprocs);
	return &TempcatShared->procs[MyProc->pgprocno];
}

/* Has this session started to keep temporary objects in memory? */
static bool tc_activated = false;

/* No reserved OID was free for the temporary schema; see tempcat_allocate_oid() */
static bool tc_slice_exhausted = false;

/* The published frozen horizon may be too old and needs recomputing. */
static bool tc_horizon_dirty = false;

/* The published roles may include roles no longer used. */
static bool tc_roles_dirty = false;

/*
 * Our temporary namespaces may have gone to disk (see tempcat_row_to_disk()),
 * and not been removed since (tempcat_namespaces_removed()).
 */
static bool tc_namespaces_to_disk = false;

static void tempcat_sweep_if_needed(bool allow_force);
static void tempcat_publish_oid(Oid oid);
static bool tempcat_bounds_cover(TempcatProcState *st, Oid oid);
static void tempcat_clear_bounds(TempcatProcState *st);
static void tempcat_sweep_db(bool force, bool wait);
static void tempcat_sweep_xact_callback(XactEvent event, void *arg);
static void tempcat_sweep_subxact_callback(SubXactEvent event,
										   SubTransactionId mySubid,
										   SubTransactionId parentSubid,
										   void *arg);
static void tempcat_sweep_exit(int code, Datum arg);
static void tempcat_xact_callback(XactEvent event, void *arg);
static void tempcat_subxact_callback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 void *arg);
static bool tc_sweep_callbacks = false;

/*
 * The (sub)transaction whose sweep marked tc_sweep_db as swept before
 * committing; see tempcat_sweep_xact_callback().
 */
static TransactionId tc_sweep_xid = InvalidTransactionId;
static SubTransactionId tc_sweep_subid = InvalidSubTransactionId;
static Oid	tc_sweep_db = InvalidOid;
static void tempcat_sweep_resolved(bool committed);
static void tempcat_unmark_swept(Oid dbid);
static bool tempcat_area_has_live_rows(void);
static void tempcat_unlink_live_relations(void);

/*
 * Runs after the temporary objects were dropped at session exit
 * (RemoveTempRelationsCallback(), registered later, runs earlier), while
 * the area is still attached.  If that cleanup did not finish (it failed,
 * or a prepared transaction kept it from seeing the objects), the on-disk
 * rows of the remaining objects are leftovers like those of a crashed
 * session, but no node restart will trigger their sweep: ask for one.
 * The files of the relations whose rows are in memory would be left with
 * nothing leading to them until the node restarts: remove them now.
 */
static void
tempcat_before_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();
	bool		leftovers;

	/*
	 * Without an area, rows went to disk only; assume the worst.  So did our
	 * namespaces if a row did not fit; the cleanup removes them (and their
	 * objects), unless it failed.
	 */
	leftovers = tc_area == NULL || tc_namespaces_to_disk ||
		tempcat_area_has_live_rows();

	/* Stop protecting our rows from the sweep before asking for it. */
	tempcat_clear_bounds(st);

	if (leftovers)
	{
		/* (first, so that an error removing files does not lose it) */
		tempcat_unmark_swept(MyDatabaseId);
		if (tc_area != NULL)
			tempcat_unlink_live_relations();
	}
}

/*
 * Our temporary namespaces were removed, in a transaction that committed:
 * by the cleanup at exit or session reset (GpDropTempTables()), after their
 * objects.  If they went to disk, nothing is left there now.
 */
void
tempcat_namespaces_removed(void)
{
	tc_namespaces_to_disk = false;
}

static void
tempcat_proc_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();

	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	tempcat_clear_bounds(st);
	st->databaseId = InvalidOid;

	SpinLockAcquire(&TempcatShared->mutex);
	st->nroles = 0;
	SpinLockRelease(&TempcatShared->mutex);
}

/*
 * Publish that an in-memory pg_shdepend row of ours refers to a role.
 * Returns false if the list is full; the row then goes to disk.
 */
static bool
tempcat_publish_role(Oid roleid)
{
	TempcatProcState *st = tempcat_my_state();
	bool		ok = true;
	int			i;

	/* Only we change our own list, so it can be read without the lock. */
	for (i = 0; i < st->nroles; i++)
	{
		if (st->roles[i] == roleid)
			return true;
	}

	SpinLockAcquire(&TempcatShared->mutex);
	if (st->nroles < TEMPCAT_MAX_ROLES)
		st->roles[st->nroles++] = roleid;
	else
		ok = false;
	SpinLockRelease(&TempcatShared->mutex);

	return ok;
}

/*
 * Do in-memory temporary objects of another session refer to the role?
 * Called by DROP ROLE (checkSharedDependencies()), which holds a lock on the
 * role that keeps sessions from adding such references meanwhile: they lock
 * it too before recording a dependency on it.
 */
bool
tempcat_role_used_elsewhere(Oid roleid)
{
	bool		found = false;
	int			i;
	int			j;

	if (TempcatShared == NULL)
		return false;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nprocs && !found; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		if (MyProc != NULL && i == MyProc->pgprocno)
			continue;
		for (j = 0; j < st->nroles; j++)
		{
			if (st->roles[j] == roleid)
			{
				found = true;
				break;
			}
		}
	}
	SpinLockRelease(&TempcatShared->mutex);

	return found;
}

/*
 * Called when the session gets its first reserved OID, i.e. when its
 * temporary namespace is created in memory (on the QD and on each QE
 * writer).
 */
static void
tempcat_activate(void)
{
	TempcatProcState *st;

	if (tc_activated)
		return;
	tc_activated = true;

	st = tempcat_my_state();
	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	st->databaseId = MyDatabaseId;
	on_shmem_exit(tempcat_proc_exit, (Datum) 0);
	before_shmem_exit(tempcat_before_exit, (Datum) 0);
	RegisterSubXactCallback(tempcat_subxact_callback, NULL);
	RegisterXactCallback(tempcat_xact_callback, NULL);

	tempcat_sweep_if_needed(true);
}

/* Lower the published horizon to cover 'xid'.  Lowering is always safe. */
static void
tempcat_cover_xid(TransactionId xid)
{
	TempcatProcState *st;
	TransactionId cur;

	if (!TransactionIdIsNormal(xid))
		return;
	st = tempcat_my_state();
	cur = pg_atomic_read_u32(&st->frozenXid);
	if (!TransactionIdIsValid(cur) || TransactionIdPrecedes(xid, cur))
		pg_atomic_write_u32(&st->frozenXid, xid);
}

/*
 * Lower the published horizon to cover a pg_class row of ours.  Lowering is
 * always safe; raising waits for tempcat_recompute_horizon().
 */
static void
tempcat_cover_pg_class_row(HeapTupleHeader hdr)
{
	Form_pg_class form = (Form_pg_class) ((char *) hdr + hdr->t_hoff);
	TempcatProcState *st = tempcat_my_state();
	MultiXactId mxid = form->relminmxid;

	tempcat_cover_xid(form->relfrozenxid);
	if (MultiXactIdIsValid(mxid))
	{
		MultiXactId cur = pg_atomic_read_u32(&st->minMulti);

		if (!MultiXactIdIsValid(cur) || MultiXactIdPrecedes(mxid, cur))
			pg_atomic_write_u32(&st->minMulti, mxid);
	}
}

/*
 * Fold the horizons published by sessions of this database into the result
 * of vac_update_datfrozenxid()'s pg_class scan.
 *
 * A temporary table created concurrently is not a problem: its relfrozenxid
 * is not older than the oldest xmin the caller started from.
 */
void
tempcat_fold_frozen_horizon(TransactionId *frozenXid, MultiXactId *minMulti)
{
	int			i;

	if (TempcatShared == NULL)
		return;

	pg_read_barrier();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];
		TransactionId xid;
		MultiXactId mxid;

		if (st->databaseId != MyDatabaseId)
			continue;

		xid = pg_atomic_read_u32(&st->frozenXid);
		mxid = pg_atomic_read_u32(&st->minMulti);
		if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, *frozenXid))
			*frozenXid = xid;
		if (MultiXactIdIsValid(mxid) && MultiXactIdPrecedes(mxid, *minMulti))
			*minMulti = mxid;
	}
}

/*
 * OIDs assigned by this session to its temporary objects, per catalog.
 */
typedef struct TempcatOwnedKey
{
	Oid			catalog;
	Oid			oid;
} TempcatOwnedKey;

typedef struct TempcatOwnedEntry
{
	TempcatOwnedKey key;
	bool		on_disk;		/* it lives, or lived, on disk */
} TempcatOwnedEntry;

static HTAB *tc_owned = NULL;

static void
tempcat_remember_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;
	TempcatOwnedEntry *entry;
	bool		found;

	if (tc_owned == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatOwnedKey);
		ctl.entrysize = sizeof(TempcatOwnedEntry);
		ctl.hcxt = TopMemoryContext;
		tc_owned = hash_create("tempcat owned OIDs", 256, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	key.catalog = catalog;
	key.oid = oid;
	entry = hash_search(tc_owned, &key, HASH_ENTER, &found);
	if (!found)
		entry->on_disk = false;

	if (IsTempcatOid(oid))
		tempcat_publish_oid(oid);
}

/*
 * Widen the published bounds to cover 'oid'; nobody else writes them.  They
 * only ever widen while the session lives: other processes read them
 * without a lock, and a row of ours must never be outside what they read.
 */
static void
tempcat_publish_oid(Oid oid)
{
	TempcatProcState *st = tempcat_my_state();
	Oid			lo = pg_atomic_read_u32(&st->ownedMin);
	Oid			hi = pg_atomic_read_u32(&st->ownedMax);
	Oid			wlo = pg_atomic_read_u32(&st->wrapMin);
	Oid			whi = pg_atomic_read_u32(&st->wrapMax);

	if (!OidIsValid(lo))
	{
		/* (the upper bound first: readers take a valid lower one for both) */
		pg_atomic_write_u32(&st->ownedMax, oid);
		pg_write_barrier();
		pg_atomic_write_u32(&st->ownedMin, oid);
		return;
	}
	if ((oid >= lo && oid <= hi) ||
		(OidIsValid(wlo) && oid >= wlo && oid <= whi))
		return;

	if (oid > hi)
		pg_atomic_write_u32(&st->ownedMax, oid);
	else if (lo - oid <= TEMPCAT_BOUNDS_GAP)
		pg_atomic_write_u32(&st->ownedMin, oid);
	else if (!OidIsValid(wlo))
	{
		/* the slice wrapped around */
		pg_atomic_write_u32(&st->wrapMax, oid);
		pg_write_barrier();
		pg_atomic_write_u32(&st->wrapMin, oid);
	}
	else if (oid < wlo)
		pg_atomic_write_u32(&st->wrapMin, oid);
	else if (oid - whi <= lo - oid)
		pg_atomic_write_u32(&st->wrapMax, oid);
	else
		pg_atomic_write_u32(&st->ownedMin, oid);
}

/* Read the published bounds of a process: up to two ranges. */
static int
tempcat_read_bounds(TempcatProcState *st, Oid *lo, Oid *hi)
{
	int			n = 0;
	Oid			l;
	Oid			h;

	l = pg_atomic_read_u32(&st->ownedMin);
	pg_read_barrier();
	h = pg_atomic_read_u32(&st->ownedMax);
	if (OidIsValid(l) && OidIsValid(h))
	{
		lo[n] = l;
		hi[n] = h;
		n++;
	}
	l = pg_atomic_read_u32(&st->wrapMin);
	pg_read_barrier();
	h = pg_atomic_read_u32(&st->wrapMax);
	if (OidIsValid(l) && OidIsValid(h))
	{
		lo[n] = l;
		hi[n] = h;
		n++;
	}
	return n;
}

/* Is the OID within the bounds a process publishes? */
static bool
tempcat_bounds_cover(TempcatProcState *st, Oid oid)
{
	Oid			lo[2];
	Oid			hi[2];
	int			n = tempcat_read_bounds(st, lo, hi);
	int			i;

	for (i = 0; i < n; i++)
	{
		if (oid >= lo[i] && oid <= hi[i])
			return true;
	}
	return false;
}

static void
tempcat_clear_bounds(TempcatProcState *st)
{
	pg_atomic_write_u32(&st->ownedMin, InvalidOid);
	pg_atomic_write_u32(&st->ownedMax, InvalidOid);
	pg_atomic_write_u32(&st->wrapMin, InvalidOid);
	pg_atomic_write_u32(&st->wrapMax, InvalidOid);
}

/*
 * The object's last row in memory is gone.  Unless some of its rows went to
 * disk (the area was full), where it may live on, so is the object.  Returns
 * whether we forgot it.
 */
static bool
tempcat_forget_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;
	TempcatOwnedEntry *entry;

	if (tc_owned == NULL)
		return false;
	key.catalog = catalog;
	key.oid = oid;
	entry = hash_search(tc_owned, &key, HASH_FIND, NULL);
	if (entry == NULL || entry->on_disk)
		return false;
	(void) hash_search(tc_owned, &key, HASH_REMOVE, NULL);
	return true;
}

/*
 * Did this session assign 'oid' to one of its temporary objects in
 * 'catalog'?
 */
bool
tempcat_owns_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL || !IsTempcatOid(oid))
		return false;
	key.catalog = catalog;
	key.oid = oid;
	return hash_search(tc_owned, &key, HASH_FIND, NULL) != NULL;
}

/*
 * The state of the writer whose area a reader looks at: NULL if not known
 * (a parallel retrieve connection to a session without an area).
 */
static TempcatProcState *
tempcat_writer_state(void)
{
	if (am_cursor_retrieve_handler)
		return tempcat_attach(false) ?
			&TempcatShared->procs[tc_root->writer_pgprocno] : NULL;
	if (SharedLocalSnapshotSlot == NULL ||
		SharedLocalSnapshotSlot->writer_proc == NULL)
		return NULL;
	return &TempcatShared->procs[SharedLocalSnapshotSlot->writer_proc->pgprocno];
}

/*
 * Is 'oid' that of a temporary object of our session?  Readers do not know
 * which OIDs their writer assigned, but it publishes bounds that cover
 * them, and no OID of another session's temporary object: each session
 * takes them from a slice of the reserved range of its own.  (What is not
 * known is taken as ours, as all the session's readers did before.)
 */
static bool
tempcat_session_owns_oid(Oid catalog, Oid oid)
{
	TempcatProcState *st;

	if (!tempcat_is_reader())
		return tempcat_owns_oid(catalog, oid);
	if (!IsTempcatOid(oid))
		return false;
	st = tempcat_writer_state();
	return st == NULL || tempcat_bounds_cover(st, oid);
}

/*
 * Does the dispatch key describe a temporary object of this session?
 * 'new_namespace' says whether a temporary namespace may be created in
 * memory.
 */
static bool
tempcat_key_is_temp(Oid catalog, const char *objname, Oid namespaceOid,
					Oid keyOid1, Oid keyOid2, bool new_namespace)
{
	switch (catalog)
	{
		case NamespaceRelationId:
			return new_namespace && objname != NULL &&
				(strncmp(objname, "pg_temp_", 8) == 0 ||
				 strncmp(objname, "pg_toast_temp_", 14) == 0);

		case RelationRelationId:
		case TypeRelationId:
		case ProcedureRelationId:
		case OperatorRelationId:
			return tempcat_owns_oid(NamespaceRelationId, namespaceOid);

		case AttrDefaultRelationId:
		case RewriteRelationId:
		case TriggerRelationId:
		case PolicyRelationId:
			return tempcat_owns_oid(RelationRelationId, keyOid1);

		case ConstraintRelationId:
			return tempcat_owns_oid(RelationRelationId, keyOid1) ||
				tempcat_owns_oid(TypeRelationId, keyOid2);

		default:
			return false;
	}
}

/*
 * Should a new object get an OID from the reserved range?  Called in the QD
 * by oid_dispatch.c with the same key it dispatches to the segments.
 */
bool
tempcat_want_temp_oid(Oid catalog, const char *objname, Oid namespaceOid,
					  Oid keyOid1, Oid keyOid2)
{
	/* the slice was full when the temporary schema was created */
	if (tc_slice_exhausted)
		return false;

	/*
	 * A utility-mode session on a segment keeps its temporary objects on
	 * disk: OIDs from the segment's slices of the reserved range could be the
	 * ones that the coordinator gives the temporary objects of its sessions
	 * on this segment, whose in-memory rows we cannot see to avoid them.
	 */
	if (Gp_role == GP_ROLE_UTILITY && !IS_QUERY_DISPATCHER())
		return false;

	return tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							   gp_enable_temp_memory_catalog);
}

static void tempcat_check_preassigned_oid(Relation relation, Oid indexId,
										  AttrNumber oidcolumn, Oid oid);

/*
 * A QE received a pre-assigned OID from the QD.  If the QD gave a temporary
 * object of this session a reserved OID ('temp' says that the object is one
 * even if the key does not tell), check that the OID is free on this node
 * (after the sweep that activating may run), then remember that we own it.
 * In this order: an OID refused here must not become ours, as our published
 * bounds would cover it for the rest of the session, and with it rows of
 * whatever has it.
 */
void
tempcat_note_preassigned_oid(Relation relation, Oid indexId,
							 AttrNumber oidcolumn, const char *objname,
							 Oid namespaceOid, Oid keyOid1, Oid keyOid2,
							 bool temp, Oid oid)
{
	Oid			catalog = RelationGetRelid(relation);

	if (IsBinaryUpgrade || !IsTempcatOid(oid))
		return;

	if (temp ||
		tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							true))
	{
		tempcat_activate();
		tempcat_check_preassigned_oid(relation, indexId, oidcolumn, oid);
		tempcat_remember_oid(catalog, oid);
	}
}

/*
 * Only relations, types, functions and operators can be created in a
 * temporary namespace kept in memory: other objects (operator classes,
 * collations, text search objects, ...) would get on-disk catalog rows
 * pointing at the namespace that other sessions cannot see.  Called by
 * oid_dispatch.c in the QD.
 */
void
tempcat_check_namespace_object(Oid catalog, Oid namespaceOid)
{
	if (catalog == RelationRelationId || catalog == TypeRelationId ||
		catalog == ProcedureRelationId || catalog == OperatorRelationId)
		return;
	if (catalog == DefaultAclRelationId &&
		tempcat_owns_oid(NamespaceRelationId, namespaceOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("default privileges cannot be set for a temporary schema kept in memory"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
	if (tempcat_owns_oid(NamespaceRelationId, namespaceOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only relations, types, functions and operators can be created in a temporary schema kept in memory"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Some objects can be created for temporary tables or types, but are not
 * supported in memory (e.g. enum labels, read by ordered scans).
 */
void
tempcat_check_unsupported(Oid catalog, Oid objectId, const char *what)
{
	if (tempcat_owns_oid(catalog, objectId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s are not supported for temporary objects kept in memory", what),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * A temporary table kept in memory cannot inherit from an ordinary table.
 * Other sessions could not see it among the ordinary table's children, and
 * would treat the ordinary table as if it had none: ANALYZE would clear its
 * relhassubclass, hiding the child's rows from the child's own session, and
 * ALTER TABLE would leave the child behind instead of failing.  Called for
 * each new pg_inherits row.
 */
void
tempcat_check_inheritance(Oid relid, Oid parentid)
{
	if (tempcat_owns_oid(RelationRelationId, relid) &&
		!tempcat_owns_oid(RelationRelationId, parentid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary tables kept in memory cannot inherit from ordinary table \"%s\"",
						get_rel_name(parentid)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Nor can it be a typed table of an ordinary composite type: other sessions
 * could not see it among the type's typed tables, and ALTER TYPE would
 * change the type without it instead of failing.
 */
void
tempcat_check_typed_table(Oid relid, Oid typid)
{
	if (tempcat_owns_oid(RelationRelationId, relid) &&
		!tempcat_owns_oid(TypeRelationId, typid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary tables kept in memory cannot be typed tables of ordinary type %s",
						format_type_be(typid)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * An operator in a temporary schema kept in memory and an ordinary operator
 * cannot be commutators or negators of each other: each one's pg_operator
 * row points at the other (oprcom, oprnegate) without a pg_depend row.  The
 * ordinary one's row would point at an operator that other sessions cannot
 * see, pg_dump would drop the link, and after a crash nobody would reset it.
 * Called for the commutator and the negator (perhaps shells just made) of
 * a new operator in 'namespaceOid'.
 */
void
tempcat_check_operator_link(Oid namespaceOid, Oid otherOprOid)
{
	if (OidIsValid(otherOprOid) &&
		tempcat_owns_oid(NamespaceRelationId, namespaceOid) !=
		tempcat_owns_oid(OperatorRelationId, otherOprOid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary operators kept in memory and ordinary operators cannot be commutators or negators of each other"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

static bool tempcat_found_object_of_other_session(const ObjectAddress *object);

/*
 * An ordinary object cannot depend on a temporary object kept in memory
 * (e.g. a column of an ordinary table of a temporary table's row type):
 * its on-disk rows would point at an object that other sessions cannot see,
 * and that is gone after a crash.  Called for each new pg_depend row.
 */
void
tempcat_check_dependency(const ObjectAddress *depender,
						 const ObjectAddress *referenced)
{
	if (tempcat_owns_oid(referenced->classId, referenced->objectId) &&
		!tempcat_owns_oid(depender->classId, depender->objectId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ordinary objects cannot depend on temporary objects kept in memory"),
				 errdetail("The new object would depend on %s.",
						   getObjectDescription(referenced)),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));

	/*
	 * Nor can anything depend on a temporary object of another session that
	 * keeps them in memory, which went to disk (other sessions can still
	 * find it by OID, e.g. a regclass or regtype constant of a view): that
	 * session could not drop the dependent object, which may be invisible
	 * to it, and the sweep would remove the object from under it.  Such an
	 * object is missing for us like those in memory.
	 */
	if (tempcat_found_object_of_other_session(referenced))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot depend on temporary objects of another session"),
				 errdetail("The new object would depend on %s.",
						   getObjectDescription(referenced))));
}

/*
 * Allocate an OID for a new temporary object in 'relation' from this
 * backend's slice of the reserved range.
 *
 * Like GetNewOidWithIndex(), make sure the OID is not used in the catalog
 * yet, neither on disk (ordinary objects may have OIDs in the range, see the
 * file header) nor in memory: the SnapshotAny scan sees both.
 *
 * The next value is kept in shared memory per PGPROC, so consecutive
 * sessions that reuse the same PGPROC keep moving forward instead of reusing
 * recent OIDs.
 */
Oid
tempcat_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	TempcatProcState *st;
	uint64		span;
	Oid			base;
	int			tries;
	int			maxtries;

	st = tempcat_my_state();
	span = ((uint64) LastTempcatObjectId - FirstTempcatObjectId + 1) /
		TempcatShared->nprocs;
	base = FirstTempcatObjectId + (Oid) (span * MyProc->pgprocno);
	maxtries = (int) Min(span, 1000000);

#ifdef FAULT_INJECTOR
	/* Test hook: ordinary objects take every OID of our slice. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_oid_exhausted") == FaultInjectorTypeSkip)
		maxtries = 0;
#endif

	for (tries = 0; tries < maxtries; tries++)
	{
		Oid			oid = st->nextOid;
		ScanKeyData key;
		SysScanDesc scan;
		bool		collides;

		if (!OidIsValid(oid))
		{
			/*
			 * First use of the slice since the node started: start at a
			 * random point rather than where its users before a restart or
			 * a failover started, whose objects may still live in QE
			 * writers on the segments (see tempcat_check_preassigned_oid()).
			 */
			oid = base + (Oid) (random() % span);
		}
		else if (oid < base || oid >= base + span)
			oid = base;
#ifdef FAULT_INJECTOR
		/* Test hook: go near the end of the slice, to wrap around soon. */
		if (SIMPLE_FAULT_INJECTOR("tempcat_slice_near_end") == FaultInjectorTypeSkip)
			oid = base + (Oid) span - 3;
#endif
		st->nextOid = oid + 1;

		ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(oid));
		scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
		collides = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);

		if (!collides)
		{
			tempcat_activate();

			/*
			 * Enum labels are no objects of their own: their rows go with
			 * their type (see tempcat_catalogs[]), and nothing looks for
			 * their OIDs among ours; remembering them, which nothing would
			 * forget, would only grow the set and our bounds.
			 */
			if (RelationGetRelid(relation) != EnumRelationId)
				tempcat_remember_oid(RelationGetRelid(relation), oid);
			return oid;
		}
	}

	/*
	 * Ordinary objects with reserved OIDs (see the file header) take every
	 * OID of our slice.  If nothing is in memory yet, i.e. this is the
	 * temporary schema, the session can keep its temporary objects on disk
	 * instead; the caller then assigns an ordinary OID.  Later, objects with
	 * ordinary OIDs in a schema kept in memory would be visible to nobody
	 * but the session, so give up.
	 */
	if (!tc_activated && RelationGetRelid(relation) == NamespaceRelationId)
	{
		tc_slice_exhausted = true;
		ereport(WARNING,
				(errmsg("no free OID for temporary objects kept in memory"),
				 errdetail("Ordinary objects use all OIDs that this session can take from the reserved range."),
				 errhint("The session keeps its temporary objects in the on-disk catalog.")));
		return InvalidOid;
	}

	ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("out of OIDs for temporary objects in memory catalog"),
			 errdetail("Ordinary objects use all OIDs that this session can take from the reserved range.")));
	return InvalidOid;			/* keep compiler quiet */
}

/*
 * A QE received a reserved OID for a temporary object of ours.  The QD
 * checked that no object of its catalog has it, but an ordinary object can
 * exist on this node alone (created in utility mode, or left by a broken
 * catalog), and two rows with one OID would corrupt the catalog: refuse.
 * Only live rows (and rows of transactions in progress) count: dead rows,
 * e.g. removed by the sweep but not vacuumed yet, which the QD may not have
 * (or no longer have), would make the OID unusable here until a VACUUM.
 */
static void
tempcat_check_preassigned_oid(Relation relation, Oid indexId,
							  AttrNumber oidcolumn, Oid oid)
{
	ScanKeyData key;
	SysScanDesc scan;
	SnapshotData snapshot;
	bool		taken;
	int			i;

	InitDirtySnapshot(snapshot);
	ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(oid));
	scan = systable_beginscan(relation, indexId, true, &snapshot, 1, &key);
	taken = HeapTupleIsValid(systable_getnext(scan));
	systable_endscan(scan);

	if (taken)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("OID of a new temporary object is already used in catalog \"%s\" on this node",
						RelationGetRelationName(relation)),
				 errhint("OID %u belongs to an object that exists on this node only; gpcheckcat reports such objects.",
						 oid)));

	/*
	 * Nor may another process of this node publish it within its bounds: a
	 * QE writer of a session whose coordinator is gone may still run, with
	 * its objects in memory, where the scan above does not see them, while
	 * the coordinator, restarted or failed over, assigns the OIDs of its
	 * slices anew.  Otherwise the slices of the coordinator's sessions do
	 * not overlap.
	 */
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		if (i == MyProc->pgprocno || st->databaseId != MyDatabaseId)
			continue;
		if (tempcat_bounds_cover(st, oid))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("OID %u of a new temporary object is in use by another session on this node",
							oid),
					 errhint("A session of a coordinator that restarted or failed over may still run here; retry when it has ended.")));
	}
}

/* The first attribute of these catalogs is their oid column. */
static inline Oid
tempcat_entry_oid(TempcatEntry *e)
{
	HeapTupleHeader hdr = TCE_DATA(e);

	return *(Oid *) ((char *) hdr + hdr->t_hoff);
}

/* Is the entry's catalog one whose own oid column is attribute 1? */
static inline bool
tempcat_catalog_has_oid(int catidx)
{
	switch (tempcat_catalogs[catidx].relid)
	{
		case RelationRelationId:
		case TypeRelationId:
		case NamespaceRelationId:
		case AttrDefaultRelationId:
		case ConstraintRelationId:
		case RewriteRelationId:
		case TriggerRelationId:
		case PolicyRelationId:
		case ProcedureRelationId:
		case OperatorRelationId:
		case StatisticExtRelationId:
			return true;
		default:
			return false;
	}
}

/*
 * A row of one of our objects goes to disk instead of memory (the area is
 * full): the object no longer lives in memory only, so keep owning its OID
 * when its rows in memory go away (tempcat_forget_oid()).
 */
static Oid	tempcat_getoid(HeapTuple tup, TupleDesc desc, AttrNumber attno);
static void tempcat_recompute_roles(void);
static TempcatEntry *tempcat_lookup_tid(Relation rel, ItemPointer tid,
										dsa_pointer *ep);

static void
tempcat_note_disk_row(Relation rel, HeapTuple tup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	TempcatOwnedKey key;
	TempcatOwnedEntry *entry;

	/*
	 * A pg_shdepend row: tempcat_route_insert() published its role, which
	 * other in-memory rows may not refer to.  Disk rows speak for themselves.
	 * Recompute the published roles at the next transaction (once, however
	 * many rows go to disk).
	 */
	if (RelationGetRelid(rel) == SharedDependRelationId)
	{
		tc_roles_dirty = true;
		return;
	}

	if (tc_owned == NULL || catidx < 0 || !tempcat_catalog_has_oid(catidx))
		return;
	key.catalog = RelationGetRelid(rel);
	key.oid = tempcat_getoid(tup, RelationGetDescr(rel), 1);
	entry = hash_search(tc_owned, &key, HASH_FIND, NULL);
	if (entry != NULL)
		entry->on_disk = true;
}

/*
 * Move the rows of our temporary namespaces from memory to disk, in the
 * current transaction, as CatalogTupleUpdate() moves a row that no longer
 * fits.  Unless done already: then the row in memory is one we deleted.
 */
static void
tempcat_namespaces_to_disk(void)
{
	Oid			nsps[2];
	int			i;

	GetTempNamespaceState(&nsps[0], &nsps[1]);
	for (i = 0; i < lengthof(nsps); i++)
	{
		HeapTuple	tup;
		Relation	rel;
		dsa_pointer ep;
		TempcatEntry *e;

		if (!IsTempcatOid(nsps[i]))
			continue;
		tup = SearchSysCacheCopy1(NAMESPACEOID, ObjectIdGetDatum(nsps[i]));
		if (!HeapTupleIsValid(tup))
			continue;
		rel = table_open(NamespaceRelationId, RowExclusiveLock);
		if (IsTempcatTid(&tup->t_self))
		{
			e = tempcat_lookup_tid(rel, &tup->t_self, &ep);
			if (!TransactionIdIsValid(e->xmax) ||
				!TransactionIdIsCurrentTransactionId(e->xmax))
			{
				tempcat_delete(rel, &tup->t_self);
				CatalogTupleInsertOnDisk(rel, tup);
				tempcat_note_disk_row(rel, tup);
				tc_namespaces_to_disk = true;
			}
		}
		table_close(rel, RowExclusiveLock);
		heap_freetuple(tup);
	}
}

/*
 * A row of ours goes to disk because it does not fit in memory.  Other
 * sessions see it there, so the namespace it is in must be on disk too:
 * code that names objects (pg_identify_object(), pg_relation_filepath(),
 * ALTER TABLE ALL IN TABLESPACE, ...) expects to find it.  Being temporary,
 * the namespace still makes its objects missing for them
 * (tempcat_object_missing()).  Move both namespaces at the first row, as
 * which rows refer to which namespace is not worth working out.
 * (pg_shdepend rows name no namespace, and go to disk when the published
 * roles run out, too.)
 */
typedef struct TempcatObject
{
	Oid			classId;
	Oid			objectId;
} TempcatObject;

/* An object to move, in tempcat_materialize_moves() */
typedef struct TempcatSetMember
{
	TempcatObject obj;
	bool		moved;			/* some of its rows were in memory */
} TempcatSetMember;

static TempcatObject tempcat_row_object(int catidx, HeapTuple tup,
										TupleDesc desc);
static void tempcat_schedule_move(Oid classId, Oid objectId);
static bool tempcat_move_pending(Oid classId, Oid objectId);
static bool tempcat_object_in_memory(Oid classId, Oid objectId);
static void tempcat_mark_on_disk(Oid classId, Oid objectId);
static void tempcat_schedule_links(Relation rel, HeapTuple tup);

static void
tempcat_row_to_disk(Relation rel, HeapTuple tup)
{
	Oid			relid = RelationGetRelid(rel);
	int			catidx = tempcat_catalog_index(relid);
	TupleDesc	desc = RelationGetDescr(rel);
	TempcatObject obj;

	tempcat_note_disk_row(rel, tup);
	if (relid == NamespaceRelationId || relid == SharedDependRelationId)
		return;
	tempcat_namespaces_to_disk();

	/* its whole object, see tempcat_materialize_moves() */
	obj = tempcat_row_object(catidx, tup, desc);
	tempcat_mark_on_disk(obj.classId, obj.objectId);
	tempcat_schedule_move(obj.classId, obj.objectId);
	/* and what the row links to (an operator's commutator, ...) */
	tempcat_schedule_links(rel, tup);
	if (relid == DependRelationId)
		tempcat_schedule_move(tempcat_getoid(tup, desc, Anum_pg_depend_refclassid),
							  tempcat_getoid(tup, desc, Anum_pg_depend_refobjid));
}

/* Does the catalog still hold an entry (of any visibility) for 'oid'? */
static bool
tempcat_memory_has_oid(int catidx, Oid oid)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	uint32		hash = DatumGetUInt32(hash_uint32(oid));
	int			k;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		TempcatHash *h = &tc_root->catalogs[catidx].keys[k];
		dsa_pointer np;

		/* the oid column is always attribute 1 */
		if (def->keyattr[k] != 1 || h->nbuckets == 0)
			continue;

		np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);

			if (node->hash == hash &&
				tempcat_entry_oid(TC_ADDR(node->entry)) == oid)
				return true;
			np = node->next;
		}
	}
	return false;
}

/* ----------------------------------------------------------------
 * Whole objects on disk
 *
 * A row that does not fit in memory takes its whole object to disk: the
 * object's rows, its parts (the objects that depend on it internally or
 * automatically: indexes, TOAST tables, row types, defaults, constraints,
 * triggers, rules, policies, partitions), and the session's objects in
 * memory that those refer to (column types, functions of defaults and
 * triggers, ...), as pg_depend tells; and the object's later rows go to
 * disk too.  So the session's rows on disk never refer to its objects in
 * memory, but for dependencies on ordinary objects and owner and ACL rows
 * (see tempcat_route_insert()): what other sessions see on disk is whole,
 * and they can use it as without the in-memory catalog (the relcache,
 * pg_get_constraintdef(), information_schema, ...).  Whether an object is
 * in memory follows from its rows, so a rollback, also of a prepared
 * transaction, needs no undoing.
 * ----------------------------------------------------------------
 */

static TempcatXidStatus tempcat_xid_status(TransactionId xid);
static void tempcat_delete_entry_moved(Relation rel, dsa_pointer ep);
static dsa_pointer tempcat_find_tid(ItemPointer tid);
static uint32 tempcat_key_hash(TempcatKeyType type, Datum value);
static HeapTuple tempcat_entry_copy(TempcatEntry *e, Oid relid);

/* Is the entry's row alive: not inserted or deleted by an aborted transaction? */
static bool
tempcat_entry_alive(TempcatEntry *e)
{
	if (!(e->flags & TCE_XMIN_COMMITTED) &&
		tempcat_xid_status(e->xmin) == TCX_ABORTED)
		return false;
	return !TransactionIdIsValid(e->xmax) ||
		tempcat_xid_status(e->xmax) == TCX_ABORTED;
}

/* The object a row belongs to: its owner, or its own for functions and operators. */
static TempcatObject
tempcat_row_object(int catidx, HeapTuple tup, TupleDesc desc)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	TempcatObject obj;

	if (def->ownerclass == NamespaceRelationId &&
		def->relid != NamespaceRelationId)
	{
		obj.classId = def->relid;
		obj.objectId = tempcat_getoid(tup, desc, 1);
	}
	else if (def->relid == ConstraintRelationId &&
			 !OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)))
	{
		/* domain constraint */
		obj.classId = TypeRelationId;
		obj.objectId = tempcat_getoid(tup, desc, Anum_pg_constraint_contypid);
	}
	else
	{
		obj.classId = def->ownerclassattr != 0 ?
			tempcat_getoid(tup, desc, def->ownerclassattr) : def->ownerclass;
		obj.objectId = tempcat_getoid(tup, desc, def->owner);
	}
	return obj;
}

static TempcatOwnedEntry *
tempcat_owned_entry(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL || !IsTempcatOid(oid))
		return NULL;
	key.catalog = catalog;
	key.oid = oid;
	return hash_search(tc_owned, &key, HASH_FIND, NULL);
}

/*
 * The alive row in memory of the object 'oid' of catalog 'catidx', whose
 * oid column is its first, or NULL.  '*deleted' says whether this
 * transaction deleted one, e.g. moving it to disk.
 */
static TempcatEntry *
tempcat_object_entry(int catidx, Oid oid, bool *deleted)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	uint32		hash = DatumGetUInt32(hash_uint32(oid));
	int			k;

	*deleted = false;
	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		TempcatHash *h = &tc_root->catalogs[catidx].keys[k];
		dsa_pointer np;

		/* the oid column is always attribute 1 */
		if (def->keyattr[k] != 1 || h->nbuckets == 0)
			continue;
		np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);
			TempcatEntry *e = TC_ADDR(node->entry);

			if (node->hash == hash && tempcat_entry_oid(e) == oid)
			{
				if (tempcat_entry_alive(e))
					return e;
				if (TransactionIdIsValid(e->xmax) &&
					TransactionIdIsCurrentTransactionId(e->xmax))
					*deleted = true;
			}
			np = node->next;
		}
		break;
	}
	return NULL;
}

/*
 * Triggers, constraints, defaults, rules, policies and statistics objects
 * have OIDs of their own, but their rows go with those of their relation
 * (or domain), see tempcat_row_object().  Is our object such a part, with
 * its row in memory?  Then set '*owner' to the object its rows go with.
 * They need not depend on it (the RI triggers of a referenced table depend
 * on their constraint only, which is the referencing table's).
 */
static bool
tempcat_part_owner(Oid classId, Oid objectId, TempcatObject *owner)
{
	int			catidx = tempcat_catalog_index(classId);
	const TempcatCatalogDef *def;
	TempcatEntry *e;
	HeapTupleData tup;
	Relation	rel;
	bool		deleted;

	if (catidx < 0 || !tempcat_catalog_has_oid(catidx) || tc_area == NULL)
		return false;
	def = &tempcat_catalogs[catidx];
	if (def->owner == 1 || def->ownerclass == NamespaceRelationId)
		return false;			/* its rows are its own */
	e = tempcat_object_entry(catidx, objectId, &deleted);
	if (e == NULL)
		return false;

	tup.t_len = e->len;
	tup.t_data = TCE_DATA(e);
	tup.t_tableOid = classId;
	ItemPointerSetInvalid(&tup.t_self);
	rel = table_open(classId, AccessShareLock);
	*owner = tempcat_row_object(catidx, &tup, RelationGetDescr(rel));
	table_close(rel, AccessShareLock);
	return true;
}

/*
 * Does our object live in memory?  Yes if its own row (pg_class, pg_type,
 * pg_proc, ...) is in memory and alive, and for a part (see above) if its
 * relation does; no if this transaction deleted the row, e.g. moving it to
 * disk; otherwise, if its own row is not in memory at all, unless rows of it
 * went to disk (a new object has none yet).  Objects not ours count as in
 * memory: nothing of theirs is routed.
 */
static bool
tempcat_object_in_memory(Oid classId, Oid objectId)
{
	TempcatOwnedEntry *owned = tempcat_owned_entry(classId, objectId);
	TempcatObject owner;
	int			catidx;
	bool		deleted = false;

	if (owned == NULL)
		return true;
	if (tempcat_move_pending(classId, objectId))
		return false;
	/* (no area yet, as at the first row, or none to be had: see below) */
	if (tc_area == NULL)
		return !owned->on_disk;

	if (tempcat_part_owner(classId, objectId, &owner))
		return tempcat_object_in_memory(owner.classId, owner.objectId);

	catidx = tempcat_catalog_index(classId);
	if (catidx >= 0 && tempcat_catalog_has_oid(catidx) &&
		tempcat_object_entry(catidx, objectId, &deleted) != NULL)
		return true;
	return !deleted && !owned->on_disk;
}

/*
 * Links between our objects that pg_depend does not record: an operator's
 * commutator and negator; and that of a pg_depend row itself, which routing
 * follows when the row is inserted (tempcat_route_insert()), but not when it
 * is changed on disk (changeDependencyFor(), e.g. ALTER FUNCTION ...
 * SUPPORT).  Report those of the row 'tup' of catalog 'relid' that lead to
 * objects of ours in memory, at most two.
 */
static int
tempcat_row_links(Oid relid, HeapTuple tup, TempcatObject *links)
{
	int			n = 0;

	if (relid == OperatorRelationId)
	{
		Form_pg_operator op = (Form_pg_operator) GETSTRUCT(tup);
		Oid			oids[2];
		int			i;

		oids[0] = op->oprcom;
		oids[1] = op->oprnegate;
		for (i = 0; i < lengthof(oids); i++)
		{
			if (tempcat_owned_entry(OperatorRelationId, oids[i]) != NULL &&
				tempcat_object_in_memory(OperatorRelationId, oids[i]))
			{
				links[n].classId = OperatorRelationId;
				links[n].objectId = oids[i];
				n++;
			}
		}
	}
	else if (relid == DependRelationId)
	{
		Form_pg_depend dep = (Form_pg_depend) GETSTRUCT(tup);

		if (tempcat_owned_entry(dep->refclassid, dep->refobjid) != NULL &&
			tempcat_object_in_memory(dep->refclassid, dep->refobjid))
		{
			links[n].classId = dep->refclassid;
			links[n].objectId = dep->refobjid;
			n++;
		}
	}
	return n;
}

/*
 * Our row 'tup' of 'rel' goes to disk, its object living there: take the
 * objects of ours that it links to there too (see above), as pg_depend
 * rows would.
 */
static void
tempcat_schedule_links(Relation rel, HeapTuple tup)
{
	TempcatObject links[2];
	int			n;
	int			i;

	if (tc_owned == NULL)
		return;
	n = tempcat_row_links(RelationGetRelid(rel), tup, links);
	for (i = 0; i < n; i++)
		tempcat_schedule_move(links[i].classId, links[i].objectId);
}

/* A row on disk is updated: perhaps one of ours, now linking to others. */
void
tempcat_disk_row_updated(Relation rel, HeapTuple tup)
{
	TempcatObject obj;
	int			catidx;

	if (tc_owned == NULL)
		return;
	catidx = tempcat_catalog_index(RelationGetRelid(rel));
	if (catidx < 0)
		return;
	obj = tempcat_row_object(catidx, tup, RelationGetDescr(rel));
	if (tempcat_owned_entry(obj.classId, obj.objectId) != NULL)
		tempcat_schedule_links(rel, tup);
}

/*
 * Objects to move to disk at the end of the command (or before commit or
 * PREPARE), see tempcat_materialize_moves(), and those moved so far in the
 * transaction, with the subtransaction level that moved them.  A move that
 * a subtransaction carried out is to be done again if it rolls back, even
 * if asked for outside: the reason for it, a row of the object that went to
 * disk, may well outlive the rollback.  So may a row that went to disk in
 * the subtransaction (e.g. a frozen one), so a move asked for there stays
 * pending too; if the object is gone with the rollback, there is nothing to
 * move.
 */
typedef struct TempcatPendingMove
{
	TempcatObject obj;
	int			done;			/* nesting level that moved it, or 0 */
} TempcatPendingMove;

static HTAB *tc_pending_moves = NULL;	/* in TopTransactionContext */
static LocalTransactionId tc_pending_moves_lxid = InvalidLocalTransactionId;

static bool
tempcat_move_pending(Oid classId, Oid objectId)
{
	TempcatPendingMove *entry;
	TempcatObject obj;

	if (tc_pending_moves == NULL || tc_pending_moves_lxid != MyProc->lxid)
		return false;
	obj.classId = classId;
	obj.objectId = objectId;
	entry = hash_search(tc_pending_moves, &obj, HASH_FIND, NULL);
	return entry != NULL && entry->done == 0;
}

/*
 * From now on, our object lives on disk: its new rows and new versions go
 * there, and its other rows follow at the end of the command, with its
 * parts and what it refers to (tempcat_materialize_moves()).  Not right
 * away: the command may still be making the object, e.g. an index whose
 * pg_index row is to come, and the rows it refers to by TID would move
 * under it.
 */
static void
tempcat_schedule_move(Oid classId, Oid objectId)
{
	TempcatPendingMove *entry;
	TempcatObject obj;
	bool		found;

	if (tempcat_owned_entry(classId, objectId) == NULL)
		return;
	if (tc_pending_moves == NULL || tc_pending_moves_lxid != MyProc->lxid)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatObject);
		ctl.entrysize = sizeof(TempcatPendingMove);
		ctl.hcxt = TopTransactionContext;
		tc_pending_moves = hash_create("tempcat objects to move", 64, &ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		tc_pending_moves_lxid = MyProc->lxid;
	}
	obj.classId = classId;
	obj.objectId = objectId;
	entry = hash_search(tc_pending_moves, &obj, HASH_ENTER, &found);
	/* (once moved, its rows go to disk anyway) */
	if (!found)
		entry->done = 0;
}

static void
tempcat_mark_on_disk(Oid classId, Oid objectId)
{
	TempcatOwnedEntry *owned = tempcat_owned_entry(classId, objectId);

	if (owned != NULL)
		owned->on_disk = true;
}

/*
 * Virtual TIDs of rows moved to disk in the current transaction, and the
 * TIDs of their copies there: callers may still hold a row's virtual TID,
 * found in this command before the move, to update or delete it
 * (tempcat_moved_tid()).  And the other way round, for in-place updates of
 * the copies (tempcat_inplace_update_moved()).
 */
typedef struct TempcatMovedKey
{
	Oid			relid;
	ItemPointerData tid;
} TempcatMovedKey;

typedef struct TempcatMovedEntry
{
	TempcatMovedKey key;
	ItemPointerData other;		/* the copy on disk, or the virtual row */
	int			level;			/* subtransaction nesting level of the move */
} TempcatMovedEntry;

/* in TopTransactionContext */
static HTAB *tc_moved = NULL;	/* by virtual TID */
static HTAB *tc_moved_back = NULL;	/* by the TID of the copy */
static LocalTransactionId tc_moved_lxid = InvalidLocalTransactionId;

/*
 * The deepest subtransaction level that moves were carried out at, or rows
 * noted as moved, in the transaction 'tc_moves_top_lxid': subtransactions
 * ending above it need not look at either (tempcat_moved_end_subxact()),
 * which spares a long run of them, e.g. PL/pgSQL exception blocks, a walk
 * over everything moved so far each.
 */
static int	tc_moves_top = 0;
static LocalTransactionId tc_moves_top_lxid = InvalidLocalTransactionId;

static void
tempcat_note_moves_level(int level)
{
	if (tc_moves_top_lxid != MyProc->lxid)
	{
		tc_moves_top_lxid = MyProc->lxid;
		tc_moves_top = 0;
	}
	if (level > tc_moves_top)
		tc_moves_top = level;
}

static void
tempcat_moved_enter(HTAB *map, Oid relid, ItemPointer tid, ItemPointer other)
{
	TempcatMovedKey key;
	TempcatMovedEntry *entry;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	key.tid = *tid;
	entry = hash_search(map, &key, HASH_ENTER, NULL);
	entry->other = *other;
	entry->level = GetCurrentTransactionNestLevel();
	tempcat_note_moves_level(entry->level);
}

static void
tempcat_note_moved(Oid relid, ItemPointer vtid, ItemPointer dtid)
{
	if (tc_moved == NULL || tc_moved_lxid != MyProc->lxid)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatMovedKey);
		ctl.entrysize = sizeof(TempcatMovedEntry);
		ctl.hcxt = TopTransactionContext;
		tc_moved = hash_create("tempcat moved rows", 64, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		tc_moved_back = hash_create("tempcat moved rows by copy", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		tc_moved_lxid = MyProc->lxid;
	}
	tempcat_moved_enter(tc_moved, relid, vtid, dtid);
	tempcat_moved_enter(tc_moved_back, relid, dtid, vtid);
}

static bool
tempcat_moved_find(HTAB *map, Relation rel, ItemPointer tid, ItemPointer other)
{
	TempcatMovedKey key;
	TempcatMovedEntry *entry;

	if (tc_moved == NULL || tc_moved_lxid != MyProc->lxid)
		return false;
	memset(&key, 0, sizeof(key));
	key.relid = RelationGetRelid(rel);
	key.tid = *tid;
	entry = hash_search(map, &key, HASH_FIND, NULL);
	if (entry == NULL)
		return false;
	*other = entry->other;
	return true;
}

/*
 * Was the virtual row 'tid' of 'rel' moved to disk in this transaction?
 * If so, set '*dtid' to its copy there, for the caller to update or delete
 * that instead.
 */
bool
tempcat_moved_tid(Relation rel, ItemPointer tid, ItemPointer dtid)
{
	return IsTempcatTid(tid) && tempcat_moved_find(tc_moved, rel, tid, dtid);
}

/*
 * An in-place update overwrote 'tup' on disk.  If that is the copy of a row
 * we moved there in this transaction, overwrite the row in memory too: it
 * comes back if the move rolls back, and in-place updates must outlive that,
 * as they outlive aborts (the AO row numbers in gp_fastsequence, which must
 * never be handed out again, relpages, ...).  Unless the row is gone, freed
 * as it was inserted in this transaction too, which then takes it back
 * anyway.
 */
void
tempcat_inplace_update_moved(Relation rel, HeapTuple tup)
{
	HeapTupleData vtup;

	if (IsTempcatTid(&tup->t_self) ||
		!tempcat_moved_find(tc_moved_back, rel, &tup->t_self, &vtup.t_self) ||
		!tempcat_attach(false) ||
		!DsaPointerIsValid(tempcat_find_tid(&vtup.t_self)))
		return;
	vtup.t_len = tup->t_len;
	vtup.t_data = tup->t_data;
	vtup.t_tableOid = tup->t_tableOid;
	tempcat_inplace_update(rel, &vtup);
}

static void
tempcat_moved_end_level(HTAB *map, int level, bool commit)
{
	HASH_SEQ_STATUS status;
	TempcatMovedEntry *entry;

	hash_seq_init(&status, map);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->level < level)
			continue;
		if (commit)
			entry->level = level - 1;
		else
			(void) hash_search(map, &entry->key, HASH_REMOVE, NULL);
	}
}

/*
 * Forget the moves of an aborted subtransaction, making them pending again,
 * or hand them to its parent.
 */
static void
tempcat_moved_end_subxact(bool commit)
{
	int			level = GetCurrentTransactionNestLevel();
	HASH_SEQ_STATUS status;
	TempcatPendingMove *pending;

	/* (nothing done at this level or below; see tc_moves_top) */
	if (tc_moves_top_lxid != MyProc->lxid || tc_moves_top < level)
		return;
	/* (and after it, nothing is deeper than the parent) */
	tc_moves_top = level - 1;

	if (tc_pending_moves != NULL && tc_pending_moves_lxid == MyProc->lxid)
	{
		hash_seq_init(&status, tc_pending_moves);
		while ((pending = hash_seq_search(&status)) != NULL)
		{
			if (pending->done < level)
				continue;
			pending->done = commit ? level - 1 : 0;
		}
	}

	if (tc_moved == NULL || tc_moved_lxid != MyProc->lxid)
		return;
	tempcat_moved_end_level(tc_moved, level, commit);
	tempcat_moved_end_level(tc_moved_back, level, commit);
}

/* Move one virtual row to disk, in the current transaction. */
static void
tempcat_delete_entry_moved(Relation rel, dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);
	HeapTuple	tup = tempcat_entry_copy(e, RelationGetRelid(rel));
	ItemPointerData vtid = e->tid;

	tempcat_delete(rel, &vtid);
	CatalogTupleInsertOnDisk(rel, tup);
	tempcat_note_moved(RelationGetRelid(rel), &vtid, &tup->t_self);
	tempcat_note_disk_row(rel, tup);
	heap_freetuple(tup);
}

/*
 * Alive entries of catalog 'catidx' whose key 'k' (an OID column) may hold
 * 'oid': those in its hash chain, which the caller checks.
 */
static List *
tempcat_key_entries(int catidx, int k, Oid oid)
{
	TempcatHash *h = &tc_root->catalogs[catidx].keys[k];
	List	   *entries = NIL;
	uint32		hash;
	dsa_pointer np;

	if (tempcat_catalogs[catidx].keytype[k] != TCK_OID || h->nbuckets == 0)
		return NIL;
	hash = tempcat_key_hash(TCK_OID, ObjectIdGetDatum(oid));
	np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
	while (DsaPointerIsValid(np))
	{
		TempcatHashNode *node = TC_ADDR(np);

		if (node->hash == hash && tempcat_entry_alive(TC_ADDR(node->entry)))
			entries = lappend(entries, (void *) node->entry);
		np = node->next;
	}
	return entries;
}

/*
 * The rows in memory of our object 'obj' in catalog 'catidx': found through
 * the catalog's first key, which is the column tempcat_row_object() takes
 * the object from (the owner, or the row's own OID), but for domain
 * constraints, whose conrelid is 0.
 */
static List *
tempcat_object_rows(int catidx, Relation rel, TempcatObject obj)
{
	List	   *rows = NIL;
	int			pass;

	for (pass = 0; pass < 2; pass++)
	{
		Oid			key = obj.objectId;
		List	   *entries;
		ListCell   *lc;

		if (pass == 1)
		{
			if (tempcat_catalogs[catidx].relid != ConstraintRelationId ||
				obj.classId != TypeRelationId)
				break;
			key = InvalidOid;
		}
		entries = tempcat_key_entries(catidx, 0, key);
		foreach(lc, entries)
		{
			dsa_pointer ep = (dsa_pointer) lfirst(lc);
			TempcatEntry *e = TC_ADDR(ep);
			HeapTupleData tup;
			TempcatObject owner;

			tup.t_len = e->len;
			tup.t_data = TCE_DATA(e);
			tup.t_tableOid = tempcat_catalogs[catidx].relid;
			ItemPointerSetInvalid(&tup.t_self);
			owner = tempcat_row_object(catidx, &tup, RelationGetDescr(rel));
			if (owner.classId == obj.classId && owner.objectId == obj.objectId)
				rows = lappend(rows, (void *) ep);
		}
		list_free(entries);
	}
	return rows;
}

/* The catalogs of tempcat_materialize_moves(), opened as needed. */
static Relation
tempcat_moves_rel(Relation *rels, int catidx)
{
	if (rels[catidx] == NULL)
		rels[catidx] = table_open(tempcat_catalogs[catidx].relid,
								  RowExclusiveLock);
	return rels[catidx];
}

/* Add 'obj' to the objects to move, and to those to look at, if new. */
static void
tempcat_add_to_move(HTAB *set, List **todo, TempcatObject obj)
{
	TempcatSetMember *member;
	bool		found;

	member = hash_search(set, &obj, HASH_ENTER, &found);
	if (!found)
	{
		member->moved = false;
		*todo = lappend(*todo, &member->obj);
	}
}

/*
 * Add to 'set' what moves with our object 'm' (see above): the objects of
 * ours in memory it refers to, those depending on it internally or
 * automatically, its parts, and the operators it links to.
 */
static void
tempcat_add_closure(HTAB *set, List **todo, TempcatObject m, Relation *rels)
{
	int			dcat = tempcat_catalog_index(DependRelationId);
	List	   *entries;
	ListCell   *lc;
	int			catidx;

	/* pg_depend rows of it, by objid, and of its dependents, by refobjid */
	entries = tempcat_key_entries(dcat, 0, m.objectId);
	foreach(lc, entries)
	{
		TempcatEntry *e = TC_ADDR((dsa_pointer) lfirst(lc));
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_depend form = (Form_pg_depend) ((char *) hdr + hdr->t_hoff);
		TempcatObject ref;

		if (form->classid != m.classId || form->objid != m.objectId)
			continue;
		ref.classId = form->refclassid;
		ref.objectId = form->refobjid;
		if (hash_search(set, &ref, HASH_FIND, NULL) == NULL &&
			tempcat_owned_entry(ref.classId, ref.objectId) != NULL &&
			tempcat_object_in_memory(ref.classId, ref.objectId))
			tempcat_add_to_move(set, todo, ref);
	}
	list_free(entries);

	entries = tempcat_key_entries(dcat, 1, m.objectId);
	foreach(lc, entries)
	{
		TempcatEntry *e = TC_ADDR((dsa_pointer) lfirst(lc));
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_depend form = (Form_pg_depend) ((char *) hdr + hdr->t_hoff);
		TempcatObject dep;

		if (form->refclassid != m.classId || form->refobjid != m.objectId ||
			(form->deptype != DEPENDENCY_INTERNAL &&
			 form->deptype != DEPENDENCY_AUTO))
			continue;
		dep.classId = form->classid;
		dep.objectId = form->objid;
		if (tempcat_owned_entry(dep.classId, dep.objectId) != NULL)
			tempcat_add_to_move(set, todo, dep);
	}
	list_free(entries);

	/*
	 * Its parts, whose rows go with it (tempcat_part_owner()), though they
	 * need not depend on it.
	 */
	for (catidx = 0; catidx < TEMPCAT_NCATALOGS; catidx++)
	{
		const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
		List	   *rows;

		if (!tempcat_catalog_has_oid(catidx) || def->owner == 1 ||
			def->ownerclass == NamespaceRelationId)
			continue;
		rows = tempcat_object_rows(catidx, tempcat_moves_rel(rels, catidx), m);
		foreach(lc, rows)
		{
			TempcatObject part;

			part.classId = def->relid;
			part.objectId = tempcat_entry_oid(TC_ADDR((dsa_pointer) lfirst(lc)));
			tempcat_add_to_move(set, todo, part);
		}
		list_free(rows);
	}

	/* and the links pg_depend does not record */
	if (m.classId == OperatorRelationId)
	{
		TempcatEntry *e;
		bool		deleted;

		e = tempcat_object_entry(tempcat_catalog_index(OperatorRelationId),
								 m.objectId, &deleted);
		if (e != NULL)
		{
			HeapTupleData tup;
			TempcatObject links[2];
			int			n;
			int			i;

			tup.t_len = e->len;
			tup.t_data = TCE_DATA(e);
			n = tempcat_row_links(OperatorRelationId, &tup, links);
			for (i = 0; i < n; i++)
				tempcat_add_to_move(set, todo, links[i]);
		}
	}
}

/*
 * Move the objects whose moves are pending to disk, with their parts and
 * the objects in memory that they refer to (see above), in the current
 * command.  Called at the end of a command, before CommandCounterIncrement()
 * makes the copies visible, and before commit and PREPARE.  Returns whether
 * rows moved.  The cost follows the objects that move, not all the rows in
 * memory: their rows are found through the hash tables.
 */
bool
tempcat_materialize_moves(void)
{
	HTAB	   *set;
	HASHCTL		ctl;
	HASH_SEQ_STATUS status;
	TempcatPendingMove *pending;
	TempcatSetMember *member;
	Relation	rels[TEMPCAT_NCATALOGS];
	List	   *todo = NIL;
	List	   *rows[TEMPCAT_NCATALOGS];
	int			catidx;
	int			level = GetCurrentTransactionNestLevel();
	bool		moved = false;

	if (tc_pending_moves == NULL || tc_pending_moves_lxid != MyProc->lxid)
		return false;

	/*
	 * Not outside a (sub)transaction in progress, as in the
	 * CommandCounterIncrement() of a ROLLBACK TO or COMMIT in an aborted
	 * one, of a SAVEPOINT whose subtransaction has not started yet, or of a
	 * subtransaction's commit: the moves stay pending, for the next command
	 * or the commit.  (Moves that a rolled-back subtransaction carried out
	 * are pending again then.)
	 */
	if (!IsTransactionState())
		return false;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TempcatObject);
	ctl.entrysize = sizeof(TempcatSetMember);
	ctl.hcxt = CurrentMemoryContext;
	set = NULL;
	hash_seq_init(&status, tc_pending_moves);
	while ((pending = hash_seq_search(&status)) != NULL)
	{
		if (pending->done != 0)
			continue;
		if (set == NULL)
			set = hash_create("tempcat objects to move", 64, &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		tempcat_add_to_move(set, &todo, pending->obj);
		pending->done = level;
		tempcat_note_moves_level(level);
	}
	if (set == NULL)
		return false;

	tempcat_namespaces_to_disk();
	if (tc_area == NULL)
	{
		hash_seq_init(&status, set);
		while ((member = hash_seq_search(&status)) != NULL)
			tempcat_mark_on_disk(member->obj.classId, member->obj.objectId);
		hash_destroy(set);
		list_free(todo);
		return false;
	}

	memset(rels, 0, sizeof(rels));
	memset(rows, 0, sizeof(rows));

	/* the closure */
	while (todo != NIL)
	{
		TempcatObject m = *(TempcatObject *) linitial(todo);

		todo = list_delete_first(todo);
		tempcat_add_closure(set, &todo, m, rels);
	}

	/* the rows of these objects */
	hash_seq_init(&status, set);
	while ((member = hash_seq_search(&status)) != NULL)
	{
		for (catidx = 0; catidx < TEMPCAT_NCATALOGS; catidx++)
		{
			List	   *found;

			found = tempcat_object_rows(catidx, tempcat_moves_rel(rels, catidx),
										member->obj);
			if (found != NIL)
				member->moved = true;
			rows[catidx] = list_concat(rows[catidx], found);
		}
	}
	for (catidx = 0; catidx < TEMPCAT_NCATALOGS; catidx++)
	{
		ListCell   *lc;

		foreach(lc, rows[catidx])
			tempcat_delete_entry_moved(rels[catidx], (dsa_pointer) lfirst(lc));
		list_free(rows[catidx]);
		if (rels[catidx] != NULL)
			table_close(rels[catidx], RowExclusiveLock);
	}

	/* (not those gone, e.g. with a subtransaction that asked for them) */
	hash_seq_init(&status, set);
	while ((member = hash_seq_search(&status)) != NULL)
	{
		if (member->moved)
		{
			tempcat_mark_on_disk(member->obj.classId, member->obj.objectId);
			moved = true;
		}
	}

	hash_destroy(set);
	return moved;
}

/* ----------------------------------------------------------------
 * Shared hash tables
 * ----------------------------------------------------------------
 */

/*
 * Double the number of buckets.  Failing to allocate the new array is not an
 * error: chains just get longer.
 */
static void
tempcat_hash_grow(TempcatHash *h)
{
	uint32		newn = h->nbuckets * 2;
	dsa_pointer newb;
	dsa_pointer *oldarr;
	dsa_pointer *newarr;
	uint32		i;

	newb = dsa_allocate_extended(tc_area, sizeof(dsa_pointer) * newn,
								 DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(newb))
		return;

	oldarr = TC_ADDR(h->buckets);
	newarr = TC_ADDR(newb);
	for (i = 0; i < h->nbuckets; i++)
	{
		dsa_pointer np = oldarr[i];

		while (DsaPointerIsValid(np))
		{
			TempcatHashNode *node = TC_ADDR(np);
			dsa_pointer next = node->next;
			uint32		idx = node->hash & (newn - 1);

			node->next = newarr[idx];
			newarr[idx] = np;
			np = next;
		}
	}

	dsa_free(tc_area, h->buckets);
	h->buckets = newb;
	h->nbuckets = newn;
}

/* Make sure the table has buckets.  Returns false if out of memory. */
static bool
tempcat_hash_prepare(TempcatHash *h)
{
	if (h->nbuckets == 0)
	{
		h->buckets = dsa_allocate_extended(tc_area, sizeof(dsa_pointer) * 64,
										   DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
		if (!DsaPointerIsValid(h->buckets))
			return false;
		h->nbuckets = 64;
	}
	else if (h->nentries >= h->nbuckets * 2)
		tempcat_hash_grow(h);
	return true;
}

/* Link a preallocated node.  The table must have been prepared. */
static void
tempcat_hash_link(TempcatHash *h, dsa_pointer np, uint32 hash, dsa_pointer entry)
{
	TempcatHashNode *node = TC_ADDR(np);
	dsa_pointer *arr = TC_ADDR(h->buckets);
	uint32		idx = hash & (h->nbuckets - 1);

	node->hash = hash;
	node->entry = entry;
	node->next = arr[idx];
	arr[idx] = np;
	h->nentries++;
}

static void
tempcat_hash_remove(TempcatHash *h, uint32 hash, dsa_pointer entry)
{
	dsa_pointer *link;

	Assert(h->nbuckets > 0);
	link = &((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
	while (DsaPointerIsValid(*link))
	{
		TempcatHashNode *node = TC_ADDR(*link);

		if (node->entry == entry)
		{
			dsa_pointer np = *link;

			*link = node->next;
			dsa_free(tc_area, np);
			h->nentries--;
			return;
		}
		link = &node->next;
	}
	elog(ERROR, "tempcat: hash entry not found");
}

static inline uint32
tempcat_tid_hash(ItemPointer tid)
{
	return DatumGetUInt32(hash_any((const unsigned char *) tid,
								   sizeof(ItemPointerData)));
}

static uint32
tempcat_key_hash(TempcatKeyType type, Datum value)
{
	if (type == TCK_OID)
		return DatumGetUInt32(hash_uint32(DatumGetObjectId(value)));
	else
	{
		/* works for both a Name and a C string argument */
		const char *s = DatumGetCString(value);

		return DatumGetUInt32(hash_any((const unsigned char *) s,
									   strnlen(s, NAMEDATALEN)));
	}
}

/* ----------------------------------------------------------------
 * Entries
 * ----------------------------------------------------------------
 */

static dsa_pointer
tempcat_find_tid(ItemPointer tid)
{
	uint32		hash = tempcat_tid_hash(tid);
	TempcatHash *h = &tc_root->tids;
	dsa_pointer np;

	if (h->nbuckets == 0)
		return InvalidDsaPointer;

	np = ((dsa_pointer *) TC_ADDR(h->buckets))[hash & (h->nbuckets - 1)];
	while (DsaPointerIsValid(np))
	{
		TempcatHashNode *node = TC_ADDR(np);

		if (node->hash == hash &&
			ItemPointerEquals(&((TempcatEntry *) TC_ADDR(node->entry))->tid, tid))
			return node->entry;
		np = node->next;
	}
	return InvalidDsaPointer;
}

static ItemPointerData
tempcat_new_tid(void)
{
	ItemPointerData tid;
	uint64		n = tc_root->next_tid++;

	ItemPointerSetBlockNumber(&tid, (BlockNumber) (n / 0x4000));
	tid.ip_posid = TEMPCAT_TID_BIT | (OffsetNumber) ((n % 0x4000) + 1);
	return tid;
}

/*
 * Create an entry for a tuple and link it into the catalog's list and hash
 * tables.  Returns InvalidDsaPointer if the area is out of memory, in which
 * case nothing has changed.  Caller holds the exclusive lock.
 */
static dsa_pointer
tempcat_entry_create(int catidx, HeapTuple tup, TupleDesc desc,
					 TransactionId xmin, CommandId cmin)
{
	const TempcatCatalogDef *def = &tempcat_catalogs[catidx];
	TempcatCatalog *cat = &tc_root->catalogs[catidx];
	dsa_pointer ep;
	dsa_pointer nodes[TEMPCAT_NKEYS + 1];
	TempcatEntry *e;
	HeapTupleHeader hdr;
	int			nnodes = 0;
	int			k;

	/* Allocate everything up front, so that running out changes nothing. */
	ep = dsa_allocate_extended(tc_area,
							   MAXALIGN(sizeof(TempcatEntry)) + tup->t_len,
							   DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(ep))
		return InvalidDsaPointer;
	for (k = 0; k <= TEMPCAT_NKEYS; k++)
	{
		if (k < TEMPCAT_NKEYS && def->keytype[k] == TCK_NONE)
			continue;
		if ((k < TEMPCAT_NKEYS && !tempcat_hash_prepare(&cat->keys[k])) ||
			(k == TEMPCAT_NKEYS && !tempcat_hash_prepare(&tc_root->tids)))
			goto oom;
		nodes[nnodes] = dsa_allocate_extended(tc_area, sizeof(TempcatHashNode),
											  DSA_ALLOC_NO_OOM);
#ifdef FAULT_INJECTOR
		/* Test hook: the area runs full in the middle of adding a row. */
		if (DsaPointerIsValid(nodes[nnodes]) &&
			SIMPLE_FAULT_INJECTOR("tempcat_hash_node_alloc") == FaultInjectorTypeSkip)
		{
			dsa_free(tc_area, nodes[nnodes]);
			nodes[nnodes] = InvalidDsaPointer;
		}
#endif
		if (!DsaPointerIsValid(nodes[nnodes]))
			goto oom;
		nnodes++;
	}

	e = TC_ADDR(ep);
	memset(e, 0, sizeof(TempcatEntry));
	e->xmin = xmin;
	e->xmax = InvalidTransactionId;
	e->cmin = cmin;
	e->cmax = InvalidCommandId;
	e->catidx = catidx;
	e->len = tup->t_len;
	e->tid = tempcat_new_tid();
	e->tidhash = tempcat_tid_hash(&e->tid);

	hdr = TCE_DATA(e);
	memcpy(hdr, tup->t_data, tup->t_len);
	hdr->t_infomask &= ~HEAP_XACT_MASK;
	hdr->t_ctid = e->tid;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		bool		isnull;
		Datum		value;

		if (def->keytype[k] == TCK_NONE)
			continue;
		value = heap_getattr(tup, def->keyattr[k], desc, &isnull);
		e->keyhash[k] = isnull ? 0 : tempcat_key_hash(def->keytype[k], value);
	}

	/* link into the catalog's list */
	e->prev = InvalidDsaPointer;
	e->next = cat->head;
	if (DsaPointerIsValid(cat->head))
		((TempcatEntry *) TC_ADDR(cat->head))->prev = ep;
	cat->head = ep;
	cat->nentries++;

	nnodes = 0;
	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] != TCK_NONE)
			tempcat_hash_link(&cat->keys[k], nodes[nnodes++], e->keyhash[k], ep);
	}
	tempcat_hash_link(&tc_root->tids, nodes[nnodes], e->tidhash, ep);

	return ep;

oom:
	while (nnodes > 0)
		dsa_free(tc_area, nodes[--nnodes]);
	dsa_free(tc_area, ep);
	return InvalidDsaPointer;
}

/* Caller holds the exclusive lock. */
static void
tempcat_entry_remove(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);
	const TempcatCatalogDef *def = &tempcat_catalogs[e->catidx];
	TempcatCatalog *cat = &tc_root->catalogs[e->catidx];
	int			k;

	if (def->relid == SharedDependRelationId)
		tc_roles_dirty = true;

	if (DsaPointerIsValid(e->prev))
		((TempcatEntry *) TC_ADDR(e->prev))->next = e->next;
	else
		cat->head = e->next;
	if (DsaPointerIsValid(e->next))
		((TempcatEntry *) TC_ADDR(e->next))->prev = e->prev;
	cat->nentries--;

	for (k = 0; k < TEMPCAT_NKEYS; k++)
	{
		if (def->keytype[k] != TCK_NONE)
			tempcat_hash_remove(&cat->keys[k], e->keyhash[k], ep);
	}
	tempcat_hash_remove(&tc_root->tids, e->tidhash, ep);

	dsa_free(tc_area, ep);
}

/* Build a palloc'd HeapTuple from an entry, with its header filled in. */
static HeapTuple
tempcat_entry_copy(TempcatEntry *e, Oid relid)
{
	HeapTuple	tup = (HeapTuple) palloc(HEAPTUPLESIZE + e->len);
	HeapTupleHeader hdr;

	tup->t_len = e->len;
	tup->t_self = e->tid;
	tup->t_tableOid = relid;
	tup->t_data = hdr = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);
	memcpy(hdr, TCE_DATA(e), e->len);

	hdr->t_infomask &= ~HEAP_XACT_MASK;
	hdr->t_infomask |= HEAP_XMAX_INVALID;
	if (e->flags & TCE_XMIN_COMMITTED)
		hdr->t_infomask |= HEAP_XMIN_COMMITTED;
	HeapTupleHeaderSetXmin(hdr, e->xmin);
	HeapTupleHeaderSetXmax(hdr, InvalidTransactionId);
	HeapTupleHeaderSetCmin(hdr, e->cmin);
	hdr->t_ctid = e->tid;

	return tup;
}

/* Mirror what heap_insert() leaves in the caller's tuple. */
static void
tempcat_set_caller_tuple(HeapTuple tup, Relation rel, TempcatEntry *e)
{
	tup->t_self = e->tid;
	tup->t_tableOid = RelationGetRelid(rel);
	tup->t_data->t_infomask &= ~HEAP_XACT_MASK;
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, e->xmin);
	HeapTupleHeaderSetXmax(tup->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tup->t_data, e->cmin);
	tup->t_data->t_ctid = e->tid;
}

/* The current transaction changed virtual rows; see tempcat_check_prepare(). */
static bool tc_xact_changed = false;

/*
 * The deepest subtransaction level whose changes of virtual rows may be on
 * the pending list; see tempcat_abort_subtransaction().
 */
static int	tc_change_level = 0;


/*
 * Make room for 'n' more pending entries.  Called before creating or
 * changing entries: running out of memory after that would leave an entry
 * that no transaction end looks at (an aborted insertion never freed).
 */
static void
tempcat_reserve_pending(int n)
{
	if (tc_npending + n > tc_maxpending)
	{
		int			newmax = Max(64, Max(tc_maxpending * 2, tc_npending + n));

		if (tc_pending == NULL)
			tc_pending = MemoryContextAlloc(TopMemoryContext,
											sizeof(dsa_pointer) * newmax);
		else
			tc_pending = repalloc(tc_pending, sizeof(dsa_pointer) * newmax);
		tc_maxpending = newmax;
	}
}

/* Caller made room with tempcat_reserve_pending(). */
static void
tempcat_add_pending(dsa_pointer ep)
{
	TempcatEntry *e = TC_ADDR(ep);

	tc_xact_changed = true;
	tc_change_level = Max(tc_change_level, GetCurrentTransactionNestLevel());

	/*
	 * Until the outcome of its transactions is applied to the row, when the
	 * session goes idle or starts its next transaction, its xids are looked
	 * up in the clog: keep vac_update_datfrozenxid(), and so clog
	 * truncation, from passing them.
	 */
	if (!(e->flags & TCE_XMIN_COMMITTED))
		tempcat_cover_xid(e->xmin);
	tempcat_cover_xid(e->xmax);

	if (e->flags & TCE_PENDING)
		return;

	Assert(tc_npending < tc_maxpending);
	tc_pending[tc_npending++] = ep;
	e->flags |= TCE_PENDING;
}

/*
 * Take entry 'i' off the pending list (the last one takes its place).  Do
 * it before anything that can fail, e.g. freeing the row or forgetting its
 * object (which queues a statistics purge): an error must not leave the
 * list holding a row freed already, which the next pass would free again.
 */
static void
tempcat_pending_take(int i)
{
	Assert(i >= 0 && i < tc_npending);
	tc_pending[i] = tc_pending[--tc_npending];
}

/* ----------------------------------------------------------------
 * Visibility
 * ----------------------------------------------------------------
 */

static bool
tempcat_entry_visible(TempcatEntry *e, Snapshot snapshot)
{
	bool		use_cid;

	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_ANY:
			return true;
		case SNAPSHOT_DIRTY:
			snapshot->xmin = snapshot->xmax = InvalidTransactionId;
			snapshot->speculativeToken = 0;
			use_cid = false;
			break;
		case SNAPSHOT_MVCC:
		case SNAPSHOT_HISTORIC_MVCC:
			use_cid = true;
			break;
		default:
			use_cid = false;
			break;
	}

	if (!(e->flags & TCE_XMIN_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(e->xmin))
		{
			if (use_cid && e->cmin >= snapshot->curcid)
				return false;	/* inserted after scan started */
		}
		else if (!TransactionIdDidCommit(e->xmin))
			return false;		/* aborted, or prepared and not finished */
	}

	if (!TransactionIdIsValid(e->xmax))
		return true;

	if (TransactionIdIsCurrentTransactionId(e->xmax))
		return use_cid && e->cmax >= snapshot->curcid;

	return !TransactionIdDidCommit(e->xmax);
}

static TempcatXidStatus
tempcat_xid_status(TransactionId xid)
{
	if (TransactionIdIsCurrentTransactionId(xid) ||
		TransactionIdIsInProgress(xid))
		return TCX_IN_PROGRESS;
	if (TransactionIdDidCommit(xid))
		return TCX_COMMITTED;
	return TCX_ABORTED;
}

/*
 * Apply the outcome of finished transactions to the pending entries: drop
 * aborted inserts and committed deletes, clear aborted deletes.  Entries of
 * a prepared transaction that has not been finished yet stay pending.
 *
 * Called by the writer at the start of each transaction, and when it goes
 * idle (tempcat_idle()), when none of the pending XIDs can be the current
 * one anymore.
 */
/*
 * After removing an entry, stop treating its object's OID as ours if no
 * other version of the object is left in memory.  Keeps the owned set from
 * growing in long sessions.
 */
static void
tempcat_forget_if_gone(int catidx, Oid objoid)
{
#ifdef FAULT_INJECTOR
	/* Test hook: fail after the row was freed; see tempcat_pending_take(). */
	SIMPLE_FAULT_INJECTOR("tempcat_forget_if_gone");
#endif

	if (tempcat_catalogs[catidx].relid == RelationRelationId)
		tc_horizon_dirty = true;

	if (OidIsValid(objoid) && !tempcat_memory_has_oid(catidx, objoid) &&
		tempcat_forget_oid(tempcat_catalogs[catidx].relid, objoid))
	{
		/*
		 * pgstat_vacuum_stat() keeps the statistics of OIDs within our
		 * published bounds, which never shrink: purge those of our dropped
		 * tables and functions ourselves.
		 */
		if (tempcat_catalogs[catidx].relid == RelationRelationId)
			pgstat_purge_relation(objoid);
		else if (tempcat_catalogs[catidx].relid == ProcedureRelationId)
			pgstat_purge_function(objoid);
	}
}

static void
tempcat_resolve_pending(void)
{
	int			i = 0;
	int			npending = tc_npending;

	if (tc_npending == 0)
		return;

	Assert(tc_area != NULL);

#ifdef FAULT_INJECTOR
	/* Test hook: count the walks; see tempcat_abort_subtransaction(). */
	SIMPLE_FAULT_INJECTOR("tempcat_resolve_pending");
#endif

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);

	while (i < tc_npending)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);

		/* (a row freed already would fail this; see tempcat_pending_take()) */
		Assert(e->catidx >= 0 && e->catidx < TEMPCAT_NCATALOGS &&
			   (e->flags & TCE_PENDING));
		int			catidx = e->catidx;
		Oid			objoid = tempcat_catalog_has_oid(catidx) ? tempcat_entry_oid(e) : InvalidOid;
		bool		in_progress = false;

		if (!(e->flags & TCE_XMIN_COMMITTED))
		{
			switch (tempcat_xid_status(e->xmin))
			{
				case TCX_ABORTED:
					tempcat_pending_take(i);
					tempcat_entry_remove(ep);
					tempcat_forget_if_gone(catidx, objoid);
					continue;
				case TCX_COMMITTED:
					e->flags |= TCE_XMIN_COMMITTED;
					break;
				case TCX_IN_PROGRESS:
					in_progress = true;
					break;
			}
		}

		if (TransactionIdIsValid(e->xmax))
		{
			switch (tempcat_xid_status(e->xmax))
			{
				case TCX_COMMITTED:
					tempcat_pending_take(i);
					tempcat_entry_remove(ep);
					tempcat_forget_if_gone(catidx, objoid);
					continue;
				case TCX_ABORTED:
					e->xmax = InvalidTransactionId;
					e->cmax = InvalidCommandId;
					break;
				case TCX_IN_PROGRESS:
					in_progress = true;
					break;
			}
		}

		if (in_progress)
			i++;
		else
		{
			tempcat_pending_take(i);
			e->flags &= ~TCE_PENDING;
		}
	}

	LWLockRelease(&tc_root->lock);

	/* Their xids need no cover any more; see tempcat_add_pending(). */
	if (tc_npending < npending)
		tc_horizon_dirty = true;
}

/*
 * Recompute the published horizon from all our in-memory pg_class rows, of
 * any visibility, after tables were dropped or vacuumed.
 */
static void
tempcat_recompute_horizon(void)
{
	int			catidx = tempcat_catalog_index(RelationRelationId);
	TempcatProcState *st = tempcat_my_state();
	TransactionId minxid = InvalidTransactionId;
	MultiXactId minmxid = InvalidMultiXactId;
	dsa_pointer ep;
	int			i;

	tc_horizon_dirty = false;

	/* the xids of rows still pending; see tempcat_add_pending() */
	for (i = 0; i < tc_npending; i++)
	{
		TempcatEntry *e = TC_ADDR(tc_pending[i]);

		if (!(e->flags & TCE_XMIN_COMMITTED) && TransactionIdIsNormal(e->xmin) &&
			(!TransactionIdIsValid(minxid) || TransactionIdPrecedes(e->xmin, minxid)))
			minxid = e->xmin;
		if (TransactionIdIsNormal(e->xmax) &&
			(!TransactionIdIsValid(minxid) || TransactionIdPrecedes(e->xmax, minxid)))
			minxid = e->xmax;
	}

	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_class form = (Form_pg_class) ((char *) hdr + hdr->t_hoff);

		if (TransactionIdIsNormal(form->relfrozenxid) &&
			(!TransactionIdIsValid(minxid) ||
			 TransactionIdPrecedes(form->relfrozenxid, minxid)))
			minxid = form->relfrozenxid;
		if (MultiXactIdIsValid(form->relminmxid) &&
			(!MultiXactIdIsValid(minmxid) ||
			 MultiXactIdPrecedes(form->relminmxid, minmxid)))
			minmxid = form->relminmxid;
		ep = e->next;
	}

	pg_atomic_write_u32(&st->frozenXid, minxid);
	pg_atomic_write_u32(&st->minMulti, minmxid);
}

/*
 * Command IDs of the snapshots a QE writer handed to cursors in the current
 * transaction (dumpSharedLocalSnapshot_forCursor()).  The cursors' reader
 * gangs scan with them in other processes, where SnapshotsSeeCommandRange()
 * does not see them; they are kept for the rest of the transaction, as the
 * writer does not learn when the cursors close.  (The coordinator registers
 * a cursor's snapshot itself, which its entry-db readers use.)
 */
static List *tc_cursor_cids = NIL;
static LocalTransactionId tc_cursor_lxid = InvalidLocalTransactionId;

void
tempcat_note_cursor_snapshot(CommandId cid)
{
	MemoryContext oldcxt;

	if (Gp_role != GP_ROLE_EXECUTE || !IsTransactionState())
		return;
	if (tc_cursor_lxid != MyProc->lxid)
	{
		/* (the list of an earlier transaction went with its memory) */
		tc_cursor_cids = NIL;
		tc_cursor_lxid = MyProc->lxid;
	}
	if (list_member_int(tc_cursor_cids, (int) cid))
		return;
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	tc_cursor_cids = lappend_int(tc_cursor_cids, (int) cid);
	MemoryContextSwitchTo(oldcxt);
}

/* Can a cursor's snapshot see a row of this command range? */
static bool
tempcat_cursors_see_command_range(CommandId cmin, CommandId cmax)
{
	ListCell   *lc;

	if (tc_cursor_lxid != MyProc->lxid)
		return false;
	foreach(lc, tc_cursor_cids)
	{
		CommandId	cid = (CommandId) lfirst_int(lc);

		if (cmin < cid && cid <= cmax)
			return true;
	}
	return false;
}

/*
 * Free rows that the current transaction inserted and then deleted in an
 * earlier command, before the transaction ends.  Called when the area is
 * full, so that a transaction creating and dropping many temporary tables
 * in a loop does not overflow to the on-disk catalog.  Caller holds the
 * exclusive lock.  Returns the number of rows freed.
 *
 * Such a row is dead whatever the transaction's fate, as long as its
 * deletion cannot be undone on its own: rows deleted by the top-level
 * transaction itself, or by the same (sub)transaction that inserted them
 * (any rollback undoing the deletion then undoes the insertion too, e.g. a
 * loop body with an exception block).  Not rows deleted by a subtransaction
 * that may roll back while their insertion stays.  And only rows that no
 * snapshot of ours still in use can see (SnapshotsSeeCommandRange()): an
 * older snapshot of the transaction, e.g. that of the outer query or of a
 * cursor, still sees rows deleted after it was taken.  Scans copy the rows
 * they return when they start.  (The readers of a QE writer take its
 * snapshots, but a reader of a cursor opened earlier in the transaction
 * could miss such rows in a scan it starts later, unlike on-disk rows.)
 */
static int
tempcat_reclaim_dead(void)
{
	TransactionId topxid = GetTopTransactionIdIfAny();
	CommandId	curcid = GetCurrentCommandId(false);
	int			nfreed = 0;
	bool		waiting = false;
	int			i = 0;

	if (!TransactionIdIsValid(topxid))
		return 0;

	/*
	 * The walk is linear in the pending rows, and once the area is full it
	 * is tried for every row added.  Do not repeat one that freed nothing
	 * unless something could have changed: rows deleted since, or, if rows
	 * were left only because of the command or the snapshots in use, a new
	 * command.  (A snapshot released within a command is noticed at the
	 * next one: until then, the rows it could see only wait to be freed.)
	 */
	if (tc_reclaim_failed &&
		TransactionIdEquals(tc_reclaim_xid, topxid) &&
		tc_reclaim_ndeletions == tc_ndeletions &&
		(!tc_reclaim_waiting || tc_reclaim_cid == curcid))
		return 0;

	while (i < tc_npending)
	{
		dsa_pointer ep = tc_pending[i];
		TempcatEntry *e = TC_ADDR(ep);
		bool		candidate;

		/* (a row freed already would fail this; see tempcat_pending_take()) */
		Assert(e->catidx >= 0 && e->catidx < TEMPCAT_NCATALOGS &&
			   (e->flags & TCE_PENDING));

		candidate = (TransactionIdEquals(e->xmax, topxid) ||
					 TransactionIdEquals(e->xmax, e->xmin)) &&
			!(e->flags & TCE_XMIN_COMMITTED) &&
			TransactionIdIsCurrentTransactionId(e->xmin);
		if (candidate &&
			(e->cmax >= curcid || SnapshotsSeeCommandRange(e->cmin, e->cmax) ||
			 tempcat_cursors_see_command_range(e->cmin, e->cmax)))
		{
			waiting = true;
			candidate = false;
		}

		if (candidate)
		{
			int			catidx = e->catidx;
			Oid			objoid = tempcat_catalog_has_oid(catidx) ?
				tempcat_entry_oid(e) : InvalidOid;

			tempcat_pending_take(i);
			tempcat_entry_remove(ep);
			tempcat_forget_if_gone(catidx, objoid);
			nfreed++;
		}
		else
			i++;
	}

	tc_reclaim_failed = (nfreed == 0);
	tc_reclaim_xid = topxid;
	tc_reclaim_cid = curcid;
	tc_reclaim_ndeletions = tc_ndeletions;
	tc_reclaim_waiting = waiting;

	return nfreed;
}

/*
 * Republish the roles that our in-memory pg_shdepend rows, of any
 * visibility, refer to, after some of the rows went away.
 */
static void
tempcat_recompute_roles(void)
{
	int			catidx = tempcat_catalog_index(SharedDependRelationId);
	TempcatProcState *st = tempcat_my_state();
	Oid			roles[TEMPCAT_MAX_ROLES];
	int			nroles = 0;
	dsa_pointer ep;
	int			i;

	tc_roles_dirty = false;

	/* (no area: the rows that published roles all went to disk) */
	for (ep = tc_area != NULL ? tc_root->catalogs[catidx].head : InvalidDsaPointer;
		 DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);
		Form_pg_shdepend form = (Form_pg_shdepend) ((char *) hdr + hdr->t_hoff);

		for (i = 0; i < nroles; i++)
		{
			if (roles[i] == form->refobjid)
				break;
		}
		/* every row's role was published, so they fit */
		if (i == nroles && nroles < TEMPCAT_MAX_ROLES)
			roles[nroles++] = form->refobjid;
		ep = e->next;
	}

	SpinLockAcquire(&TempcatShared->mutex);
	memcpy(st->roles, roles, sizeof(Oid) * nroles);
	st->nroles = nroles;
	SpinLockRelease(&TempcatShared->mutex);
}

/*
 * Does the area still hold rows that are, or may become, visible?  Used at
 * session exit, outside any transaction.
 */
static bool
tempcat_area_has_live_rows(void)
{
	int			i;

	for (i = 0; i < TEMPCAT_NCATALOGS; i++)
	{
		dsa_pointer ep = tc_root->catalogs[i].head;

		while (DsaPointerIsValid(ep))
		{
			TempcatEntry *e = TC_ADDR(ep);

			if (((e->flags & TCE_XMIN_COMMITTED) ||
				 tempcat_xid_status(e->xmin) != TCX_ABORTED) &&
				(!TransactionIdIsValid(e->xmax) ||
				 tempcat_xid_status(e->xmax) != TCX_COMMITTED))
				return true;
			ep = e->next;
		}
	}
	return false;
}

/*
 * Remove the files of a temporary relation of a session that is gone, or
 * going: nothing else leads to them, as its catalog rows are gone with the
 * session's area, or are leftovers that the sweep removes.  Not
 * transactional, and that is fine: nobody can use such a relation.
 */
static void
tempcat_unlink_files(Form_pg_class form)
{
	RelFileNode rnode;
	SMgrRelation srel;

	if (form->relpersistence != RELPERSISTENCE_TEMP ||
		!OidIsValid(form->relfilenode))
		return;

	rnode.spcNode = OidIsValid(form->reltablespace) ?
		form->reltablespace : MyDatabaseTableSpace;
	rnode.dbNode = MyDatabaseId;
	rnode.relNode = form->relfilenode;
	srel = smgropen(rnode, TempRelBackendId,
					IsAccessMethodAO(form->relam) ? SMGR_AO : SMGR_MD);
	/* (isRedo: the files may be gone already) */
	smgrdounlinkall(&srel, 1, true);
	smgrclose(srel);
}

/* At session exit: see tempcat_before_exit(). */
static void
tempcat_unlink_live_relations(void)
{
	int			catidx = tempcat_catalog_index(RelationRelationId);
	dsa_pointer ep = tc_root->catalogs[catidx].head;

	while (DsaPointerIsValid(ep))
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);

		if (!TransactionIdIsValid(e->xmax) ||
			tempcat_xid_status(e->xmax) != TCX_COMMITTED)
			tempcat_unlink_files((Form_pg_class) ((char *) hdr + hdr->t_hoff));
		ep = e->next;
	}
}

void
tempcat_start_transaction(void)
{
	tc_xact_changed = false;
	tc_change_level = 0;

	/*
	 * A retrieve connection can outlive the writer of the cursor session it
	 * serves, which ends or is replaced (e.g. after its gang was reset).
	 * Detach from that writer's area then: the area, segments and all, stays
	 * in memory as long as anyone is attached.  The next lookup attaches to
	 * the session's current area, if any.
	 */
	if (am_cursor_retrieve_handler && tc_area != NULL)
	{
		dsa_handle	handle;
		dsa_pointer rootptr;
		int			sessionId = RetrieveSessionId();

		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr) ||
			handle != dsa_get_handle(tc_area))
		{
			dsa_detach(tc_area);
			tc_area = NULL;
			tc_root = NULL;
		}
	}

	/* A prepared sweep was finished meanwhile; see tempcat_sweep_exit(). */
	if (TransactionIdIsValid(tc_sweep_xid))
	{
		TempcatXidStatus status = tempcat_xid_status(tc_sweep_xid);

		if (status != TCX_IN_PROGRESS)
			tempcat_sweep_resolved(status == TCX_COMMITTED);
	}

	tempcat_resolve_pending();
	if (tc_horizon_dirty && tc_area != NULL)
		tempcat_recompute_horizon();
	if (tc_roles_dirty)
		tempcat_recompute_roles();
}

/*
 * The session goes idle, out of any transaction: apply the outcome of its
 * finished transactions to the pending rows now, rather than at the start
 * of its next transaction, let the published horizon rise (see
 * tempcat_add_pending()), and republish the roles its rows refer to.  The
 * next transaction may be long in coming, especially for a QE writer, which
 * gets none while the session's queries go to other segments or to the
 * coordinator only: until then, the XIDs of its last transaction (committed
 * in two phases, after its last start) held back datfrozenxid on its
 * segment, even with all its temporary objects dropped, and roles it no
 * longer used could not be dropped by other sessions.
 *
 * Out of a transaction, an error would not release the lock, and could
 * repeat at every return here: log it instead, and leave the rows to the
 * start of the next transaction, which reports any error as usual.  The
 * pending list stays consistent (see tempcat_pending_take()).
 */
void
tempcat_idle(void)
{
	MemoryContext oldcxt = CurrentMemoryContext;

	if (!tc_roles_dirty &&
		(tc_area == NULL || (tc_npending == 0 && !tc_horizon_dirty)))
		return;

	PG_TRY();
	{
		if (tc_area != NULL)
		{
			tempcat_resolve_pending();
			if (tc_horizon_dirty)
				tempcat_recompute_horizon();
		}
		/* and the roles that DROP ROLE in other sessions sees us use */
		if (tc_roles_dirty)
			tempcat_recompute_roles();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		LWLockReleaseAll();
		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		ereport(LOG,
				(errmsg("could not apply the outcome of finished transactions to in-memory catalog rows: %s",
						edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

/*
 * PREPARE TRANSACTION by a user (possible in utility mode only) of a
 * transaction that changed virtual rows.  Upstream refuses to prepare
 * transactions that operated on temporary objects; Greengage cannot, as its
 * own two-phase commit prepares every distributed transaction on the
 * segments.  For in-memory rows it matters more: if the session ends before
 * the prepared transaction is finished, its changes to the session's rows
 * are lost, while its on-disk effects (relation files, dependencies on
 * ordinary objects) are committed later anyway.
 */
void
tempcat_check_prepare(void)
{
	if (Gp_role == GP_ROLE_UTILITY && tc_xact_changed)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has operated on temporary objects kept in memory")));
}

/*
 * Did the current transaction change virtual rows?  A QE reports it to the
 * dispatcher like a WAL write for a permanent relation (see dest.c), so that
 * the transaction is committed in two phases: in one phase, an error at
 * commit on one segment (e.g. a deferred unique check) would leave the
 * changes committed on the others while the coordinator rolls back.
 */
bool
tempcat_xact_changed(void)
{
	return tc_xact_changed;
}

/*
 * After a subtransaction abort, free the rows it inserted and undo its
 * deletions right away, so that loops with exception blocks do not keep
 * dead rows around until the end of the top-level transaction.  Rows of the
 * still-running transaction are left alone (they are "in progress").  Only
 * if it, or a subtransaction committed into it, changed rows: the pending
 * list holds those of the whole transaction, and a walk over it at every
 * abort of an exception block made long transactions slow.
 */
void
tempcat_abort_subtransaction(void)
{
	int			level = GetCurrentTransactionNestLevel();

	if (tc_change_level < level)
		return;
	tempcat_resolve_pending();
	tc_change_level = level - 1;
}

/*
 * Our relations and functions dropped in this transaction that lived on
 * disk, with the subtransaction level that dropped them: their statistics
 * are purged when it commits or prepares.  Those of objects in memory are
 * purged as their last row is freed (tempcat_forget_if_gone()), but nothing
 * frees rows on disk so, and pgstat_vacuum_stat() keeps the statistics of
 * all OIDs within our bounds.
 */
typedef struct TempcatDropped
{
	TempcatObject obj;
	int			level;
} TempcatDropped;

static List *tc_dropped = NIL;	/* in TopTransactionContext */
static LocalTransactionId tc_dropped_lxid = InvalidLocalTransactionId;

void
tempcat_note_drop(const ObjectAddress *object)
{
	TempcatOwnedEntry *owned;
	TempcatDropped *d;
	MemoryContext oldcxt;

	if (object->objectSubId != 0 ||
		(object->classId != RelationRelationId &&
		 object->classId != ProcedureRelationId))
		return;
	owned = tempcat_owned_entry(object->classId, object->objectId);
	if (owned == NULL || !owned->on_disk)
		return;
	if (tc_dropped_lxid != MyProc->lxid)
	{
		tc_dropped = NIL;
		tc_dropped_lxid = MyProc->lxid;
	}
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	d = palloc(sizeof(TempcatDropped));
	d->obj.classId = object->classId;
	d->obj.objectId = object->objectId;
	d->level = GetCurrentTransactionNestLevel();
	tc_dropped = lappend(tc_dropped, d);
	MemoryContextSwitchTo(oldcxt);
}

/* Forget the drops of an aborted subtransaction, or hand them to its parent. */
static void
tempcat_dropped_end_subxact(bool commit)
{
	int			level = GetCurrentTransactionNestLevel();
	List	   *kept = NIL;
	ListCell   *lc;
	MemoryContext oldcxt;

	if (tc_dropped_lxid != MyProc->lxid || tc_dropped == NIL)
		return;
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	foreach(lc, tc_dropped)
	{
		TempcatDropped *d = (TempcatDropped *) lfirst(lc);

		if (d->level >= level)
		{
			if (!commit)
				continue;
			d->level = level - 1;
		}
		kept = lappend(kept, d);
	}
	list_free(tc_dropped);
	tc_dropped = kept;
	MemoryContextSwitchTo(oldcxt);
}

/* The transaction is about to commit or prepare: purge their statistics. */
static void
tempcat_purge_dropped(void)
{
	ListCell   *lc;

	if (tc_dropped_lxid != MyProc->lxid)
		return;
	foreach(lc, tc_dropped)
	{
		TempcatDropped *d = (TempcatDropped *) lfirst(lc);

		if (d->obj.classId == RelationRelationId)
			pgstat_purge_relation(d->obj.objectId);
		else
			pgstat_purge_function(d->obj.objectId);
	}
	tc_dropped = NIL;
}

/*
 * Objects to move to disk move before the transaction commits or prepares,
 * and the copies become visible, for what runs before the end (ON COMMIT
 * actions), as at the end of a command.
 */
static void
tempcat_xact_callback(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_PRE_COMMIT && event != XACT_EVENT_PRE_PREPARE)
		return;
	if (tempcat_materialize_moves())
		CommandCounterIncrement();
	tempcat_purge_dropped();
}

/* A committed subtransaction's changes are its parent's now. */
static void
tempcat_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						 SubTransactionId parentSubid, void *arg)
{
	int			level;

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		tempcat_moved_end_subxact(false);
		tempcat_dropped_end_subxact(false);
	}
	if (event != SUBXACT_EVENT_COMMIT_SUB)
		return;
	tempcat_moved_end_subxact(true);
	tempcat_dropped_end_subxact(true);
	level = GetCurrentTransactionNestLevel();
	if (tc_change_level >= level)
		tc_change_level = level - 1;
}

/* ----------------------------------------------------------------
 * Catalog DML
 * ----------------------------------------------------------------
 */

static Oid
tempcat_getoid(HeapTuple tup, TupleDesc desc, AttrNumber attno)
{
	bool		isnull;
	Datum		value = heap_getattr(tup, attno, desc, &isnull);

	return isnull ? InvalidOid : DatumGetObjectId(value);
}

/* Does an owner column of the row hold one of our temporary objects? */
static bool
tempcat_owner_is_temp(HeapTuple tup, TupleDesc desc, AttrNumber attr,
					  Oid ownerclass, AttrNumber ownerclassattr)
{
	Oid			catalog = ownerclassattr != 0 ?
		tempcat_getoid(tup, desc, ownerclassattr) : ownerclass;

	return tempcat_owns_oid(catalog, tempcat_getoid(tup, desc, attr));
}

/*
 * Should this new catalog row be kept in memory?
 */
bool
tempcat_route_insert(Relation rel, HeapTuple tup)
{
	Oid			relid = RelationGetRelid(rel);
	TupleDesc	desc = RelationGetDescr(rel);
	const TempcatCatalogDef *def;
	int			catidx;

	/* The session has no temporary objects in memory: nothing to route. */
	if (tc_owned == NULL)
		return false;

	catidx = tempcat_catalog_index(relid);
	if (catidx < 0)
		return false;
	def = &tempcat_catalogs[catidx];

	/*
	 * pg_shdepend: rows of this database recording that a temporary object
	 * of ours is owned by, or grants privileges to, a role.  The role is
	 * published instead, for DROP ROLE in other sessions to see.
	 */
	if (relid == SharedDependRelationId)
		return tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid) == MyDatabaseId &&
			tempcat_getoid(tup, desc, Anum_pg_shdepend_refclassid) == AuthIdRelationId &&
			tempcat_owner_is_temp(tup, desc, def->owner, def->ownerclass,
								  def->ownerclassattr) &&
			tempcat_publish_role(tempcat_getoid(tup, desc, Anum_pg_shdepend_refobjid));

	if (relid == ConstraintRelationId &&
		!OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)))
	{
		/* domain constraint */
		if (!tempcat_owns_oid(TypeRelationId,
							  tempcat_getoid(tup, desc, Anum_pg_constraint_contypid)))
			return false;
	}
	else if (!tempcat_owner_is_temp(tup, desc, def->owner,
									def->ownerclass, def->ownerclassattr))
		return false;

	if (def->owner2 != 0 &&
		!tempcat_owner_is_temp(tup, desc, def->owner2,
							   def->owner2class, def->owner2classattr))
		return false;

	/*
	 * Rows of objects that live on disk go there too, and no row on disk
	 * may refer to an object of ours in memory (see tempcat_materialize_moves()):
	 * a dependency of an object on disk takes what it refers to to disk,
	 * and a part goes where its object is.
	 */
	if (relid == DependRelationId)
	{
		Oid			depclass = tempcat_getoid(tup, desc, Anum_pg_depend_classid);
		Oid			depobj = tempcat_getoid(tup, desc, Anum_pg_depend_objid);
		Oid			refclass = tempcat_getoid(tup, desc, Anum_pg_depend_refclassid);
		Oid			refobj = tempcat_getoid(tup, desc, Anum_pg_depend_refobjid);
		bool		depmem = tempcat_object_in_memory(depclass, depobj);
		bool		refmem = tempcat_object_in_memory(refclass, refobj);
		bool		isnull;
		char		deptype = DatumGetChar(heap_getattr(tup, Anum_pg_depend_deptype,
														desc, &isnull));

		if (!depmem && refmem)
		{
			tempcat_schedule_move(refclass, refobj);
			return false;
		}
		if (depmem && !refmem &&
			(deptype == DEPENDENCY_INTERNAL || deptype == DEPENDENCY_AUTO))
		{
			tempcat_schedule_move(depclass, depobj);
			return false;
		}
		return depmem;
	}
	else
	{
		TempcatObject obj = tempcat_row_object(catidx, tup, desc);

		if (tempcat_object_in_memory(obj.classId, obj.objectId))
			return true;
		/* a part (trigger, constraint, ...) of an object on disk is there */
		tempcat_note_disk_row(rel, tup);
		tempcat_schedule_links(rel, tup);
		return false;
	}
}

/*
 * Refuse a row that would not fit on disk, as heap_insert() and
 * heap_update() would: in a catalog without a TOAST table, one that
 * compression does not bring under MaxHeapTupleSize (e.g. a long column
 * default in pg_attribute, long options in pg_class).  Kept in memory, it
 * would fail every move of its object to disk, and with that every row of
 * the object that did not fit.
 */
static void
tempcat_check_row_size(Relation rel, HeapTuple tup)
{
	HeapTuple	compressed;
	Size		len;

	if (tup->t_len <= MaxHeapTupleSize ||
		OidIsValid(rel->rd_rel->reltoastrelid))
		return;
	/* (without a TOAST table, this only compresses values in place) */
	compressed = toast_insert_or_update(rel, tup, NULL, TOAST_TUPLE_TARGET, 0);
	len = MAXALIGN(compressed->t_len);
	if (compressed != tup)
		heap_freetuple(compressed);
	if (len > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %zu, maximum size %zu",
						len, MaxHeapTupleSize)));
}

/*
 * Insert a virtual row.  Returns false, changing nothing, if the session's
 * area is full; the caller then stores the row on disk.  Scans merge both
 * sources, so where a row lives does not affect what anybody sees.
 */
bool
tempcat_insert(Relation rel, HeapTuple tup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	TransactionId xid;
	CommandId	cid;
	dsa_pointer ep;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader());
	tempcat_check_row_size(rel, tup);

	/* No area (DSM space is short): the row goes to disk. */
	if (!tempcat_attach(true))
	{
		tempcat_row_to_disk(rel, tup);
		return false;
	}

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	tempcat_reserve_pending(1);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	ep = tempcat_entry_create(catidx, tup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(ep) && tempcat_reclaim_dead() > 0)
		ep = tempcat_entry_create(catidx, tup, RelationGetDescr(rel), xid, cid);
	if (DsaPointerIsValid(ep))
		tempcat_add_pending(ep);
	LWLockRelease(&tc_root->lock);

	if (!DsaPointerIsValid(ep))
	{
		tempcat_row_to_disk(rel, tup);
		return false;
	}

	tempcat_set_caller_tuple(tup, rel, TC_ADDR(ep));
	if (RelationGetRelid(rel) == RelationRelationId)
		tempcat_cover_pg_class_row(tup->t_data);

	CacheInvalidateHeapTuple(rel, tup, NULL);
	return true;
}

static TempcatEntry *
tempcat_lookup_tid(Relation rel, ItemPointer tid, dsa_pointer *ep)
{
	if (!tempcat_attach(false))
		elog(ERROR, "tempcat: no in-memory catalog for virtual tuple (%u,%u)",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid);

	*ep = tempcat_find_tid(tid);
	if (!DsaPointerIsValid(*ep))
		elog(ERROR, "tempcat: virtual tuple (%u,%u) of \"%s\" not found",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid,
			 RelationGetRelationName(rel));

	return TC_ADDR(*ep);
}

/* Complain if the row cannot be deleted, like heap_delete() would. */
static void
tempcat_check_deletable(TempcatEntry *e)
{
	if (TransactionIdIsValid(e->xmax))
	{
		if (TransactionIdIsCurrentTransactionId(e->xmax))
			elog(ERROR, "tuple already updated by self");
		if (TransactionIdDidCommit(e->xmax))
			elog(ERROR, "tuple concurrently updated");
		/* deleted by an aborted transaction: may be overwritten */
	}
}

/*
 * For SQL DML on a catalog (heapam_handler.c): has the current transaction
 * already deleted or updated the row?  Like heap_delete() and
 * heap_update(): TM_SelfModified, with 'tmfd' filled in, if the current
 * command (or a later one) did, e.g. one that meets the row twice, and the
 * executor skips the row; TM_Invisible if an earlier command did, which the
 * caller's snapshot should not have let it see; TM_Ok otherwise.  (A
 * TM_Result, as an int for tempcat.h.)
 */
int
tempcat_self_modified(Relation rel, ItemPointer tid, TM_FailureData *tmfd)
{
	dsa_pointer ep;
	TempcatEntry *e = tempcat_lookup_tid(rel, tid, &ep);

	if (!TransactionIdIsValid(e->xmax) ||
		!TransactionIdIsCurrentTransactionId(e->xmax))
		return TM_Ok;
	if (e->cmax < GetCurrentCommandId(false))
		return TM_Invisible;
	tmfd->ctid = *tid;
	tmfd->xmax = e->xmax;
	tmfd->cmax = e->cmax;
	return TM_SelfModified;
}

/* Set xmax on an entry.  Caller holds the lock and has checked it. */
static void
tempcat_mark_deleted(TempcatEntry *e, dsa_pointer ep,
					 TransactionId xid, CommandId cid)
{
	e->xmax = xid;
	e->cmax = cid;
	tempcat_add_pending(ep);
	tc_ndeletions++;
}

/*
 * Replace a virtual row by a new version.  Returns false, changing nothing,
 * if the area is full; see tempcat_insert().
 */
bool
tempcat_update(Relation rel, ItemPointer otid, HeapTuple newtup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	dsa_pointer oldep;
	dsa_pointer newep;
	TempcatEntry *olde;
	HeapTuple	oldtup;
	TransactionId xid;
	CommandId	cid;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader());
	tempcat_check_row_size(rel, newtup);

	olde = tempcat_lookup_tid(rel, otid, &oldep);
	tempcat_check_deletable(olde);
	oldtup = tempcat_entry_copy(olde, RelationGetRelid(rel));

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	tempcat_reserve_pending(2);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	newep = tempcat_entry_create(catidx, newtup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(newep) && tempcat_reclaim_dead() > 0)
		newep = tempcat_entry_create(catidx, newtup, RelationGetDescr(rel), xid, cid);
	if (!DsaPointerIsValid(newep))
	{
		LWLockRelease(&tc_root->lock);
		heap_freetuple(oldtup);
		tempcat_row_to_disk(rel, newtup);
		return false;
	}
	tempcat_mark_deleted(olde, oldep, xid, cid);
	tempcat_add_pending(newep);
	LWLockRelease(&tc_root->lock);

	tempcat_set_caller_tuple(newtup, rel, TC_ADDR(newep));
	if (RelationGetRelid(rel) == RelationRelationId)
	{
		tempcat_cover_pg_class_row(newtup->t_data);
		tc_horizon_dirty = true;
	}

	CacheInvalidateHeapTuple(rel, oldtup, newtup);
	heap_freetuple(oldtup);
	return true;
}

void
tempcat_delete(Relation rel, ItemPointer tid)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTuple	oldtup;
	TransactionId xid;
	CommandId	cid;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, tid, &ep);
	tempcat_check_deletable(e);
	oldtup = tempcat_entry_copy(e, RelationGetRelid(rel));

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	tempcat_reserve_pending(1);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	tempcat_mark_deleted(e, ep, xid, cid);
	LWLockRelease(&tc_root->lock);

	if (RelationGetRelid(rel) == RelationRelationId)
		tc_horizon_dirty = true;

	CacheInvalidateHeapTuple(rel, oldtup, NULL);
	heap_freetuple(oldtup);
}

/*
 * In-place updates overwrite fixed-width columns only, which come before
 * the variable-width ones in the catalogs that get them (pg_class, pg_index,
 * gp_fastsequence, ...).  A row moved to disk may be stored compressed there
 * (pg_class has no TOAST table, so heap_insert() compresses its long values
 * within the row), unlike in memory, and the two versions then differ in
 * length.  Returns the length of the data that an in-place update of 'dst'
 * with 'src' overwrites: the leading fixed-width columns then.
 */
uint32
tempcat_fixed_part_len(Relation rel, HeapTupleHeader dst, HeapTupleHeader src)
{
	TupleDesc	desc = RelationGetDescr(rel);
	int			natts = Min(desc->natts, HeapTupleHeaderGetNatts(src));
	bool		hasnulls = (src->t_infomask & HEAP_HASNULL) != 0;
	uint32		len = 0;
	int			i;

	/* (same columns, same nulls) */
	if (dst->t_hoff != src->t_hoff ||
		HeapTupleHeaderGetNatts(dst) != HeapTupleHeaderGetNatts(src) ||
		((dst->t_infomask & HEAP_HASNULL) != 0) != hasnulls ||
		(hasnulls && memcmp(dst->t_bits, src->t_bits,
							BITMAPLEN(HeapTupleHeaderGetNatts(src))) != 0))
		elog(ERROR, "wrong tuple length");

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (hasnulls && att_isnull(i, src->t_bits))
			continue;
		if (att->attlen <= 0)
			break;
		len = att_align_nominal(len, att->attalign);
		len += att->attlen;
	}
	return len;
}

/*
 * Overwrite a row in place, non-transactionally, like heap_inplace_update().
 */
void
tempcat_inplace_update(Relation rel, HeapTuple tup)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTupleHeader hdr;
	uint32		len;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, &tup->t_self, &ep);
	hdr = TCE_DATA(e);

	/* (from the copy on disk of a row moved there, perhaps compressed) */
	if (tup->t_len == e->len && tup->t_data->t_hoff == hdr->t_hoff)
		len = tup->t_len - hdr->t_hoff;
	else
		len = tempcat_fixed_part_len(rel, hdr, tup->t_data);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	memcpy((char *) hdr + hdr->t_hoff,
		   (char *) tup->t_data + tup->t_data->t_hoff, len);
	LWLockRelease(&tc_root->lock);

	/* e.g. VACUUM advancing relfrozenxid */
	if (RelationGetRelid(rel) == RelationRelationId)
	{
		tempcat_cover_pg_class_row(hdr);
		tc_horizon_dirty = true;
	}

	(void) GetCurrentCommandId(true);
	CacheInvalidateHeapTuple(rel, tup, NULL);
}

/*
 * Make a row visible to everybody regardless of the inserting transaction's
 * fate, like heap_freeze_tuple_wal_logged().
 */
void
tempcat_freeze(Relation rel, HeapTuple tup)
{
	dsa_pointer ep;
	TempcatEntry *e;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, &tup->t_self, &ep);

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	e->xmin = FrozenTransactionId;
	e->flags |= TCE_XMIN_COMMITTED;
	LWLockRelease(&tc_root->lock);

	HeapTupleHeaderSetXmin(tup->t_data, FrozenTransactionId);
}

/* ----------------------------------------------------------------
 * Catalog scans
 * ----------------------------------------------------------------
 */

/*
 * Collect the visible virtual rows matching the scan keys.  Must be called
 * while the keys still refer to heap attribute numbers, i.e. before
 * systable_beginscan() converts them to index column numbers.
 *
 * Returns NULL if there can be no virtual rows for this catalog.
 */
/* Compare two rows of the scanned catalog by the scan's index key. */
static int
tempcat_compare(TempcatScan scan, HeapTuple a, HeapTuple b)
{
	int			i;

	for (i = 0; i < scan->nidxkeys; i++)
	{
		AttrNumber	attno = scan->irel->rd_index->indkey.values[i];
		bool		anull;
		bool		bnull;
		Datum		ad = heap_getattr(a, attno, scan->heapdesc, &anull);
		Datum		bd = heap_getattr(b, attno, scan->heapdesc, &bnull);
		int			cmp;

		if (anull || bnull)
		{
			if (anull && bnull)
				continue;
			cmp = anull ? 1 : -1;	/* NULLS LAST */
		}
		else
			cmp = DatumGetInt32(FunctionCall2Coll(scan->procs[i],
												  scan->irel->rd_indcollation[i],
												  ad, bd));
		if (scan->irel->rd_indoption[i] & INDOPTION_DESC)
			cmp = -cmp;
		if (cmp != 0)
			return cmp;
	}
	return 0;
}

static int
tempcat_qsort_cmp(const void *a, const void *b, void *arg)
{
	return tempcat_compare((TempcatScan) arg,
						   *(const HeapTuple *) a, *(const HeapTuple *) b);
}

TempcatScan
tempcat_beginscan(Relation rel, Relation irel, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	int			catidx;
	const TempcatCatalogDef *def;
	TempcatCatalog *cat;
	TempcatScan scan;
	TupleDesc	desc;
	bool		reader;
	int			usekey = -1;
	uint32		usehash = 0;
	dsa_pointer np;
	int			k;
	int			i;
	int			maxtuples;

	/* the area first: there is none with the feature off, most often */
	if (!tempcat_attach(false))
		return NULL;
	catidx = tempcat_catalog_index(RelationGetRelid(rel));
	if (catidx < 0)
		return NULL;

	cat = &tc_root->catalogs[catidx];
	if (cat->nentries == 0)
		return NULL;

	def = &tempcat_catalogs[catidx];
	desc = RelationGetDescr(rel);

	/* Use a hashed column if the scan has an equality key on it. */
	for (k = 0; k < TEMPCAT_NKEYS && usekey < 0; k++)
	{
		if (def->keytype[k] == TCK_NONE)
			continue;
		for (i = 0; i < nkeys; i++)
		{
			/*
			 * The argument must have the column's type for the hash to
			 * match: index scan keys can be cross-type (e.g. name = text).
			 */
			if (key[i].sk_attno == def->keyattr[k] &&
				key[i].sk_strategy == BTEqualStrategyNumber &&
				!(key[i].sk_flags & SK_ISNULL) &&
				(key[i].sk_subtype == InvalidOid ||
				 key[i].sk_subtype == (def->keytype[k] == TCK_OID ? OIDOID : NAMEOID)))
			{
				usekey = k;
				usehash = tempcat_key_hash(def->keytype[k], key[i].sk_argument);
				break;
			}
		}
	}

	scan = palloc(sizeof(struct TempcatScanData));
	scan->ntuples = 0;
	scan->next = 0;
	scan->on_virtual = false;
	scan->irel = irel;
	scan->heapdesc = desc;
	scan->nidxkeys = 0;
	scan->procs = NULL;
	scan->disk_tuple = NULL;
	scan->disk_done = false;
	scan->direction = 0;
	maxtuples = 8;
	scan->tuples = palloc(sizeof(HeapTuple) * maxtuples);

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);

	if (usekey >= 0)
	{
		TempcatHash *h = &cat->keys[usekey];

		np = h->nbuckets == 0 ? InvalidDsaPointer :
			((dsa_pointer *) TC_ADDR(h->buckets))[usehash & (h->nbuckets - 1)];
	}
	else
		np = cat->head;

	while (DsaPointerIsValid(np))
	{
		TempcatEntry *e;
		HeapTupleData htup;
		bool		matches;

		if (usekey >= 0)
		{
			TempcatHashNode *node = TC_ADDR(np);

			np = node->next;
			if (node->hash != usehash)
				continue;
			e = TC_ADDR(node->entry);
		}
		else
		{
			e = TC_ADDR(np);
			np = e->next;
		}

		if (!tempcat_entry_visible(e, snapshot))
			continue;

		htup.t_len = e->len;
		htup.t_self = e->tid;
		htup.t_tableOid = RelationGetRelid(rel);
		htup.t_data = TCE_DATA(e);
		HeapKeyTest(&htup, desc, nkeys, key, matches);
		if (!matches)
			continue;

		if (scan->ntuples >= maxtuples)
		{
			maxtuples *= 2;
			scan->tuples = repalloc(scan->tuples, sizeof(HeapTuple) * maxtuples);
		}
		scan->tuples[scan->ntuples++] = tempcat_entry_copy(e, RelationGetRelid(rel));
	}

	if (reader)
		LWLockRelease(&tc_root->lock);

	if (irel != NULL)
	{
		scan->nidxkeys = IndexRelationGetNumberOfKeyAttributes(irel);
		scan->procs = palloc(sizeof(FmgrInfo *) * scan->nidxkeys);
		for (i = 0; i < scan->nidxkeys; i++)
			scan->procs[i] = index_getprocinfo(irel, i + 1, BTORDER_PROC);
		if (scan->ntuples > 1)
			qsort_arg(scan->tuples, scan->ntuples, sizeof(HeapTuple),
					  tempcat_qsort_cmp, scan);
	}

	return scan;
}

/*
 * Return the next row: virtual rows merged with the on-disk rows that
 * fetch_disk() returns.  For scans through an index both come in index
 * order (backward if !forward, for ordered scans); otherwise the virtual
 * rows come first.
 */
HeapTuple
tempcat_getnext_dir(TempcatScan scan, bool forward,
					HeapTuple (*fetch_disk) (void *), void *arg)
{
	int			dir = forward ? 1 : -1;
	HeapTuple	vtup;

	if (scan->irel == NULL)
	{
		if (scan->next < scan->ntuples)
		{
			scan->on_virtual = true;
			return scan->tuples[scan->next++];
		}
		scan->on_virtual = false;
		return fetch_disk(arg);
	}

	if (scan->direction == 0)
		scan->direction = dir;
	else if (scan->direction != dir)
		elog(ERROR, "cannot change the direction of a catalog scan that includes in-memory temporary objects");

	if (scan->disk_tuple == NULL && !scan->disk_done)
	{
		scan->disk_tuple = fetch_disk(arg);
		if (scan->disk_tuple == NULL)
			scan->disk_done = true;
	}

	vtup = tempcat_scan_peek(scan, forward);
	if (vtup != NULL &&
		(scan->disk_tuple == NULL ||
		 tempcat_compare(scan, vtup, scan->disk_tuple) * dir <= 0))
	{
		scan->next++;
		scan->on_virtual = true;
		return vtup;
	}

	scan->on_virtual = false;
	if (scan->disk_tuple != NULL)
	{
		HeapTuple	tup = scan->disk_tuple;

		scan->disk_tuple = NULL;
		return tup;
	}
	return NULL;
}

HeapTuple
tempcat_getnext(TempcatScan scan, HeapTuple (*fetch_disk) (void *), void *arg)
{
	return tempcat_getnext_dir(scan, true, fetch_disk, arg);
}

bool
tempcat_scan_on_virtual(TempcatScan scan)
{
	return scan != NULL && scan->on_virtual;
}

void
tempcat_endscan(TempcatScan scan)
{
	int			i;

	for (i = 0; i < scan->ntuples; i++)
		pfree(scan->tuples[i]);
	pfree(scan->tuples);
	if (scan->procs)
		pfree(scan->procs);
	pfree(scan);
}

/* ----------------------------------------------------------------
 * Objects of other sessions
 * ----------------------------------------------------------------
 */

typedef enum TempcatObjectLookup
{
	TCO_FOUND,
	TCO_MISSING,
	TCO_UNKNOWN					/* cannot look up objects of this class */
} TempcatObjectLookup;

/*
 * Look an object up by OID in its catalog, with the catalog snapshot (so
 * our own in-memory rows count, other sessions' do not).
 */
static TempcatObjectLookup
tempcat_object_lookup(Oid classId, Oid objectId)
{
	Relation	rel;
	HeapTuple	tup;
	bool		found;

	if (classId == AttrDefaultRelationId)
	{
		/* not in objectaddress.c's ObjectProperty in this version */
		SysScanDesc scan;
		ScanKeyData key;

		rel = table_open(AttrDefaultRelationId, AccessShareLock);
		ScanKeyInit(&key, Anum_pg_attrdef_oid, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(objectId));
		scan = systable_beginscan(rel, AttrDefaultOidIndexId, true, NULL, 1, &key);
		found = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);
		table_close(rel, AccessShareLock);
		return found ? TCO_FOUND : TCO_MISSING;
	}

	if (!is_objectclass_supported(classId))
		return TCO_UNKNOWN;

	rel = table_open(classId, AccessShareLock);
	tup = get_catalog_object_by_oid(rel, get_object_attnum_oid(classId), objectId);
	found = HeapTupleIsValid(tup);
	table_close(rel, AccessShareLock);
	return found ? TCO_FOUND : TCO_MISSING;
}

/*
 * The relation an object without a namespace of its own belongs to (a
 * default, trigger, rule or policy), or InvalidOid.
 */
static Oid
tempcat_object_relation(Oid classId, Oid objectId)
{
	Oid			indexId;
	AttrNumber	oidattr;
	AttrNumber	relattr;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;
	Oid			relid = InvalidOid;

	switch (classId)
	{
		case AttrDefaultRelationId:
			indexId = AttrDefaultOidIndexId;
			oidattr = Anum_pg_attrdef_oid;
			relattr = Anum_pg_attrdef_adrelid;
			break;
		case TriggerRelationId:
			indexId = TriggerOidIndexId;
			oidattr = Anum_pg_trigger_oid;
			relattr = Anum_pg_trigger_tgrelid;
			break;
		case RewriteRelationId:
			indexId = RewriteOidIndexId;
			oidattr = Anum_pg_rewrite_oid;
			relattr = Anum_pg_rewrite_ev_class;
			break;
		case PolicyRelationId:
			indexId = PolicyOidIndexId;
			oidattr = Anum_pg_policy_oid;
			relattr = Anum_pg_policy_polrelid;
			break;
		default:
			return InvalidOid;
	}

	rel = table_open(classId, AccessShareLock);
	ScanKeyInit(&key, oidattr, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));
	scan = systable_beginscan(rel, indexId, true, NULL, 1, &key);
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		bool		isnull;
		Datum		d = heap_getattr(tup, relattr, RelationGetDescr(rel), &isnull);

		if (!isnull)
			relid = DatumGetObjectId(d);
	}
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	return relid;
}

/*
 * Is this an in-memory temporary object we cannot see?
 *
 * On-disk dependency rows may point at temporary objects of other sessions
 * (e.g. a temporary table column of a regular type).  Their catalog rows
 * live in the other session's memory, so callers that want to describe or
 * drop them must handle their absence.
 *
 * An object that went to disk, because it did not fit in the other
 * session's area, is missing too for anything but reading, like such a
 * relation (see tempcat_other_session_relation()): what refers to it in
 * turn, e.g. the session's later table using a domain, may be in memory,
 * where dropping or changing it could not follow.  It is told by
 * its namespace: one with a reserved OID that is not ours, and that we
 * cannot see (it is in the other session's memory) or that is temporary.
 * Ordinary objects with reserved OIDs live in ordinary namespaces.  Objects
 * without a namespace (defaults, triggers, rules, policies) go with their
 * relation.  Readers tell their session's objects by its writer's bounds
 * (tempcat_session_owns_oid()).
 */
bool
tempcat_object_missing(const ObjectAddress *object)
{
	Oid			relid;
	Oid			nspid;

	if (!IsTempcatOid(object->objectId))
		return false;

	if (object->classId == RelationRelationId)
		return tempcat_relation_missing(object->objectId);

	switch (tempcat_object_lookup(object->classId, object->objectId))
	{
		case TCO_MISSING:
			return true;
		case TCO_UNKNOWN:
			return false;
		case TCO_FOUND:
			break;
	}

	if (tempcat_session_owns_oid(object->classId, object->objectId))
		return false;
	if (object->classId == NamespaceRelationId)
	{
		char	   *nspname = get_namespace_name(object->objectId);
		bool		other;

		other = nspname != NULL &&
			tempcat_namespace_of_other_session(object->objectId, nspname);
		if (nspname != NULL)
			pfree(nspname);
		return other;
	}
	relid = tempcat_object_relation(object->classId, object->objectId);
	if (OidIsValid(relid))
		return tempcat_relation_missing(relid);
	if (!is_objectclass_supported(object->classId))
		return false;
	nspid = get_object_namespace(object);
	if (!IsTempcatOid(nspid) ||
		tempcat_session_owns_oid(NamespaceRelationId, nspid))
		return false;
	return !SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(nspid)) ||
		isAnyTempNamespace(nspid);
}

/*
 * Is 'nspid', named 'nspname', the temporary namespace of another live
 * session, which went to disk with the first row of the session that did
 * not fit (tempcat_row_to_disk())?  To other sessions it does not exist, as
 * when it was in memory: the objects in it that are in memory are missing
 * for them, and they must not find it by name (get_namespace_oid()), nor
 * drop or change it, which would leave those objects behind.  (Namespaces
 * left by sessions that are gone are the sweep's; ordinary namespaces with
 * reserved OIDs have other names.)
 */
static bool tempcat_oid_live_now(Oid oid, Oid dbid);

/*
 * Set while running a query that GPDB builds with the name of an AO table's
 * aux relation (get_ao_distribution(), the compression ratio functions,
 * ...), see tempcat_spi_execute_aux(): such a namespace may be found by name
 * then, as these only read the relation, which other sessions may open.
 */
static bool tc_name_other_namespaces = false;

bool
tempcat_namespace_of_other_session(Oid nspid, const char *nspname)
{
	if (!IsTempcatOid(nspid) || TempcatShared == NULL ||
		tc_name_other_namespaces)
		return false;
	if (strncmp(nspname, "pg_temp_", 8) != 0 &&
		strncmp(nspname, "pg_toast_temp_", 14) != 0)
		return false;
	if (tempcat_session_owns_oid(NamespaceRelationId, nspid))
		return false;
	return tempcat_oid_live_now(nspid, MyDatabaseId);
}

/*
 * SPI_execute() a query that names an AO table's aux relation, which may be
 * in another session's temporary namespace (see tc_name_other_namespaces).
 * With search_path set to pg_catalog meanwhile, so that no function,
 * operator or type of the user's (e.g. an aggregate named like the query's
 * sum()) runs while that namespace may be found by name.
 */
int
tempcat_spi_execute_aux(const char *src, bool read_only, long tcount)
{
	bool		save = tc_name_other_namespaces;
	int			save_nestlevel;
	int			ret;

	save_nestlevel = NewGUCNestLevel();
	(void) set_config_option("search_path", "pg_catalog",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);
	tc_name_other_namespaces = true;
	PG_TRY();
	{
		ret = SPI_execute(src, read_only, tcount);
	}
	PG_CATCH();
	{
		tc_name_other_namespaces = save;
		PG_RE_THROW();
	}
	PG_END_TRY();
	tc_name_other_namespaces = save;
	AtEOXact_GUC(false, save_nestlevel);
	return ret;
}

/*
 * ALTER DOMAIN and ALTER TYPE follow pg_depend from a type to the columns
 * and the types (sub-domains, domains and ranges over it, arrays) that use
 * it, to check them.  Another session's in-memory temporary relations and
 * types cannot be checked: only their on-disk dependency rows are visible
 * here, not what uses them in turn.  Fail rather than miss them.
 *
 * Unless the session is gone: then nothing uses them anymore, and their
 * rows on disk are leftovers that the sweep removes.  Returns true for
 * those, which the caller skips.
 */
bool
tempcat_check_type_user(Oid classId, Oid objectId, const char *typeName)
{
	ObjectAddress object;

	object.classId = classId;
	object.objectId = objectId;
	object.objectSubId = 0;
	if (!tempcat_object_missing(&object))
		return false;
	if (!tempcat_oid_maybe_live(objectId, MyDatabaseId))
		return true;
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot alter type \"%s\" because a temporary object of another session uses it",
					typeName)));
	return false;				/* keep compiler quiet */
}

/*
 * Is the relation missing for us, or another session's, though perhaps its
 * pg_class row is not?  See tempcat_other_session_relation().
 */
bool
tempcat_relation_missing(Oid relid)
{
	HeapTuple	tup;
	bool		missing;

	if (!IsTempcatOid(relid))
		return false;
	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	missing = !HeapTupleIsValid(tup) || tempcat_other_session_relation(tup);
	if (HeapTupleIsValid(tup))
		ReleaseSysCache(tup);
	return missing;
}

/*
 * Is this pg_class row that of a temporary relation of another session that
 * keeps them in memory (or kept, if it crashed and the row is a leftover)?
 * Such a row is on disk with the whole relation, as it did not fit in the
 * session's area (see tempcat_materialize_moves()), so other sessions can
 * build a relcache entry from it, as without the in-memory catalog; but
 * the relation is not theirs to drop, change or depend on, as what refers
 * to it may be in that session's memory.  Readers tell their session's
 * relations by its writer's bounds (tempcat_session_owns_oid()).
 */
bool
tempcat_other_session_relation(HeapTuple tup)
{
	Form_pg_class form = (Form_pg_class) GETSTRUCT(tup);

	if (form->relpersistence != RELPERSISTENCE_TEMP ||
		!IsTempcatOid(form->oid))
		return false;
	return !tempcat_session_owns_oid(RelationRelationId, form->oid);
}

/*
 * Is the object a temporary object of another session that we can find,
 * its rows having gone to disk?  (Not one that we cannot find, e.g. made by
 * this very command, which may be ours or ordinary.)
 */
static bool
tempcat_found_object_of_other_session(const ObjectAddress *object)
{
	HeapTuple	tup;
	bool		other;

	if (!IsTempcatOid(object->objectId) ||
		tempcat_session_owns_oid(object->classId, object->objectId))
		return false;
	if (object->classId == RelationRelationId)
	{
		tup = SearchSysCache1(RELOID, ObjectIdGetDatum(object->objectId));
		if (!HeapTupleIsValid(tup))
			return false;
		other = tempcat_other_session_relation(tup);
		ReleaseSysCache(tup);
		return other;
	}
	return tempcat_object_lookup(object->classId,
								 object->objectId) == TCO_FOUND &&
		tempcat_object_missing(object);
}

/*
 * On-disk rows of other sessions' temporary objects
 *
 * Rows of in-memory temporary objects can still be on disk: dependencies on
 * ordinary objects, and rows that went to disk when the session's area was
 * full.  To another session they point at objects that do not exist, which
 * is what catalog consistency checks look for, and what made pg_dump fail.
 * gpcheckcat and pg_dump therefore set gp_temp_memory_catalog_hide_others,
 * which hides from SQL scans the on-disk rows whose owner column holds a
 * reserved OID of a temporary object of another session: such a catalog
 * looks as if the other sessions had no temporary objects.  That includes
 * the leftovers of sessions that are gone, until the sweep removes them
 * (pg_upgrade's pg_dump runs before any sweep).  Ordinary objects with
 * reserved OIDs (see the file header) are not hidden
 * (tempcat_ordinary_object()).  Rows of other databases (pg_shdepend)
 * cannot be looked up and are hidden if their OIDs lie within the bounds
 * that a live session of their database publishes.
 */
typedef struct TempcatLiveRange
{
	Oid			databaseId;
	Oid			lo;
	Oid			hi;
} TempcatLiveRange;

static TempcatLiveRange *tc_live = NULL;
static int	tc_nlive = 0;
static LocalTransactionId tc_live_lxid = InvalidLocalTransactionId;
static CommandId tc_live_cid = InvalidCommandId;

/* Collect the published bounds, once per command. */
static void
tempcat_collect_live_ranges(void)
{
	CommandId	cid = GetCurrentCommandId(false);
	int			i;

	if (tc_live != NULL && tc_live_lxid == MyProc->lxid && tc_live_cid == cid)
		return;

	if (tc_live == NULL)
		tc_live = MemoryContextAlloc(TopMemoryContext,
									 sizeof(TempcatLiveRange) * 2 * TempcatShared->nprocs);
	tc_nlive = 0;
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];
		Oid			lo[2];
		Oid			hi[2];
		Oid			db;
		int			n;
		int			k;

		if (i == MyProc->pgprocno)
			continue;
		n = tempcat_read_bounds(st, lo, hi);
		db = st->databaseId;
		if (!OidIsValid(db))
			continue;
		for (k = 0; k < n; k++)
		{
			tc_live[tc_nlive].databaseId = db;
			tc_live[tc_nlive].lo = lo[k];
			tc_live[tc_nlive].hi = hi[k];
			tc_nlive++;
		}
	}
	tc_live_lxid = MyProc->lxid;
	tc_live_cid = cid;
}

/*
 * Is the OID within the bounds a live session of the database publishes
 * right now?  For the leftover sweep, which must decide with the bounds read
 * after it saw the row: a session publishes them before inserting rows, so
 * any committed row of a live session is covered.  (The sweep runs while
 * sessions keep creating objects, so bounds collected before it started
 * are too old.)
 */
static bool
tempcat_oid_live_now(Oid oid, Oid dbid)
{
	int			i;

	if (!IsTempcatOid(oid))
		return false;
	pg_read_barrier();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		/* (our own objects count: the sweep can run in a session with some) */
		if (st->databaseId != dbid)
			continue;
		if (tempcat_bounds_cover(st, oid))
			return true;
	}
	return false;
}

/*
 * May the OID belong to a temporary object that a live session of the
 * database keeps in memory?  For pgstat_vacuum_stat(), which must not purge
 * the statistics of objects it cannot see.
 */
bool
tempcat_oid_maybe_live(Oid oid, Oid dbid)
{
	return TempcatShared != NULL && tempcat_oid_live_now(oid, dbid);
}

static bool
tempcat_oid_live_elsewhere(Oid oid, Oid dbid)
{
	int			i;

	if (!IsTempcatOid(oid))
		return false;
	for (i = 0; i < tc_nlive; i++)
	{
		if (tc_live[i].databaseId == dbid &&
			oid >= tc_live[i].lo && oid <= tc_live[i].hi)
			return true;
	}
	return false;
}

/*
 * An object's row by OID, as of the snapshot of the query that is running,
 * if any: a row that a scan returns must be judged by the same snapshot
 * (e.g. pg_dump's, which sees objects dropped since it started).
 */
static HeapTuple
tempcat_fetch_object_row(Relation rel, Oid indexId, AttrNumber oidattr,
						 Oid objectId)
{
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;

	ScanKeyInit(&key, oidattr, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));
	scan = systable_beginscan(rel, indexId, true,
							  ActiveSnapshotSet() ? GetActiveSnapshot() : NULL,
							  1, &key);
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
		tup = heap_copytuple(tup);
	systable_endscan(scan);
	return tup;
}

/*
 * Is the object an ordinary one rather than a temporary object of another
 * session?  An ordinary object has its catalog row on disk, outside the
 * temporary schemas.  A temporary object of another session has its row in
 * that session's memory, where we cannot see it, or on disk in that
 * session's temporary schema if the row did not fit in memory.  Follow the
 * owner columns of the catalogs (a relation's or a type's namespace, a
 * trigger's relation, ...) up to the namespace.
 */
static bool
tempcat_ordinary_object(Oid classId, Oid objectId, int depth)
{
	const TempcatCatalogDef *def;
	int			catidx;
	Relation	rel;
	HeapTuple	tup;
	AttrNumber	oidattr;
	AttrNumber	parentattr;
	Oid			parentclass;
	Oid			parent = InvalidOid;
	bool		found;

	/* (chains are short: attribute default -> relation -> namespace) */
	if (!IsTempcatOid(objectId) || depth > 3)
		return true;

	if (classId == NamespaceRelationId)
	{
		bool		ordinary = false;

		rel = table_open(NamespaceRelationId, AccessShareLock);
		tup = tempcat_fetch_object_row(rel, NamespaceOidIndexId,
									   Anum_pg_namespace_oid, objectId);
		table_close(rel, AccessShareLock);
		if (HeapTupleIsValid(tup))
		{
			const char *nspname = NameStr(((Form_pg_namespace) GETSTRUCT(tup))->nspname);

			ordinary = strncmp(nspname, "pg_temp_", 8) != 0 &&
				strncmp(nspname, "pg_toast_temp_", 14) != 0;
			heap_freetuple(tup);
		}
		return ordinary;
	}

	/* Objects of other classes are never kept in memory. */
	catidx = tempcat_catalog_index(classId);
	if (catidx < 0 ||
		(classId != AttrDefaultRelationId && !is_objectclass_supported(classId)))
		return true;
	def = &tempcat_catalogs[catidx];

	oidattr = classId == AttrDefaultRelationId ?
		Anum_pg_attrdef_oid : get_object_attnum_oid(classId);
	if (def->owner == oidattr)
	{
		/* a relation or a type: its own row decides */
		parentattr = get_object_attnum_namespace(classId);
		parentclass = NamespaceRelationId;
	}
	else
	{
		parentattr = def->owner;
		parentclass = def->ownerclass;
	}

	rel = table_open(classId, AccessShareLock);
	/* (pg_attrdef is not in objectaddress.c's ObjectProperty in this version) */
	tup = tempcat_fetch_object_row(rel,
								   classId == AttrDefaultRelationId ?
								   AttrDefaultOidIndexId : get_object_oid_index(classId),
								   oidattr, objectId);

	found = HeapTupleIsValid(tup);
	if (found)
	{
		parent = tempcat_getoid(tup, RelationGetDescr(rel), parentattr);
		if (classId == ConstraintRelationId && !OidIsValid(parent))
		{
			/* domain constraint */
			parent = tempcat_getoid(tup, RelationGetDescr(rel),
									Anum_pg_constraint_contypid);
			parentclass = TypeRelationId;
		}
		heap_freetuple(tup);
	}
	table_close(rel, AccessShareLock);

	/* in another session's memory */
	if (!found)
		return false;
	return tempcat_ordinary_object(parentclass, parent, depth + 1);
}

/*
 * Does the owner column of an on-disk row point at a temporary object of
 * another session, live or gone?
 */
static bool
tempcat_owner_hidden(Oid classId, Oid objectId, Oid dbid)
{
	if (!IsTempcatOid(objectId))
		return false;
	/* (we cannot look up objects of other databases: pg_shdepend) */
	if (dbid != MyDatabaseId)
		return tempcat_oid_live_elsewhere(objectId, dbid);
	/* (we see our own objects, wherever their rows are) */
	if (OidIsValid(classId) && tempcat_session_owns_oid(classId, objectId))
		return false;
	return !tempcat_ordinary_object(classId, objectId, 0);
}

/*
 * Should a SQL scan skip this on-disk row of 'rel'?  Only when
 * gp_temp_memory_catalog_hide_others is set; see above.
 */
bool
tempcat_hide_disk_row(Relation rel, HeapTuple tup)
{
	Oid			relid = RelationGetRelid(rel);
	TupleDesc	desc = RelationGetDescr(rel);
	const TempcatCatalogDef *def;
	int			catidx;
	Oid			dbid = MyDatabaseId;
	Oid			owner;
	Oid			ownerclass;

	if (!gp_temp_memory_catalog_hide_others || TempcatShared == NULL)
		return false;
	catidx = tempcat_catalog_index(relid);
	if (catidx < 0)
		return false;
	def = &tempcat_catalogs[catidx];

	tempcat_collect_live_ranges();

	if (relid == SharedDependRelationId)
		dbid = tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid);

	owner = tempcat_getoid(tup, desc, def->owner);
	ownerclass = def->ownerclassattr != 0 ?
		tempcat_getoid(tup, desc, def->ownerclassattr) : def->ownerclass;
	if (relid == ConstraintRelationId && !OidIsValid(owner))
	{
		owner = tempcat_getoid(tup, desc, Anum_pg_constraint_contypid);
		ownerclass = TypeRelationId;
	}

	return tempcat_owner_hidden(ownerclass, owner, dbid) ||
		(def->owner2 != 0 &&
		 tempcat_owner_hidden(def->owner2classattr != 0 ?
							  tempcat_getoid(tup, desc, def->owner2classattr) :
							  def->owner2class,
							  tempcat_getoid(tup, desc, def->owner2), dbid));
}

bool
tempcat_hide_disk_slot(Relation rel, TupleTableSlot *slot)
{
	return tempcat_hide_disk_row(rel, ExecFetchSlotHeapTuple(slot, false, NULL));
}

/* ----------------------------------------------------------------
 * Leftovers of crashed sessions
 *
 * A session that ends normally drops its temporary objects, removing their
 * on-disk rows too: dependencies on ordinary objects (pg_depend,
 * pg_shdepend) and rows that went to disk because the in-memory area was
 * full.  A crashed session leaves them behind, pointing at objects whose
 * main catalog rows were only in memory.  (Their files are removed when the
 * node restarts after the crash.)
 *
 * The first session of a database that starts keeping temporary objects in
 * memory after the node started removes such rows: at that point no live
 * session of the database has in-memory objects, so on-disk rows that refer
 * to reserved OIDs whose objects do not exist on disk are garbage.
 * Ordinary objects with reserved OIDs (see the file header) exist on disk
 * and are left alone.
 * ----------------------------------------------------------------
 */

static bool
tempcat_db_swept(Oid dbid)
{
	bool		found = false;
	int			i;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nswept; i++)
	{
		if (TempcatShared->swept[i] == dbid)
		{
			found = true;
			break;
		}
	}
	SpinLockRelease(&TempcatShared->mutex);
	return found;
}

/* Ask for another sweep of the database. */
static void
tempcat_unmark_swept(Oid dbid)
{
	int			i;

	SpinLockAcquire(&TempcatShared->mutex);
	/* (counted even if not marked: a sweep may be about to mark it) */
	TempcatShared->nunmarks++;
	for (i = 0; i < TempcatShared->nswept; i++)
	{
		if (TempcatShared->swept[i] == dbid)
		{
			TempcatShared->swept[i] = TempcatShared->swept[--TempcatShared->nswept];
			break;
		}
	}
	SpinLockRelease(&TempcatShared->mutex);
}

/* The count that tempcat_mark_swept() checks; see there. */
static uint64
tempcat_unmark_count(void)
{
	uint64		n;

	SpinLockAcquire(&TempcatShared->mutex);
	n = TempcatShared->nunmarks;
	SpinLockRelease(&TempcatShared->mutex);
	return n;
}

static bool
tempcat_mark_swept(Oid dbid, uint64 nunmarks)
{
	int			i;

	/*
	 * Not if someone asked for a sweep since 'nunmarks' was read, before the
	 * sweep began: a session that leaves leftovers stops protecting them
	 * (clears its OID bounds) before it asks, so the sweep may have skipped
	 * them as rows of a live session.  The next sweep removes them.
	 */
	SpinLockAcquire(&TempcatShared->mutex);
	if (TempcatShared->nunmarks != nunmarks)
	{
		SpinLockRelease(&TempcatShared->mutex);
		return false;
	}

	/*
	 * Once only, even if forced to sweep again (tempcat_unmark_swept()
	 * removes one entry).  If the list is full, replace the entries in turn:
	 * the databases they named are swept again, which is cheap (see
	 * tempcat_sweep_catalog()).
	 */
	for (i = 0; i < TempcatShared->nswept; i++)
	{
		if (TempcatShared->swept[i] == dbid)
		{
			SpinLockRelease(&TempcatShared->mutex);
			return true;
		}
	}
	if (TempcatShared->nswept < TEMPCAT_MAX_SWEPT_DBS)
		TempcatShared->swept[TempcatShared->nswept++] = dbid;
	else
	{
		TempcatShared->swept[TempcatShared->nextswept] = dbid;
		TempcatShared->nextswept = (TempcatShared->nextswept + 1) % TEMPCAT_MAX_SWEPT_DBS;
	}
	SpinLockRelease(&TempcatShared->mutex);
	return true;
}

/*
 * Delete an on-disk leftover row, unless somebody else deleted or updated it
 * meanwhile (its session at exit, or another sweep): the sweep runs inside
 * users' commands, which must not fail because of it.  Nor wait: if a
 * transaction still in progress deletes the row (another sweep that has
 * not committed yet, or a transaction prepared by a session that is gone),
 * leave it alone, and the database to be swept again in case that
 * transaction aborts (tc_sweep_incomplete).  Rows in our own memory are
 * never leftovers.
 */
static bool tc_sweep_incomplete = false;

static bool
tempcat_sweep_delete(Relation rel, HeapTuple tup)
{
	TM_Result	result;
	TM_FailureData tmfd;

	if (IsTempcatTid(&tup->t_self))
		return false;

	result = heap_delete(rel, &tup->t_self, GetCurrentCommandId(true),
						 InvalidSnapshot, false /* do not wait */ ,
						 &tmfd, false /* changingPart */ );
	switch (result)
	{
		case TM_Ok:
			return true;
		case TM_BeingModified:
			tc_sweep_incomplete = true;
			return false;
		case TM_SelfModified:
		case TM_Updated:
		case TM_Deleted:
			return false;
		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			return false;		/* keep compiler quiet */
	}
}

/*
 * The sweep scans with a dirty snapshot, which also returns rows that a
 * transaction still in progress inserted.  Outside the OID bounds of live
 * sessions, and for objects that do not exist (both checked first: a
 * transaction changing an ordinary object with a reserved OID inserts such
 * rows too), those are leftovers to be: rows of a transaction prepared by a
 * QE writer that is gone, which distributed recovery may still commit.  They
 * cannot be deleted yet; leave the database to be swept again.  Call after
 * fetching the row, before fetching the next one.
 */
static bool
tempcat_sweep_row_in_progress(Snapshot dirty)
{
	if (!TransactionIdIsValid(dirty->xmin))
		return false;
	tc_sweep_incomplete = true;
	return true;
}

/* Does the object exist in the on-disk catalogs? */
static bool
tempcat_disk_object_exists(Oid classId, Oid objectId)
{
	switch (classId)
	{
		case RelationRelationId:
			return SearchSysCacheExists1(RELOID, ObjectIdGetDatum(objectId));
		case TypeRelationId:
			return SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(objectId));
		case NamespaceRelationId:
			return SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(objectId));
		default:
			break;
	}

	/* Keep rows of object classes we cannot look up. */
	return tempcat_object_lookup(classId, objectId) != TCO_MISSING;
}

/*
 * Classes of the objects that can be kept in memory (see also
 * tempcat_catalog_has_oid())
 */
static const Oid tempcat_object_classes[] =
{
	RelationRelationId, TypeRelationId, NamespaceRelationId,
	AttrDefaultRelationId, ConstraintRelationId, RewriteRelationId,
	TriggerRelationId, PolicyRelationId, ProcedureRelationId,
	OperatorRelationId, StatisticExtRelationId
};

/* A btree index of 'rel' whose leading key columns are 'atts', if any. */
static Oid
tempcat_leading_index(Relation rel, int natts, const AttrNumber *atts)
{
	List	   *indexes = RelationGetIndexList(rel);
	ListCell   *lc;
	Oid			result = InvalidOid;

	foreach(lc, indexes)
	{
		Relation	irel = index_open(lfirst_oid(lc), AccessShareLock);
		int			i;

		if (irel->rd_rel->relam == BTREE_AM_OID &&
			irel->rd_index->indisvalid &&
			IndexRelationGetNumberOfKeyAttributes(irel) >= natts)
		{
			for (i = 0; i < natts; i++)
			{
				if (irel->rd_index->indkey.values[i] != atts[i])
					break;
			}
			if (i == natts)
				result = RelationGetRelid(irel);
		}
		index_close(irel, AccessShareLock);
		if (OidIsValid(result))
			break;
	}
	list_free(indexes);
	return result;
}

/*
 * Delete the rows of 'catalog' whose column 'attr' holds a reserved OID of
 * an object (of class 'objclass', or of the class in column 'classattr')
 * that does not exist on disk.  For pg_shdepend, only rows of this
 * database.  For pg_constraint, 'domain' selects domain constraints
 * (conrelid = 0, owner in contypid).
 */
static int
tempcat_sweep_catalog(Oid catalog, AttrNumber attr, Oid objclass,
					  AttrNumber classattr, bool domain)
{
	Relation	rel;
	TupleDesc	desc;
	SysScanDesc scan;
	ScanKeyData key[3];
	AttrNumber	atts[3];
	AttrNumber	keyattr = attr;
	int			nkeys = 0;
	Oid			indexId;
	int			nclasses = 1;
	int			c;
	HeapTuple	tup;
	int			ndeleted = 0;
	SnapshotData dirty;

	InitDirtySnapshot(dirty);
	rel = table_open(catalog, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/*
	 * Scan only the rows with reserved OIDs, with an index on the column if
	 * there is one: the catalogs (pg_attribute, pg_depend, ...) can be large.
	 * The class of a dependency-like row (pg_depend.classid, ...) precedes
	 * the object in its index, so scan per class of temporary objects then.
	 */
	atts[0] = attr;
	indexId = tempcat_leading_index(rel, 1, atts);
	if (!OidIsValid(indexId) &&
		(catalog == ProcedureRelationId || catalog == OperatorRelationId))
	{
		/* (functions and operators in a reserved namespace have reserved OIDs) */
		atts[0] = keyattr = catalog == ProcedureRelationId ?
			Anum_pg_proc_oid : Anum_pg_operator_oid;
		indexId = tempcat_leading_index(rel, 1, atts);
	}
	if (!OidIsValid(indexId) && classattr != 0)
	{
		if (catalog == SharedDependRelationId)
			atts[nkeys++] = Anum_pg_shdepend_dbid;
		atts[nkeys++] = classattr;
		atts[nkeys++] = attr;
		indexId = tempcat_leading_index(rel, nkeys, atts);
		if (OidIsValid(indexId))
			nclasses = lengthof(tempcat_object_classes);
		else
			nkeys = 0;
	}

	for (c = 0; c < nclasses; c++)
	{
		int			k = 0;

		if (nkeys > 0)
		{
			if (catalog == SharedDependRelationId)
				ScanKeyInit(&key[k++], Anum_pg_shdepend_dbid, BTEqualStrategyNumber,
							F_OIDEQ, ObjectIdGetDatum(MyDatabaseId));
			ScanKeyInit(&key[k++], classattr, BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(tempcat_object_classes[c]));
		}
		ScanKeyInit(&key[k++], keyattr, BTGreaterEqualStrategyNumber, F_OIDGE,
					ObjectIdGetDatum(FirstTempcatObjectId));

		scan = systable_beginscan(rel, indexId, OidIsValid(indexId), &dirty, k, key);
		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Oid			objid = tempcat_getoid(tup, desc, attr);
			Oid			cls = classattr != 0 ? tempcat_getoid(tup, desc, classattr) : objclass;

			CHECK_FOR_INTERRUPTS();

			if (!IsTempcatOid(objid))
				continue;
			if (catalog == SharedDependRelationId &&
				tempcat_getoid(tup, desc, Anum_pg_shdepend_dbid) != MyDatabaseId)
				continue;
			if (catalog == ConstraintRelationId &&
				OidIsValid(tempcat_getoid(tup, desc, Anum_pg_constraint_conrelid)) == domain)
				continue;
			if (tempcat_oid_live_now(objid, MyDatabaseId) ||
				tempcat_disk_object_exists(cls, objid) ||
				tempcat_sweep_row_in_progress(&dirty))
				continue;

			if (tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
		systable_endscan(scan);
	}
	table_close(rel, RowExclusiveLock);

	/* Let later passes (e.g. pg_depend by refobjid) see the deletions. */
	CommandCounterIncrement();

	return ndeleted;
}

/*
 * The other live session of the database that would take 'nspname' as the
 * name of its temporary namespace (pg_temp_N or pg_toast_temp_N, N its
 * session ID), or NULL.  The session removes a left namespace of its name
 * itself, in its own transaction, when it comes to create its own
 * (InitTempTableNamespace()).  Were it removed by a sweep elsewhere, the
 * session would find the row still there until that sweep's transaction
 * ended, and fail.  (Not the names of utility mode, a 0 and a backend ID,
 * which any backend may hold.)
 */
static PGPROC *
tempcat_namespace_claimant(const char *nspname, int *sessionId)
{
	const char *digits;
	char	   *end;
	long		n;
	PGPROC	   *proc;

	if (strncmp(nspname, "pg_temp_", 8) == 0)
		digits = nspname + 8;
	else if (strncmp(nspname, "pg_toast_temp_", 14) == 0)
		digits = nspname + 14;
	else
		return NULL;
	if (!isdigit((unsigned char) digits[0]))
		return NULL;
	n = strtol(digits, &end, 10);
	if (*end != '\0' || n <= 0 || n > INT_MAX || digits[0] == '0')
		return NULL;

	proc = FindProcByGpSessionId(n);
	*sessionId = (int) n;
	return proc != NULL && proc->databaseId == MyDatabaseId ? proc : NULL;
}

/*
 * A sweep leaves a left temporary namespace to the live session
 * 'sessionId', in 'proc', that would take its name
 * (tempcat_namespace_claimant()).  Remember the session, so that the
 * database is swept again once it is gone, in case it never took the name
 * (tempcat_check_claims()); asking for another sweep right away would make
 * every DROP sweep while the session lives.  The session is the one of the
 * name: 'proc' may have been taken by another one since it was found, and
 * then the claim counts as gone.  Returns false if there is no room: then
 * the database must not count as swept.
 */
static bool
tempcat_note_claim(PGPROC *proc, int sessionId)
{
	bool		noted = false;
	int			i;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nclaims; i++)
	{
		TempcatClaim *c = &TempcatShared->claims[i];

		if (c->databaseId == MyDatabaseId && c->pgprocno == proc->pgprocno &&
			c->sessionId == sessionId)
		{
			noted = true;
			break;
		}
	}
	if (!noted && TempcatShared->nclaims < TEMPCAT_MAX_CLAIMS)
	{
		TempcatClaim *c = &TempcatShared->claims[TempcatShared->nclaims++];

		c->databaseId = MyDatabaseId;
		c->pgprocno = proc->pgprocno;
		c->sessionId = sessionId;
		noted = true;
	}
	SpinLockRelease(&TempcatShared->mutex);
	return noted;
}

/*
 * Is a namespace named 'nspname' left, for a sweep: one with a reserved OID
 * that no live session publishes?  (A session's own namespace has an
 * ordinary OID, or one within its bounds.)
 */
static bool
tempcat_namespace_left(const char *nspname)
{
	Oid			nspid = GetSysCacheOid1(NAMESPACENAME, Anum_pg_namespace_oid,
										CStringGetDatum(nspname));

	return OidIsValid(nspid) && IsTempcatOid(nspid) &&
		!tempcat_oid_live_now(nspid, MyDatabaseId);
}

/*
 * Ask for another sweep of this database if a session that a sweep left a
 * namespace to is gone (see tempcat_note_claim()).  Its PGPROC has been
 * given up, or taken by another session, then.  Forget the claims of
 * sessions that took their names (their transactions that removed the left
 * namespaces committed), which need no sweep, and would keep others from
 * being noted.
 */
static void
tempcat_check_claims(void)
{
	TempcatClaim live[TEMPCAT_MAX_CLAIMS];
	int			nlive = 0;
	bool		gone = false;
	int			i;

	/* (a claim noted meanwhile is checked the next time) */
	if (TempcatShared->nclaims == 0)
		return;

	SpinLockAcquire(&TempcatShared->mutex);
	for (i = 0; i < TempcatShared->nclaims; i++)
	{
		TempcatClaim *c = &TempcatShared->claims[i];
		PGPROC	   *proc = &ProcGlobal->allProcs[c->pgprocno];

		if (c->databaseId != MyDatabaseId)
			continue;
		if (proc->pid != 0 && proc->mppSessionId == c->sessionId &&
			proc->databaseId == c->databaseId)
		{
			live[nlive++] = *c;
			continue;
		}
		*c = TempcatShared->claims[--TempcatShared->nclaims];
		i--;
		gone = true;
	}
	SpinLockRelease(&TempcatShared->mutex);

	for (i = 0; i < nlive; i++)
	{
		char		nspname[NAMEDATALEN];
		char		toastname[NAMEDATALEN];
		int			j;

		snprintf(nspname, sizeof(nspname), "pg_temp_%d", live[i].sessionId);
		snprintf(toastname, sizeof(toastname), "pg_toast_temp_%d",
				 live[i].sessionId);
		if (tempcat_namespace_left(nspname) || tempcat_namespace_left(toastname))
			continue;

		SpinLockAcquire(&TempcatShared->mutex);
		for (j = 0; j < TempcatShared->nclaims; j++)
		{
			TempcatClaim *c = &TempcatShared->claims[j];

			if (c->databaseId == live[i].databaseId &&
				c->pgprocno == live[i].pgprocno &&
				c->sessionId == live[i].sessionId)
			{
				*c = TempcatShared->claims[--TempcatShared->nclaims];
				break;
			}
		}
		SpinLockRelease(&TempcatShared->mutex);
	}

	if (gone)
		tempcat_unmark_swept(MyDatabaseId);
}

/*
 * Delete objects that went to disk when the in-memory area was full but
 * whose namespace did not: temporary namespaces with reserved OIDs, and
 * relations and types with reserved OIDs in a namespace that is gone.
 * The namespaces that live backends would take are left to them.
 */
static int
tempcat_sweep_objects(void)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tup;
	int			ndeleted = 0;
	SnapshotData dirty;

	InitDirtySnapshot(dirty);
	rel = table_open(NamespaceRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_namespace_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, NamespaceOidIndexId, true, &dirty, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_namespace form = (Form_pg_namespace) GETSTRUCT(tup);

		CHECK_FOR_INTERRUPTS();
		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			(strncmp(NameStr(form->nspname), "pg_temp_", 8) == 0 ||
			 strncmp(NameStr(form->nspname), "pg_toast_temp_", 14) == 0))
		{
			/*
			 * Left to the live session that would take its name: sweep
			 * again once it is gone, in case it never does (with its
			 * objects, which stay as long as their namespace).
			 */
			PGPROC	   *claimant;
			int			sessionId;

			claimant = tempcat_namespace_claimant(NameStr(form->nspname),
												  &sessionId);
			if (claimant != NULL)
			{
				if (!tempcat_note_claim(claimant, sessionId))
					tc_sweep_incomplete = true;
			}
			else if (!tempcat_sweep_row_in_progress(&dirty) &&
					 tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
	CommandCounterIncrement();

	rel = table_open(RelationRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_class_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, ClassOidIndexId, true, &dirty, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_class form = (Form_pg_class) GETSTRUCT(tup);

		CHECK_FOR_INTERRUPTS();
		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			!tempcat_disk_object_exists(NamespaceRelationId, form->relnamespace) &&
			!tempcat_sweep_row_in_progress(&dirty))
		{
			if (tempcat_sweep_delete(rel, tup))
			{
				tempcat_unlink_files(form);
				ndeleted++;
			}
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	rel = table_open(TypeRelationId, RowExclusiveLock);
	ScanKeyInit(&key, Anum_pg_type_oid, BTGreaterEqualStrategyNumber,
				F_OIDGE, ObjectIdGetDatum(FirstTempcatObjectId));
	scan = systable_beginscan(rel, TypeOidIndexId, true, &dirty, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_type form = (Form_pg_type) GETSTRUCT(tup);

		CHECK_FOR_INTERRUPTS();
		if (IsTempcatOid(form->oid) &&
			!tempcat_oid_live_now(form->oid, MyDatabaseId) &&
			!tempcat_disk_object_exists(NamespaceRelationId, form->typnamespace) &&
			!tempcat_sweep_row_in_progress(&dirty))
		{
			if (tempcat_sweep_delete(rel, tup))
				ndeleted++;
		}
	}
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
	CommandCounterIncrement();

	return ndeleted;
}

/* One pass over the catalogs whose rows are owned by another object. */
static int
tempcat_sweep_owned_rows(void)
{
	int			ndeleted = 0;
	int			i;

	for (i = 0; i < TEMPCAT_NCATALOGS; i++)
	{
		const TempcatCatalogDef *def = &tempcat_catalogs[i];

		switch (def->relid)
		{
			case RelationRelationId:
			case TypeRelationId:
			case NamespaceRelationId:
			case DependRelationId:
			case DescriptionRelationId:
			case SharedDependRelationId:
				continue;
			default:
				break;
		}
		ndeleted += tempcat_sweep_catalog(def->relid, def->owner, def->ownerclass,
										  def->ownerclassattr, false);
		if (def->relid == ConstraintRelationId)
			ndeleted += tempcat_sweep_catalog(ConstraintRelationId,
											  Anum_pg_constraint_contypid,
											  TypeRelationId, 0, true);
	}
	return ndeleted;
}

static void
tempcat_sweep_orphans(void)
{
	int			ndeleted;
	int			nbefore;
	int			pass = 0;

	ndeleted = tempcat_sweep_objects();

	/*
	 * Rows owned by objects that are gone, except dependency-like rows.
	 * Repeat while something goes away: owners can be such rows themselves
	 * (e.g. pg_aggregate rows of pg_proc rows of a namespace that is gone).
	 */
	do
	{
		nbefore = ndeleted;
		ndeleted += tempcat_sweep_owned_rows();
	} while (ndeleted != nbefore && ++pass < 4);

	/* Then rows pointing at any object that is gone. */
	ndeleted += tempcat_sweep_catalog(DependRelationId, Anum_pg_depend_objid,
									  InvalidOid, Anum_pg_depend_classid, false);
	ndeleted += tempcat_sweep_catalog(DependRelationId, Anum_pg_depend_refobjid,
									  InvalidOid, Anum_pg_depend_refclassid, false);
	ndeleted += tempcat_sweep_catalog(DescriptionRelationId, Anum_pg_description_objoid,
									  InvalidOid, Anum_pg_description_classoid, false);
	ndeleted += tempcat_sweep_catalog(SharedDependRelationId, Anum_pg_shdepend_objid,
									  InvalidOid, Anum_pg_shdepend_classid, false);
	CommandCounterIncrement();

	if (ndeleted > 0)
		ereport(LOG,
				(errmsg("removed %d catalog rows left by crashed sessions' temporary objects",
						ndeleted)));
}

static void
tempcat_sweep_if_needed(bool allow_force)
{
	bool		force = false;

#ifdef FAULT_INJECTOR
	/* Test hook: sweep even if this database was swept already. */
	if (allow_force &&
		SIMPLE_FAULT_INJECTOR("tempcat_force_sweep") == FaultInjectorTypeSkip)
		force = true;
#endif

	tempcat_sweep_db(force, false);
}

/*
 * Sweep this database, unless swept already and not 'force'd to.  Unless
 * told to 'wait', give up if another session is sweeping.
 */
static void
tempcat_sweep_db(bool force, bool wait)
{
	tempcat_check_claims();
	if (!force && tempcat_db_swept(MyDatabaseId))
		return;

	/*
	 * One sweeper at a time, and nobody waits for it: if another session is
	 * sweeping, go on (its sweep may not have removed everything yet, which
	 * at worst makes a DROP fail like before).  Waiting, with the lock held
	 * until commit, deadlocked distributed transactions that took it on the
	 * segments in different orders; so release it right after the sweep.
	 * Waiting for it is safe then: a sweeper holds it only while it sweeps,
	 * and waits for no transaction meanwhile (see tempcat_sweep_catalog()).
	 */
	if (wait)
		LockDatabaseObject(DatabaseRelationId, MyDatabaseId,
						   TEMPCAT_SWEEP_LOCK_SUBID, ExclusiveLock);
	else if (!ConditionalLockDatabaseObject(DatabaseRelationId, MyDatabaseId,
											TEMPCAT_SWEEP_LOCK_SUBID,
											ExclusiveLock))
		return;

	if (force || !tempcat_db_swept(MyDatabaseId))
	{
		uint64		nunmarks = tempcat_unmark_count();

		tc_sweep_incomplete = false;
		tempcat_sweep_orphans();
#ifdef FAULT_INJECTOR
		/* Test hook: let a session leave leftovers after the scans. */
		SIMPLE_FAULT_INJECTOR("tempcat_sweep_before_mark");
#endif
		if (tc_sweep_incomplete ||
			!tempcat_mark_swept(MyDatabaseId, nunmarks))
		{
			/*
			 * Not even marked by an earlier sweep, if forced to run again:
			 * the next one finishes.  (And leave the tracking of the earlier
			 * sweep as it is.)
			 */
			if (tc_sweep_incomplete)
				tempcat_unmark_swept(MyDatabaseId);
			UnlockDatabaseObject(DatabaseRelationId, MyDatabaseId,
								 TEMPCAT_SWEEP_LOCK_SUBID, ExclusiveLock);
			return;
		}

		/* If the deletions do not commit, sweep again; see below. */
		if (!tc_sweep_callbacks)
		{
			RegisterXactCallback(tempcat_sweep_xact_callback, NULL);
			RegisterSubXactCallback(tempcat_sweep_subxact_callback, NULL);
			before_shmem_exit(tempcat_sweep_exit, 0);
			tc_sweep_callbacks = true;
		}
		tc_sweep_xid = GetCurrentTransactionIdIfAny();
		tc_sweep_subid = GetCurrentSubTransactionId();
		tc_sweep_db = MyDatabaseId;
	}

	UnlockDatabaseObject(DatabaseRelationId, MyDatabaseId,
						 TEMPCAT_SWEEP_LOCK_SUBID, ExclusiveLock);
}

/* The sweep's (sub)transaction ended: stop tracking it. */
static void
tempcat_sweep_resolved(bool committed)
{
	if (!committed)
		tempcat_unmark_swept(tc_sweep_db);
	tc_sweep_xid = InvalidTransactionId;
	tc_sweep_subid = InvalidSubTransactionId;
	tc_sweep_db = InvalidOid;
}

/*
 * A sweep marks the database as swept before its transaction ends.  If the
 * (sub)transaction aborts, ask for another sweep right away: the session
 * may end before it starts another transaction.  The outcome of a prepared
 * transaction is checked at the start of later transactions and at exit.
 */
static void
tempcat_sweep_xact_callback(XactEvent event, void *arg)
{
	if (!OidIsValid(tc_sweep_db))
		return;
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:

			/*
			 * Unless the sweep's transaction was prepared: the transaction
			 * that finishes it with COMMIT or ROLLBACK PREPARED commits
			 * either way (on a QE, the one that runs ABORT PREPARED).
			 */
			if (TransactionIdIsValid(tc_sweep_xid) &&
				!TransactionIdIsCurrentTransactionId(tc_sweep_xid))
				break;
			tempcat_sweep_resolved(true);
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			tempcat_sweep_resolved(false);
			break;
		default:
			break;
	}
}

static void
tempcat_sweep_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
							   SubTransactionId parentSubid, void *arg)
{
	/* (only if the sweep ran in this subtransaction or a child of it) */
	if (!OidIsValid(tc_sweep_db) || tc_sweep_subid < mySubid)
		return;
	if (event == SUBXACT_EVENT_ABORT_SUB)
		tempcat_sweep_resolved(false);
	else if (event == SUBXACT_EVENT_COMMIT_SUB)
		tc_sweep_subid = parentSubid;
}

static void
tempcat_sweep_exit(int code, Datum arg)
{
	if (OidIsValid(tc_sweep_db) &&
		(!TransactionIdIsValid(tc_sweep_xid) ||
		 tempcat_xid_status(tc_sweep_xid) != TCX_COMMITTED))
		tempcat_unmark_swept(tc_sweep_db);
}

/*
 * Called by autovacuum in each database it visits, so that leftovers of
 * crashed sessions do not wait for a session to keep temporary objects in
 * memory (they make pg_dump fail, and keep ordinary objects from being
 * dropped).  Must be called in a transaction.
 */
void
tempcat_autovacuum_sweep(void)
{
	if (TempcatShared == NULL || IsBinaryUpgrade)
		return;
#ifdef FAULT_INJECTOR
	/* Test hook: leave leftovers to other sweeps. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_skip_autovacuum_sweep") == FaultInjectorTypeSkip)
		return;
#endif
	tempcat_sweep_if_needed(false);
}

/*
 * Called before dropping objects (and roles): leftover dependencies of
 * temporary objects that no longer exist would otherwise block the drop
 * until the next sweep.  A no-op unless the database needs a sweep, i.e.
 * after the node started or a session could not clean up at exit.
 */
void
tempcat_sweep_before_drop(void)
{
	if (TempcatShared == NULL || IsBinaryUpgrade || !IsUnderPostmaster ||
		RecoveryInProgress())
		return;
	tempcat_sweep_if_needed(false);
}

/*
 * The transaction in progress that deletes the namespace's row, if any.
 * '*found' says whether the row is there at all (else a deletion committed).
 */
static TransactionId
tempcat_namespace_deleter(Oid namespaceId, bool *found)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key;
	SnapshotData dirty;
	TransactionId deleter = InvalidTransactionId;

	InitDirtySnapshot(dirty);
	rel = table_open(NamespaceRelationId, AccessShareLock);
	ScanKeyInit(&key, Anum_pg_namespace_oid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(namespaceId));
	scan = systable_beginscan(rel, NamespaceOidIndexId, true, &dirty, 1, &key);
	*found = HeapTupleIsValid(systable_getnext(scan));
	if (*found)
		deleter = dirty.xmax;
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	return deleter;
}

/*
 * InitTempTableNamespace() found the temporary namespace 'namespaceName',
 * 'namespaceId', left on disk by a session that is gone (its area could not
 * be created, so the namespace went to disk), and is about to remove it.
 * With a reserved OID, that is the sweep's to do: RemoveTempRelations()
 * would take its objects for another session's (see
 * tempcat_object_missing()), and the sweep that performDeletion() runs
 * first would remove the namespace under it.  Sweep now, even if the
 * database was swept already, waiting for a sweep under way (after a
 * restart, many sessions may come for their namespaces at once).  If
 * another transaction deletes it, e.g. a sweep that did not know this session
 * yet (on a segment, before its writer started), wait for that transaction
 * and look again (tempcat_wait_for_deleter()).  Returns the namespace's OID
 * as found then, which the caller removes as usual if valid: if it still
 * has a reserved OID, e.g. inserted by a transaction in progress, fail
 * rather than try that.
 */
static bool tempcat_wait_for_deleter(TransactionId xid);

Oid
tempcat_sweep_leftover_namespace(const char *namespaceName, Oid namespaceId)
{
	if (!IsTempcatOid(namespaceId) ||
		TempcatShared == NULL || IsBinaryUpgrade || !IsUnderPostmaster ||
		RecoveryInProgress())
		return namespaceId;

	for (;;)
	{
		TransactionId deleter;
		bool		found;

		tempcat_sweep_db(true, true);
		CommandCounterIncrement();

		namespaceId = get_namespace_oid(namespaceName, true);
		if (!IsTempcatOid(namespaceId))
			return namespaceId;

#ifdef FAULT_INJECTOR
		/* Test hook: let another transaction remove it before we look on. */
		SIMPLE_FAULT_INJECTOR("tempcat_before_namespace_deleter");
#endif

		/*
		 * Gone already, though the catalog cache still had it: its deletion
		 * committed since we last took invalidations.  Look again.
		 */
		deleter = tempcat_namespace_deleter(namespaceId, &found);
		if (!found)
		{
			AcceptInvalidationMessages();
			continue;
		}

		/* (if the deleter aborts, the next round sweeps the namespace) */
		if (!TransactionIdIsValid(deleter) ||
			!tempcat_wait_for_deleter(deleter))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("temporary schema \"%s\" left by an earlier session is not removed yet",
							namespaceName),
					 errhint("Try again.")));
		AcceptInvalidationMessages();
	}
}

/*
 * Wait for the transaction 'xid' to end.  On a segment, only as long as
 * deadlock_timeout, and return false if it has not ended by then: that
 * transaction may itself be waiting for ours, on another node (e.g. on the
 * coordinator for a lock we took earlier in our transaction), and no
 * deadlock detector sees such a cycle.  (Locks are waited for on the
 * coordinator, whose detector sees the waits there.)  So on the coordinator
 * too, once our transaction has a part on a segment: that transaction may
 * be waiting there for ours, e.g. for a key we inserted.
 */
static bool
tempcat_wait_for_deleter(TransactionId xid)
{
	TimestampTz start;

	if (Gp_role != GP_ROLE_EXECUTE && MyTmGxactLocal->dtxSegments == NIL)
	{
		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		return true;
	}

	start = GetCurrentTimestamp();
	while (!ConditionalXactLockTableWait(xid))
	{
		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(),
									   DeadlockTimeout))
			return false;
		CHECK_FOR_INTERRUPTS();
		pg_usleep(10000L);
	}
	return true;
}

/* ----------------------------------------------------------------
 * SQL-level catalog scans
 *
 * Catalog queries see the virtual rows through sequential and bitmap
 * scans, which return them after the on-disk rows (nodeSeqscan.c,
 * nodeBitmapHeapscan.c), and plain and index-only scans, which merge them in
 * index order (nodeIndexscan.c, nodeIndexonlyscan.c), across mark and
 * restore too.  So plans need not care whether, or since when, a session
 * keeps temporary objects in memory, in its writer or in its readers.
 *
 * Only gp_temp_memory_catalog_hide_others needs the heap rows, to tell those
 * of other sessions' temporary objects (tempcat_hide_disk_row()): while it is
 * set, the planner does not use index-only scans of such catalogs
 * (tempcat_plan_catalog_index_scans()), ORCA leaves queries on them to the
 * planner, and the executor refuses such scans in plans made before.
 * ----------------------------------------------------------------
 */
bool
tempcat_restrict_catalog_index_scans(Oid relid)
{
	if (tempcat_catalog_index(relid) < 0)
		return false;
	if (tc_activated || gp_temp_memory_catalog_hide_others)
		return true;
	/* a reader of a session that keeps temporary objects in memory */
	return tempcat_is_reader() && tempcat_attach(false);
}

/*
 * Plans made before gp_temp_memory_catalog_hide_others was turned on may
 * have index-only scans of such catalogs, which the executor then refuses:
 * have the cached ones made anew.  (Others, e.g. those of SQL functions,
 * live for the rest of the statement.)
 */
void
assign_gp_temp_memory_catalog_hide_others(bool newval, void *extra)
{
	if (newval && !gp_temp_memory_catalog_hide_others)
		ResetPlanCache();
}

/*
 * May the planner use index-only scans of catalog 'relid'?  Not while hiding
 * other sessions' temporary objects; see above.
 */
bool
tempcat_plan_catalog_index_scans(Oid relid)
{
	return !gp_temp_memory_catalog_hide_others ||
		tempcat_catalog_index(relid) < 0;
}

/* A scan without virtual rows, for scans that filter on-disk rows only. */
static TempcatScan
tempcat_empty_scan(void)
{
	TempcatScan scan = palloc0(sizeof(struct TempcatScanData));

	scan->tuples = palloc(sizeof(HeapTuple));
	return scan;
}

/* Virtual rows of 'rel' visible to the query snapshot, or NULL if none. */
TempcatScan
tempcat_begin_sql_scan(Relation rel, Snapshot snapshot)
{
	/* Developer option: look at the on-disk catalog only */
	if (gp_temp_memory_catalog_disk_only)
		return NULL;
	return tempcat_beginscan(rel, NULL, snapshot, 0, NULL);
}

HeapTuple
tempcat_next_virtual(TempcatScan scan)
{
	if (scan->next < scan->ntuples)
		return scan->tuples[scan->next++];
	return NULL;
}

/*
 * Virtual rows for an index scan, sorted in index order, or NULL if none.
 * Simple index scan keys are applied (translated to heap attributes); the
 * caller filters the rows by the complete index quals.
 */
TempcatScan
tempcat_begin_index_sql_scan(Relation rel, Relation irel, Snapshot snapshot,
							 ScanKey indexkeys, int nindexkeys)
{
	ScanKey		heapkeys;
	int			nheapkeys = 0;
	int			i;
	TempcatScan scan;

	if (tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return NULL;

	/*
	 * Developer options: look at the on-disk catalog only, and possibly
	 * filter it (the caller merges an empty scan, which does that).
	 */
	if (gp_temp_memory_catalog_disk_only)
		return gp_temp_memory_catalog_hide_others ? tempcat_empty_scan() : NULL;

	/*
	 * Nothing to merge in without an area (always, with the feature off).
	 * Index scans begin again at every rescan, e.g. in each loop of a nested
	 * loop join, in the query's memory context: allocate nothing then.
	 */
	if (!tempcat_attach(false))
		return gp_temp_memory_catalog_hide_others ? tempcat_empty_scan() : NULL;

	heapkeys = palloc(sizeof(ScanKeyData) * Max(nindexkeys, 1));
	for (i = 0; i < nindexkeys; i++)
	{
		ScanKey		k = &indexkeys[i];

		if (k->sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_ROW_MEMBER |
						   SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL |
						   SK_ORDER_BY))
			continue;
		if (k->sk_attno < 1 || k->sk_attno > IndexRelationGetNumberOfKeyAttributes(irel))
			continue;
		heapkeys[nheapkeys] = *k;
		heapkeys[nheapkeys].sk_attno = irel->rd_index->indkey.values[k->sk_attno - 1];
		nheapkeys++;
	}

	scan = tempcat_beginscan(rel, irel, snapshot, nheapkeys, heapkeys);
	pfree(heapkeys);			/* (the scan keeps no pointer to them) */
	if (scan == NULL && gp_temp_memory_catalog_hide_others)
		scan = tempcat_empty_scan();
	return scan;
}

/* Drop the rows for which keep() returns false. */
void
tempcat_scan_filter(TempcatScan scan, bool (*keep) (HeapTuple, void *), void *arg)
{
	int			i;
	int			n = 0;

	for (i = 0; i < scan->ntuples; i++)
	{
		if (keep(scan->tuples[i], arg))
			scan->tuples[n++] = scan->tuples[i];
		else
			pfree(scan->tuples[i]);
	}
	scan->ntuples = n;
}

/* Has the scan no rows to return (left)? */
bool
tempcat_scan_is_empty(TempcatScan scan)
{
	return scan->next >= scan->ntuples;
}

/* Next row in the given direction without consuming it, or NULL. */
HeapTuple
tempcat_scan_peek(TempcatScan scan, bool forward)
{
	if (scan->next >= scan->ntuples)
		return NULL;
	return scan->tuples[forward ? scan->next : scan->ntuples - 1 - scan->next];
}

void
tempcat_scan_advance(TempcatScan scan)
{
	scan->next++;
}

/* How many rows the merge took, and going back there (mark/restore). */
int
tempcat_scan_getpos(TempcatScan scan)
{
	return scan->next;
}

void
tempcat_scan_setpos(TempcatScan scan, int pos)
{
	Assert(pos >= 0 && pos <= scan->ntuples);
	scan->next = pos;
}

int
tempcat_scan_compare(TempcatScan scan, HeapTuple a, HeapTuple b)
{
	return tempcat_compare(scan, a, b);
}

/*
 * Like tempcat_scan_compare(), with an on-disk row as an index-only scan has
 * it: the values of the index's columns in a slot (in index column order).
 */
int
tempcat_scan_compare_index(TempcatScan scan, HeapTuple a, TupleTableSlot *islot)
{
	int			i;

	for (i = 0; i < scan->nidxkeys; i++)
	{
		AttrNumber	attno = scan->irel->rd_index->indkey.values[i];
		bool		anull;
		bool		bnull;
		Datum		ad = heap_getattr(a, attno, scan->heapdesc, &anull);
		Datum		bd = slot_getattr(islot, i + 1, &bnull);
		int			cmp;

		if (anull || bnull)
		{
			if (anull && bnull)
				continue;
			cmp = anull ? 1 : -1;	/* NULLS LAST */
		}
		else
			cmp = DatumGetInt32(FunctionCall2Coll(scan->procs[i],
												  scan->irel->rd_indcollation[i],
												  ad, bd));
		if (scan->irel->rd_indoption[i] & INDOPTION_DESC)
			cmp = -cmp;
		if (cmp != 0)
			return cmp;
	}
	return 0;
}

/*
 * Fetch a virtual row by TID for a TID scan, if visible to the snapshot.
 * Returns a palloc'd copy, or NULL.
 */
HeapTuple
tempcat_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	dsa_pointer ep;
	HeapTuple	result = NULL;
	bool		reader;

	if (gp_temp_memory_catalog_disk_only ||
		tempcat_catalog_index(RelationGetRelid(rel)) < 0 ||
		!tempcat_attach(false))
		return NULL;

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);

	ep = tempcat_find_tid(tid);
	if (DsaPointerIsValid(ep))
	{
		TempcatEntry *e = TC_ADDR(ep);

		if (tempcat_catalogs[e->catidx].relid == RelationGetRelid(rel) &&
			tempcat_entry_visible(e, snapshot))
			result = tempcat_entry_copy(e, RelationGetRelid(rel));
	}

	if (reader)
		LWLockRelease(&tc_root->lock);
	return result;
}
