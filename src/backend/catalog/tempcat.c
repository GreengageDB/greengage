/*-------------------------------------------------------------------------
 *
 * tempcat.c
 *	  In-memory catalog rows for temporary objects.
 *
 * Every temporary table leaves dead tuples in pg_class, pg_attribute,
 * pg_type, pg_depend and friends, on the coordinator and on every segment.
 * Applications that create and drop many temporary tables therefore bloat
 * the catalogs and keep autovacuum busy.  This module keeps the catalog rows
 * of temporary objects out of the on-disk catalogs, in memory private to
 * the session.
 *
 * Which rows are kept in memory is decided by the data, not by the code
 * path that writes them:
 *
 * - When gp_enable_temp_memory_catalog is on at the moment the session's
 *   temporary namespace is created, the namespace gets an OID from the
 *   reserved range FirstTempcatObjectId..LastTempcatObjectId.  Relations,
 *   types, functions and operators created in such a namespace, and the
 *   attribute defaults, constraints, rules, triggers and policies of such
 *   relations, get OIDs from the same range (see tempcat_want_temp_oid(),
 *   called from oid_dispatch.c).  The coordinator assigns the OIDs and
 *   dispatches them to the segments as usual, so all nodes agree on them.
 *
 * - The range is split into slices of TEMPCAT_SLICE_SIZE OIDs.  A session
 *   takes its OIDs from a slice of its own, which its coordinator backend
 *   takes when it creates the temporary namespace, so OIDs of concurrent
 *   sessions never collide and lock tags stay unique.  On every node, the
 *   process keeping the session's objects (the coordinator backend, or the
 *   writer QE on a segment) holds the slice for as long as it lives (see
 *   tempcat_claim_free_slice() and tempcat_claim_slice()).
 *
 * - A row inserted into one of the catalogs listed in tempcat_catalogs[] is
 *   kept in memory if its owner column (pg_attribute.attrelid,
 *   pg_index.indrelid, ...) holds an OID that this session assigned to a
 *   temporary object.  Ownership is tracked per (catalog, OID) in a
 *   backend-local set, filled when the OID is allocated (coordinator) or
 *   received from the coordinator (segments), so ordinary objects that
 *   happen to have OIDs in the reserved range (created before the range was
 *   reserved, or carried over by pg_upgrade) are never affected.
 *
 * - Rows that also reference another object (pg_depend, pg_inherits) are
 *   kept in memory only if that object is the session's temporary object
 *   too.  So dependencies of temporary objects on ordinary objects stay on
 *   disk and keep protecting those objects from DROP in other sessions;
 *   those find them (and refuse to drop the objects, see
 *   tempcat_foreign_object()), or recognize them as leftovers of a session
 *   that is gone and remove them.  Ordinary objects cannot depend on
 *   temporary objects in memory at all (tempcat_check_dependency()).
 *   pg_shdepend rows of the owners and grantees of temporary objects are
 *   kept in memory, the session publishes their roles instead, for DROP
 *   ROLE in other sessions (tempcat_publish_role()); beyond TEMPCAT_MAX_ROLES
 *   roles, they go to disk too.
 *
 * Nothing else of a temporary object kept in memory is ever on disk: other
 * sessions cannot see it at all.
 *
 * The rows live in a dynamic shared memory area owned by the session's
 * writer process.  Its handle is published in the writer's
 * SharedSnapshotSlot, so reader gangs and entry-db readers of the same
 * session attach to it and see the same rows, including uncommitted ones,
 * without any serialization.  The area grows in memory up to
 * gp_temp_memory_catalog_max_size, and beyond that, or when dynamic shared
 * memory runs short, with segments backed by files in pg_dynshmem (see
 * dsa_set_memory_limit()) up to gp_temp_memory_catalog_max_file_size more:
 * the kernel keeps their pages in memory as long as it can spare it.  A
 * session that needs more gets an error.
 *
 * Every row carries xmin/xmax/cmin/cmax and is checked with the usual MVCC
 * rules, so subtransactions, aborts and two-phase commit behave like on-disk
 * rows: an aborted insert simply never becomes visible.  Rows whose fate is
 * known are cleaned up by the writer when it goes idle, or at the start of
 * its next transaction.  Frozen inserts and in-place updates
 * (gp_fastsequence, relpages, ...) stay non-transactional, like on disk.
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

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/gp_partition_template.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_index.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_callback.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_stat_last_operation.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_encoding.h"
#include "catalog/tempcat.h"
#include "cdb/cdbendpoint.h"
#include "commands/tablespace.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/dsm.h"
#include "storage/dsm_impl.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/dsa.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sharedsnapshot.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/vmem_tracker.h"

/* GUCs */
bool		gp_enable_temp_memory_catalog = false;
int			gp_temp_memory_catalog_max_size = 16384;	/* kB, per writer */
int			gp_temp_memory_catalog_max_file_size = 1048576; /* kB, per writer */

/* ----------------------------------------------------------------
 * Catalogs that can hold rows in memory
 * ----------------------------------------------------------------
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
	 * A row is kept in memory if its owner column holds an OID that this
	 * session assigned to a temporary object of catalog 'ownerclass' (or of
	 * the catalog named by column 'ownerclassattr'), and, if 'owner2' is set,
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
 * Catalogs whose objects get reserved OIDs, whose pg_depend and pg_shdepend
 * rows on disk may refer to them (see tempcat_sweep_slice()).
 */
static const Oid tempcat_object_classes[] =
{
	RelationRelationId,
	TypeRelationId,
	NamespaceRelationId,
	ProcedureRelationId,
	OperatorRelationId,
	ConstraintRelationId,
	AttrDefaultRelationId,
	RewriteRelationId,
	TriggerRelationId,
	PolicyRelationId,
	StatisticExtRelationId,
};

/* ----------------------------------------------------------------
 * Structures in the session's area
 *
 * Everything below lives in the area and refers to other objects by
 * dsa_pointer.  The writer modifies them under the exclusive lock; readers
 * take the lock in shared mode.
 * ----------------------------------------------------------------
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

/* ----------------------------------------------------------------
 * Process-local state
 * ----------------------------------------------------------------
 */
static dsa_area *tc_area = NULL;
static TempcatRoot *tc_root = NULL;

/*
 * Writer only: entries inserted or deleted by transactions whose outcome
 * has not been applied yet.
 */
static dsa_pointer *tc_pending = NULL;
static int	tc_npending = 0;
static bool tc_resolve_wanted = false;	/* see tempcat_abort_subtransaction() */
static void tempcat_resolve_pending(void);
static int	tc_maxpending = 0;

/* Rows deleted so far, and the last walk of tempcat_reclaim_dead() */
static uint64 tc_ndeletions = 0;
static bool tc_reclaim_failed = false;	/* it freed nothing */
static TransactionId tc_reclaim_xid = InvalidTransactionId;
static CommandId tc_reclaim_cid = InvalidCommandId;
static uint64 tc_reclaim_ndeletions = 0;
static bool tc_reclaim_waiting = false;	/* rows left for a later command */

/* The current transaction changed rows in memory; see tempcat_check_prepare() */
static bool tc_xact_changed = false;

/*
 * The deepest subtransaction level whose changes of rows in memory may be
 * on the pending list; see tempcat_abort_subtransaction().
 */
static int	tc_change_level = 0;

/* Has this session started to keep temporary objects in memory? */
static bool tc_activated = false;

/* The published frozen horizon may be too old and needs recomputing. */
static bool tc_horizon_dirty = false;

/* The published roles may include roles no longer used. */
static bool tc_roles_dirty = false;

struct TempcatScanData
{
	HeapTuple  *tuples;
	int			ntuples;
	int			next;
	bool		on_virtual;		/* last tuple returned was in memory */

	/*
	 * For scans through an index: rows in memory are sorted by the index key
	 * and merged with the on-disk rows, so that callers relying on index
	 * order (e.g. catcache lists of pg_attribute by attnum) see it.
	 */
	Relation	irel;
	TupleDesc	heapdesc;
	int			nidxkeys;
	FmgrInfo  **procs;
	TupleDesc	idxcoldesc;		/* see tempcat_index_columns() */
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
static void tempcat_activate(Oid oid);
static void tempcat_forget_if_gone(int catidx, Oid objoid);
static int	tempcat_reclaim_dead(void);
static bool tempcat_memory_has_oid(int catidx, Oid oid);
static bool tempcat_take_namespace_oid(void);
static bool tempcat_slice_crowded(int slice);
static void tempcat_let_go_of_slice(void);
static void tempcat_abandon_slice(void);
static bool tc_slice_used = false;	/* see tempcat_abandon_slice() */

/* OIDs assigned by this session to its temporary objects, see below */
static HTAB *tc_owned = NULL;

/*
 * The OID of the session's temporary namespace, taken before it decided to
 * keep it in memory (see tempcat_want_temp_oid()).
 */
static Oid	tc_namespace_oid = InvalidOid;

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

/* The first attribute of these catalogs is their oid column. */
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

static inline Oid
tempcat_entry_oid(TempcatEntry *e)
{
	HeapTupleHeader hdr = TCE_DATA(e);

	return *(Oid *) ((char *) hdr + hdr->t_hoff);
}

static Oid
tempcat_getoid(HeapTuple tup, TupleDesc desc, AttrNumber attno)
{
	bool		isnull;
	Datum		value = heap_getattr(tup, attno, desc, &isnull);

	return isnull ? InvalidOid : DatumGetObjectId(value);
}

/* ----------------------------------------------------------------
 * Shared memory
 *
 * Per PGPROC: the oldest relfrozenxid/relminmxid of the in-memory temporary
 * tables of the session using it, which stand in for the pg_class rows that
 * vac_update_datfrozenxid() cannot see, the roles its in-memory pg_shdepend
 * rows refer to, and the slice it holds.
 *
 * Per slice of the reserved range: the process of this node holding it, the
 * database whose leftovers it has removed (see tempcat_claim_slice()), and,
 * on the coordinator, the next OID to assign.  And where the coordinator
 * looks for a free slice next (tempcat_claim_free_slice()).
 * ----------------------------------------------------------------
 */
#define TEMPCAT_SLICE_BITS	16
#define TEMPCAT_SLICE_SIZE	((Oid) 1 << TEMPCAT_SLICE_BITS)
#define TEMPCAT_NSLICES \
	((int) ((((uint64) LastTempcatObjectId - FirstTempcatObjectId) >> TEMPCAT_SLICE_BITS) + 1))

#define tempcat_slice_of(oid) \
	((int) (((Oid) (oid) - FirstTempcatObjectId) >> TEMPCAT_SLICE_BITS))

#define TEMPCAT_MAX_ROLES	16

typedef struct TempcatProcState
{
	Oid			databaseId;		/* database of the published horizon */
	pg_atomic_uint32 frozenXid; /* or InvalidTransactionId */
	pg_atomic_uint32 minMulti;	/* or InvalidMultiXactId */
	int			slice;			/* the slice held, or -1 */

	/*
	 * Roles that in-memory pg_shdepend rows of the session refer to (owners
	 * and grantees of its temporary objects), standing in for the rows
	 * DROP ROLE cannot see.  Changed under TempcatShared->mutex.
	 */
	int			nroles;
	Oid			roles[TEMPCAT_MAX_ROLES];
} TempcatProcState;

typedef struct TempcatSliceState
{
	pg_atomic_uint32 holder;	/* pgprocno + 1, or 0 */
	Oid			databaseId;		/* set once its leftovers there are gone */
	Oid			nextOid;		/* coordinator: next OID to assign */
} TempcatSliceState;

typedef struct TempcatSharedData
{
	int			tranche_id;		/* LWLock tranche of the session areas */
	slock_t		mutex;			/* protects the roles */
	int			nprocs;
	pg_atomic_uint32 next_slice;	/* coordinator: see tempcat_claim_free_slice() */
	TempcatSliceState slices[TEMPCAT_NSLICES];
	TempcatProcState procs[FLEXIBLE_ARRAY_MEMBER];
} TempcatSharedData;

static TempcatSharedData *TempcatShared = NULL;

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
	/*
	 * Start the search for free slices at a random point, not where the
	 * coordinator before a restart or a failover started: writer QEs of its
	 * sessions may still hold their slices on the segments for a while (until
	 * they notice that their coordinator is gone), where new holders would
	 * wait for them (tempcat_claim_slice()).
	 */
	{
		uint32		start;

		if (!pg_strong_random(&start, sizeof(start)))
			start = (uint32) (GetCurrentTimestamp() ^ MyProcPid);
		pg_atomic_init_u32(&TempcatShared->next_slice, start % TEMPCAT_NSLICES);
	}
	for (i = 0; i < TEMPCAT_NSLICES; i++)
	{
		TempcatSliceState *ss = &TempcatShared->slices[i];

		pg_atomic_init_u32(&ss->holder, 0);
		ss->databaseId = InvalidOid;
		ss->nextOid = InvalidOid;
	}
	TempcatShared->nprocs = tempcat_total_procs();
	for (i = 0; i < TempcatShared->nprocs; i++)
	{
		TempcatProcState *st = &TempcatShared->procs[i];

		st->databaseId = InvalidOid;
		pg_atomic_init_u32(&st->frozenXid, InvalidTransactionId);
		pg_atomic_init_u32(&st->minMulti, InvalidMultiXactId);
		st->slice = -1;
		st->nroles = 0;
	}
}

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

/* ----------------------------------------------------------------
 * The session's area
 * ----------------------------------------------------------------
 */

/*
 * The area does not grow into the last quarter of the space for dynamic
 * shared memory (/dev/shm with the default dynamic_shared_memory_type =
 * posix, 64 MB in a default Docker container): other users of DSM (e.g.
 * combo CIDs shared with reader gangs) need some too.  Nor into the last
 * quarter of the slots of the DSM control segment, which every segment
 * takes one of.  Beyond that, segments are files (dsa_set_memory_limit()),
 * if allowed.
 */
#define TEMPCAT_DSA_FIRST_SEGMENT	(1024 * 1024)
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
tempcat_memory_bytes(void)
{
	return (size_t) gp_temp_memory_catalog_max_size * 1024;
}

static inline size_t
tempcat_file_bytes(void)
{
	return (size_t) gp_temp_memory_catalog_max_file_size * 1024;
}

/*
 * Create the session's area (writer only).  Returns false if it cannot be
 * created: no slot of the DSM control segment to spare, nor space in memory
 * or (if allowed) on disk.
 *
 * The coordinator decides whether a session keeps its temporary objects in
 * memory, and leaves room to other users of dynamic shared memory
 * ('headroom'): if it cannot, the session keeps them on disk.  A segment
 * cannot decide otherwise, and fails only if the area cannot be created at
 * all.  It leaves the room in memory to others by starting the area in a
 * file, if files are allowed (several primaries of a host share its
 * /dev/shm, and other users of it fail when it is full), and as the area
 * grows.  Slots of the DSM control segment it cannot leave: segments in
 * files take them too.
 */
static bool
tempcat_create_area(bool headroom)
{
	MemoryContext oldcxt;
	dsa_pointer rootptr;
	dsa_area   *area = NULL;
	bool		files = tempcat_file_bytes() > 0;
	bool		try_memory;

	Assert(tc_area == NULL && !tempcat_is_reader());

	if (headroom && tempcat_dsm_slots_low())
		return false;

	/*
	 * The area's memory is the session's, though shared: charge it to the
	 * session's vmem (gp_vmem_protect_limit, resource groups), its first
	 * segment here and later ones as the area grows (dsa_set_vmem_tracked()).
	 * Its segments in files are not charged.  Charge it before creating the
	 * area, so that a refusal costs nothing; dsa_try_create() can still fail
	 * with an error (e.g. out of local memory), which must not leave vmem
	 * charged.
	 */
	try_memory = !((headroom || files) &&
				   tempcat_dsm_space_low(2 * TEMPCAT_DSA_FIRST_SEGMENT));
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	if (try_memory &&
		VmemTracker_ReserveVmem(TEMPCAT_DSA_FIRST_SEGMENT) == MemoryAllocation_Success)
	{
		PG_TRY();
		{
			area = dsa_try_create(tempcat_tranche_id(), false);
		}
		PG_CATCH();
		{
			VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (area == NULL)
			VmemTracker_ReleaseVmem(TEMPCAT_DSA_FIRST_SEGMENT);
	}
	if (area == NULL && files)
	{
		PrepareTempTablespaces();
		area = dsa_try_create(tempcat_tranche_id(), true);
	}
	MemoryContextSwitchTo(oldcxt);
	if (area == NULL)
		return false;

	dsa_pin_mapping(area);
	dsa_set_vmem_tracked(area);

	dsa_set_size_limit(area, tempcat_memory_bytes() + tempcat_file_bytes());
	if (files)
		dsa_set_memory_limit(area, tempcat_memory_bytes());
	{
		Size		free_bytes;
		Size		total_bytes;
		uint32		nfree;
		uint32		nmax;

		if (dsm_impl_free_space(&free_bytes, &total_bytes))
			dsa_set_min_free_space(area, total_bytes / TEMPCAT_DSM_HEADROOM_DIV);
		if (dsm_free_slots(&nfree, &nmax))
			dsa_set_min_free_slots(area, nmax / TEMPCAT_DSM_HEADROOM_DIV);
	}

	/* (in the first segment, which has room for it) */
	rootptr = dsa_allocate0(area, sizeof(TempcatRoot));
	tc_root = dsa_get_address(area, rootptr);
	LWLockInitialize(&tc_root->lock, tempcat_tranche_id());
	tc_root->writer_pgprocno = MyProc->pgprocno;
	tc_area = area;

	/* Publish the area to the readers of this session. */
	if (SharedLocalSnapshotSlot != NULL)
	{
		SharedLocalSnapshotSlot->tempcat_root = rootptr;
		pg_write_barrier();
		SharedLocalSnapshotSlot->tempcat_handle = dsa_get_handle(tc_area);
	}

	return true;
}

/*
 * Attach to the session's area: readers to the writer's, if it has one.
 * Returns false if there is no area (yet).
 */
static bool
tempcat_attach(void)
{
	static bool tranche_registered = false;
	static LocalTransactionId no_area_lxid = InvalidLocalTransactionId;
	MemoryContext oldcxt;
	dsa_pointer rootptr;
	dsa_handle	handle;

	if (tc_area != NULL)
		return true;

	/*
	 * Once per process, also where no area is to come: pg_stat_activity
	 * names other processes' waits for the lock by this process's tranches.
	 */
	if (unlikely(!tranche_registered))
	{
		if (TempcatShared == NULL)
			return false;
		LWLockRegisterTranche(tempcat_tranche_id(), "tempcat");
		tranche_registered = true;
	}

	if (!tempcat_is_reader())
		return false;

	if (am_cursor_retrieve_handler)
	{
		/*
		 * A retrieve connection outputs the rows of another session's
		 * parallel retrieve cursor and must see that session's temporary
		 * types.  It runs its own transactions, so it sees their committed
		 * catalog rows only, like on disk.  (Do not search the slots, under
		 * their lock, at every catalog scan of a transaction that found no
		 * area.)
		 */
		int			sessionId = RetrieveSessionId();

		if (no_area_lxid == MyProc->lxid && MyProc->lxid != InvalidLocalTransactionId)
			return false;
		if (sessionId < 0 ||
			!SharedSnapshotGetTempcatArea(sessionId, &handle, &rootptr))
		{
			no_area_lxid = MyProc->lxid;
			return false;
		}
	}
	else
	{
		if (SharedLocalSnapshotSlot == NULL)
			return false;
		handle = SharedLocalSnapshotSlot->tempcat_handle;
		if (handle == DSM_HANDLE_INVALID)
			return false;
		pg_read_barrier();
		rootptr = SharedLocalSnapshotSlot->tempcat_root;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	tc_area = dsa_attach(handle);
	dsa_pin_mapping(tc_area);
	MemoryContextSwitchTo(oldcxt);

	tc_root = dsa_get_address(tc_area, rootptr);
	return true;
}

/* ----------------------------------------------------------------
 * Slices
 * ----------------------------------------------------------------
 */

/* How long to wait for an earlier holder of a slice to go away */
#define TEMPCAT_CLAIM_TIMEOUT_MS	60000

/* The slice this process holds, or -1 */
static int	tc_slice = -1;

static inline Oid
tempcat_slice_base(int slice)
{
	return FirstTempcatObjectId + ((Oid) slice << TEMPCAT_SLICE_BITS);
}

static inline Oid
tempcat_slice_last(int slice)
{
	Oid			base = tempcat_slice_base(slice);

	return Min((uint64) base + TEMPCAT_SLICE_SIZE - 1, (uint64) LastTempcatObjectId);
}

static void tempcat_proc_exit(int code, Datum arg);
static void tempcat_before_exit(int code, Datum arg);
static void tempcat_xact_callback(XactEvent event, void *arg);
static void tempcat_subxact_callback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 void *arg);

/*
 * Register what lets go of a slice and settles the session's rows: before
 * taking a slice, so that it cannot be kept for good.
 */
static void
tempcat_register_callbacks(void)
{
	static bool registered = false;

	if (registered)
		return;
	on_shmem_exit(tempcat_proc_exit, (Datum) 0);
	before_shmem_exit(tempcat_before_exit, (Datum) 0);
	RegisterSubXactCallback(tempcat_subxact_callback, NULL);
	RegisterXactCallback(tempcat_xact_callback, NULL);
	registered = true;
}

/*
 * The coordinator takes hold of a free slice for its session, for the rest
 * of the process's life.  It looks for one from where the last search
 * stopped, so that a slice let go of is taken again only after all others
 * were: the writer QEs of the session that held it may still be dropping
 * their objects on the segments, where a new holder would wait for them
 * (tempcat_claim_slice()).  It passes over slices whose OIDs ordinary
 * objects of its database take too many of (tempcat_slice_crowded()).
 * Returns false if no slice is left.
 */
static bool
tempcat_claim_free_slice(void)
{
	uint32		me = MyProc->pgprocno + 1;
	int			i;

	if (tc_slice >= 0)
		return true;
	tempcat_register_callbacks();
#ifdef FAULT_INJECTOR
	/*
	 * Test hook: start the search at the slice taken last, as if all others
	 * had been taken since, to take it again once its holder let go of it
	 * here (see tempcat_claim_slice()).
	 */
	if (SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_again") == FaultInjectorTypeSkip)
		pg_atomic_fetch_sub_u32(&TempcatShared->next_slice, 1);
#endif
	for (i = 0; i < TEMPCAT_NSLICES; i++)
	{
		int			slice = pg_atomic_fetch_add_u32(&TempcatShared->next_slice, 1) %
		TEMPCAT_NSLICES;
		uint32		expected = 0;

		if (pg_atomic_compare_exchange_u32(&TempcatShared->slices[slice].holder,
										   &expected, me))
		{
			if (tempcat_slice_crowded(slice))
			{
				pg_atomic_write_u32(&TempcatShared->slices[slice].holder, 0);
				continue;
			}
			tc_slice = slice;
			tempcat_my_state()->slice = slice;
			return true;
		}
	}
	return false;
}

/*
 * A writer QE takes hold of the slice of the OIDs its coordinator assigns,
 * on this node, for the rest of the process's life.  The writer QE of an
 * earlier session that held the slice may still be on its way out (e.g.
 * dropping its temporary objects): wait for it.
 */
static void
tempcat_claim_slice(int slice)
{
	TempcatSliceState *ss = &TempcatShared->slices[slice];
	uint32		me = MyProc->pgprocno + 1;
	int			waited = 0;

	for (;;)
	{
		uint32		expected = 0;

		if (pg_atomic_compare_exchange_u32(&ss->holder, &expected, me))
			break;
		if (expected == me)
			break;
#ifdef FAULT_INJECTOR
		/* Test hook: report the wait for the earlier holder. */
		if (waited == 0)
			SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_wait");
		/* Test hook: give up at once. */
		if (waited == 0 &&
			SIMPLE_FAULT_INJECTOR("tempcat_claim_slice_timeout") == FaultInjectorTypeSkip)
			waited = TEMPCAT_CLAIM_TIMEOUT_MS;
#endif
		if (waited >= TEMPCAT_CLAIM_TIMEOUT_MS)
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("an earlier session using the same temporary object OIDs is still running on this node"),
					 errdetail("Process %d holds OIDs %u to %u.",
							   ProcGlobal->allProcs[expected - 1].pid,
							   tempcat_slice_base(slice),
							   tempcat_slice_last(slice)),
					 errhint("Retry once that session has ended.")));
		CHECK_FOR_INTERRUPTS();
		pg_usleep(10000L);
		waited += 10;
	}
	tc_slice = slice;
	tempcat_my_state()->slice = slice;
}

/*
 * Leftovers of earlier holders of our slice: on-disk pg_depend and
 * pg_shdepend rows of their temporary objects, whose other rows were in
 * their memory, are removed when a holder takes the slice in a database
 * (tempcat_sweep_slice()).  Until that committed, other sessions take such
 * rows for leftovers too (see tempcat_foreign_object()).  The removal is
 * part of the transaction that takes the slice: if that, or its
 * subtransaction, rolls back, the rows come back, and are removed again
 * before the next OID is assigned or received.
 */
typedef enum TempcatSweepState
{
	TCS_NONE,					/* not swept */
	TCS_IN_XACT,				/* swept by the transaction in progress */
	TCS_PREPARED,				/* swept by tc_sweep_xid, prepared */
	TCS_DONE					/* swept and committed */
} TempcatSweepState;

static TempcatSweepState tc_sweep_state = TCS_NONE;
static SubTransactionId tc_sweep_subid = InvalidSubTransactionId;
static TransactionId tc_sweep_xid = InvalidTransactionId;

/* The OID column and index of the objects of catalog 'classId', if known */
static bool
tempcat_object_oid_index(Oid classId, AttrNumber *oidattr, Oid *indexId)
{
	/* (not in objectaddress.c's ObjectProperty in this version) */
	if (classId == AttrDefaultRelationId)
	{
		*oidattr = Anum_pg_attrdef_oid;
		*indexId = AttrDefaultOidIndexId;
	}
	else if (is_objectclass_supported(classId))
	{
		*oidattr = get_object_attnum_oid(classId);
		*indexId = get_object_oid_index(classId);
	}
	else
		return false;
	return true;
}

/* Is there an object of 'classId' with this OID on disk? */
static bool
tempcat_disk_object_exists(Oid classId, Oid objectId)
{
	Relation	rel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tup;
	bool		found = false;
	AttrNumber	oidattr;
	Oid			indexId;

	if (!tempcat_object_oid_index(classId, &oidattr, &indexId))
		return false;

	rel = table_open(classId, AccessShareLock);
	ScanKeyInit(&key, oidattr, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(objectId));
	scan = systable_beginscan(rel, indexId, true, NULL, 1, &key);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		if (!IsTempcatTid(&tup->t_self))
		{
			found = true;
			break;
		}
	}
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	return found;
}

/*
 * Do ordinary objects of this database (created before the range was
 * reserved, or carried over by pg_upgrade) take so many OIDs of the slice in
 * one of the catalogs whose objects can be temporary objects in memory that
 * the session could run out of them?  Then it takes another slice.  The
 * scans of the OID indexes over the slice usually find nothing.
 */
#define TEMPCAT_SLICE_CROWDED	(TEMPCAT_SLICE_SIZE / 2)

static bool
tempcat_slice_crowded(int slice)
{
	Oid			lo = tempcat_slice_base(slice);
	Oid			hi = tempcat_slice_last(slice);
	int			limit = TEMPCAT_SLICE_CROWDED;
	int			i;

#ifdef FAULT_INJECTOR
	/* Test hook: one ordinary object is too many. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_slice_crowded") == FaultInjectorTypeSkip)
		limit = 0;
#endif

	for (i = 0; i < lengthof(tempcat_object_classes); i++)
	{
		Oid			classId = tempcat_object_classes[i];
		AttrNumber	oidattr;
		Oid			indexId;
		Relation	rel;
		ScanKeyData key[2];
		SysScanDesc scan;
		HeapTuple	tup;
		int			n = 0;

		if (!tempcat_object_oid_index(classId, &oidattr, &indexId))
			continue;
		rel = table_open(classId, AccessShareLock);
		ScanKeyInit(&key[0], oidattr, BTGreaterEqualStrategyNumber,
					F_OIDGE, ObjectIdGetDatum(lo));
		ScanKeyInit(&key[1], oidattr, BTLessEqualStrategyNumber,
					F_OIDLE, ObjectIdGetDatum(hi));
		scan = systable_beginscan(rel, indexId, true, NULL, 2, key);
		while (n <= limit && HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			if (!IsTempcatTid(&tup->t_self))
				n++;
		}
		systable_endscan(scan);
		table_close(rel, AccessShareLock);
		if (n > limit)
			return true;
	}
	return false;
}

/*
 * Delete an on-disk leftover row, unless somebody else is deleting or
 * changing it (that one may be a leftover sweep too).
 */
static bool
tempcat_delete_leftover(Relation rel, ItemPointer tid)
{
	TM_FailureData tmfd;

	return heap_delete(rel, tid, GetCurrentCommandId(true), InvalidSnapshot,
					   false, &tmfd, false) == TM_Ok;
}

/*
 * Remove the on-disk pg_depend and pg_shdepend rows of temporary objects of
 * earlier holders of our slice in this database; see above.  Rows of
 * ordinary objects with OIDs in the slice (see the file header) stay.
 */
static void
tempcat_sweep_slice(int slice)
{
	Oid			lo = tempcat_slice_base(slice);
	Oid			hi = tempcat_slice_last(slice);
	Relation	deprel;
	Relation	sdeprel;
	int			i;
	bool		deleted = false;

	deprel = table_open(DependRelationId, RowExclusiveLock);
	sdeprel = table_open(SharedDependRelationId, RowExclusiveLock);

	for (i = 0; i < lengthof(tempcat_object_classes); i++)
	{
		Oid			classId = tempcat_object_classes[i];
		ScanKeyData key[4];
		SysScanDesc scan;
		HeapTuple	tup;
		Oid			last = InvalidOid;
		bool		ordinary = false;

		ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(classId));
		ScanKeyInit(&key[1], Anum_pg_depend_objid, BTGreaterEqualStrategyNumber,
					F_OIDGE, ObjectIdGetDatum(lo));
		ScanKeyInit(&key[2], Anum_pg_depend_objid, BTLessEqualStrategyNumber,
					F_OIDLE, ObjectIdGetDatum(hi));
		scan = systable_beginscan(deprel, DependDependerIndexId, true,
								  NULL, 3, key);
		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_depend dep = (Form_pg_depend) GETSTRUCT(tup);

			CHECK_FOR_INTERRUPTS();
			if (IsTempcatTid(&tup->t_self))
				continue;
			if (dep->objid != last)
			{
				last = dep->objid;
				ordinary = tempcat_disk_object_exists(classId, dep->objid);
			}
			if (!ordinary && tempcat_delete_leftover(deprel, &tup->t_self))
				deleted = true;
		}
		systable_endscan(scan);

		last = InvalidOid;
		ScanKeyInit(&key[0], Anum_pg_shdepend_dbid, BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(MyDatabaseId));
		ScanKeyInit(&key[1], Anum_pg_shdepend_classid, BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(classId));
		ScanKeyInit(&key[2], Anum_pg_shdepend_objid, BTGreaterEqualStrategyNumber,
					F_OIDGE, ObjectIdGetDatum(lo));
		ScanKeyInit(&key[3], Anum_pg_shdepend_objid, BTLessEqualStrategyNumber,
					F_OIDLE, ObjectIdGetDatum(hi));
		scan = systable_beginscan(sdeprel, SharedDependDependerIndexId, true,
								  NULL, 4, key);
		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_shdepend dep = (Form_pg_shdepend) GETSTRUCT(tup);

			CHECK_FOR_INTERRUPTS();
			if (IsTempcatTid(&tup->t_self))
				continue;
			if (dep->objid != last)
			{
				last = dep->objid;
				ordinary = tempcat_disk_object_exists(classId, dep->objid);
			}
			if (!ordinary && tempcat_delete_leftover(sdeprel, &tup->t_self))
				deleted = true;
		}
		systable_endscan(scan);
	}

	table_close(sdeprel, RowExclusiveLock);
	table_close(deprel, RowExclusiveLock);
	if (deleted)
		CommandCounterIncrement();
}

/*
 * The (sub)transaction that swept our slice rolled back: the leftovers are
 * back.  Until the next sweep, other sessions take them for leftovers again
 * (tempcat_foreign_object()): no object of ours in the slice can have
 * committed, as a slice is swept before its first object.
 */
static void
tempcat_sweep_rolled_back(void)
{
	tc_sweep_state = TCS_NONE;
	if (tc_slice >= 0)
		TempcatShared->slices[tc_slice].databaseId = InvalidOid;
}

/* Has the sweep of our slice committed?  Apply a prepared one's outcome. */
static bool
tempcat_slice_swept(void)
{
	/*
	 * (A prepared sweep that rolled back leaves the slice published: later
	 * transactions may have committed objects in it meanwhile.  The next
	 * sweep, below, removes the leftovers again.)
	 */
	if (tc_sweep_state == TCS_PREPARED &&
		!TransactionIdIsInProgress(tc_sweep_xid))
		tc_sweep_state = TransactionIdDidCommit(tc_sweep_xid) ?
			TCS_DONE : TCS_NONE;
	return tc_sweep_state == TCS_DONE || tc_sweep_state == TCS_IN_XACT ||
		tc_sweep_state == TCS_PREPARED;
}

/*
 * The session gets a reserved OID of slice 'slice(oid)' for one of its
 * temporary objects: on the coordinator when it assigns one, on a segment
 * when it receives one.  Take hold of the slice, remove the leftovers of
 * its earlier holders, and publish that our objects in this database are
 * live (see tempcat_foreign_object()).
 */
static void
tempcat_activate(Oid oid)
{
	int			slice = tempcat_slice_of(oid);

	/*
	 * A writer QE whose prepared transaction that started to use its slice
	 * was rolled back keeps the slice, but the coordinator took another one
	 * (tempcat_abandon_slice()): follow it if nothing is left in the old one.
	 */
	if (tc_slice >= 0 && tc_slice != slice && Gp_role == GP_ROLE_EXECUTE &&
		tc_npending == 0 &&
		(tc_owned == NULL || hash_get_num_entries(tc_owned) == 0))
	{
		tempcat_let_go_of_slice();
		tc_activated = false;
		tc_sweep_state = TCS_NONE;
		tc_slice_used = false;
	}
	if (tc_slice >= 0 && tc_slice != slice)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("OID %u of a temporary object is outside the OIDs of the session (%u to %u)",
						oid, tempcat_slice_base(tc_slice),
						tempcat_slice_last(tc_slice))));

	if (!tc_activated)
	{
		TempcatProcState *st;

		if (tc_slice < 0)
		{
			tempcat_register_callbacks();
			tempcat_claim_slice(slice);
		}
		tc_activated = true;

		st = tempcat_my_state();
		pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
		pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
		st->databaseId = MyDatabaseId;
	}

	if (!tempcat_slice_swept())
	{
		tempcat_sweep_slice(slice);
		tc_sweep_state = TCS_IN_XACT;
		tc_sweep_subid = GetCurrentSubTransactionId();
		TempcatShared->slices[slice].databaseId = MyDatabaseId;
	}
}

/*
 * Let go of our slice, after the cleanup of our temporary objects at exit
 * (RemoveTempRelationsCallback() runs earlier).  Whatever of theirs is left
 * on disk is leftovers now.
 */
static void
tempcat_proc_exit(int code, Datum arg)
{
	TempcatProcState *st = tempcat_my_state();

	pg_atomic_write_u32(&st->frozenXid, InvalidTransactionId);
	pg_atomic_write_u32(&st->minMulti, InvalidMultiXactId);
	st->databaseId = InvalidOid;

	SpinLockAcquire(&TempcatShared->mutex);
	st->nroles = 0;
	SpinLockRelease(&TempcatShared->mutex);

	if (tc_slice >= 0)
	{
#ifdef FAULT_INJECTOR
		/* Test hook: hold on to the slice on the way out (with 'suspend'). */
		SIMPLE_FAULT_INJECTOR("tempcat_release_slice");
#endif
		tempcat_let_go_of_slice();
	}
}

/* Let go of our slice. */
static void
tempcat_let_go_of_slice(void)
{
	TempcatSliceState *ss = &TempcatShared->slices[tc_slice];

	ss->databaseId = InvalidOid;
	pg_write_barrier();
	pg_atomic_write_u32(&ss->holder, 0);
	tempcat_my_state()->slice = -1;
	tc_slice = -1;
}

/*
 * The transaction that started to use our slice rolled back, and nothing
 * with its OIDs was committed: let go of it, so that the next attempt takes
 * another one.  Otherwise a slice that a segment refused (its earlier
 * holder's writer QE lingers there, see tempcat_claim_slice()) would fail
 * every attempt of the session.  Writer QEs do the same, and those whose
 * transaction was prepared before the rollback take the next slice in
 * tempcat_activate().
 */
static void
tempcat_abandon_slice(void)
{
	TempcatProcState *st = tempcat_my_state();

	tempcat_let_go_of_slice();
	tc_activated = false;
	tc_sweep_state = TCS_NONE;
	tc_namespace_oid = InvalidOid;
	st->databaseId = InvalidOid;
	if (tc_owned != NULL)
	{
		hash_destroy(tc_owned);
		tc_owned = NULL;
	}
}

/* ----------------------------------------------------------------
 * Ownership
 * ----------------------------------------------------------------
 */

/* OIDs assigned by this session to its temporary objects, per catalog. */
typedef struct TempcatOwnedKey
{
	Oid			catalog;
	Oid			oid;
} TempcatOwnedKey;

static void
tempcat_remember_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TempcatOwnedKey);
		ctl.entrysize = sizeof(TempcatOwnedKey);
		ctl.hcxt = TopMemoryContext;
		tc_owned = hash_create("tempcat owned OIDs", 256, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	key.catalog = catalog;
	key.oid = oid;
	(void) hash_search(tc_owned, &key, HASH_ENTER, NULL);
}

/* Returns whether we owned it. */
static bool
tempcat_forget_oid(Oid catalog, Oid oid)
{
	TempcatOwnedKey key;

	if (tc_owned == NULL)
		return false;
	key.catalog = catalog;
	key.oid = oid;
	return hash_search(tc_owned, &key, HASH_REMOVE, NULL) != NULL;
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
		return tempcat_attach() ?
			&TempcatShared->procs[tc_root->writer_pgprocno] : NULL;
	if (SharedLocalSnapshotSlot == NULL ||
		SharedLocalSnapshotSlot->writer_proc == NULL)
		return NULL;
	return &TempcatShared->procs[SharedLocalSnapshotSlot->writer_proc->pgprocno];
}

/*
 * Is 'oid' that of a temporary object of our session?  Readers do not know
 * which OIDs their writer assigned, but all of them are in the slice it
 * holds.  (What is not known is taken as ours.)
 */
static bool
tempcat_session_owns_oid(Oid catalog, Oid oid)
{
	TempcatProcState *st;
	int			slice;

	if (!tempcat_is_reader())
		return tempcat_owns_oid(catalog, oid);
	if (!IsTempcatOid(oid))
		return false;
	st = tempcat_writer_state();
	if (st == NULL)
		return true;
	slice = st->slice;
	return slice < 0 || slice == tempcat_slice_of(oid);
}

/* ----------------------------------------------------------------
 * Published horizon and roles
 * ----------------------------------------------------------------
 */

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

	/*
	 * The row may yet fail to be stored (no room, refused, an error): have
	 * the roles published anew when the session goes idle or starts its next
	 * transaction, from the rows there are then.
	 */
	if (ok)
		tc_roles_dirty = true;

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

/* ----------------------------------------------------------------
 * OID assignment
 * ----------------------------------------------------------------
 */

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
 * Can this session keep its temporary objects in memory?  Decided when its
 * temporary namespace is created on the coordinator.
 */
static bool
tempcat_can_start(void)
{
	/*
	 * A utility-mode session on a segment keeps its temporary objects on
	 * disk: OIDs from the reserved range could be the ones that the
	 * coordinator gives the temporary objects of its sessions on this
	 * segment.
	 */
	if (Gp_role == GP_ROLE_UTILITY && !IS_QUERY_DISPATCHER())
		return false;
	if (IsBinaryUpgrade || !IsUnderPostmaster || TempcatShared == NULL)
		return false;

	/* the memory to keep them in, and a slice of the reserved range */
	return (tc_area != NULL || tempcat_create_area(true)) &&
		tempcat_claim_free_slice();
}

/*
 * Should a new object get an OID from the reserved range?  Called on the
 * coordinator by oid_dispatch.c with the same key it dispatches to the
 * segments.
 */
bool
tempcat_want_temp_oid(Oid catalog, const char *objname, Oid namespaceOid,
					  Oid keyOid1, Oid keyOid2)
{
	/*
	 * The temporary namespace decides, and the temporary toast namespace,
	 * created right after it, goes where it went: its relations belong to
	 * the other's.
	 */
	static bool namespace_in_memory = false;

	if (catalog == NamespaceRelationId && objname != NULL &&
		strncmp(objname, "pg_toast_temp_", 14) == 0)
		return namespace_in_memory;
	if (catalog == NamespaceRelationId)
		return (namespace_in_memory =
				tempcat_key_is_temp(catalog, objname, namespaceOid,
									keyOid1, keyOid2,
									gp_enable_temp_memory_catalog) &&
				tempcat_can_start() &&
				tempcat_take_namespace_oid());
	return tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1,
							   keyOid2, false);
}

/*
 * Only relations, types, functions and operators can be created in a
 * temporary namespace kept in memory: other objects (operator classes,
 * collations, text search objects, statistics objects, ...) would get
 * on-disk catalog rows pointing at the namespace that other sessions cannot
 * see.  Called by oid_dispatch.c on the coordinator.
 */
void
tempcat_check_namespace_object(Oid catalog, Oid namespaceOid)
{
	if (catalog == RelationRelationId || catalog == TypeRelationId ||
		catalog == ProcedureRelationId || catalog == OperatorRelationId ||
		!tempcat_owns_oid(NamespaceRelationId, namespaceOid))
		return;
	if (catalog == DefaultAclRelationId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("default privileges cannot be set for a temporary schema kept in memory"),
				 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("only relations, types, functions and operators can be created in a temporary schema kept in memory"),
			 errhint("Turn off gp_enable_temp_memory_catalog before the session creates its first temporary object.")));
}

/*
 * Allocate an OID for a new temporary object in 'relation' from this
 * session's slice of the reserved range.
 *
 * Like GetNewOidWithIndex(), make sure the OID is not used in the catalog
 * yet, neither on disk (ordinary objects may have OIDs in the range, see the
 * file header) nor in memory: the SnapshotAny scan sees both.
 *
 * The next value is kept in shared memory per slice, so consecutive
 * sessions that use the same slice keep moving forward instead of reusing
 * recent OIDs.
 */
static Oid
tempcat_try_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	int			slice = tc_slice;
	TempcatSliceState *ss;
	Oid			base;
	Oid			span;
	Oid			tries;
	Oid			collisions = 0;
	bool		reclaimed = false;

	if (slice < 0)
		elog(ERROR, "tempcat: no slice of the reserved OIDs is held");
	ss = &TempcatShared->slices[slice];
	base = tempcat_slice_base(slice);
	span = tempcat_slice_last(slice) - base + 1;

#ifdef FAULT_INJECTOR
	/* Test hook: ordinary objects take every OID of the slice. */
	if (SIMPLE_FAULT_INJECTOR("tempcat_oid_exhausted") == FaultInjectorTypeSkip)
		tries = span;
	else
#endif
		tries = 0;

	for (; tries < span; tries++)
	{
		Oid			oid = ss->nextOid;
		ScanKeyData key;
		SysScanDesc scan;
		bool		collides;

		/*
		 * First use of the slice since the node started: start at a random
		 * point rather than where its users before a restart or a failover
		 * started, whose objects may still be kept in writer QEs on the
		 * segments.
		 */
		if (!OidIsValid(oid))
			oid = base + (Oid) (random() % span);
		else if (oid < base || oid - base >= span)
			oid = base;
#ifdef FAULT_INJECTOR
		/* Test hook: go near the end of the slice, to wrap around soon. */
		if (SIMPLE_FAULT_INJECTOR("tempcat_slice_near_end") == FaultInjectorTypeSkip)
			oid = base + span - 3;
#endif
		ss->nextOid = oid + 1;

		/* (before the lookup: the leftovers' rows go first) */
		tempcat_activate(oid);

		ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(oid));
		scan = systable_beginscan(relation, indexId, true, SnapshotAny, 1, &key);
		collides = HeapTupleIsValid(systable_getnext(scan));
		systable_endscan(scan);

		if (!collides)
		{
			/*
			 * Enum labels are no objects of their own: their rows go with
			 * their type (see tempcat_catalogs[]), and nothing looks for
			 * their OIDs among ours.
			 */
			if (RelationGetRelid(relation) != EnumRelationId)
				tempcat_remember_oid(RelationGetRelid(relation), oid);
			return oid;
		}

		/*
		 * The OIDs of rows that the current transaction inserted and deleted
		 * stay taken until it ends, as dead rows on disk would until vacuum:
		 * a transaction that creates and drops temporary objects in a loop
		 * goes through the slice.  Once the OIDs collide time and again,
		 * free such rows that no snapshot can see any more, as when the area
		 * is full, and go on.
		 */
		if (++collisions >= 1000 && !reclaimed && tc_area != NULL)
		{
			reclaimed = true;
			LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
			(void) tempcat_reclaim_dead();
			LWLockRelease(&tc_root->lock);
		}
	}

	return InvalidOid;
}


Oid
tempcat_allocate_oid(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
	Oid			oid;

	if (RelationGetRelid(relation) == NamespaceRelationId &&
		OidIsValid(tc_namespace_oid))
	{
		oid = tc_namespace_oid;
		tc_namespace_oid = InvalidOid;
		return oid;
	}

	oid = tempcat_try_allocate_oid(relation, indexId, oidcolumn);
	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of OIDs for temporary objects kept in memory"),
				 errdetail("All %u OIDs that the session can give its temporary objects in catalog \"%s\" are taken by them, or by ordinary objects.",
						   tempcat_slice_last(tc_slice) - tempcat_slice_base(tc_slice) + 1,
						   RelationGetRelationName(relation)),
				 errhint("Drop temporary objects, or end the transaction that dropped some.")));
	return oid;
}

/*
 * Take the OID of the session's temporary namespace from its slice.  If
 * ordinary objects took every OID of the slice (created before the range was
 * reserved, or carried over by pg_upgrade), the session keeps its temporary
 * objects on disk, and holds on to the slice, so that other sessions do not
 * take it.
 */
static bool
tempcat_take_namespace_oid(void)
{
	Relation	rel;

	/* (taken already by an attempt that failed later) */
	if (OidIsValid(tc_namespace_oid))
		return true;
	rel = table_open(NamespaceRelationId, AccessShareLock);
	tc_namespace_oid = tempcat_try_allocate_oid(rel, NamespaceOidIndexId,
												Anum_pg_namespace_oid);
	table_close(rel, AccessShareLock);

	if (!OidIsValid(tc_namespace_oid))
		ereport(WARNING,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of OIDs for temporary objects kept in memory"),
				 errdetail("Ordinary objects have all OIDs that the session could give its temporary objects; it keeps them on disk.")));
	return OidIsValid(tc_namespace_oid);
}

/*
 * A segment received a reserved OID for a temporary object of ours.  The
 * coordinator checked that no object of its catalog has it, but an ordinary
 * object can exist on this node alone (created in utility mode, or left by
 * a broken catalog), and two rows with one OID would corrupt the catalog:
 * refuse.  Only live rows (and rows of transactions in progress) count.
 */
static void
tempcat_check_preassigned_oid(Relation relation, Oid indexId,
							  AttrNumber oidcolumn, Oid oid)
{
	ScanKeyData key;
	SysScanDesc scan;
	SnapshotData snapshot;
	bool		taken;

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
}

/*
 * A segment received a pre-assigned OID from the coordinator.  If the
 * coordinator gave a temporary object of this session a reserved OID ('temp'
 * says that the object is one even if the key does not tell), take its
 * slice, check that the OID is free on this node, then remember that we own
 * it.
 */
void
tempcat_note_preassigned_oid(Relation relation, Oid indexId,
							 AttrNumber oidcolumn, const char *objname,
							 Oid namespaceOid, Oid keyOid1, Oid keyOid2,
							 bool temp, Oid oid)
{
	Oid			catalog = RelationGetRelid(relation);

	if (IsBinaryUpgrade || !IsTempcatOid(oid) || tempcat_is_reader())
		return;

	if (temp ||
		tempcat_key_is_temp(catalog, objname, namespaceOid, keyOid1, keyOid2,
							true))
	{
		if (tc_area == NULL && !tempcat_create_area(false))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not create the in-memory catalog of temporary objects on this node"),
					 errdetail("Dynamic shared memory, or slots for its segments, ran out, and so did disk space for files."),
					 errhint("Turn off gp_enable_temp_memory_catalog to keep the session's temporary objects on disk.")));
		tempcat_activate(oid);
		tempcat_check_preassigned_oid(relation, indexId, oidcolumn, oid);
		tempcat_remember_oid(catalog, oid);
	}
}

/* ----------------------------------------------------------------
 * Object restrictions
 * ----------------------------------------------------------------
 */

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
 * see, and after a crash nobody would reset it.  Called for the commutator
 * and the negator (perhaps shells just made) of a new operator in
 * 'namespaceOid'.
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

/*
 * An ordinary object cannot depend on a temporary object kept in memory
 * (e.g. a column of an ordinary table of a temporary table's row type):
 * its on-disk rows would point at an object that other sessions cannot see,
 * and that is gone after a crash.  Nor can anything depend on a temporary
 * object of another session.  Called for each new pg_depend row.
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

	if (tempcat_foreign_object(referenced->classId, referenced->objectId) == TCF_LIVE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot depend on temporary objects of another session")));
}

/*
 * Some objects can be made for temporary types kept in memory, but would
 * not be kept in memory themselves (casts, transforms): refuse.
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

/* ----------------------------------------------------------------
 * Hash tables in the area
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

/* ----------------------------------------------------------------
 * Pending entries and visibility
 * ----------------------------------------------------------------
 */

/*
 * Make room for 'n' more pending entries.  Called before creating or
 * changing entries: running out of memory after that would leave an entry
 * that no transaction end looks at (an aborted insertion never freed).
 */
static void
tempcat_reserve_pending(int n)
{
	/* (see tempcat_abort_subtransaction()) */
	if (tc_resolve_wanted)
		tempcat_resolve_pending();

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
 * it before anything that can fail, e.g. freeing the row: an error must not
 * leave the list holding a row freed already, which the next pass would
 * free again.
 */
static void
tempcat_pending_take(int i)
{
	Assert(i >= 0 && i < tc_npending);
	tc_pending[i] = tc_pending[--tc_npending];
}

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

	/*
	 * Only this session changes the rows, one transaction after another, so
	 * a transaction that is not ours is visible once committed.
	 */
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
 * After removing an entry, stop treating its object's OID as ours if no
 * other version of the object is left in memory.  Keeps the owned set from
 * growing in long sessions.
 */
static void
tempcat_forget_if_gone(int catidx, Oid objoid)
{
#ifdef FAULT_INJECTOR
	/* Test hook: fail while the outcomes are applied (with 'error'). */
	SIMPLE_FAULT_INJECTOR("tempcat_forget_if_gone");
#endif

	if (tempcat_catalogs[catidx].relid == RelationRelationId)
		tc_horizon_dirty = true;

	if (OidIsValid(objoid) && !tempcat_memory_has_oid(catidx, objoid) &&
		tempcat_forget_oid(tempcat_catalogs[catidx].relid, objoid))
	{
		/*
		 * pgstat_vacuum_stat() keeps the statistics of OIDs of live
		 * sessions' temporary objects (tempcat_oid_maybe_live()): purge those
		 * of our dropped tables and functions ourselves.
		 */
		if (tempcat_catalogs[catidx].relid == RelationRelationId)
			pgstat_purge_relation(objoid);
		else if (tempcat_catalogs[catidx].relid == ProcedureRelationId)
			pgstat_purge_function(objoid);
	}
}

/*
 * Apply the outcome of finished transactions to the pending entries: drop
 * aborted inserts and committed deletes, clear aborted deletes.  Entries of
 * a prepared transaction that has not been finished yet stay pending.
 *
 * Called by the writer at the start of each transaction, and when it goes
 * idle (tempcat_idle()), when none of the pending XIDs can be the current
 * one anymore, and after a subtransaction abort.
 */
static void
tempcat_resolve_pending(void)
{
	int			i = 0;
	int			npending = tc_npending;

	tc_resolve_wanted = false;
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
		int			catidx = e->catidx;
		Oid			objoid;
		bool		in_progress = false;

		/* (a row freed already would fail this; see tempcat_pending_take()) */
		Assert(catidx >= 0 && catidx < TEMPCAT_NCATALOGS &&
			   (e->flags & TCE_PENDING));
		objoid = tempcat_catalog_has_oid(catidx) ? tempcat_entry_oid(e) : InvalidOid;

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
 * Command IDs of the snapshots a writer QE handed to cursors in the current
 * transaction (dumpSharedLocalSnapshot_forCursor()).  The cursors' reader
 * gangs scan with them in other processes, where SnapshotsSeeCommandRange()
 * does not see them; they are kept for the rest of the transaction, as the
 * writer does not learn when the cursors close.
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
 * in a loop does not run out of room.  Caller holds the exclusive lock.
 * Returns the number of rows freed.
 *
 * Such a row is dead whatever the transaction's fate, as long as its
 * deletion cannot be undone on its own: rows deleted by the top-level
 * transaction itself, or by the same (sub)transaction that inserted them
 * (any rollback undoing the deletion then undoes the insertion too).  And
 * only rows that no snapshot of ours still in use can see
 * (SnapshotsSeeCommandRange()): an older snapshot of the transaction, e.g.
 * that of the outer query or of a cursor, still sees rows deleted after it
 * was taken.  Scans copy the rows they return when they start.
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
	 * command.
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

/* ----------------------------------------------------------------
 * Transactions
 * ----------------------------------------------------------------
 */

void
tempcat_start_transaction(void)
{
	tc_xact_changed = false;
	tc_change_level = 0;

	/*
	 * A retrieve connection can outlive the writer of the cursor session it
	 * serves, which ends or is replaced (e.g. after its gang was reset).
	 * Detach from that writer's area then.  The next lookup attaches to the
	 * session's current area, if any.
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

	if (tempcat_is_reader() || tc_area == NULL)
		return;

	tempcat_resolve_pending();
	if (tc_horizon_dirty)
		tempcat_recompute_horizon();
	if (tc_roles_dirty)
		tempcat_recompute_roles();
}

/*
 * The session goes idle, out of any transaction: apply the outcome of its
 * finished transactions to the pending rows now, rather than at the start
 * of its next transaction, let the published horizon rise (see
 * tempcat_add_pending()), and republish the roles its rows refer to.  The
 * next transaction may be long in coming, especially for a writer QE.
 *
 * Out of a transaction, an error would not release the lock, and could
 * repeat at every return here: log it instead, and leave the rows to the
 * start of the next transaction, which reports any error as usual.
 */
void
tempcat_idle(void)
{
	MemoryContext oldcxt = CurrentMemoryContext;

	if (tc_area == NULL || tempcat_is_reader() ||
		(tc_npending == 0 && !tc_horizon_dirty && !tc_roles_dirty))
		return;

	PG_TRY();
	{
		tempcat_resolve_pending();
		if (tc_horizon_dirty)
			tempcat_recompute_horizon();
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
 * transaction that changed rows in memory.  Upstream refuses to prepare
 * transactions that operated on temporary objects; Greengage cannot, as its
 * own two-phase commit prepares every distributed transaction on the
 * segments.  For rows in memory it matters more: if the session ends before
 * the prepared transaction is finished, its changes to them are lost,
 * while its on-disk effects (relation files, dependencies on ordinary
 * objects) are committed later anyway.
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
 * Did the current transaction change rows in memory?  A QE reports it to the
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
 * deletions soon, so that loops with exception blocks do not keep dead rows
 * around until the end of the top-level transaction.  Only if it, or a
 * subtransaction committed into it, changed rows.  Not here, in the middle
 * of the abort, where an error could not be handled: before the next change
 * of rows in memory (tempcat_reserve_pending()), or at the start or end of
 * the next transaction, which see the same outcomes.
 */
void
tempcat_abort_subtransaction(void)
{
	int			level = GetCurrentTransactionNestLevel();

	if (tc_change_level < level || tc_area == NULL || tempcat_is_reader())
		return;
	tc_resolve_wanted = true;
	tc_change_level = level - 1;
}

/* The fate of the transaction that swept our slice; see tempcat_activate(). */
static void
tempcat_xact_callback(XactEvent event, void *arg)
{
	/* Whether anything with the OIDs of our slice may have been kept */
	if (tc_slice >= 0 && !tc_slice_used)
	{
		if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT ||
			event == XACT_EVENT_PREPARE)
			tc_slice_used = true;
		else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		{
			tempcat_abandon_slice();
			return;
		}
	}

	if (tc_sweep_state != TCS_IN_XACT)
		return;
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			tc_sweep_state = TCS_DONE;
			break;
		case XACT_EVENT_PRE_PREPARE:
			tc_sweep_xid = GetTopTransactionIdIfAny();
			break;
		case XACT_EVENT_PREPARE:
			tc_sweep_state = TransactionIdIsValid(tc_sweep_xid) ?
				TCS_PREPARED : TCS_NONE;
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			tempcat_sweep_rolled_back();
			break;
		default:
			break;
	}
}

static void
tempcat_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						 SubTransactionId parentSubid, void *arg)
{
	int			level;

	if (tc_sweep_state == TCS_IN_XACT && tc_sweep_subid == mySubid)
	{
		if (event == SUBXACT_EVENT_ABORT_SUB)
			tempcat_sweep_rolled_back();
		else if (event == SUBXACT_EVENT_COMMIT_SUB)
			tc_sweep_subid = parentSubid;
	}

	/* A committed subtransaction's changes are its parent's now. */
	if (event != SUBXACT_EVENT_COMMIT_SUB)
		return;
	level = GetCurrentTransactionNestLevel();
	if (tc_change_level >= level)
		tc_change_level = level - 1;
}

/*
 * Remove the files of a temporary relation whose catalog rows are in
 * memory and going away with the session.
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

/*
 * Runs after the temporary objects were dropped at session exit
 * (RemoveTempRelationsCallback(), registered later, runs earlier), while
 * the area is still attached.  If that cleanup did not finish (it failed, or
 * a prepared transaction kept it from seeing the objects), the files of the
 * relations whose rows are in memory would be left with nothing leading to
 * them until the node restarts: remove them now.
 */
static void
tempcat_before_exit(int code, Datum arg)
{
	int			catidx;
	dsa_pointer ep;

	if (tc_area == NULL)
		return;

	/*
	 * Readers that come later, e.g. retrieve connections, must not attach to
	 * an area that is going away with us.
	 */
	if (SharedLocalSnapshotSlot != NULL &&
		SharedLocalSnapshotSlot->tempcat_handle == dsa_get_handle(tc_area))
		SharedLocalSnapshotSlot->tempcat_handle = DSM_HANDLE_INVALID;

	catidx = tempcat_catalog_index(RelationRelationId);
	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep);)
	{
		TempcatEntry *e = TC_ADDR(ep);
		HeapTupleHeader hdr = TCE_DATA(e);

		if (!TransactionIdIsValid(e->xmax) ||
			tempcat_xid_status(e->xmax) != TCX_COMMITTED)
			tempcat_unlink_files((Form_pg_class) ((char *) hdr + hdr->t_hoff));
		ep = e->next;
	}
}

/* ----------------------------------------------------------------
 * Catalog DML
 * ----------------------------------------------------------------
 */

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
	 * published instead, for DROP ROLE in other sessions to see; beyond what
	 * can be published, the rows go to disk.
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
		return tempcat_owns_oid(TypeRelationId,
								tempcat_getoid(tup, desc, Anum_pg_constraint_contypid));
	}

	if (!tempcat_owner_is_temp(tup, desc, def->owner,
							   def->ownerclass, def->ownerclassattr))
		return false;

	/* Dependencies on ordinary objects stay on disk; see the file header. */
	if (def->owner2 != 0 &&
		!tempcat_owner_is_temp(tup, desc, def->owner2,
							   def->owner2class, def->owner2classattr))
		return false;

	return true;
}

/*
 * A catalog row that SQL writes (INSERT, COPY FROM or UPDATE, under
 * allow_system_table_mods) is checked as the code that makes such rows
 * checks them: dependencies as recordDependencyOn() does, inheritance as
 * StoreSingleInheritance(), operator links as OperatorCreate(), typed tables
 * as heap_create_with_catalog().
 */
void
tempcat_sql_check(Relation rel, HeapTuple tup)
{
	TupleDesc	desc = RelationGetDescr(rel);
	bool		isnull;

	if (TempcatShared == NULL || tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return;
	switch (RelationGetRelid(rel))
	{
		case DependRelationId:
			{
				ObjectAddress depender;
				ObjectAddress referenced;

				depender.classId = tempcat_getoid(tup, desc, Anum_pg_depend_classid);
				depender.objectId = tempcat_getoid(tup, desc, Anum_pg_depend_objid);
				depender.objectSubId = DatumGetInt32(heap_getattr(tup, Anum_pg_depend_objsubid,
																  desc, &isnull));
				referenced.classId = tempcat_getoid(tup, desc, Anum_pg_depend_refclassid);
				referenced.objectId = tempcat_getoid(tup, desc, Anum_pg_depend_refobjid);
				referenced.objectSubId = DatumGetInt32(heap_getattr(tup, Anum_pg_depend_refobjsubid,
																	desc, &isnull));
				tempcat_check_dependency(&depender, &referenced);
			}
			break;
		case InheritsRelationId:
			tempcat_check_inheritance(tempcat_getoid(tup, desc, Anum_pg_inherits_inhrelid),
									  tempcat_getoid(tup, desc, Anum_pg_inherits_inhparent));
			break;
		case OperatorRelationId:
			{
				Oid			nsp = tempcat_getoid(tup, desc, Anum_pg_operator_oprnamespace);

				tempcat_check_operator_link(nsp, tempcat_getoid(tup, desc, Anum_pg_operator_oprcom));
				tempcat_check_operator_link(nsp, tempcat_getoid(tup, desc, Anum_pg_operator_oprnegate));
			}
			break;
		case RelationRelationId:
			{
				Oid			typid = tempcat_getoid(tup, desc, Anum_pg_class_reloftype);

				if (OidIsValid(typid))
					tempcat_check_typed_table(tempcat_getoid(tup, desc, Anum_pg_class_oid),
											  typid);
			}
			break;
		default:
			break;
	}
}

/*
 * A catalog row that SQL adds to the heap (INSERT, COPY FROM, or UPDATE of a
 * row on disk): checked as above, and not one of our objects kept in memory,
 * for which the executor would make index entries.
 */
void
tempcat_sql_insert(Relation rel, HeapTuple tup)
{
	if (TempcatShared == NULL || tempcat_catalog_index(RelationGetRelid(rel)) < 0)
		return;
	tempcat_sql_check(rel, tup);
	if (tempcat_route_insert(rel, tup))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add catalog rows of temporary objects kept in memory with SQL")));
}

/*
 * Refuse a row that would not fit on disk, as heap_insert() and
 * heap_update() would: in a catalog without a TOAST table, one that
 * compression does not bring under MaxHeapTupleSize (e.g. a long column
 * default in pg_attribute, long options in pg_class).
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

/* The area is full even after freeing what could be freed. */
static void
tempcat_report_full(void)
{
	size_t		total;
	size_t		memory;

	dsa_get_sizes(tc_area, &total, &memory);
	ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("out of room for the catalog rows of temporary objects"),
			 errdetail("The session keeps them in %zu kB of memory and %zu kB of files.",
					   memory / 1024, (total - memory) / 1024),
			 errhint("Drop temporary objects, or raise gp_temp_memory_catalog_max_size or gp_temp_memory_catalog_max_file_size for new sessions.")));
}

/*
 * Create an entry, freeing dead rows of the current transaction if the area
 * is full.  Caller holds the exclusive lock, and releases it on error.
 */
static dsa_pointer
tempcat_entry_create_or_reclaim(int catidx, HeapTuple tup, TupleDesc desc,
								TransactionId xid, CommandId cid)
{
	dsa_pointer ep;

	ep = tempcat_entry_create(catidx, tup, desc, xid, cid);
	if (!DsaPointerIsValid(ep) && tempcat_reclaim_dead() > 0)
		ep = tempcat_entry_create(catidx, tup, desc, xid, cid);
	return ep;
}

/* Insert a row in memory. */
void
tempcat_insert(Relation rel, HeapTuple tup)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	TransactionId xid;
	CommandId	cid;
	dsa_pointer ep;

	Assert(catidx >= 0);
	Assert(!tempcat_is_reader() && tc_area != NULL);
	tempcat_check_row_size(rel, tup);

	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	/* (where a segment that is a file would go; see dsm_create()) */
	PrepareTempTablespaces();
	tempcat_reserve_pending(1);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	ep = tempcat_entry_create_or_reclaim(catidx, tup, RelationGetDescr(rel),
										 xid, cid);
	if (DsaPointerIsValid(ep))
		tempcat_add_pending(ep);
	LWLockRelease(&tc_root->lock);

	if (!DsaPointerIsValid(ep))
		tempcat_report_full();

	tempcat_set_caller_tuple(tup, rel, TC_ADDR(ep));
	if (RelationGetRelid(rel) == RelationRelationId)
		tempcat_cover_pg_class_row(tup->t_data);

	CacheInvalidateHeapTuple(rel, tup, NULL);
}

static TempcatEntry *
tempcat_lookup_tid(Relation rel, ItemPointer tid, dsa_pointer *ep)
{
	if (tc_area == NULL && !tempcat_attach())
		elog(ERROR, "tempcat: no in-memory catalog for row (%u,%u)",
			 ItemPointerGetBlockNumberNoCheck(tid), tid->ip_posid);

	*ep = tempcat_find_tid(tid);
	if (!DsaPointerIsValid(*ep))
		elog(ERROR, "tempcat: row (%u,%u) of \"%s\" not found in memory",
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

/* Replace a row in memory by a new version, in memory. */
void
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

	PrepareTempTablespaces();
	tempcat_reserve_pending(2);
	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	newep = tempcat_entry_create_or_reclaim(catidx, newtup, RelationGetDescr(rel),
											xid, cid);
	if (!DsaPointerIsValid(newep))
	{
		LWLockRelease(&tc_root->lock);
		tempcat_report_full();
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
 * Overwrite a row in place, non-transactionally, like heap_inplace_update().
 * In-place updates overwrite fixed-width columns only.
 */
void
tempcat_inplace_update(Relation rel, HeapTuple tup)
{
	dsa_pointer ep;
	TempcatEntry *e;
	HeapTupleHeader hdr;

	Assert(!tempcat_is_reader());

	e = tempcat_lookup_tid(rel, &tup->t_self, &ep);
	hdr = TCE_DATA(e);
	if (tup->t_len != e->len || tup->t_data->t_hoff != hdr->t_hoff)
		elog(ERROR, "wrong tuple length");

	LWLockAcquire(&tc_root->lock, LW_EXCLUSIVE);
	memcpy((char *) hdr + hdr->t_hoff,
		   (char *) tup->t_data + tup->t_data->t_hoff,
		   tup->t_len - hdr->t_hoff);
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

/* Compare two values of key column 'i' of the scan's index. */
static int
tempcat_compare_key(TempcatScan scan, int i,
					Datum ad, bool anull, Datum bd, bool bnull)
{
	int			cmp;

	if (anull || bnull)
	{
		bool		nulls_first =
		(scan->irel->rd_indoption[i] & INDOPTION_NULLS_FIRST) != 0;

		if (anull && bnull)
			return 0;
		return (anull == nulls_first) ? -1 : 1;
	}

	cmp = DatumGetInt32(FunctionCall2Coll(scan->procs[i],
										  scan->irel->rd_indcollation[i],
										  ad, bd));
	if (scan->irel->rd_indoption[i] & INDOPTION_DESC)
		cmp = -cmp;
	return cmp;
}

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
		int			cmp = tempcat_compare_key(scan, i, ad, anull, bd, bnull);

		if (cmp != 0)
			return cmp;
	}
	return 0;
}

/* Compare a row of the scanned catalog with an entry of the scan's index. */
static int
tempcat_compare_itup(TempcatScan scan, HeapTuple a,
					 IndexTuple b, TupleDesc itupdesc)
{
	int			i;

	for (i = 0; i < scan->nidxkeys; i++)
	{
		AttrNumber	attno = scan->irel->rd_index->indkey.values[i];
		bool		anull;
		bool		bnull;
		Datum		ad = heap_getattr(a, attno, scan->heapdesc, &anull);
		Datum		bd = index_getattr(b, i + 1, itupdesc, &bnull);
		int			cmp = tempcat_compare_key(scan, i, ad, anull, bd, bnull);

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

/*
 * Collect the visible rows in memory matching the scan keys.  Must be called
 * while the keys still refer to heap attribute numbers, i.e. before
 * systable_beginscan() converts them to index column numbers.  With an
 * index, the rows are sorted by its key, to be merged with the on-disk rows
 * (tempcat_getnext()).
 *
 * Returns NULL if there can be no rows in memory for this catalog.
 */
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
	if (tc_area == NULL && !tempcat_attach())
		return NULL;
	catidx = tempcat_catalog_index(RelationGetRelid(rel));
	if (catidx < 0)
		return NULL;

	cat = &tc_root->catalogs[catidx];
	if (cat->nentries == 0)
		return NULL;

	def = &tempcat_catalogs[catidx];
	desc = RelationGetDescr(rel);
	if (snapshot == NULL)
		snapshot = GetCatalogSnapshot(RelationGetRelid(rel));

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
	scan->idxcoldesc = NULL;
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
 * Begin a scan of the visible rows in memory of catalog 'rel' for a scan of
 * its index 'irel' by a plan, or return NULL if there are none.  Index and
 * index-only scans merge the rows into the scan of the index (see
 * index_getnext_slot() and index_getnext_tid()), bitmap heap scans return
 * them after the heap (see BitmapHeapNext()); without 'irel' and keys for
 * the latter, all rows.  The keys are those of the index scan: those on
 * simple index columns are converted to heap ones, the others are dropped,
 * as the executor rechecks the rows in memory against the original quals.
 */
TempcatScan
tempcat_begin_index_scan(Relation rel, Relation irel, Snapshot snapshot,
						 int nkeys, ScanKey keys)
{
	TempcatScan scan;
	ScanKey		hkeys;
	int			nhkeys = 0;
	int			i;

	Assert(irel != NULL || nkeys == 0);
	if (snapshot == NULL || !IsMVCCSnapshot(snapshot) ||
		!tempcat_has_rows(rel))
		return NULL;

	hkeys = palloc(sizeof(ScanKeyData) * Max(nkeys, 1));
	for (i = 0; i < nkeys; i++)
	{
		ScanKey		k = &keys[i];
		AttrNumber	attno;

		if (k->sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_SEARCHARRAY |
						   SK_SEARCHNULL | SK_SEARCHNOTNULL | SK_ORDER_BY))
			continue;
		if (k->sk_attno < 1 ||
			k->sk_attno > IndexRelationGetNumberOfKeyAttributes(irel))
			continue;
		attno = irel->rd_index->indkey.values[k->sk_attno - 1];
		if (attno <= 0)
			continue;
		hkeys[nhkeys] = *k;
		hkeys[nhkeys].sk_attno = attno;
		nhkeys++;
	}

	scan = tempcat_beginscan(rel, irel, snapshot, nhkeys, hkeys);
	pfree(hkeys);
	if (scan != NULL && scan->ntuples == 0)
	{
		tempcat_endscan(scan);
		scan = NULL;
	}
	return scan;
}

/* The next row in memory in index order, without taking it. */
static HeapTuple
tempcat_merge_peek(TempcatScan scan, bool forward)
{
	int			dir = forward ? 1 : -1;

	Assert(scan->irel != NULL);
	if (scan->direction == 0)
		scan->direction = dir;
	else if (scan->direction != dir)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the direction of a catalog scan that includes rows of temporary objects kept in memory")));

	if (scan->next >= scan->ntuples)
		return NULL;
	return scan->tuples[forward ? scan->next : scan->ntuples - 1 - scan->next];
}

/* The position in the rows in memory of a scan through an index */
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

/*
 * The next row in memory of a scan through an index to return before the
 * on-disk row 'disk' (NULL if there are no more of those), or NULL if 'disk'
 * comes first.  Both come in index order, backward if !forward.
 */
HeapTuple
tempcat_merge_next(TempcatScan scan, bool forward, HeapTuple disk)
{
	HeapTuple	vtup = tempcat_merge_peek(scan, forward);

	if (vtup == NULL ||
		(disk != NULL && tempcat_compare(scan, vtup, disk) * (forward ? 1 : -1) > 0))
		return NULL;
	scan->next++;
	return vtup;
}

/* Likewise, before the index entry 'disk' of an index-only scan */
HeapTuple
tempcat_merge_next_itup(TempcatScan scan, bool forward,
						IndexTuple disk, TupleDesc itupdesc)
{
	HeapTuple	vtup = tempcat_merge_peek(scan, forward);

	if (vtup == NULL ||
		(disk != NULL &&
		 tempcat_compare_itup(scan, vtup, disk, itupdesc) * (forward ? 1 : -1) > 0))
		return NULL;
	scan->next++;
	return vtup;
}

/*
 * The columns of the scan's index of row 'tup', as an index-only scan
 * returns them: in the types of the catalog's columns, which those of the
 * index may differ from (btree name_ops store cstrings), with their row
 * type in *desc.
 */
HeapTuple
tempcat_index_columns(TempcatScan scan, HeapTuple tup, TupleDesc *desc)
{
	int			natts = IndexRelationGetNumberOfAttributes(scan->irel);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	int			i;

	if (scan->idxcoldesc == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(scan));

		scan->idxcoldesc = CreateTemplateTupleDesc(natts);
		for (i = 0; i < natts; i++)
		{
			AttrNumber	attno = scan->irel->rd_index->indkey.values[i];

			if (attno <= 0)
				elog(ERROR, "cannot return column %d of index \"%s\" of a row in memory",
					 i + 1, RelationGetRelationName(scan->irel));
			TupleDescCopyEntry(scan->idxcoldesc, i + 1, scan->heapdesc, attno);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	for (i = 0; i < natts; i++)
		values[i] = heap_getattr(tup, scan->irel->rd_index->indkey.values[i],
								 scan->heapdesc, &isnull[i]);
	*desc = scan->idxcoldesc;
	return heap_form_tuple(scan->idxcoldesc, values, isnull);
}

/*
 * Return the next row: rows in memory merged with the on-disk rows that
 * fetch_disk() returns.  For scans through an index both come in index
 * order (backward if !forward, for ordered scans); otherwise the rows in
 * memory come first.
 */
HeapTuple
tempcat_getnext_dir(TempcatScan scan, bool forward,
					HeapTuple (*fetch_disk) (void *), void *arg)
{
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

	if (scan->disk_tuple == NULL && !scan->disk_done)
	{
		scan->disk_tuple = fetch_disk(arg);
		if (scan->disk_tuple == NULL)
			scan->disk_done = true;
	}

	vtup = tempcat_merge_next(scan, forward, scan->disk_tuple);
	if (vtup != NULL)
	{
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

/* Does the catalog hold rows in memory that this process can see? */
bool
tempcat_has_rows(Relation rel)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));

	return catidx >= 0 && (tc_area != NULL || tempcat_attach()) &&
		tc_root->catalogs[catidx].nentries > 0;
}

/* The next row in memory of a scan without an index, or NULL. */
HeapTuple
tempcat_next_virtual(TempcatScan scan)
{
	if (scan->next < scan->ntuples)
		return scan->tuples[scan->next++];
	return NULL;
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
	if (scan->idxcoldesc)
		FreeTupleDesc(scan->idxcoldesc);
	pfree(scan);
}

/*
 * Fetch a row in memory by TID, if visible to the snapshot.  Returns a
 * palloc'd copy, or NULL.
 */
HeapTuple
tempcat_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	dsa_pointer ep;
	HeapTuple	result = NULL;
	bool		reader;

	if (tempcat_catalog_index(RelationGetRelid(rel)) < 0 ||
		(tc_area == NULL && !tempcat_attach()))
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

/*
 * Does catalog 'rel' hold rows in memory that 'snapshot' sees?
 */
static bool
tempcat_has_visible_rows(Relation rel, Snapshot snapshot)
{
	int			catidx = tempcat_catalog_index(RelationGetRelid(rel));
	bool		reader;
	bool		found = false;
	dsa_pointer ep;

	if (catidx < 0 || (tc_area == NULL && !tempcat_attach()) ||
		tc_root->catalogs[catidx].nentries == 0)
		return false;
	if (snapshot == NULL)
		return true;

	reader = tempcat_is_reader();
	if (reader)
		LWLockAcquire(&tc_root->lock, LW_SHARED);
	for (ep = tc_root->catalogs[catidx].head; DsaPointerIsValid(ep) && !found;)
	{
		TempcatEntry *e = TC_ADDR(ep);

		found = tempcat_entry_visible(e, snapshot);
		ep = e->next;
	}
	if (reader)
		LWLockRelease(&tc_root->lock);
	return found;
}

/*
 * Sample scans of a catalog do not see its rows in memory (they read pages
 * of the heap): refuse them rather than miss rows.
 */
void
tempcat_check_sample_scan(Relation rel, Snapshot snapshot)
{
	if (tempcat_has_visible_rows(rel, snapshot))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot sample catalog \"%s\", which holds rows of temporary objects kept in memory",
						RelationGetRelationName(rel))));
}

/* ----------------------------------------------------------------
 * Objects of other sessions
 * ----------------------------------------------------------------
 */

/*
 * What is the object, which an on-disk pg_depend or pg_shdepend row of this
 * database refers to, to us?  A temporary object of another session kept
 * in memory is visible to that session only: if it lives on, the row
 * protects whatever it depends on (TCF_LIVE); if the session is gone, it is
 * a leftover (TCF_GONE), which tempcat_forget_leftover() removes.  Objects
 * of our own session and ordinary objects (also those with reserved OIDs,
 * see the file header) are TCF_NONE.
 *
 * A session publishes its slice of the reserved range as live in its
 * database once it has removed the leftovers of the slice's earlier
 * holders there (tempcat_activate()); until it lets go of it at exit, all
 * rows in the slice are its own.
 */
TempcatForeign
tempcat_foreign_object(Oid classId, Oid objectId)
{
	TempcatSliceState *ss;
	int			slice;
	int			i;

	if (TempcatShared == NULL || !IsTempcatOid(objectId) || IsBinaryUpgrade)
		return TCF_NONE;

	/* only objects of these catalogs can be temporary objects in memory */
	for (i = 0; i < lengthof(tempcat_object_classes); i++)
	{
		if (tempcat_object_classes[i] == classId)
			break;
	}
	if (i == lengthof(tempcat_object_classes))
		return TCF_NONE;

	if (tempcat_session_owns_oid(classId, objectId))
		return TCF_NONE;
	if (tempcat_disk_object_exists(classId, objectId))
		return TCF_NONE;

	/*
	 * In our own slice, what is not ours is a leftover, e.g. one that the
	 * sweep of the slice left as somebody else was deleting it then.
	 */
	slice = tempcat_slice_of(objectId);
	if (!tempcat_is_reader() && slice == tc_slice)
		return TCF_GONE;

	ss = &TempcatShared->slices[slice];
	if (pg_atomic_read_u32(&ss->holder) != 0)
	{
		pg_read_barrier();
		if (ss->databaseId == MyDatabaseId)
			return TCF_LIVE;
	}
	return TCF_GONE;
}

/*
 * Ordinary tables and functions with reserved OIDs (created before the
 * range was reserved, or carried over by pg_upgrade) dropped by the current
 * transaction, at subtransaction nesting levels.  pgstat_vacuum_stat() keeps
 * their statistics as long as a live session holds the slice of their OIDs
 * (tempcat_oid_maybe_live()): purge them at commit.
 */
typedef struct TempcatDiskDrop
{
	Oid			catalog;
	Oid			oid;
	int			level;
} TempcatDiskDrop;

static TempcatDiskDrop *tc_disk_drops = NULL;
static int	tc_ndisk_drops = 0;
static int	tc_max_disk_drops = 0;

/*
 * Those of the transaction this process prepared last, until it finishes it
 * (FinishPreparedTransaction(), on a QE in the same process): purge them
 * only if it commits.  If another process finishes it, e.g. after a
 * restart, they are not purged, as before this was done.
 */
static TempcatDiskDrop *tc_prepared_drops = NULL;
static int	tc_nprepared_drops = 0;
static TransactionId tc_prepared_drops_xid = InvalidTransactionId;
static TransactionId tc_preparing_xid = InvalidTransactionId;

static void
tempcat_purge_disk_drops(TempcatDiskDrop *drops, int n)
{
	int			i;

	for (i = 0; i < n; i++)
	{
		if (drops[i].catalog == RelationRelationId)
			pgstat_purge_relation(drops[i].oid);
		else
			pgstat_purge_function(drops[i].oid);
	}
}

static void
tempcat_disk_drop_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			tempcat_purge_disk_drops(tc_disk_drops, tc_ndisk_drops);
			tc_ndisk_drops = 0;
			break;
		case XACT_EVENT_ABORT:
			tc_ndisk_drops = 0;
			break;
		case XACT_EVENT_PRE_PREPARE:
			tc_preparing_xid = GetTopTransactionIdIfAny();
			break;
		case XACT_EVENT_PREPARE:
			/* (a transaction prepared earlier and not finished here: lost) */
			if (tc_prepared_drops != NULL)
				pfree(tc_prepared_drops);
			tc_prepared_drops = NULL;
			tc_nprepared_drops = 0;
			tc_prepared_drops_xid = InvalidTransactionId;
			if (tc_ndisk_drops > 0 && TransactionIdIsValid(tc_preparing_xid))
			{
				tc_prepared_drops = MemoryContextAlloc(TopMemoryContext,
													   sizeof(TempcatDiskDrop) * tc_ndisk_drops);
				memcpy(tc_prepared_drops, tc_disk_drops,
					   sizeof(TempcatDiskDrop) * tc_ndisk_drops);
				tc_nprepared_drops = tc_ndisk_drops;
				tc_prepared_drops_xid = tc_preparing_xid;
			}
			tc_ndisk_drops = 0;
			tc_preparing_xid = InvalidTransactionId;
			break;
		default:
			break;
	}
}

/* The prepared transaction 'xid' is committed or rolled back. */
void
tempcat_finish_prepared(TransactionId xid, bool isCommit)
{
	if (tc_nprepared_drops == 0 || !TransactionIdEquals(xid, tc_prepared_drops_xid))
		return;
	if (isCommit)
		tempcat_purge_disk_drops(tc_prepared_drops, tc_nprepared_drops);
	pfree(tc_prepared_drops);
	tc_prepared_drops = NULL;
	tc_nprepared_drops = 0;
	tc_prepared_drops_xid = InvalidTransactionId;
}

static void
tempcat_disk_drop_subxact_callback(SubXactEvent event,
								   SubTransactionId mySubid,
								   SubTransactionId parentSubid, void *arg)
{
	int			level = GetCurrentTransactionNestLevel();
	int			i;

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		while (tc_ndisk_drops > 0 &&
			   tc_disk_drops[tc_ndisk_drops - 1].level >= level)
			tc_ndisk_drops--;
	}
	else if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		for (i = 0; i < tc_ndisk_drops; i++)
		{
			if (tc_disk_drops[i].level >= level)
				tc_disk_drops[i].level = level - 1;
		}
	}
}

/* An on-disk pg_class or pg_proc row at 'tid' with OID 'oid' is deleted. */
void
tempcat_note_disk_drop(Oid catalog, Oid oid, ItemPointer tid)
{
	static bool registered = false;

	Assert(catalog == RelationRelationId || catalog == ProcedureRelationId);
	if (!IsTempcatOid(oid) || IsTempcatTid(tid) || IsBinaryUpgrade)
		return;

	if (!registered)
	{
		RegisterXactCallback(tempcat_disk_drop_xact_callback, NULL);
		RegisterSubXactCallback(tempcat_disk_drop_subxact_callback, NULL);
		registered = true;
	}
	if (tc_ndisk_drops >= tc_max_disk_drops)
	{
		int			newmax = Max(16, tc_max_disk_drops * 2);

		if (tc_disk_drops == NULL)
			tc_disk_drops = MemoryContextAlloc(TopMemoryContext,
											   sizeof(TempcatDiskDrop) * newmax);
		else
			tc_disk_drops = repalloc(tc_disk_drops,
									 sizeof(TempcatDiskDrop) * newmax);
		tc_max_disk_drops = newmax;
	}
	tc_disk_drops[tc_ndisk_drops].catalog = catalog;
	tc_disk_drops[tc_ndisk_drops].oid = oid;
	tc_disk_drops[tc_ndisk_drops].level = GetCurrentTransactionNestLevel();
	tc_ndisk_drops++;
}

/*
 * May 'oid' be that of a temporary object that a live session of database
 * 'dbid' keeps in memory?  For pgstat_vacuum_stat(), which cannot see them.
 */
bool
tempcat_oid_maybe_live(Oid oid, Oid dbid)
{
	TempcatSliceState *ss;

	if (TempcatShared == NULL || !IsTempcatOid(oid))
		return false;
	ss = &TempcatShared->slices[tempcat_slice_of(oid)];
	if (pg_atomic_read_u32(&ss->holder) == 0)
		return false;
	pg_read_barrier();
	return ss->databaseId == dbid;
}

/* Remove a pg_depend or pg_shdepend row of a leftover (TCF_GONE). */
void
tempcat_forget_leftover(Relation rel, HeapTuple tup)
{
	Assert(RelationGetRelid(rel) == DependRelationId ||
		   RelationGetRelid(rel) == SharedDependRelationId);
	if (!IsTempcatTid(&tup->t_self))
		(void) tempcat_delete_leftover(rel, &tup->t_self);
}

/*
 * ALTER TYPE or ALTER DOMAIN found an object using type 'typeName' through
 * an on-disk pg_depend row, and is about to open and check it.  A temporary
 * object of another session kept in memory cannot be: refuse.  Returns true
 * if the object is a leftover, to be skipped.
 */
bool
tempcat_check_type_user(Oid classId, Oid objectId, const char *typeName)
{
	switch (tempcat_foreign_object(classId, objectId))
	{
		case TCF_NONE:
			return false;
		case TCF_GONE:
			return true;
		case TCF_LIVE:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter type \"%s\" because a temporary object of another session uses it",
							typeName)));
	}
	return false;
}
