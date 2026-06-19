/*-------------------------------------------------------------------------
 *
 * indexing.h
 *	  This file provides some definitions to support indexing
 *	  on system catalogs
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/indexing.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXING_H
#define INDEXING_H

#include "access/htup.h"
#include "nodes/execnodes.h"
#include "utils/relcache.h"

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *CatalogIndexState;

/*
 * Cap the maximum amount of bytes allocated for multi-inserts with system
 * catalogs, limiting the number of slots used.
 */
#define MAX_CATALOG_MULTI_INSERT_BYTES 65535

/*
 * Greengage/GPDB-specific catalog indexes.  PG15 distributes index DECLAREs
 * into the per-catalog headers; the following belong to GPDB-only catalogs
 * (or GPDB-added columns on shared catalogs) and are kept here in the PG15
 * 4-arg DECLARE form so genbki generates their <Name>IndexId macros.
 */
DECLARE_INDEX(pg_authid_rolresqueue_index, 6029, AuthIdRolResQueueIndexId, on pg_authid using btree(rolresqueue oid_ops));
DECLARE_INDEX(pg_authid_rolresgroup_index, 6440, AuthIdRolResGroupIndexId, on pg_authid using btree(rolresgroup oid_ops));
DECLARE_INDEX(pg_auth_time_constraint_authid_index, 6449, AuthTimeConstraintAuthIdIndexId, on pg_auth_time_constraint using btree(authid oid_ops));
DECLARE_UNIQUE_INDEX(gp_distribution_policy_localoid_index, 8103, GpPolicyLocalOidIndexId, on gp_distribution_policy using btree(localoid oid_ops));
DECLARE_UNIQUE_INDEX(pg_appendonly_relid_index, 7141, AppendOnlyRelidIndexId, on pg_appendonly using btree(relid oid_ops));
DECLARE_UNIQUE_INDEX(gp_fastsequence_objid_objmod_index, 6067, FastSequenceObjidObjmodIndexId, on gp_fastsequence using btree(objid oid_ops, objmod  int8_ops));
DECLARE_UNIQUE_INDEX(pg_statlastop_classid_objid_staactionname_index, 6054, StatLastOpClassidObjidStaactionnameIndexId, on pg_stat_last_operation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
DECLARE_INDEX(pg_statlastshop_classid_objid_index, 6057, StatLastShOpClassidObjidIndexId, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops));
DECLARE_UNIQUE_INDEX(pg_statlastshop_classid_objid_staactionname_index, 6058, StatLastShOpClassidObjidStaactionnameIndexId, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
DECLARE_UNIQUE_INDEX(pg_resqueue_oid_index, 6027, ResQueueOidIndexId, on pg_resqueue using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_resqueue_rsqname_index, 6028, ResQueueRsqnameIndexId, on pg_resqueue using btree(rsqname name_ops));
DECLARE_UNIQUE_INDEX(pg_resourcetype_oid_index, 6061, ResourceTypeOidIndexId, on pg_resourcetype using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_resourcetype_restypid_index, 6062, ResourceTypeRestypidIndexId, on pg_resourcetype using btree(restypid int2_ops));
DECLARE_UNIQUE_INDEX(pg_resourcetype_resname_index, 6063, ResourceTypeResnameIndexId, on pg_resourcetype using btree(resname name_ops));
DECLARE_INDEX(pg_resqueuecapability_resqueueid_index, 6442, ResQueueCapabilityResqueueidIndexId, on pg_resqueuecapability using btree(resqueueid oid_ops));
DECLARE_INDEX(pg_resqueuecapability_restypid_index, 6443, ResQueueCapabilityRestypidIndexId, on pg_resqueuecapability using btree(restypid int2_ops));
DECLARE_UNIQUE_INDEX(pg_resgroup_oid_index, 6447, ResGroupOidIndexId, on pg_resgroup using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_resgroup_rsgname_index, 6444, ResGroupRsgnameIndexId, on pg_resgroup using btree(rsgname name_ops));
DECLARE_UNIQUE_INDEX(pg_resgroupcapability_resgroupid_reslimittype_index, 6445, ResGroupCapabilityResgroupidResLimittypeIndexId, on pg_resgroupcapability using btree(resgroupid oid_ops, reslimittype int2_ops));
DECLARE_INDEX(pg_resgroupcapability_resgroupid_index, 6446, ResGroupCapabilityResgroupidIndexId, on pg_resgroupcapability using btree(resgroupid oid_ops));
DECLARE_UNIQUE_INDEX(gp_segment_config_content_preferred_role_index, 7139, GpSegmentConfigContentPreferred_roleIndexId, on gp_segment_configuration using btree(content int2_ops, preferred_role char_ops));
DECLARE_UNIQUE_INDEX(gp_segment_config_dbid_index, 7140, GpSegmentConfigDbidIndexId, on gp_segment_configuration using btree(dbid int2_ops));
DECLARE_UNIQUE_INDEX(pg_extprotocol_oid_index, 7156, ExtprotocolOidIndexId, on pg_extprotocol using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_extprotocol_ptcname_index, 7177, ExtprotocolPtcnameIndexId, on pg_extprotocol using btree(ptcname name_ops));
DECLARE_INDEX(pg_attribute_encoding_attrelid_index, 6236, AttributeEncodingAttrelidIndexId, on pg_attribute_encoding using btree(attrelid oid_ops));
DECLARE_UNIQUE_INDEX(pg_attribute_encoding_attrelid_attnum_index, 6237, AttributeEncodingAttrelidAttnumIndexId, on pg_attribute_encoding using btree(attrelid oid_ops, attnum int2_ops));
DECLARE_UNIQUE_INDEX(pg_type_encoding_typid_index, 6207, TypeEncodingTypidIndexId, on pg_type_encoding using btree(typid oid_ops));
DECLARE_UNIQUE_INDEX(pg_proc_callback_profnoid_promethod_index, 9926, ProcCallbackProfnoidPromethodIndexId, on pg_proc_callback using btree(profnoid oid_ops, promethod char_ops));
DECLARE_UNIQUE_INDEX(pg_compression_compname_index, 7059, CompressionCompnameIndexId, on pg_compression using btree(compname name_ops));
DECLARE_UNIQUE_INDEX(gp_partition_template_relid_level_index, 8023, GpPartitionTemplateRelidLevelIndexId, on gp_partition_template using btree(relid oid_ops, level int2_ops));

/*
 * indexing.c prototypes
 */
extern CatalogIndexState CatalogOpenIndexes(Relation heapRel);
extern void CatalogCloseIndexes(CatalogIndexState indstate);
extern void CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleInsertWithInfo(Relation heapRel, HeapTuple tup,
									   CatalogIndexState indstate);
extern void CatalogTuplesMultiInsertWithInfo(Relation heapRel,
											 TupleTableSlot **slot,
											 int ntuples,
											 CatalogIndexState indstate);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid,
							   HeapTuple tup);
extern void CatalogTupleUpdateWithInfo(Relation heapRel,
									   ItemPointer otid, HeapTuple tup,
									   CatalogIndexState indstate);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#endif							/* INDEXING_H */
