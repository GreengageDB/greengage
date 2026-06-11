/*-------------------------------------------------------------------------
 *
 * resgroupcmds.c
 *	  Commands for manipulating resource group.
 *
 * Portions Copyright (c) 2006-2017, Greenplum inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *    src/backend/commands/resgroupcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "funcapi.h"
#include "libpq-fe.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_resgroupcapability.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "commands/resgroupcmds.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/resgroup.h"
#include "utils/cgroup.h"
#include "utils/cgroup_io_limit.h"
#include "utils/resource_manager.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/faultinjector.h"

#define RESGROUP_DEFAULT_CONCURRENCY (20)
#define RESGROUP_DEFAULT_CPU_WEIGHT (100)

#define RESGROUP_MIN_CONCURRENCY	(0)
#define RESGROUP_MAX_CONCURRENCY	(MaxConnections)

#define RESGROUP_MAX_CPU_MAX_PERCENT	(100)
#define RESGROUP_MIN_CPU_MAX_PERCENT	(1)

#define RESGROUP_MIN_CPU_WEIGHT	(1)
#define RESGROUP_MAX_CPU_WEIGHT	(500)

#define RESGROUP_MIN_MEMORY_LIMIT (0)
#define RESGROUP_DEFAULT_MEMORY_LIMIT (-1)

#define RESGROUP_MIN_MIN_COST		(0)
static int str2Int(const char *str, const char *prop);
static ResGroupLimitType getResgroupOptionType(const char* defname);
static ResGroupCap getResgroupOptionValue(DefElem *defel);
static const char *getResgroupOptionName(ResGroupLimitType type);
static void checkResgroupCapLimit(ResGroupLimitType type, ResGroupCap value);
static void parseStmtOptions(CreateResourceGroupStmt *stmt, ResGroupCaps *caps);
static void validateCapabilities(Relation rel, Oid groupid, ResGroupCaps *caps, bool newGroup);
static void insertResgroupCapabilityEntry(Relation rel, Oid groupid, uint16 type, const char *value);
static void updateResgroupCapabilityEntry(Relation rel,
										  Oid groupId,
										  ResGroupLimitType limitType,
										  ResGroupCap value,
										  const char *strValue);
static void insertResgroupCapabilities(Relation rel, Oid groupId, ResGroupCaps *caps);
static void deleteResgroupCapabilities(Oid groupid);
static void checkAuthIdForDrop(Oid groupId);
static void createResgroupCallback(XactEvent event, void *arg);
static void dropResgroupCallback(XactEvent event, void *arg);
static void alterResgroupCallback(XactEvent event, void *arg);
static void alterResgroupSubXactCallback(SubXactEvent event,
										 SubTransactionId mySubid,
										 SubTransactionId parentSubid,
										 void *arg);
static void checkCpusetSyntax(const char *cpuset);
static bool resGroupCapFieldMatches(const ResourceGroupCallbackContext *ctx,
									const ResGroupCaps *caps);
static ResourceGroupCallbackContext *resgroupAlterCallbackFindPreparedDuplicate(
											List *prepared,
											ResourceGroupCallbackContext *ctx);
static void freeResgroupAlterCallback(ResourceGroupCallbackContext *ctx);
static bool resGroupIOLimitMatches(List *left, List *right);
static bool resgroupHasLaterCpuCallbackMatchingFinal(
										List *callbacks,
										ResourceGroupCallbackContext *ctx,
										const ResGroupCaps *finalCaps);

static List *resgroup_alter_callbacks = NIL;

/*
 * CREATE RESOURCE GROUP
 */
void
CreateResourceGroup(CreateResourceGroupStmt *stmt)
{
	Relation	pg_resgroup_rel;
	Relation	pg_resgroupcapability_rel;
	TupleDesc	pg_resgroup_dsc;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Oid			groupid;
	Datum		new_record[Natts_pg_resgroup];
	bool		new_record_nulls[Natts_pg_resgroup];
	ResGroupCaps caps;
	int			nResGroups;
	MemoryContext oldContext;

	/* Permission check - only superuser can create groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create resource groups")));

	/*
	 * Check for an illegal name ('none' is used to signify no group in ALTER ROLE).
	 */
	if (strcmp(stmt->name, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("resource group name \"none\" is reserved")));

	ClearResGroupCaps(&caps);
	parseStmtOptions(stmt, &caps);

	/*
	 * Both CREATE and ALTER resource group need check the intersection of cpuset,
	 * to make it simple, acquire ExclusiveLock lock on pg_resgroupcapability at
	 * the beginning of CREATE and ALTER.
	 */
	pg_resgroupcapability_rel = table_open(ResGroupCapabilityRelationId, ExclusiveLock);
	pg_resgroup_rel = table_open(ResGroupRelationId, RowExclusiveLock);

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, false,
							   NULL, 0, NULL);
	nResGroups = 0;
	while (systable_getnext(sscan) != NULL)
		nResGroups++;
	systable_endscan(sscan);

	/* Check if MaxResourceGroups limit is reached */
	if (nResGroups >= MaxResourceGroups)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("insufficient resource groups available")));

	/*
	 * Check the pg_resgroup relation to be certain the group doesn't already
	 * exist.
	 */
	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   NULL, 1, &scankey);

	if (HeapTupleIsValid(systable_getnext(sscan)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("resource group \"%s\" already exists",
						stmt->name)));

	systable_endscan(sscan);

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroup_rsgname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->name));

	new_record[Anum_pg_resgroup_parent - 1] = ObjectIdGetDatum(0);

	groupid = GetNewOidForResGroup(pg_resgroup_rel, ResGroupOidIndexId,
								   Anum_pg_resgroup_oid,
								   stmt->name);
	new_record[Anum_pg_resgroup_oid - 1] = groupid;

	pg_resgroup_dsc = RelationGetDescr(pg_resgroup_rel);
	tuple = heap_form_tuple(pg_resgroup_dsc, new_record, new_record_nulls);

	/*
	 * Insert new record in the pg_resgroup table
	 */
	CatalogTupleInsert(pg_resgroup_rel, tuple);

	/* process the WITH (...) list items */
	validateCapabilities(pg_resgroupcapability_rel, groupid, &caps, true);
	insertResgroupCapabilities(pg_resgroupcapability_rel, groupid, &caps);

	/* Dispatch the statement to segments */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
		MetaTrackAddObject(ResGroupRelationId,
						   groupid,
						   GetUserId(), /* not ownerid */
						   "CREATE", "RESOURCE GROUP");
	}

	table_close(pg_resgroup_rel, NoLock);
	table_close(pg_resgroupcapability_rel, NoLock);

	/* Add this group into shared memory */
	if (IsResGroupActivated())
	{
		ResourceGroupCallbackContext *callbackCtx;

		AllocResGroupEntry(groupid, &caps);

		/* Argument of callback function should be allocated in heap region */
		oldContext = MemoryContextSwitchTo(TopMemoryContext);
		callbackCtx = (ResourceGroupCallbackContext *) palloc0(sizeof(*callbackCtx));
		callbackCtx->groupid = groupid;
		callbackCtx->caps = caps;

		MemoryContextSwitchTo(oldContext);
		RegisterXactCallbackOnce(createResgroupCallback, callbackCtx);

		/* Create os dependent part for this resource group */
		cgroupOpsRoutine->createcgroup(groupid);

		if (caps.io_limit != NIL)
			cgroupOpsRoutine->setio(groupid, caps.io_limit);

		if (CpusetIsEmpty(caps.cpuset))
		{
			cgroupOpsRoutine->setcpulimit(groupid, caps.cpuMaxPercent);
			cgroupOpsRoutine->setcpuweight(groupid, caps.cpuWeight);
		}
		else
		{
			EnsureCpusetIsAvailable(ERROR);

			char *cpuset = getCpuSetByRole(caps.cpuset);
			cgroupOpsRoutine->setcpuset(groupid, cpuset);
			/* reset default group, subtract new group cpu cores */
			char defaultGroupCpuset[MaxCpuSetLength];
			cgroupOpsRoutine->getcpuset(DEFAULT_CPUSET_GROUP_ID,
								  defaultGroupCpuset,
								  MaxCpuSetLength);
			CpusetDifference(defaultGroupCpuset, cpuset, MaxCpuSetLength);
			cgroupOpsRoutine->setcpuset(DEFAULT_CPUSET_GROUP_ID, defaultGroupCpuset);
		}
		SIMPLE_FAULT_INJECTOR("create_resource_group_fail");
	}
	else if (Gp_role == GP_ROLE_DISPATCH)
		ereport(WARNING,
				(errmsg("resource group is disabled"),
				 errhint("To enable set gp_resource_manager=group")));
}

/*
 * DROP RESOURCE GROUP
 */
void
DropResourceGroup(DropResourceGroupStmt *stmt)
{
	Relation	 pg_resgroup_rel;
	HeapTuple	 tuple;
	ScanKeyData	 scankey;
	SysScanDesc	 sscan;
	Oid			 groupid;
	ResourceGroupCallbackContext	*callbackCtx;

	/* Permission check - only superuser can drop resource groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop resource groups")));

	/*
	 * Check the pg_resgroup relation to be certain the resource group already
	 * exists.
	 */
	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   NULL, 1, &scankey);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource group \"%s\" does not exist",
						stmt->name)));

	/*
	 * Remember the Oid, for destroying the in-memory
	 * resource group later.
	 */
	groupid = ((Form_pg_resgroup) GETSTRUCT(tuple))->oid;

	/* cannot DROP default resource groups  */
	if (groupid == DEFAULTRESGROUP_OID
		|| groupid == ADMINRESGROUP_OID
		|| groupid == SYSTEMRESGROUP_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop default resource group \"%s\"",
						stmt->name)));

	/* check before dispatch to segment */
	if (IsResGroupActivated())
	{
		/* Argument of callback function should be allocated in heap region */
		callbackCtx = (ResourceGroupCallbackContext *)
			MemoryContextAlloc(TopMemoryContext, sizeof(*callbackCtx));
		callbackCtx->groupid = groupid;
		RegisterXactCallbackOnce(dropResgroupCallback, callbackCtx);

		ResGroupCheckForDrop(groupid, stmt->name);
	}

	/*
	 * Check to see if any roles are in this resource group.
	 */
	checkAuthIdForDrop(groupid);

	/*
	 * Delete the resource group from the catalog.
	 */
	simple_heap_delete(pg_resgroup_rel, &tuple->t_self);
	systable_endscan(sscan);
	heap_close(pg_resgroup_rel, NoLock);

	/* drop the extended attributes for this resource group */
	deleteResgroupCapabilities(groupid);

	/*
	 * Remove any comments on this resource group
	 */
	DeleteSharedComments(groupid, ResGroupRelationId);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* metadata tracking */
		MetaTrackDropObject(ResGroupRelationId, groupid);

		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									NIL,
									NULL);
	}
}

void
AlterResourceGroup(AlterResourceGroupStmt *stmt)
{
	/*
	 * Legacy ABI entry point has no ProcessUtilityContext.
	 * Treat it as top-level so IsInTransactionChain() can still detect
	 * explicit transaction blocks, subtransactions and non-standalone
	 * transaction states, but does not assume function/multi-command context.
	 */
	AlterResourceGroupExtended(stmt, true);
}

/*
 * ALTER RESOURCE GROUP
 */
void
AlterResourceGroupExtended(AlterResourceGroupStmt *stmt, bool isTopLevel)
{
	Relation	pg_resgroupcapability_rel;
	Oid			groupid;
	DefElem		*defel;
	ResGroupLimitType	limitType;
	ResGroupCaps		caps;
	ResGroupCaps		oldCaps;
	ResGroupCap			value = 0;
	const char *cpuset = NULL;
	char *io_limit = NULL;
	ResourceGroupCallbackContext	*callbackCtx;
	MemoryContext oldContext;

	/* Permission check - only superuser can alter resource groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to alter resource groups")));

	/* Currently we only support to ALTER one limit at one time */
	Assert(list_length(stmt->options) == 1);
	defel = (DefElem *) lfirst(list_head(stmt->options));

	limitType = getResgroupOptionType(defel->defname);
	if (limitType == RESGROUP_LIMIT_TYPE_UNKNOWN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not recognized", defel->defname)));
	}
	else if (limitType == RESGROUP_LIMIT_TYPE_CPUSET)
	{
		EnsureCpusetIsAvailable(ERROR);
		cpuset = defGetString(defel);
		checkCpuSetByRole(cpuset);
	}
	else if (limitType == RESGROUP_LIMIT_TYPE_IO_LIMIT)
		io_limit = defGetString(defel);
	else
	{
		value = getResgroupOptionValue(defel);
		checkResgroupCapLimit(limitType, value);
	}

	/*
	 * Check the pg_resgroup relation to be certain the resource group already
	 * exists.
	 */
	groupid = get_resgroup_oid(stmt->name, false);

	/*
	 * The session alters its own resource group inside a transaction block.
	 * The change will not be visible to this session before COMMIT.
	 */
	if (Gp_role == GP_ROLE_DISPATCH &&
		IsInTransactionBlock(isTopLevel) &&
		groupid == GetMyResGroupId())
	{
		ereport(WARNING,
				(errmsg("resource group \"%s\" settings will be applied after COMMIT",
						stmt->name)));
	}

	if (limitType == RESGROUP_LIMIT_TYPE_CONCURRENCY &&
		value == 0 &&
		groupid == ADMINRESGROUP_OID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_LIMIT_VALUE),
				 errmsg("admin_group must have at least one concurrency")));
	}

	/*
	 * We use ExclusiveLock here to prevent concurrent increase on different
	 * resource group.
	 *
	 * We can't use AccessExclusiveLock here, the reason is that, if there is
	 * a database recovery happened when run "alter resource group" and acquire
	 * this kind of lock, the initialization of resource group in function
	 * InitResGroups will be pending during database startup, since this function
	 * will open this table with AccessShareLock, AccessExclusiveLock is not
	 * compatible with any other lock. ExclusiveLock and AccessShareLock are
	 * compatible.
	 */
	pg_resgroupcapability_rel = heap_open(ResGroupCapabilityRelationId,
										  ExclusiveLock);

	/* Load current resource group capabilities */
	GetResGroupCapabilities(pg_resgroupcapability_rel, groupid, &oldCaps);
	/* If io_limit not been altered, reset io_limit field to NIL */
	if (limitType != RESGROUP_LIMIT_TYPE_IO_LIMIT && oldCaps.io_limit != NIL)
	{
		cgroupOpsRoutine->freeio(oldCaps.io_limit);
		oldCaps.io_limit = NIL;
	}
	caps = oldCaps;

	switch (limitType)
	{
		case RESGROUP_LIMIT_TYPE_CPU:
			caps.cpuMaxPercent = value;
			SetCpusetEmpty(caps.cpuset, sizeof(caps.cpuset));
			break;
		case RESGROUP_LIMIT_TYPE_CPU_SHARES:
			caps.cpuWeight = value;
			break;
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			caps.concurrency = value;
			break;
		case RESGROUP_LIMIT_TYPE_CPUSET:
			StrNCpy(caps.cpuset, cpuset, sizeof(caps.cpuset));
			caps.cpuMaxPercent = CPU_MAX_PERCENT_DISABLED;
			caps.cpuWeight = RESGROUP_DEFAULT_CPU_WEIGHT;
			break;
		case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
			caps.memory_limit = value;
			break;
		case RESGROUP_LIMIT_TYPE_MIN_COST:
			caps.min_cost = value;
			break;
		case RESGROUP_LIMIT_TYPE_IO_LIMIT:
			oldContext = CurrentMemoryContext;
			if (cgroupOpsRoutine == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("resource group must be enabled to use io limit feature")));

			MemoryContextSwitchTo(TopMemoryContext);
			caps.io_limit = cgroupOpsRoutine->parseio(io_limit);
			MemoryContextSwitchTo(oldContext);
			break;
		default:
			break;
	}

	validateCapabilities(pg_resgroupcapability_rel, groupid, &caps, false);
	AssertImply(limitType != RESGROUP_LIMIT_TYPE_IO_LIMIT, caps.io_limit == NIL);

	/* cpuset & cpu_max_percent can not coexist.
	 * if cpuset is active, then cpu_max_percent must set to CPU_RATE_LIMIT_DISABLED,
	 * if cpu_max_percent is active, then cpuset must set to "" */
	if (limitType == RESGROUP_LIMIT_TYPE_CPUSET)
	{
		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, RESGROUP_LIMIT_TYPE_CPU,
									  CPU_MAX_PERCENT_DISABLED, "");
		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, RESGROUP_LIMIT_TYPE_CPU_SHARES,
									  RESGROUP_DEFAULT_CPU_WEIGHT, "");

		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, RESGROUP_LIMIT_TYPE_CPUSET, 
									  0, caps.cpuset);
	}
	else if (limitType == RESGROUP_LIMIT_TYPE_CPU)
	{
		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, RESGROUP_LIMIT_TYPE_CPUSET,
									  0, DefaultCpuset);
		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, RESGROUP_LIMIT_TYPE_CPU,
									  value, "");
	}
	else if (limitType == RESGROUP_LIMIT_TYPE_IO_LIMIT)
	{
		if (caps.io_limit != NIL)
			updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
										  groupid, RESGROUP_LIMIT_TYPE_IO_LIMIT,
										  0, cgroupOpsRoutine->dumpio(caps.io_limit));
		else
		{
			/*
			 * When alter io_limit to -1 , the caps.io_limit will be nil.
			 * So we should update the io_limit in capability relation to -1.
			 */
			updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
										  groupid, RESGROUP_LIMIT_TYPE_IO_LIMIT,
										  0, DefaultIOLimit);
		}
	}
	else
	{
		updateResgroupCapabilityEntry(pg_resgroupcapability_rel,
									  groupid, limitType, value, "");
	}

	heap_close(pg_resgroupcapability_rel, NoLock);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		MetaTrackUpdObject(ResGroupCapabilityRelationId,
						   groupid,
						   GetUserId(),
						   "ALTER", defel->defname);

		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}

	if (IsResGroupActivated())
	{
		/* Argument of callback function should be allocated in heap region */
		oldContext = MemoryContextSwitchTo(TopMemoryContext);
		callbackCtx = (ResourceGroupCallbackContext *) palloc0(sizeof(*callbackCtx));
		callbackCtx->groupid = groupid;
		callbackCtx->limittype = limitType;
		callbackCtx->caps = caps;
		callbackCtx->oldCaps = oldCaps;
		callbackCtx->subXactId = GetCurrentSubTransactionId();

		/*
		 * Collect ALTERs and apply them together at COMMIT after PRE_COMMIT
		 * validates final catalog state. Outside an explicit transaction this
		 * list contains only one ALTER.
		 */
		resgroup_alter_callbacks = lappend(resgroup_alter_callbacks,
										   callbackCtx);
		MemoryContextSwitchTo(oldContext);

		static bool alter_tran_callback_registered = false;
		if (!alter_tran_callback_registered)
		{
			/*
			 * Use a regular callback because once callbacks are not fired
			 * at PRE_COMMIT, where we need to validate final catalog state.
			 */
			RegisterXactCallback(alterResgroupCallback, NULL);
			RegisterSubXactCallback(alterResgroupSubXactCallback, NULL);
			alter_tran_callback_registered = true;
		}
	}
}

/*
 * Get all the capabilities of one resource group in pg_resgroupcapability.
 *
 * Note: the io_limit in ResGroupCaps will be NIL if parse io_limit string failed.
 */
void
GetResGroupCapabilities(Relation rel, Oid groupId, ResGroupCaps *resgroupCaps)
{
	MemoryContext oldContext;
	SysScanDesc	sscan;
	ScanKeyData	key;
	HeapTuple	tuple;
	bool isNull;

	/*
	 * We maintain a bit mask to track which resgroup limit capability types
	 * have been retrieved, when mask is 0 then no limit capability is found
	 * for the given groupId.
	 */
	int			mask = 0;

	ClearResGroupCaps(resgroupCaps);

	/* Init cpuset with proper value */
	strcpy(resgroupCaps->cpuset, DefaultCpuset);

	ScanKeyInit(&key,
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupId));

	sscan = systable_beginscan(rel,
							   ResGroupCapabilityResgroupidIndexId,
							   true,
							   NULL, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		Datum				typeDatum;
		ResGroupLimitType	type;
		Datum				valueDatum;
		char				*value;

		typeDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_reslimittype,
								 rel->rd_att, &isNull);
		type = (ResGroupLimitType) DatumGetInt16(typeDatum);

		Assert(type > RESGROUP_LIMIT_TYPE_UNKNOWN);
		Assert(!(mask & (1 << type)));

		if (type >= RESGROUP_LIMIT_TYPE_COUNT)
			continue;

		mask |= 1 << type;

		valueDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_value,
									 rel->rd_att, &isNull);
		value = TextDatumGetCString(valueDatum);
		switch (type)
		{
			case RESGROUP_LIMIT_TYPE_CONCURRENCY:
				resgroupCaps->concurrency = str2Int(value,
													getResgroupOptionName(type));
				break;
			case RESGROUP_LIMIT_TYPE_CPU:
				resgroupCaps->cpuMaxPercent = str2Int(value,
													  getResgroupOptionName(type));
				break;
			case RESGROUP_LIMIT_TYPE_CPU_SHARES:
				resgroupCaps->cpuWeight = str2Int(value,
												  getResgroupOptionName(type));
				break;
			case RESGROUP_LIMIT_TYPE_CPUSET:
				StrNCpy(resgroupCaps->cpuset, value, sizeof(resgroupCaps->cpuset));
				break;
			case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
				resgroupCaps->memory_limit = str2Int(value,
													getResgroupOptionName(type));
				break;
			case RESGROUP_LIMIT_TYPE_MIN_COST:
				resgroupCaps->min_cost = str2Int(value,
													getResgroupOptionName(type));
				break;
			case RESGROUP_LIMIT_TYPE_IO_LIMIT:
				if (cgroupOpsRoutine != NULL)
				{
					/* if InterruptHoldoffCount doesn't restore in PG_CATCH,
					 * the catch will failed. */
					int32 savedholdoffCount = InterruptHoldoffCount;

					oldContext = CurrentMemoryContext;
					MemoryContextSwitchTo(TopMemoryContext);
					/*
					 * This function will be called in InitResGroups and AlterResourceGroup which
					 * shoud not be abort. In some circumstances, for example, the directory of a
					 * tablespace in io_limit be removed, then parseio will throw error. If
					 * InitResGroups be aborted, the cluster cannot launched. */
					PG_TRY();
					{
						resgroupCaps->io_limit = cgroupOpsRoutine->parseio(value);
					}
					PG_CATCH();
					{
						resgroupCaps->io_limit = NIL;

						InterruptHoldoffCount = savedholdoffCount;

						if (elog_demote(WARNING))
						{
							EmitErrorReport();
							FlushErrorState();
						}
					}
					PG_END_TRY();
					MemoryContextSwitchTo(oldContext);
				}
				else
					resgroupCaps->io_limit = NIL;
				break;
			default:
				break;
		}
	}

	systable_endscan(sscan);

	if (!mask)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot find limit capabilities for resource group: %d",
						groupId)));
	}
}

/*
 * Get resource group id for a role in pg_authid.
 *
 * An exception is thrown if the current role is invalid. This can happen if,
 * for example, a role was logged into psql and that role was dropped by another
 * psql session. But, normally something like "ERROR:  role 16385 was
 * concurrently dropped" would happen before the code reaches this function.
 */
Oid
GetResGroupIdForRole(Oid roleid)
{
	HeapTuple	tuple;
	Oid			groupId;
	bool		isNull;
	Relation	rel;
	ScanKeyData	key;
	SysScanDesc	 sscan;

	rel = table_open(AuthIdRelationId, AccessShareLock);

	ScanKeyInit(&key,
				Anum_pg_authid_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roleid));

	sscan = systable_beginscan(rel, AuthIdOidIndexId, true, NULL, 1, &key);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(sscan);
		table_close(rel, AccessShareLock);

		/*
		 * Role should have been dropped by other backends in this case, so this
		 * session cannot execute any command anymore
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("role with Oid %d was dropped", roleid),
				errdetail("Cannot execute commands anymore, please terminate this session.")));
	}

	/* must access tuple before systable_endscan */
	groupId = DatumGetObjectId(heap_getattr(tuple, Anum_pg_authid_rolresgroup,
							   rel->rd_att, &isNull));

	systable_endscan(sscan);

	/*
	 * release lock here to guarantee we have no lock held when acquiring
	 * resource group slot
	 */
	table_close(rel, AccessShareLock);

	if (!OidIsValid(groupId))
		groupId = InvalidOid;

	return groupId;
}

/*
 * get_resgroup_oid -- Return the Oid for a resource group name
 *
 * If missing_ok is false, throw an error if database name not found.  If
 * true, just return InvalidOid.
 *
 * Notes:
 *	Used by the various admin commands to convert a user supplied group name
 *	to Oid.
 */
Oid
get_resgroup_oid(const char *name, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(RESGROUPNAME, Anum_pg_resgroup_oid,
						  CStringGetDatum(name));

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource group \"%s\" does not exist",
						name)));

	return oid;
}

/*
 * GetResGroupNameForId -- Return the resource group name for an Oid
 */
char *
GetResGroupNameForId(Oid oid)
{
	HeapTuple	tuple;
	char		*name;

	tuple = SearchSysCache1(RESGROUPOID,
							ObjectIdGetDatum(oid));
	if (HeapTupleIsValid(tuple))
	{
		bool		isnull;
		Datum		nameDatum;
		Name		resGroupName;

		nameDatum = SysCacheGetAttr(RESGROUPOID, tuple,
									Anum_pg_resgroup_rsgname,
									&isnull);
		Assert(!isnull);
		resGroupName = DatumGetName(nameDatum);
		name = pstrdup(NameStr(*resGroupName));
	}
	else
		return "unknown";

	ReleaseSysCache(tuple);

	return name;
}

/*
 * Check to see if the group can be assigned with role
 */
void
ResGroupCheckForRole(Oid groupId)
{
	Relation pg_resgroupcapability_rel;
	ResGroupCaps caps;

	pg_resgroupcapability_rel = heap_open(ResGroupCapabilityRelationId,
										  AccessShareLock);

	/* Load current resource group capabilities */
	GetResGroupCapabilities(pg_resgroupcapability_rel, groupId, &caps);

	if (cgroupOpsRoutine != NULL && caps.io_limit != NIL)
		cgroupOpsRoutine->freeio(caps.io_limit);

	heap_close(pg_resgroupcapability_rel, AccessShareLock);
}

/*
 * Get the option type from a name string.
 *
 * @param defname  the name string
 *
 * @return the option type or UNKNOWN if the name is unknown
 */
static ResGroupLimitType
getResgroupOptionType(const char* defname)
{
	if (strcmp(defname, "cpu_max_percent") == 0)
		return RESGROUP_LIMIT_TYPE_CPU;
	else if (strcmp(defname, "concurrency") == 0)
		return RESGROUP_LIMIT_TYPE_CONCURRENCY;
	else if (strcmp(defname, "cpuset") == 0)
		return RESGROUP_LIMIT_TYPE_CPUSET;
	else if (strcmp(defname, "cpu_weight") == 0)
		return RESGROUP_LIMIT_TYPE_CPU_SHARES;
	else if (strcmp(defname, "memory_limit") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY_LIMIT;
	else if (strcmp(defname, "min_cost") == 0)
		return RESGROUP_LIMIT_TYPE_MIN_COST;
	else if (strcmp(defname, "io_limit") == 0)
		return RESGROUP_LIMIT_TYPE_IO_LIMIT;
	else
		return RESGROUP_LIMIT_TYPE_UNKNOWN;
}

/*
 * Get capability value from DefElem, convert from int64 to int
 */
static ResGroupCap
getResgroupOptionValue(DefElem *defel)
{
	int64 value;

	value = defGetInt64(defel);

	if (value < INT_MIN || value > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("capability %s is out of range", defel->defname)));
	return (ResGroupCap)value;
}

/*
 * Get the option name from type.
 *
 * @param type  the resgroup limit type
 *
 * @return the name of type
 */
static const char *
getResgroupOptionName(ResGroupLimitType type)
{
	switch (type)
	{
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			return "concurrency";
		case RESGROUP_LIMIT_TYPE_CPU:
			return "cpu_max_percent";
		case RESGROUP_LIMIT_TYPE_CPUSET:
			return "cpuset";
		case RESGROUP_LIMIT_TYPE_CPU_SHARES:
			return "cpu_weight";
		case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
			return "memory_limit";
		case RESGROUP_LIMIT_TYPE_MIN_COST:
			return "min_cost";
		default:
			return "unknown";
	}
}

/*
 * Check if capability value exceeds max and min value
 */
static void
checkResgroupCapLimit(ResGroupLimitType type, int value)
{
		switch (type)
		{
			case RESGROUP_LIMIT_TYPE_CONCURRENCY:
				if (value < RESGROUP_MIN_CONCURRENCY ||
					value > RESGROUP_MAX_CONCURRENCY)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("concurrency range is [%d, 'max_connections']",
								   RESGROUP_MIN_CONCURRENCY)));
				break;

			case RESGROUP_LIMIT_TYPE_CPU:
				if (value > RESGROUP_MAX_CPU_MAX_PERCENT ||
					(value < RESGROUP_MIN_CPU_MAX_PERCENT && value != CPU_MAX_PERCENT_DISABLED))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cpu_max_percent range is [%d, %d] or equals to %d",
								   RESGROUP_MIN_CPU_MAX_PERCENT, RESGROUP_MAX_CPU_MAX_PERCENT,
								   CPU_MAX_PERCENT_DISABLED)));
				break;

			case RESGROUP_LIMIT_TYPE_CPU_SHARES:
				if (value < RESGROUP_MIN_CPU_WEIGHT ||
					value > RESGROUP_MAX_CPU_WEIGHT)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("cpu_weight range is [%d, %d]",
										   RESGROUP_MIN_CPU_WEIGHT, RESGROUP_MAX_CPU_WEIGHT)));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
				if (value < RESGROUP_MIN_MEMORY_LIMIT && value != RESGROUP_DEFAULT_MEMORY_LIMIT)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("memory_limit range is [%d, INT_MAX] or equals to %d",
								    RESGROUP_MIN_MEMORY_LIMIT, RESGROUP_DEFAULT_MEMORY_LIMIT)));
				break;

			case RESGROUP_LIMIT_TYPE_MIN_COST:
				if (value < RESGROUP_MIN_MIN_COST)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("The min_cost value can't be less than %d.",
										RESGROUP_MIN_MIN_COST)));
				break;

			default:
				Assert(!"unexpected options");
				break;
		}
}

/*
 * Parse a statement and store the settings in options.
 *
 * @param stmt     the statement
 * @param caps     used to store the settings
 */
static void
parseStmtOptions(CreateResourceGroupStmt *stmt, ResGroupCaps *caps)
{
	MemoryContext oldContext;
	ListCell *cell;
	ResGroupCap value;
	int mask = 0;

	foreach(cell, stmt->options)
	{
		DefElem *defel = (DefElem *) lfirst(cell);
		int type = getResgroupOptionType(defel->defname);

		if (type == RESGROUP_LIMIT_TYPE_UNKNOWN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" not recognized", defel->defname)));

		if (mask & (1 << type))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("found duplicate resource group resource type: %s",
							defel->defname)));
		else
			mask |= 1 << type;

		if (type == RESGROUP_LIMIT_TYPE_CPUSET) 
		{
			const char *cpuset = defGetString(defel);
			StrNCpy(caps->cpuset, cpuset, sizeof(caps->cpuset));
      checkCpuSetByRole(cpuset);
			caps->cpuMaxPercent = CPU_MAX_PERCENT_DISABLED;
			caps->cpuWeight = RESGROUP_DEFAULT_CPU_WEIGHT;
		}
		else if (type == RESGROUP_LIMIT_TYPE_IO_LIMIT)
		{
			char *io_limit_str = defGetString(defel);

			oldContext = CurrentMemoryContext;
			if (cgroupOpsRoutine == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("resource group must be enabled to use io limit feature")));

			MemoryContextSwitchTo(TopMemoryContext);
			caps->io_limit = cgroupOpsRoutine->parseio(io_limit_str);
			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			value = getResgroupOptionValue(defel);
			checkResgroupCapLimit(type, value);

			switch (type)
			{
				case RESGROUP_LIMIT_TYPE_CONCURRENCY:
					caps->concurrency = value;
					break;
				case RESGROUP_LIMIT_TYPE_CPU:
					caps->cpuMaxPercent = value;
					SetCpusetEmpty(caps->cpuset, sizeof(caps->cpuset));
					break;
				case RESGROUP_LIMIT_TYPE_CPU_SHARES:
					caps->cpuWeight = value;
					break;
				case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
					caps->memory_limit = value;
					break;
				case RESGROUP_LIMIT_TYPE_MIN_COST:
					caps->min_cost = value;
					break;
				default:
					break;
			}
		}
	}

	if ((mask & (1 << RESGROUP_LIMIT_TYPE_CPUSET)))
		EnsureCpusetIsAvailable(ERROR);

	if ((mask & (1 << RESGROUP_LIMIT_TYPE_CPU)) &&
		(mask & (1 << RESGROUP_LIMIT_TYPE_CPUSET)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("can't specify both cpu_max_percent and cpuset")));

	if (!(mask & (1 << RESGROUP_LIMIT_TYPE_CPU)) &&
		!(mask & (1 << RESGROUP_LIMIT_TYPE_CPUSET)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("must specify cpu_max_percent or cpuset")));

	if (!(mask & (1 << RESGROUP_LIMIT_TYPE_CONCURRENCY)))
		caps->concurrency = RESGROUP_DEFAULT_CONCURRENCY;

	if (!(mask & (1 << RESGROUP_LIMIT_TYPE_MEMORY_LIMIT)))
		caps->memory_limit = -1;

	if (!(mask & (1 << RESGROUP_LIMIT_TYPE_MIN_COST)))
		caps->min_cost = 0;

	if ((mask & (1 << RESGROUP_LIMIT_TYPE_CPU)) &&
		!(mask & (1 << RESGROUP_LIMIT_TYPE_CPU_SHARES)))
		caps->cpuWeight = RESGROUP_DEFAULT_CPU_WEIGHT;
}

/*
 * Resource group call back function
 *
 * Remove resource group entry in shared memory when abort transaction which
 * creates resource groups
 */
static void
createResgroupCallback(XactEvent event, void *arg)
{
	ResourceGroupCallbackContext *callbackCtx = arg;

	if (event != XACT_EVENT_COMMIT)
	{
		ResGroupCreateOnAbort(callbackCtx);
	}

	if (callbackCtx->caps.io_limit != NIL)
		cgroupOpsRoutine->freeio(callbackCtx->caps.io_limit);
	pfree(callbackCtx);
}

/*
 * Resource group call back function
 *
 * When DROP RESOURCE GROUP transaction ends, need wake up
 * the queued transactions and cleanup shared menory entry.
 */
static void
dropResgroupCallback(XactEvent event, void *arg)
{
	ResourceGroupCallbackContext *callbackCtx = arg;

	ResGroupDropFinish(callbackCtx, event == XACT_EVENT_COMMIT);
	pfree(callbackCtx);
}

static void
freeResgroupAlterCallback(ResourceGroupCallbackContext *ctx)
{
	if (ctx->caps.io_limit != NIL)
		cgroupOpsRoutine->freeio(ctx->caps.io_limit);

	if (ctx->oldCaps.io_limit != NIL)
		cgroupOpsRoutine->freeio(ctx->oldCaps.io_limit);

	pfree(ctx);
}

/*
 * Compare two io_limit lists through their canonical catalog representation.
 *
 * dumpio walks each list in stored order, so the same entries listed in a
 * different order would dump to different strings. Sort shallow copies by
 * tablespace first so only the values decide the match.
 */
static bool
resGroupIOLimitMatches(List *left, List *right)
{
	List       *sortedLeft;
	List       *sortedRight;
	char       *leftStr;
	char       *rightStr;
	bool        result;

	if (left == NIL || right == NIL)
		return left == NIL && right == NIL;

	sortedLeft = list_qsort(left, compare_tablespace_oid);
	sortedRight = list_qsort(right, compare_tablespace_oid);

	leftStr = cgroupOpsRoutine->dumpio(sortedLeft);
	rightStr = cgroupOpsRoutine->dumpio(sortedRight);
	result = strcmp(leftStr, rightStr) == 0;

	pfree(leftStr);
	pfree(rightStr);
	list_free(sortedLeft);
	list_free(sortedRight);

	return result;
}

/*
 * Check whether the field changed by this callback has the same value in
 * the given capability snapshot.
 */
static bool
resGroupCapFieldMatches(const ResourceGroupCallbackContext *ctx,
						const ResGroupCaps *caps)
{
	switch (ctx->limittype)
	{
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			return ctx->caps.concurrency == caps->concurrency;

		case RESGROUP_LIMIT_TYPE_CPU:
			return ctx->caps.cpuMaxPercent == caps->cpuMaxPercent &&
				   strcmp(ctx->caps.cpuset, caps->cpuset) == 0;

		case RESGROUP_LIMIT_TYPE_CPU_SHARES:
			return ctx->caps.cpuWeight == caps->cpuWeight;

		case RESGROUP_LIMIT_TYPE_CPUSET:
			return ctx->caps.cpuMaxPercent == caps->cpuMaxPercent &&
				   ctx->caps.cpuWeight == caps->cpuWeight &&
				   strcmp(ctx->caps.cpuset, caps->cpuset) == 0;

		case RESGROUP_LIMIT_TYPE_MEMORY_LIMIT:
			return ctx->caps.memory_limit == caps->memory_limit;

		case RESGROUP_LIMIT_TYPE_MIN_COST:
			return ctx->caps.min_cost == caps->min_cost;

		case RESGROUP_LIMIT_TYPE_IO_LIMIT:
			return resGroupIOLimitMatches(ctx->caps.io_limit, caps->io_limit);

		case RESGROUP_LIMIT_TYPE_UNKNOWN:
		case RESGROUP_LIMIT_TYPE_COUNT:
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("invalid resource group limit type: %d", ctx->limittype)));

	return false;
}

/*
 * CPUSET has side effects: it disables CPU_MAX_PERCENT and resets CPU_WEIGHT.
 * Keep it when a later SET CPU_MAX_PERCENT or SET CPU_WEIGHT overwrites only
 * part of these side effects and the remaining part is still final.
 */
static bool
resgroupHasLaterCpuCallbackMatchingFinal(List *callbacks,
										 ResourceGroupCallbackContext *ctx,
										 const ResGroupCaps *finalCaps)
{
	ListCell   *lc;
	bool		after = false;

	foreach (lc, callbacks)
	{
		ResourceGroupCallbackContext *other = lfirst(lc);

		if (other == ctx)
		{
			after = true;
			continue;
		}

		if (!after || other->groupid != ctx->groupid)
			continue;

		/*
		 * Example:
		 *   SET CPUSET '1'; SET CPU_MAX_PERCENT 30;
		 * Keep CPUSET to preserve its CPU_WEIGHT reset.
		 */
		if (other->limittype == RESGROUP_LIMIT_TYPE_CPU &&
			other->caps.cpuMaxPercent == finalCaps->cpuMaxPercent &&
			strcmp(other->caps.cpuset, finalCaps->cpuset) == 0 &&
			ctx->caps.cpuWeight == finalCaps->cpuWeight)
			return true;

		/*
		 * Example:
		 *   SET CPUSET '1'; SET CPU_WEIGHT 120;
		 * Keep CPUSET to preserve CPUSET mode.
		 */
		if (other->limittype == RESGROUP_LIMIT_TYPE_CPU_SHARES &&
			other->caps.cpuWeight == finalCaps->cpuWeight &&
			ctx->caps.cpuMaxPercent == finalCaps->cpuMaxPercent &&
			strcmp(ctx->caps.cpuset, finalCaps->cpuset) == 0)
			return true;
	}

	return false;
}

/*
 * Find an already prepared ALTER callback with the same target and value.
 */
static ResourceGroupCallbackContext *
resgroupAlterCallbackFindPreparedDuplicate(List *prepared,
										   ResourceGroupCallbackContext *ctx)
{
	ListCell   *lc;

	foreach (lc, prepared)
	{
		ResourceGroupCallbackContext *prev = lfirst(lc);

		if (prev->groupid != ctx->groupid)
			continue;

		if (prev->limittype != ctx->limittype)
			continue;

		if (resGroupCapFieldMatches(ctx, &prev->caps))
			return prev;
	}

	return NULL;
}

/*
 * An aborted subtransaction rolls back its ALTER catalog changes, but the
 * PRE_COMMIT value match alone could still keep its queued callback when the
 * value coincides with the final catalog state. Drop such callbacks at
 * subtransaction abort. Callbacks of a committed subtransaction move up to
 * the parent.
 */
static void
alterResgroupSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
							 SubTransactionId parentSubid, void *arg)
{
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	if (resgroup_alter_callbacks == NIL)
		return;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		foreach (cell, resgroup_alter_callbacks)
		{
			ResourceGroupCallbackContext *ctx = lfirst(cell);

			if (ctx->subXactId == mySubid)
				ctx->subXactId = parentSubid;
		}
	}
	else if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		prev = NULL;
		for (cell = list_head(resgroup_alter_callbacks); cell != NULL; cell = next)
		{
			ResourceGroupCallbackContext *ctx = lfirst(cell);

			next = lnext(cell);
			if (ctx->subXactId == mySubid)
			{
				resgroup_alter_callbacks =
					list_delete_cell(resgroup_alter_callbacks, cell, prev);
				freeResgroupAlterCallback(ctx);
			}
			else
				prev = cell;
		}
	}
}

/*
 * Resource group call back function
 *
 * PRE_COMMIT: keep callbacks matching final pg_resgroupcapability state.
 * COMMIT: apply kept callbacks, then cleanup.
 * ABORT: cleanup.
 *
 * When ALTER RESOURCE GROUP SET CONCURRENCY commits, some queuing
 * transaction of this resource group may need to be woke up.
 *
 * The callback remains registered for the backend lifetime. The first check
 * is the fast path for transactions without ALTER RESOURCE GROUP.
 */
static void
alterResgroupCallback(XactEvent event, void *arg)
{
	ListCell   *lc;

	if (resgroup_alter_callbacks == NIL)
		return;

	if (event == XACT_EVENT_ABORT)
	{
		foreach (lc, resgroup_alter_callbacks)
		{
			ResourceGroupCallbackContext *ctx = lfirst(lc);
			freeResgroupAlterCallback(ctx);
		}

		list_free(resgroup_alter_callbacks);
		resgroup_alter_callbacks = NIL;
	}
	else if (event == XACT_EVENT_COMMIT)
	{
		foreach (lc, resgroup_alter_callbacks)
		{
			ResourceGroupCallbackContext *ctx = lfirst(lc);
			ResGroupAlterOnCommit(ctx);
			freeResgroupAlterCallback(ctx);
		}
		list_free(resgroup_alter_callbacks);
		resgroup_alter_callbacks = NIL;
	}
	else if (event == XACT_EVENT_PRE_COMMIT)
	{
		List	   *prepared = NIL;
		List	   *rejected = NIL;
		Relation	rel;
		ResGroupCaps finalCaps;

		/*
		 * Keep final, non-duplicate callbacks, plus CPUSET callbacks whose
		 * side effects remain in the final catalog state.
		 */
		rel = heap_open(ResGroupCapabilityRelationId, AccessShareLock);

		/*
		 * GetResGroupCapabilities can raise on a malformed stored value, which
		 * would turn this commit into an abort. Release the relation and the
		 * TopMemoryContext lists on that path so they do not leak, then let the
		 * abort proceed. The contexts themselves stay owned by
		 * resgroup_alter_callbacks, which the abort callback frees.
		 */
		finalCaps.io_limit = NIL;

		PG_TRY();
		{
			foreach (lc, resgroup_alter_callbacks)
			{
				ResourceGroupCallbackContext *ctx = lfirst(lc);

				SIMPLE_FAULT_INJECTOR("resgroup_alter_pre_commit");

				GetResGroupCapabilities(rel, ctx->groupid, &finalCaps);

				if (resGroupCapFieldMatches(ctx, &finalCaps) ||
					(ctx->limittype == RESGROUP_LIMIT_TYPE_CPUSET &&
					 resgroupHasLaterCpuCallbackMatchingFinal(
													resgroup_alter_callbacks,
													ctx,
													&finalCaps)))
				{
					MemoryContext oldcxt;

					/*
					 * COMMIT applies prepared callbacks in list order, so of
					 * two callbacks with the same target and value the later
					 * one must survive. Keeping the earlier one would let a
					 * kept CPUSET callback sitting between them apply last
					 * and override the final state.
					 */
					ResourceGroupCallbackContext *duplicate =
						resgroupAlterCallbackFindPreparedDuplicate(prepared,
																   ctx);

					oldcxt = MemoryContextSwitchTo(TopMemoryContext);

					if (duplicate != NULL)
					{
						prepared = list_delete_ptr(prepared, duplicate);
						rejected = lappend(rejected, duplicate);
					}

					prepared = lappend(prepared, ctx);
					MemoryContextSwitchTo(oldcxt);
				}
				else
				{
					MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
					rejected = lappend(rejected, ctx);
					MemoryContextSwitchTo(oldcxt);
				}

				if (finalCaps.io_limit != NIL)
				{
					cgroupOpsRoutine->freeio(finalCaps.io_limit);
					finalCaps.io_limit = NIL;
				}
			}
		}
		PG_CATCH();
		{
			if (finalCaps.io_limit != NIL)
				cgroupOpsRoutine->freeio(finalCaps.io_limit);

			heap_close(rel, AccessShareLock);
			list_free(prepared);
			list_free(rejected);
			PG_RE_THROW();
		}
		PG_END_TRY();

		heap_close(rel, AccessShareLock);

		list_free(resgroup_alter_callbacks);
		resgroup_alter_callbacks = prepared;

		foreach (lc, rejected)
		{
			ResourceGroupCallbackContext *ctx = lfirst(lc);
			freeResgroupAlterCallback(ctx);
		}
		list_free(rejected);
	}
}

/*
 * Catalog access functions
 */

/*
 * Insert all the capabilities to the capability table.
 *
 * We store the capabilities in multiple lines for one group,
 * so we have to insert them one by one. This function will
 * handle the type conversion etc..
 *
 * @param groupid  oid of the resource group
 * @param caps     the capabilities
 */
static void
insertResgroupCapabilities(Relation rel, Oid groupId, ResGroupCaps *caps)
{
	char value[64];

	snprintf(value, sizeof(value), "%d", caps->concurrency);
	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_CONCURRENCY, value);

	snprintf(value, sizeof(value), "%d", caps->cpuMaxPercent);
	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_CPU, value);

	snprintf(value, sizeof(value), "%d", caps->cpuWeight);
	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_CPU_SHARES, value);

	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_CPUSET, caps->cpuset);

	snprintf(value, sizeof(value), "%d", caps->memory_limit);
	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_MEMORY_LIMIT, value);

	snprintf(value, sizeof(value), "%d", caps->min_cost);
	insertResgroupCapabilityEntry(rel, groupId,
								  RESGROUP_LIMIT_TYPE_MIN_COST, value);

	if (caps->io_limit != NIL)
		insertResgroupCapabilityEntry(rel, groupId,
									  RESGROUP_LIMIT_TYPE_IO_LIMIT,
									  cgroupOpsRoutine->dumpio((caps->io_limit)));
	else
		insertResgroupCapabilityEntry(rel, groupId,
									  RESGROUP_LIMIT_TYPE_IO_LIMIT, DefaultIOLimit);
}

/*
 * Update all the capabilities of one resgroup in pg_resgroupcapability
 *
 * groupId and limitType are the scan keys.
 */
static void
updateResgroupCapabilityEntry(Relation rel,
							  Oid groupId,
							  ResGroupLimitType limitType,
							  ResGroupCap value,
							  const char *strValue)
{
	HeapTuple	oldTuple;
	HeapTuple	newTuple;
	SysScanDesc	sscan;
	ScanKeyData	scankey[2];
	Datum		values[Natts_pg_resgroupcapability];
	bool		isnull[Natts_pg_resgroupcapability];
	bool		repl[Natts_pg_resgroupcapability];
	char		stringBuffer[MaxCpuSetLength];

	ScanKeyInit(&scankey[0],
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupId));
	ScanKeyInit(&scankey[1],
				Anum_pg_resgroupcapability_reslimittype,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(limitType));

	sscan = systable_beginscan(rel,
							   ResGroupCapabilityResgroupidResLimittypeIndexId, true,
							   NULL, 2, scankey);

	MemSet(values, 0, sizeof(values));
	MemSet(isnull, 0, sizeof(isnull));
	MemSet(repl, 0, sizeof(repl));

	oldTuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(oldTuple))
	{
		/*
		 * It's possible for a cap to be missing, e.g. a resgroup is created
		 * with v5.0 which does not support cap=7 (cpuset), then we binary
		 * switch to v5.10 and alter it, then we'll find cap=7 missing here.
		 * Instead of raising an error we should fallback to insert a new cap.
		 */

		systable_endscan(sscan);

		insertResgroupCapabilityEntry(rel, groupId, limitType, (char *) strValue);

		return;
	}

	if (limitType == RESGROUP_LIMIT_TYPE_CPUSET)
	{
		StrNCpy(stringBuffer, strValue, sizeof(stringBuffer));
	}
	else
	{
		snprintf(stringBuffer, sizeof(stringBuffer), "%d", value);
	}

	if (limitType == RESGROUP_LIMIT_TYPE_IO_LIMIT)
	{
		/* Because stringBuffer is a limited length array, so it not suitable for io limit. */
		values[Anum_pg_resgroupcapability_value - 1] = CStringGetTextDatum(strValue);
	}
	else
		values[Anum_pg_resgroupcapability_value - 1] = CStringGetTextDatum(stringBuffer);

	isnull[Anum_pg_resgroupcapability_value - 1] = false;
	repl[Anum_pg_resgroupcapability_value - 1]  = true;

	newTuple = heap_modify_tuple(oldTuple, RelationGetDescr(rel),
								 values, isnull, repl);

	CatalogTupleUpdate(rel, &oldTuple->t_self, newTuple);

	systable_endscan(sscan);
}

/*
 * Validate the capabilities.
 *
 * The policy is resouces can't be over used, take memory for example,
 * all the allocated memory can not exceed 100.
 *
 * Also detect for duplicate settings for the group.
 *
 * @param rel      the relation
 * @param groupid  oid of the resource group
 * @param caps     the capabilities for the resource group
 */
static void
validateCapabilities(Relation rel,
					 Oid groupid,
					 ResGroupCaps *caps,
					 bool newGroup)
{
	HeapTuple tuple;
	SysScanDesc sscan;
	char cpusetAll[MaxCpuSetLength] = {0};
	char cpusetMissing[MaxCpuSetLength] = {0};
	Bitmapset *bmsCurrent = NULL;
	Bitmapset *bmsCommon = NULL;

	if (!CpusetIsEmpty(caps->cpuset))
	{
		EnsureCpusetIsAvailable(ERROR);
	}

	/*
	 * initialize the variables only when resource group is activated
	 */
	if (IsResGroupActivated() &&
		gp_resource_group_enable_cgroup_cpuset)
	{
		Bitmapset *bmsAll = NULL;
		Bitmapset *bmsMissing = NULL;

		/* Get all available cores */
		cgroupOpsRoutine->getcpuset(CGROUP_ROOT_ID,
							  cpusetAll,
							  MaxCpuSetLength);
		bmsAll = CpusetToBitset(cpusetAll, MaxCpuSetLength);

		/* Check whether the cores in this group are available */
		if (!CpusetIsEmpty(caps->cpuset))
		{
			char *cpuset = getCpuSetByRole(caps->cpuset);
			bmsCurrent = CpusetToBitset(cpuset, MaxCpuSetLength);

			bmsCommon = bms_intersect(bmsCurrent, bmsAll);
			bmsMissing = bms_difference(bmsCurrent, bmsCommon);

			if (!bms_is_empty(bmsMissing))
			{
				BitsetToCpuset(bmsMissing, cpusetMissing, MaxCpuSetLength);

				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cpu cores %s are unavailable on the system",
						cpusetMissing)));
			}
		}
	}

	sscan = systable_beginscan(rel, ResGroupCapabilityResgroupidIndexId,
							   true, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		Datum				groupIdDatum;
		Datum				typeDatum;
		Datum				valueDatum;
		ResGroupLimitType	reslimittype;
		Oid					resgroupid;
		char				*valueStr;
		bool				isNull;

		groupIdDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_resgroupid,
									rel->rd_att, &isNull);
		resgroupid = DatumGetObjectId(groupIdDatum);

		if (resgroupid == groupid)
		{
			if (!newGroup)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("found duplicate resource group id: %d",
							groupid)));
		}

		typeDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_reslimittype,
								 rel->rd_att, &isNull);
		reslimittype = (ResGroupLimitType) DatumGetInt16(typeDatum);

		valueDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_value,
									 rel->rd_att, &isNull);

		/* we need to check the configuration of cpuset for intersection. */
		if (reslimittype == RESGROUP_LIMIT_TYPE_CPUSET)
		{
			/*
			 * do the check when resource group is activated
			 */
			if (IsResGroupActivated() && !CpusetIsEmpty(caps->cpuset))
			{
				valueStr = TextDatumGetCString(valueDatum);
				if (!CpusetIsEmpty(valueStr))
				{
					Bitmapset *bmsOther = NULL;

					EnsureCpusetIsAvailable(ERROR);

					Assert(!bms_is_empty(bmsCurrent));

					char *cpuset = getCpuSetByRole(valueStr);
					bmsOther = CpusetToBitset(cpuset, MaxCpuSetLength);
					bmsCommon = bms_intersect(bmsCurrent, bmsOther);

					if (!bms_is_empty(bmsCommon))
					{
						BitsetToCpuset(bmsCommon, cpusetMissing, MaxCpuSetLength);

						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("cpu cores %s are used by resource group %s",
										cpusetMissing,
										GetResGroupNameForId(resgroupid))));
					}
				}
			}
		}
	}

	systable_endscan(sscan);
}

/*
 * Insert one capability to the capability table.
 *
 * @param rel      the relation
 * @param groupid  oid of the resource group
 * @param type     the resource limit type
 * @param value    the limit value
 */
static void
insertResgroupCapabilityEntry(Relation rel,
							 Oid groupid,
							 uint16 type,
							 const char *value)
{
	Datum new_record[Natts_pg_resgroupcapability];
	bool new_record_nulls[Natts_pg_resgroupcapability];
	HeapTuple tuple;
	TupleDesc tupleDesc = RelationGetDescr(rel);

	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroupcapability_resgroupid - 1] = ObjectIdGetDatum(groupid);
	new_record[Anum_pg_resgroupcapability_reslimittype - 1] = UInt16GetDatum(type);
	new_record[Anum_pg_resgroupcapability_value - 1] = CStringGetTextDatum(value);

	tuple = heap_form_tuple(tupleDesc, new_record, new_record_nulls);
	CatalogTupleInsert(rel, tuple);
}

/*
 * Delete capability entries of one resource group.
 */
static void
deleteResgroupCapabilities(Oid groupid)
{
	Relation	 resgroup_capability_rel;
	HeapTuple	 tuple;
	ScanKeyData	 scankey;
	SysScanDesc	 sscan;

	resgroup_capability_rel = heap_open(ResGroupCapabilityRelationId,
										RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupid));

	sscan = systable_beginscan(resgroup_capability_rel,
							   ResGroupCapabilityResgroupidIndexId,
							   true, NULL, 1, &scankey);

	while ((tuple = systable_getnext(sscan)) != NULL)
		simple_heap_delete(resgroup_capability_rel, &tuple->t_self);

	systable_endscan(sscan);

	heap_close(resgroup_capability_rel, NoLock);
}

/*
 * Check to see if any roles are in this resource group.
 */
static void
checkAuthIdForDrop(Oid groupId)
{
	Relation	 authIdRel;
	ScanKeyData	 authidScankey;
	SysScanDesc	 authidScan;

	authIdRel = heap_open(AuthIdRelationId, RowExclusiveLock);
	ScanKeyInit(&authidScankey,
				Anum_pg_authid_rolresgroup,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupId));

	authidScan = systable_beginscan(authIdRel, AuthIdRolResGroupIndexId, true,
									NULL, 1, &authidScankey);

	if (HeapTupleIsValid(systable_getnext(authidScan)))
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("resource group is used by at least one role")));

	systable_endscan(authidScan);
	heap_close(authIdRel, RowExclusiveLock);
}
/*
 * Convert a C str to a integer value.
 *
 * @param str   the C str
 * @param prop  the property name
 */
static int
str2Int(const char *str, const char *prop)
{
	char *end = NULL;
	double val = strtod(str, &end);

	/* both the property name and value are already checked
	 * by the syntax parser, but we'll check it again anyway for safe. */
	if (end == NULL || end == str || *end != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s requires a numeric value", prop)));

	return floor(val);
}

/*
 * check whether the cpuset value is syntactically right
 */
static void
checkCpusetSyntax(const char *cpuset)
{
	if (cpuset == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cpuset invalid")));
	}

	if (strlen(cpuset) >= MaxCpuSetLength)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the length of cpuset reached the upper limit %d",
						MaxCpuSetLength)));
	}
	if (!CpusetToBitset(cpuset,
						 strlen(cpuset)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cpuset invalid")));
	}

	return;
}

/*
 * Check Cpuset by coordinator and segment
 */
extern void
checkCpuSetByRole(const char *cpuset)
{
	char *first = NULL;
	char *last = NULL;

	if (cpuset == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cpuset invalid")));
	}

	first = strchr(cpuset, ';');
	last = strrchr(cpuset, ';');
	/*
	 * If point first not equal last, that means
	 * ';' character exceed limit numbers.
	 */
	if (last != first)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cpuset invalid")));
	}

	if (first == NULL)
		checkCpusetSyntax(cpuset);
	else
	{
		char mcpu[MaxCpuSetLength] = {0};
		strncpy(mcpu, cpuset, first - cpuset);

		checkCpusetSyntax(mcpu);
		checkCpusetSyntax(first + 1);
	}

	return;
}

/*
 * Seperate cpuset by coordinator and segment
 * Return as splitcpuset
 *
 * ex:
 * cpuset = "1;4"
 * then we should assign '1' to corrdinator and '4' to segment
 * 
 * cpuset = "1"
 * assign '1' to both coordinator and segment
 */
extern char *
getCpuSetByRole(const char *cpuset)
{
	char *splitcpuset = NULL;

	if (cpuset == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("Unexpected cpuset invalid in getCpuSetByRole")));
	}

	char *first = strchr(cpuset, ';');
	if (first == NULL)
		splitcpuset = (char *)cpuset;
	else
	{
		char *scpu = first + 1;

		/* Get result cpuset by IS_QUERY_DISPATCHER(), on coordinator or segment */
		if (IS_QUERY_DISPATCHER())
			splitcpuset = scpu;
		else
		{
			char *mcpu = (char *)palloc0(sizeof(char) * MaxCpuSetLength);
			strncpy(mcpu, cpuset, first - cpuset);
			splitcpuset = mcpu;
		}
	}

	return splitcpuset;
}

