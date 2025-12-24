#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_tablespace.h"
#include "miscadmin.h"
#include "track_dbsize.h"

#include "utils/dbsize.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/rel.h"


/*
 * Calculates relation size among all the forks. No lock is acquired on table.
 * RelationData is partially initialized. Only necessary fields are taken from
 * pg_class tuple to determine segment file location. Since the locks are not acquired
 * stat call for a file may fail. In that case we simply ignore the error and interpret
 * size as zero.
 */
int64
dbsize_calc_size(Form_pg_class relform)
{
	RelationData	rel = {0};
	int64		size = 0;
	RelFileNode rnode;

	/*
	 * Initialize Relfilenode field of RelationData.
	 */
	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOVISIMAP:
		case RELKIND_AOBLOCKDIR:
			/* okay, these have storage */

			/* This logic should match RelationInitPhysicalAddr */
			if (relform->reltablespace)
				rnode.spcNode = relform->reltablespace;
			else
				rnode.spcNode = MyDatabaseTableSpace;
			if (rnode.spcNode == GLOBALTABLESPACE_OID)
				rnode.dbNode = InvalidOid;
			else
				rnode.dbNode = MyDatabaseId;
			if (relform->relfilenode)
				rnode.relNode = relform->relfilenode;
			else				/* Consult the relation mapper */
				rnode.relNode = RelationMapOidToFilenode(relform->oid,
														 relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			rnode.relNode = InvalidOid;
			/* some compilers generate warnings without these next two lines */
			rnode.dbNode = InvalidOid;
			rnode.spcNode = InvalidOid;
			break;
	}

	if (rnode.relNode == InvalidOid)
		return size;

	rel.rd_node = rnode;

	rel.rd_rel = relform;

	/*
	 * Initialize BackendId field of RelationData.
	 */
	switch (relform->relpersistence)
	{
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			rel.rd_backend = InvalidBackendId;
			rel.rd_islocaltemp = false;
			break;
		case RELPERSISTENCE_TEMP:
			if (isTempOrTempToastNamespace(relform->relnamespace))
			{
				rel.rd_backend = BackendIdForTempRelations();
				rel.rd_islocaltemp = true;
			}
			else
			{
				rel.rd_backend = GetTempNamespaceBackendId(relform->relnamespace);
				rel.rd_islocaltemp = false;
			}
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c",
					relform->relpersistence);
			break;
	}

	for (int forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
	{
		size += calculate_relation_size(&rel, forkNum,
										/* include_ao_aux */ false,
										/* physical_ao_size */ true,
										/* on_error_continue */ true);
	}

	return size;
}
