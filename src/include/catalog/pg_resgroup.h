/*-------------------------------------------------------------------------
 *
 * pg_resgroup.h
 *	  definition of the system "resource group" relation (pg_resgroup).
 *
 *
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_RESGROUP_H
#define PG_RESGROUP_H

#include "catalog/genbki.h"
#include "catalog/pg_resgroup_d.h"

/* ----------------
 *	pg_resgroup definition.  cpp turns this into
 *	typedef struct FormData_pg_resgroup
 * ----------------
 */

CATALOG(pg_resgroup,6436,ResGroupRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */

	NameData	rsgname;		/* name of resource group */

	Oid			parent;			/* parent resource group */
} FormData_pg_resgroup;

/* no foreign keys */

/* ----------------
 *	Form_pg_resgroup corresponds to a pointer to a tuple with
 *	the format of pg_resgroup relation.
 * ----------------
 */
typedef FormData_pg_resgroup *Form_pg_resgroup;

/* ----------------
 *	pg_resgroupcapability definition.  cpp turns this into
 *	typedef struct FormData_pg_resgroupcapability
 * ----------------
 */

typedef enum ResGroupLimitType
{
	RESGROUP_LIMIT_TYPE_UNKNOWN = 0,

	RESGROUP_LIMIT_TYPE_CONCURRENCY,
	RESGROUP_LIMIT_TYPE_CPU,
	RESGROUP_LIMIT_TYPE_MEMORY,
	RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA,
	RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO,
	RESGROUP_LIMIT_TYPE_MEMORY_AUDITOR,
	RESGROUP_LIMIT_TYPE_CPUSET,

	RESGROUP_LIMIT_TYPE_COUNT,
} ResGroupLimitType;


/* GPDB-specific index(es) (moved from indexing.h: PG15 genbki emits IndexId per-catalog) */
DECLARE_UNIQUE_INDEX(pg_resgroup_oid_index, 6447, ResGroupOidIndexId, pg_resgroup, btree(oid oid_ops));
MAKE_SYSCACHE(RESGROUPOID, pg_resgroup_oid_index, 128);
DECLARE_UNIQUE_INDEX(pg_resgroup_rsgname_index, 6444, ResGroupRsgnameIndexId, pg_resgroup, btree(rsgname name_ops));
MAKE_SYSCACHE(RESGROUPNAME, pg_resgroup_rsgname_index, 128);

#endif   /* PG_RESGROUP_H */
