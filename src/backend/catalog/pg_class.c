/*-------------------------------------------------------------------------
 *
 * pg_class.c
 *	  routines to support manipulation of the pg_class relation
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_class.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"

/*
 * Issue an errdetail() informing that the relkind is not supported for this
 * operation.
 */
int
errdetail_relkind_not_supported(char relkind)
{
	/*
	 * GPDB: errdetail() returns void in this tree, so call it as a statement
	 * and return the conventional 0 (matching upstream's int signature).
	 */
	switch (relkind)
	{
		case RELKIND_RELATION:
			errdetail("This operation is not supported for tables.");
			break;
		case RELKIND_INDEX:
			errdetail("This operation is not supported for indexes.");
			break;
		case RELKIND_SEQUENCE:
			errdetail("This operation is not supported for sequences.");
			break;
		case RELKIND_TOASTVALUE:
			errdetail("This operation is not supported for TOAST tables.");
			break;
		case RELKIND_VIEW:
			errdetail("This operation is not supported for views.");
			break;
		case RELKIND_MATVIEW:
			errdetail("This operation is not supported for materialized views.");
			break;
		case RELKIND_COMPOSITE_TYPE:
			errdetail("This operation is not supported for composite types.");
			break;
		case RELKIND_FOREIGN_TABLE:
			errdetail("This operation is not supported for foreign tables.");
			break;
		case RELKIND_PARTITIONED_TABLE:
			errdetail("This operation is not supported for partitioned tables.");
			break;
		case RELKIND_PARTITIONED_INDEX:
			errdetail("This operation is not supported for partitioned indexes.");
			break;
		default:
			elog(ERROR, "unrecognized relkind: '%c'", relkind);
			break;
	}

	return 0;
}
