#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "cdb/cdbutil.h"
#include "catalog/pg_type.h"
#include "storage/lock.h"
#include "utils/gpexpand.h"

static void
errorOutOnLock(void)
{
	LOCKTAG gp_expand_locktag =
	{
		/* FIXME: how to fill the locktag? */
		.locktag_field1 = 0xdead,
		.locktag_field2 = 0xdead,
		.locktag_field3 = 0xdead,
		.locktag_field4 = 0xdd,
		.locktag_type = LOCKTAG_USERLOCK,
		.locktag_lockmethodid = USER_LOCKMETHOD,
	};
	LockAcquireResult acquired = LockAcquire(&gp_expand_locktag, AccessExclusiveLock, false, true);

	if (acquired != LOCKACQUIRE_ALREADY_HELD)
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			errmsg("ggrebalance not in progress, function can be used only under catalog lock")));
}

/*
 * Get the rebalance numsegments when creating a table.
 */
PG_FUNCTION_INFO_V1(gp_get_rebalance_numsegments);
Datum
gp_get_rebalance_numsegments(PG_FUNCTION_ARGS)
{
	errorOutOnLock();

	uint32 gp_rebalance_numsegs = pg_atomic_read_u32(gp_create_table_rebalance_numsegments);

	PG_RETURN_UINT32(gp_rebalance_numsegs);
}

/*
 * Set the numsegments when creating tables during ggrebalance run.
 *
 * For integer argument the valid range is [1, gp_num_contents_in_cluster].
 */
PG_FUNCTION_INFO_V1(gp_set_rebalance_numsegments);
Datum
gp_set_rebalance_numsegments(PG_FUNCTION_ARGS)
{
	int			numsegments = -1;

	Assert(1 == PG_NARGS());

	errorOutOnLock();

	numsegments = PG_GETARG_INT32(0);

	if (numsegments < 1 || numsegments > getgpsegmentCount())
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("invalid integer value for default numsegments: %d",
						numsegments),
				 errhint("Valid range: [1, %d (gp_num_contents_in_cluster)]",
						 getgpsegmentCount())));

	pg_atomic_write_u32(gp_create_table_rebalance_numsegments, numsegments);

	return gp_get_rebalance_numsegments(fcinfo);
}

/*
 * Reset the rebalance numsegments when creating a table.
 */
PG_FUNCTION_INFO_V1(gp_reset_rebalance_numsegments);
Datum
gp_reset_rebalance_numsegments(PG_FUNCTION_ARGS)
{
	errorOutOnLock();

	pg_atomic_write_u32(gp_create_table_rebalance_numsegments, GP_DEFAULT_NUMSEGMENTS_SHARED_UNSET);

	PG_RETURN_VOID();
}
