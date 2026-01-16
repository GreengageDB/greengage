#include "gg_tables_tracking_guc.h"

#include "cdb/cdbvars.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_db_role_setting.h"
#include <limits.h>
#include "utils/guc.h"
#include "tf_shmem.h"

int			bloom_size = DEFAULT_BLOOM_SIZE_BYTES;
int			db_track_count = DEFAULT_DB_TRACK_COUNT;
bool		is_tracked = DEFAULT_IS_TRACKED;
bool		get_full_snapshot_on_recovery = DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY;
int			drops_count = DEFAULT_DROPS_COUNT;
char	   *tracked_schemas = DEFAULT_TRACKED_SCHEMAS;
char	   *tracked_rel_ams = DEFAULT_TRACKED_REL_AMS;
char	   *tracked_rel_kinds = DEFAULT_TRACKED_REL_KINDS;
int			tracking_worker_naptime_sec = DEFAULT_NAPTIME_SEC;

/*
 * Variable controlling GUC setting. Only extension functions are allowed
 * to set GUC during NormalProcessing mode.
 */
static bool guc_is_unlocked = false;

void
tf_guc_unlock(void)
{
	guc_is_unlocked = true;
}

/*
 * Prohibit changing the GUC value manually except several cases.
 * This is not called for RESET, so RESET is not guarded
 */
static bool
check_guc(GucSource source, const char *handle)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && guc_is_unlocked))
	{
		guc_is_unlocked = false;

		if (source != PGC_S_DATABASE &&
			source != PGC_S_DEFAULT &&
			source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the %s function", handle);
	return false;
}

/*
 * Prohibit changing the gg_tables_tracking.tracking_is_db_tracked value manually
 */
static bool
check_tracked(bool *newval, void **extra, GucSource source)
{
	return check_guc(source, "tracking_register_db");
}

/*
 * Prohibit changing the gg_tables_tracking.tracking_snapshot_on_recovery value manually
 */
static bool
check_get_full_snapshot_on_recovery(bool *newval, void **extra, GucSource source)
{
	return check_guc(source, "tracking_set_snapshot_on_recovery");
}

/*
 * Prohibit changing the gg_tables_tracking.tracking_relkinds value manually
 */
static bool
check_relkinds(char **newval, void **extra, GucSource source)
{
	return check_guc(source, "tracking_register_relkinds");
}

/*
 * Prohibit changing the gg_tables_tracking.tracking_schemas value manually
 */
static bool
check_schemas(char **newval, void **extra, GucSource source)
{
	return check_guc(source, "tracking_register_schema");
}

/*
 * Prohibit changing the gg_tables_tracking.tracking_relstorages value manually
 */
static bool
check_relams(char **newval, void **extra, GucSource source)
{
	return check_guc(source, "tracking_register_relams");
}

void
tf_guc_define(void)
{
	DefineCustomIntVariable("gg_tables_tracking.tracking_bloom_size",
				   "Size of bloom filter in bytes for each tracked database",
							NULL,
							&bloom_size,
							DEFAULT_BLOOM_SIZE_BYTES,
							MIN_BLOOM_SIZE_BYTES,
							MAX_BLOOM_SIZE_BYTES,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
		);

	DefineCustomIntVariable("gg_tables_tracking.tracking_db_track_count",
							"Count of tracked databases.",
							NULL,
							&db_track_count,
							DEFAULT_DB_TRACK_COUNT,
							MIN_DB_TRACK_COUNT,
							MAX_DB_TRACK_COUNT,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
		);

	DefineCustomBoolVariable("gg_tables_tracking.tracking_is_db_tracked",
							 "Is current database tracked.",
							 NULL,
							 &is_tracked,
							 DEFAULT_IS_TRACKED,
							 PGC_SUSET,
							 0,
							 check_tracked,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("gg_tables_tracking.tracking_snapshot_on_recovery",
							 "Return full snapshot at startup/recovery.",
							 NULL,
							 &get_full_snapshot_on_recovery,
							 DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY,
							 PGC_SUSET,
							 0,
							 check_get_full_snapshot_on_recovery,
							 NULL,
							 NULL);

	DefineCustomIntVariable("gg_tables_tracking.tracking_drops_count",
							"Count of max monitored drop events.",
							NULL,
							&drops_count,
							DEFAULT_DROPS_COUNT,
							MIN_DROPS_COUNT,
							MAX_DROPS_COUNT,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("gg_tables_tracking.tracking_schemas",
							   "Tracked schema names.",
							   NULL,
							   &tracked_schemas,
							   DEFAULT_TRACKED_SCHEMAS,
							   PGC_SUSET,
							   0,
							   check_schemas,
							   NULL,
							   NULL);

	DefineCustomStringVariable("gg_tables_tracking.tracking_relams",
							   "Tracked relation storage types.",
							   NULL,
							   &tracked_rel_ams,
							   DEFAULT_TRACKED_REL_AMS,
							   PGC_SUSET,
							   0,
							   check_relams,
							   NULL,
							   NULL);

	DefineCustomStringVariable("gg_tables_tracking.tracking_relkinds",
							   "Tracked relation kinds.",
							   NULL,
							   &tracked_rel_kinds,
							   DEFAULT_TRACKED_REL_KINDS,
							   PGC_SUSET,
							   0,
							   check_relkinds,
							   NULL,
							   NULL);


	DefineCustomIntVariable("gg_tables_tracking.tracking_worker_naptime_sec",
							"Toolkit background worker nap time",
							NULL,
							&tracking_worker_naptime_sec,
							DEFAULT_NAPTIME_SEC,
							MIN_NAPTIME_SEC,
							MAX_NAPTIME_SEC,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
}
