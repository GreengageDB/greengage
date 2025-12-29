#include "postgres.h"

#include "access/xlog.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/shmem.h"

#include "gg_tables_tracking_guc.h"
#include "gg_tables_tracking_worker.h"
#include "drops_track.h"
#include "file_hook.h"
#include "tf_shmem.h"
#include "track_files.h"

void		_PG_init(void);
void		_PG_fini(void);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	tf_guc_define();
	tf_shmem_init();
	file_hook_init();
	drops_track_init();

	if (IS_QUERY_DISPATCHER())
		track_setup_executor_hooks();

	gg_tables_tracking_worker_register();
}

void
_PG_fini(void)
{
	if (IS_QUERY_DISPATCHER())
		track_uninstall_executor_hooks();

	drops_track_deinit();
	file_hook_deinit();
	tf_shmem_deinit();
}
