/*-------------------------------------------------------------------------
 *
 * gpexpand.h
 *	  Helper functions for gpexpand.
 *
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * src/include/utils/gpexpand.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPEXPAND_H
#define GPEXPAND_H

#include "fmgr.h"
#include "port/atomics.h"
#include "utils/relcache.h"

extern int GpExpandVersionShmemSize(void);
extern void GpExpandVersionShmemInit(void);
extern int GetGpExpandVersion(void);

extern Datum gp_expand_lock_catalog(PG_FUNCTION_ARGS);

extern void gp_expand_protect_catalog_changes(Relation relation);

extern Datum gp_expand_bump_version(PG_FUNCTION_ARGS);

extern volatile pg_atomic_uint32	*gp_create_table_rebalance_numsegments;

extern void GgRebalanceNumsegmentsShmemInit(void);
extern int GgRebalanceNumsegmentsShmemSize(void);

#endif   /* GPEXPAND_H */

