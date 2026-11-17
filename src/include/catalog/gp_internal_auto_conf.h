/*-------------------------------------------------------------------------
 *
 * gp_internal_auto_conf.h
 *	  Name of the per-segment internal configuration file.
 *
 * Kept apart from catalog.h so that frontend programs (initdb,
 * pg_basebackup, pg_rewind) can use it without pulling in backend-only
 * headers such as utils/relcache.h.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * src/include/catalog/gp_internal_auto_conf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_INTERNAL_AUTO_CONF_H
#define GP_INTERNAL_AUTO_CONF_H

/*
 * This file is used to store internal configuration information specific to a
 * server that's not same between primary and mirror pair. For example it
 * stores gp_dbid, which is different for primary and mirror pair, even if
 * contentid is same for them. This file is not copied over during
 * pg_basebackup and pg_rewind to mirror from primary.
 */
#define GP_INTERNAL_AUTO_CONF_FILE_NAME "internal.auto.conf"

#endif							/* GP_INTERNAL_AUTO_CONF_H */
