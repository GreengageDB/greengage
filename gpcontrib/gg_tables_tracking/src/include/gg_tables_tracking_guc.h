#ifndef ARENADATA_TOOLKIT_GUC_H
#define ARENADATA_TOOLKIT_GUC_H

#include "postgres.h"

#define DEFAULT_BLOOM_SIZE_BYTES 1048576
#define DEFAULT_DB_TRACK_COUNT 5
#define DEFAULT_IS_TRACKED false
#define DEFAULT_DROPS_COUNT 100000
#define DEFAULT_TRACKED_SCHEMAS "public,gg_tables_tracking,pg_catalog,pg_toast,pg_aoseg,information_schema"
#define DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY true
#define DEFAULT_TRACKED_REL_AMS "heap,ao_row,ao_column,btree,hash,gist,gin,spgist,brin,bitmap"
#define DEFAULT_TRACKED_REL_KINDS "r,i,t,m,o,b,M,p,I"
#define DEFAULT_NAPTIME_SEC 60

#define MIN_BLOOM_SIZE_BYTES 64
#define MIN_DB_TRACK_COUNT 1
#define MIN_DROPS_COUNT 1
#define MIN_NAPTIME_SEC 1

#define MAX_BLOOM_SIZE_BYTES 128000000
#define MAX_DB_TRACK_COUNT 1000
#define MAX_DROPS_COUNT 1000000
#define MAX_NAPTIME_SEC (OID_MAX & 0x7FFFFFFF)

extern int	bloom_size;
extern int	db_track_count;
extern int	drops_count;
extern bool get_full_snapshot_on_recovery;
extern char *tracked_schemas;
extern char *tracked_rel_ams;
extern char *tracked_rel_kinds;
extern int	tracking_worker_naptime_sec;

void		tf_guc_unlock(void);
void		tf_guc_define(void);

#endif   /* ARENADATA_TOOLKIT_GUC_H */
