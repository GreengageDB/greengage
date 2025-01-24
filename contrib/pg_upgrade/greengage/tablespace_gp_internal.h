/*
 *	greengage/tablespace_gp_internal.h
 *
 *  Internal functions to be used by the greengage components.
 *
 *	Portions Copyright (c) 2019-Present, Pivotal Software Inc
 *	contrib/pg_upgrade/greengage/tablespace_gp_internal.h
 */
void populate_old_cluster_with_old_tablespaces(ClusterInfo *oldCluster,
	const char *file_path);
