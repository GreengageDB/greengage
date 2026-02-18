/*-------------------------------------------------------------------------
 *
 * cdb_topology.h
 *    External storage for cluster topology in YAML format
 *
 * Portions Copyright (c) 2026-Present Greenergy Software, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDB_TOPOLOGY_H
#define CDB_TOPOLOGY_H

#include "postgres.h"
#include "cdb/cdbutil.h"

/* Name of the topology file */
#define GG_TOPOLOGY_FILE_NAME "gg_topology.yml"

/* Function declarations */
extern bool topology_file_exists(void);
extern GpSegConfigEntry *readGpSegConfigFromTopologyFile(int *total_dbs);
extern void writeGpSegConfigToTopologyFile(void);
extern void load_topology_from_file_if_exists(void);

#endif /* CDB_TOPOLOGY_H */