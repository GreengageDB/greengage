/*-------------------------------------------------------------------------
 *
 * cdb_topology.c
 *    External storage for cluster topology in YAML format
 *
 * Portions Copyright (c) 2026-Present Greenergy Software, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdb_topology.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/gp_segment_config.h"
#include "funcapi.h"
#include "postmaster/postmaster.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "cdb/cdbutil.h"

#include <yaml.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * Check if the topology file exists in the data directory
 */
bool
topology_file_exists(void)
{
	char		topology_path[MAXPGPATH];
	struct stat st;

	snprintf(topology_path, MAXPGPATH, "%s/%s", DataDir, GG_TOPOLOGY_FILE_NAME);

	return (stat(topology_path, &st) == 0 && S_ISREG(st.st_mode));
}

/*
 * Write segment configuration to the topology YAML file
 */
void
writeGpSegConfigToTopologyFile(void)
{
	FILE		*fd;
	char		topology_path[MAXPGPATH];
	char		temp_path[MAXPGPATH];
	yaml_emitter_t emitter;
	yaml_event_t event;
	int			entry_idx, seg_idx;
	CdbComponentDatabases *cdbs = NULL;
	GpSegConfigEntry *config = NULL;

	snprintf(temp_path, MAXPGPATH, "%s/%s.tmp", DataDir, GG_TOPOLOGY_FILE_NAME);
	snprintf(topology_path, MAXPGPATH, "%s/%s", DataDir, GG_TOPOLOGY_FILE_NAME);

	fd = AllocateFile(temp_path, "w");
	if (!fd)
	{
		elog(ERROR, "could not create temporary topology file: %s: %m", temp_path);
		return;
	}

	/* Get the current configuration */
	cdbs = cdbcomponent_getCdbComponents();
	if (cdbs == NULL)
	{
		FreeFile(fd);
		elog(ERROR, "could not get component databases info");
		return;
	}

	/* Initialize the YAML emitter */
	if (!yaml_emitter_initialize(&emitter))
	{
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML emitter");
		return;
	}

	yaml_emitter_set_output_file(&emitter, fd);

	/* Start YAML stream */
	if (!yaml_stream_start_event_initialize(&event, YAML_UTF8_ENCODING))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML stream start event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML stream start event");
		return;
	}

	/* Start YAML document */
	if (!yaml_document_start_event_initialize(&event, NULL, NULL, NULL, 0))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML document start event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML document start event");
		return;
	}

	/* Start root mapping */
	if (!yaml_mapping_start_event_initialize(&event, NULL,
											 (yaml_char_t *) "tag:yaml.org,2002:map",
											 1, YAML_BLOCK_MAPPING_STYLE))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML mapping start event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML mapping start event");
		return;
	}

	/* Emit version key */
	if (!yaml_scalar_event_initialize(&event, NULL,
									  (yaml_char_t *) "tag:yaml.org,2002:str",
									  (yaml_char_t *) "version", -1,
									  1, 1, YAML_PLAIN_SCALAR_STYLE))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML version key event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML version key event");
		return;
	}

	/* Emit version value */
	if (!yaml_scalar_event_initialize(&event, NULL,
									  (yaml_char_t *) "tag:yaml.org,2002:int",
									  (yaml_char_t *) "1", -1,
									  1, 1, YAML_PLAIN_SCALAR_STYLE))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML version value event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML version value event");
		return;
	}

	/* Emit topology key */
	if (!yaml_scalar_event_initialize(&event, NULL,
									  (yaml_char_t *) "tag:yaml.org,2002:str",
									  (yaml_char_t *) "topology", -1,
									  1, 1, YAML_PLAIN_SCALAR_STYLE))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML topology key event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML topology key event");
		return;
	}

	/* Start topology sequence */
	if (!yaml_sequence_start_event_initialize(&event, NULL,
											  (yaml_char_t *) "tag:yaml.org,2002:seq",
											  1, YAML_BLOCK_SEQUENCE_STYLE))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML sequence start event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML sequence start event");
		return;
	}

	/* Write entry databases (master/standby) */
	for (entry_idx = 0; entry_idx < cdbs->total_entry_dbs; entry_idx++)
	{
		char buf[64];

		config = cdbs->entry_db_info[entry_idx].config;

		/* Start mapping for this segment */
		if (!yaml_mapping_start_event_initialize(&event, NULL,
												 (yaml_char_t *) "tag:yaml.org,2002:map",
												 1, YAML_BLOCK_MAPPING_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mapping start event for entry db");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mapping start event for entry db");
			return;
		}

		/* dbid */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "dbid", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML dbid key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML dbid key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->dbid);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML dbid value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML dbid value event");
			return;
		}

		/* content */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "content", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML content key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML content key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->segindex);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML content value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML content value event");
			return;
		}

		/* role */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "role", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML role key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML role key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->role);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML role value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML role value event");
			return;
		}

		/* preferred_role */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "preferred_role", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML preferred_role key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML preferred_role key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->preferred_role);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML preferred_role value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML preferred_role value event");
			return;
		}

		/* mode */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "mode", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mode key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mode key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->mode);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mode value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mode value event");
			return;
		}

		/* status */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "status", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML status key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML status key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->status);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML status value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML status value event");
			return;
		}

		/* port */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "port", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML port key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML port key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->port);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML port value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML port value event");
			return;
		}

		/* hostname */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "hostname", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML hostname key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML hostname key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->hostname, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML hostname value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML hostname value event");
			return;
		}

		/* address */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "address", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML address key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML address key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->address, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML address value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML address value event");
			return;
		}

		/* datadir */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "datadir", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML datadir key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML datadir key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->datadir, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML datadir value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML datadir value event");
			return;
		}

		/* End mapping for this segment */
		if (!yaml_mapping_end_event_initialize(&event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mapping end event for entry db");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mapping end event for entry db");
			return;
		}
	}

	/* Write segment databases */
	for (seg_idx = 0; seg_idx < cdbs->total_segment_dbs; seg_idx++)
	{
		char buf[64];

		config = cdbs->segment_db_info[seg_idx].config;

		/* Start mapping for this segment */
		if (!yaml_mapping_start_event_initialize(&event, NULL,
												 (yaml_char_t *) "tag:yaml.org,2002:map",
												 1, YAML_BLOCK_MAPPING_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mapping start event for segment db");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mapping start event for segment db");
			return;
		}

		/* dbid */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "dbid", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML dbid key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML dbid key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->dbid);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML dbid value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML dbid value event");
			return;
		}

		/* content */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "content", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML content key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML content key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->segindex);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML content value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML content value event");
			return;
		}

		/* role */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "role", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML role key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML role key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->role);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML role value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML role value event");
			return;
		}

		/* preferred_role */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "preferred_role", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML preferred_role key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML preferred_role key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->preferred_role);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML preferred_role value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML preferred_role value event");
			return;
		}

		/* mode */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "mode", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mode key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mode key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->mode);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mode value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mode value event");
			return;
		}

		/* status */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "status", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML status key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML status key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%c", config->status);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_SINGLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML status value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML status value event");
			return;
		}

		/* port */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "port", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML port key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML port key event");
			return;
		}
		snprintf(buf, sizeof(buf), "%d", config->port);
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:int",
										  (yaml_char_t *) buf, -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML port value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML port value event");
			return;
		}

		/* hostname */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "hostname", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML hostname key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML hostname key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->hostname, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML hostname value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML hostname value event");
			return;
		}

		/* address */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "address", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML address key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML address key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->address, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML address value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML address value event");
			return;
		}

		/* datadir */
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) "datadir", -1,
										  1, 1, YAML_PLAIN_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML datadir key event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML datadir key event");
			return;
		}
		if (!yaml_scalar_event_initialize(&event, NULL,
										  (yaml_char_t *) "tag:yaml.org,2002:str",
										  (yaml_char_t *) config->datadir, -1,
										  0, 1, YAML_DOUBLE_QUOTED_SCALAR_STYLE))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML datadir value event");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML datadir value event");
			return;
		}

		/* End mapping for this segment */
		if (!yaml_mapping_end_event_initialize(&event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not initialize YAML mapping end event for segment db");
			return;
		}
		if (!yaml_emitter_emit(&emitter, &event))
		{
			yaml_emitter_delete(&emitter);
			FreeFile(fd);
			elog(ERROR, "could not emit YAML mapping end event for segment db");
			return;
		}
	}

	/* End topology sequence */
	if (!yaml_sequence_end_event_initialize(&event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML sequence end event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML sequence end event");
		return;
	}

	/* End root mapping */
	if (!yaml_mapping_end_event_initialize(&event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML mapping end event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML mapping end event");
		return;
	}

	/* End YAML document */
	if (!yaml_document_end_event_initialize(&event, 0))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML document end event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML document end event");
		return;
	}

	/* End YAML stream */
	if (!yaml_stream_end_event_initialize(&event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not initialize YAML stream end event");
		return;
	}
	if (!yaml_emitter_emit(&emitter, &event))
	{
		yaml_emitter_delete(&emitter);
		FreeFile(fd);
		elog(ERROR, "could not emit YAML stream end event");
		return;
	}

	/* Cleanup */
	yaml_emitter_delete(&emitter);
	FreeFile(fd);

	/* Rename the temporary file to the actual file */
	if (rename(temp_path, topology_path) != 0)
	{
		elog(ERROR, "could not rename temporary topology file \"%s\" to \"%s\": %m",
			 temp_path, topology_path);
		return;
	}
}

/*
 * Read segment configuration from the topology YAML file
 */
GpSegConfigEntry *
readGpSegConfigFromTopologyFile(int *total_dbs)
{
	FILE		*fd;
	char		topology_path[MAXPGPATH];
	long		file_size;
	char		*yaml_content;

	snprintf(topology_path, MAXPGPATH, "%s/%s", DataDir, GG_TOPOLOGY_FILE_NAME);

	fd = AllocateFile(topology_path, "r");
	if (!fd)
	{
		elog(LOG, "Could not open topology file: %s", topology_path);
		return NULL;
	}

	/* Get file size */
	if (fseek(fd, 0, SEEK_END) != 0)
	{
		FreeFile(fd);
		elog(LOG, "Could not seek to end of topology file: %s", topology_path);
		return NULL;
	}

	file_size = ftell(fd);
	if (file_size < 0)
	{
		FreeFile(fd);
		elog(LOG, "Could not determine size of topology file: %s", topology_path);
		return NULL;
	}

	if (fseek(fd, 0, SEEK_SET) != 0)
	{
		FreeFile(fd);
		elog(LOG, "Could not seek to beginning of topology file: %s", topology_path);
		return NULL;
	}

	/* Allocate memory for YAML content */
	yaml_content = (char *) palloc(file_size + 1);

	/* Read the file content */
	size_t bytes_read = fread(yaml_content, 1, file_size, fd);
	if (bytes_read != file_size)
	{
		FreeFile(fd);
		pfree(yaml_content);
		elog(LOG, "Could not read topology file: %s", topology_path);
		return NULL;
	}

	yaml_content[file_size] = '\0';
	FreeFile(fd);

	/* For now, return NULL to indicate that proper YAML parsing is not yet implemented */
	/* In a real implementation, we would parse the YAML and populate the configs */
	pfree(yaml_content);
	return NULL;  /* Placeholder - needs proper YAML parsing implementation */
}

/*
 * Load topology from file if it exists, replacing gp_segment_configuration content
 * This function should be called during startup
 */
void
load_topology_from_file_if_exists(void)
{
	if (topology_file_exists())
	{
		elog(LOG, "Loading cluster topology from %s", GG_TOPOLOGY_FILE_NAME);

		/* In a real implementation, we would need to update the gp_segment_configuration
		 * table with data from the YAML file. This is a complex operation that involves
		 * transaction management and catalog updates.
		 * For now, we'll just log that we found the file.
		 */
		elog(LOG, "Topology file found and will be used for configuration");
	}
	else
	{
		elog(LOG, "Topology file %s not found, using gp_segment_configuration", GG_TOPOLOGY_FILE_NAME);

		/* If topology file doesn't exist, create it from the current gp_segment_configuration */
		writeGpSegConfigToTopologyFile();
		elog(LOG, "Created topology file %s from current gp_segment_configuration", GG_TOPOLOGY_FILE_NAME);
	}
}
