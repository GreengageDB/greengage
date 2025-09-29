#ifndef PG_UPGRADE_GREENGAGE_H
#define PG_UPGRADE_GREENGAGE_H
/*
 *	greengage/pg_upgrade_greengage.h
 *
 *	Portions Copyright (c) 2019-Present, VMware, Inc. or its affiliates
 *	src/bin/pg_upgrade/greengage/pg_upgrade_greengage.h
 */


#include "pg_upgrade.h"

#ifdef EXTRA_DYNAMIC_MEMORY_DEBUG
#include "utils/palloc_memory_debug_undef.h"
#endif

#define PG_OPTIONS_UTILITY_MODE_VERSION(major_version) \
	( (GET_MAJOR_VERSION(major_version)) < 1200 ?      \
		" PGOPTIONS='-c gp_session_role=utility' " :   \
		" PGOPTIONS='-c gp_role=utility' ")

/*
 * Enumeration for operations in the progress report
 */
typedef enum
{
	CHECK,
	SCHEMA_DUMP,
	SCHEMA_RESTORE,
	FILE_MAP,
	FILE_COPY,
	FIXUP,
	ABORT,
	DONE
} progress_type;

typedef enum {
	GREENGAGE_MODE_OPTION = 10,
	GREENGAGE_PROGRESS_OPTION = 11,
	GREENGAGE_CONTINUE_CHECK_ON_FATAL = 12,
	GREENGAGE_SKIP_TARGET_CHECK = 13,
	GREENGAGE_SKIP_CHECKS = 14,
	GREENGAGE_OUTPUT_DIR = 15
} greengageOption;

#define GREENGAGE_OPTIONS \
	{"mode", required_argument, NULL, GREENGAGE_MODE_OPTION}, \
	{"progress", no_argument, NULL, GREENGAGE_PROGRESS_OPTION}, \
	{"continue-check-on-fatal", no_argument, NULL, GREENGAGE_CONTINUE_CHECK_ON_FATAL}, \
	{"skip-target-check", no_argument, NULL, GREENGAGE_SKIP_TARGET_CHECK}, \
	{"skip-checks", no_argument, NULL, GREENGAGE_SKIP_CHECKS}, \
	{"output-dir", required_argument, NULL, GREENGAGE_OUTPUT_DIR},

#define GREENGAGE_USAGE "\
      --mode=TYPE               designate node type to upgrade, \"segment\" or \"dispatcher\" (default \"segment\")\n\
      --progress                enable progress reporting\n\
      --continue-check-on-fatal continue to run through all pg_upgrade checks without upgrade. Stops on major issues\n\
      --skip-target-check       skip all checks on new/target cluster\n\
      --skip-checks             skip all checks\n\
      --output-dir              directory to output logs. Default=\"COORDINATOR_DATA_DIRECTORY/pg_upgrade.d\"\n\
"

/* option_gp.c */
void initialize_greengage_user_options(void);
bool process_greengage_option(greengageOption option);
bool is_greengage_dispatcher_mode(void);
bool is_show_progress_mode(void);
bool is_continue_check_on_fatal(void);
void set_check_fatal_occured(void);
bool get_check_fatal_occurred(void);
bool is_skip_target_check(void);
bool skip_checks(void);
char *get_output_dir(void);

/* controldata_gp.c */
void freeze_master_data(void);
void reset_system_identifier(void);

/* aotable.c */

void		restore_aosegment_tables(void);
bool        is_appendonly(char relstorage);

/* version_gp.c */

void check_hash_partition_usage(void);
void old_GPDB6_check_for_unsupported_sha256_password_hashes(void);
void new_gpdb_invalidate_bitmap_indexes(void);

/* check_gp.c */

void check_greengage(void);
void setup_GPDB6_data_type_checks(ClusterInfo *cluster);
void teardown_GPDB6_data_type_checks(ClusterInfo *cluster);

/* reporting.c */

void report_progress(ClusterInfo *cluster, progress_type op, char *fmt,...)
pg_attribute_printf(3, 4);
void close_progress(void);

/* util.c */
void make_outputdirs_gp(char *pgdata);

#endif /* PG_UPGRADE_GREENGAGE_H */
