#include "postgres_fe.h"

#include "pg_upgrade_greenplum.h"

typedef enum
{
	DISPATCHER = 0,
	SEGMENT
} segmentMode;

typedef struct {
	bool progress;
	segmentMode segment_mode;
	bool continue_check_on_fatal;
	bool skip_target_check;
	bool skip_checks;
	char *output_dir;
} GreenplumUserOpts;

static GreenplumUserOpts greenplum_user_opts;
static bool check_fatal_occurred;

void
initialize_greenplum_user_options(void)
{
	greenplum_user_opts.segment_mode = SEGMENT;
	greenplum_user_opts.continue_check_on_fatal = false;
	greenplum_user_opts.skip_target_check = false;
	greenplum_user_opts.skip_checks = false;
	greenplum_user_opts.output_dir = NULL;
}

bool
process_greenplum_option(greenplumOption option)
{
	switch (option)
	{
		case GREENPLUM_MODE_OPTION:        /* --mode={dispatcher|segment} */
			if (pg_strcasecmp("dispatcher", optarg) == 0)
				greenplum_user_opts.segment_mode = DISPATCHER;
			else if (pg_strcasecmp("segment", optarg) == 0)
				greenplum_user_opts.segment_mode = SEGMENT;
			else
			{
				pg_log(PG_FATAL, "invalid segment configuration\n");
				exit(1);
			}
			break;

		case GREENPLUM_PROGRESS_OPTION:        /* --progress */
			greenplum_user_opts.progress = true;
			break;

		case GREENPLUM_CONTINUE_CHECK_ON_FATAL:
			if (user_opts.check)
			{
				greenplum_user_opts.continue_check_on_fatal = true;
				check_fatal_occurred = false;
			}
			else
			{
				pg_log(PG_FATAL,
					"--continue-check-on-fatal: should be used with check mode (-c)\n");
				exit(1);
			}
			break;

		case GREENPLUM_SKIP_TARGET_CHECK:
			if (user_opts.check)
					greenplum_user_opts.skip_target_check = true;
			else
			{
					pg_log(PG_FATAL,
						"--skip-target-check: should be used with check mode (-c)\n");
					exit(1);
			}
			break;

		case GREENPLUM_SKIP_CHECKS:
			greenplum_user_opts.skip_checks = true;
			break;

		case GREENPLUM_OUTPUT_DIR:
			greenplum_user_opts.output_dir = pg_strdup(optarg);
			break;

		default:
			return false;
	}

	return true;
}

bool
is_greenplum_dispatcher_mode()
{
	return greenplum_user_opts.segment_mode == DISPATCHER;
}

bool
is_show_progress_mode(void)
{
	return greenplum_user_opts.progress;
}

bool
is_continue_check_on_fatal(void)
{
	return greenplum_user_opts.continue_check_on_fatal;
}

void
set_check_fatal_occured(void)
{
	check_fatal_occurred = true;
}

bool
get_check_fatal_occurred(void)
{
	return check_fatal_occurred;
}

bool
is_skip_target_check(void)
{
	return greenplum_user_opts.skip_target_check;
}

bool
skip_checks(void)
{
	return greenplum_user_opts.skip_checks;
}

char*
get_output_dir(void)
{
	return greenplum_user_opts.output_dir;
}
