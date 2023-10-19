import os
import tempfile
import yaml
from datetime import datetime, timedelta

from behave import given, when, then
from test.behave_utils.utils import *

def parse_processed_files(cmdStderr, cmdStdout):
    res = []
    counts = {}
    for line in cmdStdout.splitlines():
        # Currently gplogfilter doesn't replace all delimeters with '|'
        # We should check both ',' and '|' before splitting
        if '|' in line:
            filename = line.split('|')[1]
        elif ',' in line:
            filename = line.split(',')[1]
        else:
            continue

        if filename not in counts:
            counts[filename] = 0
        counts[filename] += 1
    for line in cmdStderr.splitlines():
        if line.startswith('-'):
            filename = os.path.basename(line.strip('- '))
            count = counts.get(filename, 0)
            res.append({'name': filename, 'count': count})
    return res

# Expects a table with columns `name` and `start_time`.
# For each row creates a log file with the given name and 10 lines of logs.
# The first line has time `start_time`, and other 9 lines have an interval of 10 seconds.
# For example if start_time is 2024-01-01 09:00:00, then we will have 10 lines with times
# ranging from 2024-01-01 09:00:00 to 2024-01-01 09:01:30.
@given('log directory with files')
def impl(context):
    newMasterDir = tempfile.mkdtemp()

    logDir = os.path.join(newMasterDir, 'log')
    os.mkdir(logDir)
    for r in context.table:
        filename = r["name"]
        with open(os.path.join(logDir, filename), 'w') as f:
            datetime_format = '%Y-%m-%d %H:%M:%S'
            start_time = datetime.strptime(r["start_time"], datetime_format)
            for i in range(10):
                time = start_time + timedelta(seconds=10 * i)
                s = datetime.strftime(time, datetime_format)
                f.write(s + ' UTC,' + filename + ','*28 + ' tagged line\n')

    context.newMasterDir = newMasterDir

@when('under changed DATA_DIR user runs "{command}"')
def impl(context, command):
    run_gpcommand(context, command, "COORDINATOR_DATA_DIRECTORY=%s" % context.newMasterDir)

# Expects a table with columns `name` and `count`.
# Each row corresponds to a log file that was read,
# and `count` is the number of lines that passed the filter.
# If all log lines pass the filter, the `count` will be 10.
# If no log lines passed the filter, but the file was read, the `count` will be 0.
# If the file was filtered out entirely, there won't be a corresponding row in the table.
@then("gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)")
def impl(context):
    processed = parse_processed_files(context.error_message, context.stdout_message)
    expected = [{"name": row["name"], "count": int(row["count"])} for row in context.table]

    if len(processed) != len(expected):
        raise Exception('got:\n%s\ndiffers with expected:\n%s'
                        % (yaml.dump(processed), yaml.dump(expected)))

    for i in range(0, len(processed)):
        name_matches = processed[i]["name"] == expected[i]["name"]
        count_matches = processed[i]["count"] == expected[i]["count"]
        if not name_matches or not count_matches:
            raise Exception('got:\n%s\ndiffers with expected:\n%s'
                            % (yaml.dump(processed), yaml.dump(expected)))
