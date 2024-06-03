import os
import tempfile
import yaml

from behave import given, when, then
from test.behave_utils.utils import *

def parse_processed_files(cmdOutput):
    res = []
    for line in cmdOutput.splitlines():
        if line.startswith('-'):
            res.append(os.path.basename(line.strip('- ')))
    return res

@given('log directory with files')
def impl(context):
    newMasterDir = tempfile.mkdtemp()
    fNames = [r["name"] for r in context.table]

    logDir = os.path.join(newMasterDir, 'pg_log')
    os.mkdir(logDir)
    for name in fNames:
        open(os.path.join(logDir, name), 'a').close()

    context.newMasterDir = newMasterDir

@when('under changed DATA_DIR user runs "{command}"')
def impl(context, command):
    run_gpcommand(context, command, "MASTER_DATA_DIRECTORY=%s" % context.newMasterDir)

@then("gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)")
def impl(context):
    processed = parse_processed_files(context.error_message)
    expected = [row["name"] for row in context.table]

    if len(processed) != len(expected):
        raise Exception('got:\n%s\ndiffers with expected:\n%s'
                        % (yaml.dump(processed), yaml.dump(expected)))

    for i in range(0, len(processed)):
        if processed[i] != context.table[i]["name"]:
            raise Exception('got:\n%s\ndiffers with expected:\n%s'
                            % (yaml.dump(processed), yaml.dump(expected)))
