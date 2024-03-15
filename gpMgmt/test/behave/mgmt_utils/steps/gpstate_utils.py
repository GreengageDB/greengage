from behave import given, when, then
import os
import re

from gppylib.db import dbconn
from gppylib.gparray import GpArray, ROLE_MIRROR
from test.behave_utils.utils import check_stdout_msg, check_string_not_present_stdout
from gppylib.commands.gp import get_masterdatadir
from gppylib.commands import unix

@then('a sample recovery_progress.file is created from saved lines')
def impl(context):
    with open('{}/gpAdminLogs/recovery_progress.file'.format(os.path.expanduser("~")), 'w+') as fp:
        fp.writelines(context.recovery_lines)

@given('a sample recovery_progress.file is created with ongoing recoveries in gpAdminLogs')
def impl(context):
    with open('{}/gpAdminLogs/recovery_progress.file'.format(os.path.expanduser("~")), 'w+') as fp:
        fp.write("full:5: 1164848/1371715 kB (84%), 0/1 tablespace (...t1/demoDataDir0/base/16384/40962)\n")
        fp.write("incremental:6: 1/1371875 kB (1%)")

@then('a sample {lock_file} directory is created using the background pid in master_data_directory')
@given('a sample {lock_file} directory is created using the background pid in master_data_directory')
def impl(context, lock_file):
    if 'bg_pid' in context:
        bg_pid = context.bg_pid
        if not unix.check_pid(bg_pid):
            raise Exception("The background process with PID {} is not running.".format(bg_pid))
    else:
        bg_pid = ""

    lock_dir = os.path.join(get_masterdatadir() + '/{0}'.format(lock_file))
    os.mkdir(lock_dir)

    utility_pidfile = os.path.join(lock_dir, 'PID')

    with open(utility_pidfile, 'w') as f:
        f.write(bg_pid)

@given('a sample recovery_progress.file is created with completed recoveries in gpAdminLogs')
def impl(context):
    with open('{}/gpAdminLogs/recovery_progress.file'.format(os.path.expanduser("~")), 'w+') as fp:
        fp.write("incremental:5: pg_rewind: Done!\n")
        fp.write("full:6: 1164848/1371715 kB (84%), 0/1 tablespace (...t1/demoDataDir0/base/16384/40962)\n")
        fp.write("full:7: pg_basebackup: completed")

@then('gpstate output looks like')
def impl(context):
    # Check the header line first.
    header_pattern = r'[ \t]+'.join(context.table.headings)
    check_stdout_msg_in_order(context, header_pattern)

    check_rows_exist(context)

@then('gpstate output contains "{recovery_types}" entries for mirrors of content {contents}')
def impl(context, recovery_types, contents):
    recovery_types = recovery_types.split(',')
    contents = [int(c) for c in contents.split(',')]
    contents = set(contents)

    all_segments = GpArray.initFromCatalog(dbconn.DbURL()).getDbList()
    segments_to_display = []
    segments_to_not_display = []
    for seg in all_segments:
        if seg.getSegmentContentId() in contents and seg.getSegmentRole() == ROLE_MIRROR:
            segments_to_display.append(seg)
        else:
            segments_to_not_display.append(seg)

    for index, seg_to_display in enumerate(segments_to_display):
        hostname = seg_to_display.getSegmentHostName()
        port = seg_to_display.getSegmentPort()
        if recovery_types[index] == "differential":
            expected_msg = "{}[ \t]+{}[ \t]+{}[ \t]+(.+?)[ \t]+([\d,]+)[ \t]+[0-9]+\%".format(hostname, port,
                                                                                              recovery_types[index])
        else:
            expected_msg = "{}[ \t]+{}[ \t]+{}[ \t]+[0-9]+[ \t]+[0-9]+[ \t]+[0-9]+\%".format(hostname, port,
                                                                                             recovery_types[index])
        check_stdout_msg(context, expected_msg)

    #TODO assert that only segments_to_display are printed to the console
    # for seg_to_not_display in segments_to_not_display:
    #     check_string_not_present_stdout(context, str(seg_to_not_display.getSegmentPort()))

@then('gpstate output has rows')
def impl(context):
    check_rows_exist(context)


@then('gpstate output has rows with keys values')
def impl(context):
    # Check that every row exists in the standard out in the specified order.
    # We accept any amount of horizontal whitespace in between columns.
    def check_row(row):
        split_row = map(lambda str: str.strip(), ''.join(row).split('='))
        row_pattern = r'[ \t]+=[ \t]+'.join(split_row)
        check_stdout_msg_in_order(context, row_pattern)

    check_row(context.table.headings)

    for row in context.table:
        check_row(row)


def check_rows_exist(context):
    # Check that every row exists in the standard out. We accept any amount
    # of horizontal whitespace in between columns.
    for row in context.table:
        row_pattern = r'[ \t]+'.join(row)
        check_stdout_msg_in_order(context, row_pattern)

def check_stdout_msg_in_order(context, msg):
    """
    Searches forward in context.stdout_message for a string matching the msg
    pattern. Once output has been matched, it's no longer considered for future
    matching. Use this matcher for order-dependent output tests.
    """
    # Lazily initialize the stdout_position -- if this is the first time we've
    # called this, start at the beginning.
    if 'stdout_position' not in context:
        context.stdout_position = 0

    pat = re.compile(msg)

    match = pat.search(context.stdout_message, pos=context.stdout_position)
    if not match:
        err_str = (
            "Expected stdout string '%s' in remaining output:\n"
            "%s\n\n"
            "Full output was\n%s"
        ) % (msg, context.stdout_message[context.stdout_position:], context.stdout_message)
        raise Exception(err_str)

    context.stdout_position = match.end()


@given('a sample recovery_progress.file is created with ongoing differential recoveries in gpAdminLogs')
def impl(context):
    with open('{}/gpAdminLogs/recovery_progress.file'.format(os.path.expanduser("~")), 'w+') as fp:
        fp.write(
            "differential:5:     16,454,866   4%   16.52MB/s    0:00:00 (xfr#216, ir-chk=9669/9907) :Syncing pg_data "
            "of dbid 5\n")
        fp.write("differential:6:          8,192 100%    7.81MB/s    0:00:00 (xfr#1, to-chk=0/1) :Syncing tablespace of "
                 "dbid 6 for oid 20516")


