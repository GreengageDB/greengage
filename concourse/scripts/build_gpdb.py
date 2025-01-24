#!/usr/bin/python3

import glob
import optparse
import os
import shutil
import stat
import subprocess
import sys

from builds.GpBuild import GpBuild

INSTALL_DIR = "/usr/local/gpdb"
DEPENDENCY_INSTALL_DIR = "/usr/local"
SCRIPT_LOC=os.path.dirname(os.path.realpath(__file__))
GPDB_SRC_DIR="{0}/../../".format(SCRIPT_LOC)

INSTALL_DIR = "/usr/local/greengage-db-devel"

def create_gpadmin_user():
    status = subprocess.call("{0}/concourse/scripts/setup_gpadmin_user.bash".format(GPDB_SRC_DIR))
    os.chmod('/bin/ping', os.stat('/bin/ping').st_mode | stat.S_ISUID)
    if status:
        return status

def extract_explain_test_suite():
    tarfiles = glob.glob('explain_test_suite/*.tar.gz')
    if len(tarfiles) != 1:
        print("Expected to find 1 tar file.")
        return 1
    status = subprocess.call(["tar", "xvf", tarfiles[0]])
    return status

def tar_explain_output():
    status = subprocess.call(["tar", "czvf", "output/explain_ouput.tar.gz", "out/"])
    return status

def fail_on_error(status):
    if status:
        sys.exit(status)


def main():
    parser = optparse.OptionParser()
    parser.add_option("--mode", choices=['orca', 'planner'])
    parser.add_option("--output_dir", dest="output_dir", default=INSTALL_DIR)
    parser.add_option("--num_segments", dest="num_segments", default=32, help="number of segments")
    parser.add_option("--configure-option", dest="configure_option", action="append",
                      help="Configure flags, ex --configure_option=--disable-orca --configure_option=--disable-gpcloud")
    parser.add_option("--action", choices=['build', 'test', 'test_explain_suite'], dest="action", default='build',
                      help="Build GPDB or Run Install Check")
    parser.add_option("--dbexists", dest="dbexists", action="store_true", default=False, help="create new demo cluster")
    (options, args) = parser.parse_args()

    gpBuild = GpBuild(options.mode)

    status = gpBuild.install_dependency("bin_gpdb", INSTALL_DIR)
    fail_on_error(status)

    if not options.dbexists:
        status = create_gpadmin_user()
        fail_on_error(status)
    else:
        status = extract_explain_test_suite()
        fail_on_error(status)
    status = gpBuild.run_explain_test_suite(options.dbexists, options.num_segments)
    fail_on_error(status)
    status = tar_explain_output()
    fail_on_error(status)
    return 0

if __name__ == "__main__":
    sys.exit(main())
