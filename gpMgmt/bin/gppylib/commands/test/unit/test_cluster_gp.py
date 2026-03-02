#!/usr/bin/env python

import os
import psutil
import shutil
import tempfile
import unittest

from gppylib.db import dbconn
from gppylib.commands.gp import getPostmasterPID, get_postmaster_pid_locally


class GpCommandTestCase(unittest.TestCase):
    def setUp(self):
        # Connect to the database pointed to by PGHOST et al.
        self.url = dbconn.DbURL()

        with dbconn.connect(self.url) as conn:
            result = dbconn.execSQL(
                conn,
                "SELECT port, datadir FROM gp_segment_configuration"
            ).fetchall()

        for port, datadir in result:
            if not psutil.pid_exists(port):
                # Process with PID as port number doesn't exist
                # use it for test
                self.port = port
                self.datadir = datadir
                break
        else:
            # Unexpectedly all postmaster's ports have corresponding PIDs
            self.port = -1
            self.pid = -1
            self.datadir = ""


    def test_get_postmaster_pid_locally_valid_pid(self):
        result = get_postmaster_pid_locally(self.datadir)
        self.assertIsInstance(result, int)
        self.assertNotEqual(result, -1)

    def test_getPostmasterPID_valid_pid(self):
        result = getPostmasterPID("localhost", self.datadir)
        self.assertIsInstance(result, int)
        self.assertNotEqual(result, -1)

    def test_get_postmaster_pid_locally_nonexistent_pid(self):
        if self.port == -1:
            self.skipTest("Unexpectedly all postmaster's ports have corresponding PIDs")

        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, "postmaster.pid")
        with open(temp_file_path, "w") as f:
            f.write(str(self.port))

        result = get_postmaster_pid_locally(temp_dir)
        shutil.rmtree(temp_dir)
        self.assertIsInstance(result, int)
        self.assertEqual(result, -1, "PID file pid: {}, found pid: {}".format(self.port, result))

    def test_get_getPostmasterPID_nonexistent_pid(self):
        if self.port == -1:
            self.skipTest("Unexpectedly all postmaster's ports have corresponding PIDs")

        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, "postmaster.pid")
        with open(temp_file_path, "w") as f:
            f.write(str(self.port))

        result = getPostmasterPID("localhost", temp_dir)
        shutil.rmtree(temp_dir)
        self.assertIsInstance(result, int)
        self.assertEqual(result, -1, "PID file pid: {}, found pid: {}".format(self.port, result))

#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
