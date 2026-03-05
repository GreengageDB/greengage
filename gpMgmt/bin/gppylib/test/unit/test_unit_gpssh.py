from __future__ import absolute_import
import os

import sys
from mock import patch

from .gp_unittest import GpTestCase, load_module

if sys.version_info[0] == 3:
    import io
    StringIO = io.StringIO
else:
    import StringIO
    StringIO = BytesIO = StringIO.StringIO


class GpSshTestCase(GpTestCase):
    def setUp(self):
        # because gpssh does not have a .py extension, we have to use imp to import it
        # if we had a gpssh.py, this is equivalent to:
        #   import gpssh
        #   self.subject = gpssh
        gpssh_file = os.path.abspath(os.path.dirname(__file__) + "/../../../gpssh")
        self.subject = load_module('gpssh', gpssh_file)

        self.old_sys_argv = sys.argv
        sys.argv = []

    def tearDown(self):
        sys.argv = self.old_sys_argv

    @patch('sys.exit')
    def test_when_run_without_args_prints_help_text(self, sys_exit_mock):
        sys_exit_mock.side_effect = Exception("on purpose")
        # GOOD_MOCK_EXAMPLE of stdout
        with patch('sys.stdout', new=StringIO()) as mock_stdout:
            with self.assertRaisesRe(Exception, "on purpose"):
                self.subject.main()
        self.assertIn('gpssh -- ssh access to multiple hosts at once', mock_stdout.getvalue())

    @patch('sys.exit')
    def test_happy_ssh_to_localhost_succeeds(self, sys_mock):
        sys.argv = ['', '-h', 'localhost', 'uptime']

        self.subject.main()
        sys_mock.assert_called_with(0)
