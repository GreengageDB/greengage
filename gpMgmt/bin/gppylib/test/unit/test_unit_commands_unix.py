import os
import StringIO

from mock import patch

from commands.unix import RemoveFile, RemoveDirectory, RemoveDirectoryContents, RemoveGlob, REMOTE,Command,isScpEnabled
from gp_unittest import *
from gppylib.commands.base import CommandResult


class CommandsUnix(GpTestCase):
    """
    this is technically an integration test since it uses actual bash shell processes
    """

    def setUp(self):
        self.dir = "/tmp/command_unix_test"
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        self.filepath = self.dir + "/foo"

        self.apply_patches([
            patch("uuid.uuid4", return_value="Patched"),
        ])
        open(self.filepath, 'a').close()

    def tearDown(self):
        RemoveDirectory.local("", self.dir)
        Command.propagate_env_map.clear()
        super(CommandsUnix, self).tearDown()


    def test_RemoveFile_for_file_succeeds_locally(self):
        RemoveFile.local("testing", self.filepath)
        self.assertFalse(os.path.exists(self.filepath))
        self.assertTrue(os.path.exists(self.dir))

    def test_RemoveFile_for_file_succeeds_with_environment(self):
        cmd = RemoveFile("testing", self.filepath)
        cmd.propagate_env_map['foo'] = 1
        cmd.run(validateAfter=True)
        self.assertFalse(os.path.exists(self.filepath))
        self.assertTrue(os.path.exists(self.dir))

    def test_RemoveFile_for_file_with_nonexistent_file_succeeds(self):
        RemoveFile.local("testing", "/doesnotexist")
        self.assertTrue(os.path.exists(self.filepath))
        self.assertTrue(os.path.exists(self.dir))


    def test_RemoveDirectory_succeeds_locally(self):
        RemoveDirectory.local("testing", self.dir)
        self.assertFalse(os.path.exists(self.dir))
        self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    def test_RemoveDirectory_with_nonexistent_dir_succeeds(self):
        RemoveDirectory.local("testing", "/doesnotexist")
        self.assertTrue(os.path.exists(self.dir))

    def test_RemoveDirectory_succeeds_with_slash(self):
        RemoveDirectory.local("testing", self.dir + "/")
        self.assertFalse(os.path.exists(self.dir))
        self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    def test_RemoveDirectoryContents_succeeds_locally(self):
        RemoveDirectoryContents.local("testing", self.dir)
        self.assertTrue(os.path.exists(self.dir))
        self.assertFalse(os.path.exists(self.filepath))
        self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    def test_RemoveDirectoryContents_with_nonexistent_dir_succeeds(self):
        RemoveDirectoryContents.local("testing", "/doesnotexist")
        self.assertTrue(os.path.exists(self.dir))
        self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    def test_RemoveDirectoryContents_with_slash(self):
        RemoveDirectoryContents.local("testing", self.dir + "/")
        self.assertTrue(os.path.exists(self.dir))
        self.assertFalse(os.path.exists(self.filepath))
        self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    def test_RemoveGlob_succeeds_locally(self):
        RemoveGlob.local("testing", self.dir + "/f*")
        self.assertFalse(os.path.exists(self.filepath))
        self.assertTrue(os.path.exists(self.dir))

    def test_RemoveGlob_with_nonexistent_succeeds(self):
        RemoveGlob.local("testing", "/doesnotexist")
        self.assertTrue(os.path.exists(self.filepath))
        self.assertTrue(os.path.exists(self.dir))

    # Note: remote tests use ssh to localhost, which is not really something that should be done in a true unit test.
    # we leave them here, commented out, for development usage only.

    # def test_RemoveFile_for_file_succeeds_remotely(self):
    #     RemoveFile.remote("testing", "localhost", self.filepath)
    #     self.assertFalse(os.path.exists(self.filepath))
    #     self.assertTrue(os.path.exists(self.dir))

    # def test_RemoveFile_for_file_succeeds_remotely_with_environment(self):
    #     cmd = RemoveFile("testing", self.filepath, REMOTE, "localhost")
    #     cmd.propagate_env_map["foo"] = 1
    #     cmd.run(validateAfter=True)
    #     self.assertFalse(os.path.exists(self.filepath))
    #     self.assertTrue(os.path.exists(self.dir))
    #
    # def test_RemoveDirectory_succeeds_remotely(self):
    #     RemoveDirectory.remote("testing", "localhost", self.dir)
    #     self.assertFalse(os.path.exists(self.dir))
    #     self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    # def test_RemoveDirectoryContents_succeeds_remotely(self):
    #     RemoveDirectoryContents.remote("testing", "localhost", self.dir)
    #     self.assertTrue(os.path.exists(self.dir))
    #     self.assertFalse(os.path.exists(self.filepath))
    #     self.assertFalse(os.path.exists("/tmp/emptyForRemovePatched"))

    # def test_RemoveGlob_succeeds_remotely(self):
    #     RemoveGlob.remote("testing", "localhost", self.dir + "/f*")
    #     self.assertFalse(os.path.exists(self.filepath))
    #     self.assertTrue(os.path.exists(self.dir))

    @patch('gppylib.commands.unix.Command.run')
    def test_isScpEnabled_when_executable_is_present(self, mock_cmd_run):
        self.assertTrue(isScpEnabled(["localhost"]))

    @patch('sys.stdout', new_callable=StringIO.StringIO)
    @patch('gppylib.commands.unix.Command.run', side_effect=[None, Exception()])
    def test_isScpEnabled_when_executable_is_not_present(self, mock_cmd_run, mock_stdout):
        self.assertFalse(isScpEnabled(["localhost", "invalid_host"]))
        self.assertIn("[Warning] Either scp is not available or does not have execute permission on host:invalid_host", mock_stdout.getvalue())

if __name__ == '__main__':
    run_tests()
