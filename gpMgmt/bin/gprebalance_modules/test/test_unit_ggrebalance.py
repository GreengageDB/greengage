import os
import imp
import sys

from gppylib.test.unit.gp_unittest import *
from mock import *
from gppylib.db.dbconn import DbURL
from gppylib.gplog import *
from gppylib.system.configurationInterface import GpConfigurationProvider
from gppylib.system.environment import GpCoordinatorEnvironment
from .config import initGparrayFromFile

def rebalance_only(numsegs):
    def inner(func):
        def wrapper(self, *args, **kwargs):
            self.options.target_segment_count = numsegs
            ret = func(self, *args, **kwargs)
            self.options.target_segment_count = None
            return ret
        return wrapper
    return inner

def check_query(conn, query):
    if  "SELECT COUNT(1) FROM pg_namespace WHERE nspname =" in query:
        return [0]
    return None

#Test options 
class GpTestRebalanceValidation(GpTestCase):
    def setUp(self):
        gprebalance_file = os.path.abspath(
            os.path.dirname(__file__) + "/../../ggrebalance")
        self.subject = imp.load_source('ggrebalance', gprebalance_file)
        self.old_sys_argv = sys.argv
        sys.argv = []
        self.options, self.args, self.parser = self.subject.parseargs()

        self.subject.logger = Mock(
            spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal', 'exception'])

        self.subject.check_running_gputils = Mock(return_value=False)

        self.apply_patches([
            patch('builtins.open', mock_open(), create=True),
            patch('builtins.input'),
            patch('ggrebalance.dbconn.DbURL', return_value=Mock(), spec=DbURL),
            patch('ggrebalance.dbconn.connect', return_value=Mock()),
            patch('ggrebalance.GpCoordinatorEnvironment',
                  return_value=Mock(), spec=GpCoordinatorEnvironment),
            patch('ggrebalance.configurationInterface.getConfigurationProvider'),
            patch('os.path.exists', return_value=Mock()),
            patch('ggrebalance.get_default_logger',
                  return_value=self.subject.logger),
        ])

        self.getConfigProviderFunctionMock = self.get_mock_from_apply_patch(
            "getConfigurationProvider")
        self.gpCoordinatorEnvironmentMock = self.get_mock_from_apply_patch(
            "GpCoordinatorEnvironment")
        self.previous_coordinator_data_directory = os.getenv(
            'COORDINATOR_DATA_DIRECTORY', '')
        os.environ["COORDINATOR_DATA_DIRECTORY"] = '/tmp/dirdoesnotexist'
        os.environ["GPHOME"] = '/tmp/dirdoesnotexist'
        configProviderMock = Mock(spec=GpConfigurationProvider)
        self.getConfigProviderFunctionMock.return_value = configProviderMock
        configProviderMock.initializeProvider.return_value = configProviderMock
        self.gpCoordinatorEnvironmentMock.return_value.getCoordinatorPort.return_value = 123456

    def tearDown(self):
        os.environ['COORDINATOR_DATA_DIRECTORY'] = self.previous_coordinator_data_directory
        sys.argv = self.old_sys_argv
        super(GpTestRebalanceValidation, self).tearDown()

    @patch('ggrebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_grouped_6"))
    @patch('os.path.exists', side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('gppylib.db.dbconn.queryRow', side_effect=check_query)
    @rebalance_only(numsegs = 6)
    def test_already_balanced_grouped(self, mockCatalog, mockOsPath, mockCursor):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced, no segment moves will be held.")
    
    @patch('ggrebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("seg_down"))
    @patch('os.path.exists', side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('gppylib.db.dbconn.queryRow', side_effect=check_query)
    @rebalance_only(numsegs = 4)
    def test_segment_down(self, mockCatalog, mockOsPath, mockCursor):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            "ggrebalance failed: Some segments in 'down' status. ggrebalance can't proceed further \n\nExiting...")

    @patch('ggrebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("balanced_spread_24"))
    @patch('os.path.exists', side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('gppylib.db.dbconn.queryRow', side_effect=check_query)
    @rebalance_only(numsegs = 24)
    def test_invalid_target_datadir(self, mockCatalog, mockOsPath, mockCursor):
        self.options.target_hosts = "sdw1, sdw2, sdw3"
        self.options.target_datadirs = '/data/primary/gpseg{content}, /data/mirror/gpseg<content>'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            'ggrebalance failed: --target_datadirs options should have format like '
            '"/data/primary/gpseg{content} , /data/mirror/gpseg{content}" \n\nExiting...')
    
    @patch('ggrebalance.GpArray.initFromCatalog',
           return_value=initGparrayFromFile("role_mismatch"))
    @patch('os.path.exists', side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('gppylib.db.dbconn.queryRow', side_effect=check_query)
    @rebalance_only(numsegs = 4)
    def test_role_mistmatch(self, mockCatalog, mockOsPath, mockCursor):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            "ggrebalance failed: Current role does not match preferred role for several segments. \n\nExiting...")
    

if __name__ == '__main__':
    run_tests()
