import os
import imp
import sys
import socket

from gppylib.test.unit.gp_unittest import *
from mock import *
from gppylib.db.dbconn import DbURL
from gppylib.gplog import *
from gppylib.system.configurationInterface import GpConfigurationProvider
from gppylib.system.environment import GpCoordinatorEnvironment

from gprebalance_modules.test.config import initGparrayFromFile
from gprebalance_modules.planner import Planner
from gprebalance_modules.rebalance_commons import Host, HostStatus, ValidationError

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

class TestRebalanceUtilityCLI(GpTestCase):
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
        super(TestRebalanceUtilityCLI, self).tearDown()

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
        self.options.target_datadirs = '/data/primary/gpseg{content}'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            'ggrebalance failed: --target-datadirs should have format: '
                '"/data/primary/gpseg{content}, /data/mirror/gpseg{content}". '
                'Available templated parameters: {hostname}, {content} \n\nExiting...')
    
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


class TestHostsOptions(GpTestCase):

    def setUp(self):
        self.options = Mock()
        self.options.target_hosts = None
        self.options.add_hosts = None
        self.options.remove_hosts = None
        self.options.target_datadirs = None
        self.options.target_hosts_file = None
        self.options.add_hosts_file = None
        self.options.remove_hosts_file = None
        self.options.target_datadirs_file = None

    def test_target_hosts_invalid_chars(self):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.target_hosts = "sdw1, sdw2, asfsa@"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("contains invalid characters", str(ctx.exception))
    
    def test_target_hosts_duplicates(self):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.target_hosts = "sdw1, sdw2, sdw2"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("Duplicate host", str(ctx.exception))
    
    def test_target_hosts_ip_and_name(self):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.target_hosts = "sdw1, sdw2, 192.168.0.15"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("must not contain IP adress and hostname simultaniously", str(ctx.exception))
    
    @patch('gprebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value='sdw1')
    def test_add_hosts_existing(self, MockResolver):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.add_hosts = "sdw1"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--add-hosts: Host 'sdw1' already exists in cluster as 'sdw1'", str(ctx.exception))
    
    @patch('gprebalance_modules.planner.HostResolver.resolve_ip', return_value='sdw1')
    @patch('gprebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value='sdw1')
    def test_add_hosts_existing_ip(self, MockResolver, mock_ip):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.add_hosts = "172.20.0.6"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--add-hosts: Host '172.20.0.6' already exists in cluster as 'sdw1'", str(ctx.exception))
    
    @patch('gprebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value=None)
    def test_remove_hosts_unexisting(self, MockResolver):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.remove_hosts = "sdw3"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--remove-hosts: Host 'sdw3' does not exist in cluster", str(ctx.exception))
    
    @patch('gprebalance_modules.planner.HostResolver.resolve_ip', return_value='sdw3')
    @patch('gprebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value=None)
    def test_remove_hosts_unexisting_ip(self, MockResolver, mock_ip):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.remove_hosts = "172.20.0.9"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--remove-hosts: Host '172.20.0.9' does not exist in cluster", str(ctx.exception))
    
    @patch('socket.getaddrinfo')
    def test_target_hosts_positive(self, mock_getaddrinfo):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.target_hosts = "sdw1, sdw2"

        mock_getaddrinfo.side_effect = [
            [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))],
            [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))]
        ]
        planner = Planner(logger=Mock(),
                          dburl=Mock(),
                          gpArray=gparray,
                          options=self.options)
        self.assertTrue(planner.target_hosts == [Host('sdw1', '192.168.1.10'), Host('sdw2', '192.168.1.11')])
    
    @patch('socket.getaddrinfo')
    def test_target_hosts_positive_ip(self, mock_getaddrinfo):
        gparray = initGparrayFromFile('unbalanced_9_ip')
        self.options.target_hosts = "sdw1, sdw2"
        mock_getaddrinfo.side_effect = [
            [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('172.20.0.6', 0))],
            [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('172.20.0.7', 0))],
            [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('172.20.0.8', 0))]
        ]
        planner = Planner(logger=Mock(),
                          dburl=Mock(),
                          gpArray=gparray,
                          options=self.options)
        self.assertTrue(planner.target_hosts == [Host('sdw1', '172.20.0.6'), Host('sdw3', '172.20.0.8'), Host('sdw2', '172.20.0.7')])
        self.assertTrue(planner.target_hosts[1].status == HostStatus.DECOMMISSIONED)
    
    @patch('gprebalance_modules.planner.HostResolver')
    def test_target_hosts_ip(self, mockHostResolver):
        gparray = initGparrayFromFile('unbalanced_9_ip')
        self.options.target_hosts = "172.20.0.7, 172.20.0.8, 172.20.0.9"
        mockHostResolver.return_value.resolve_hostname.side_effect = ['172.20.0.6', '172.20.0.7', '172.20.0.8', '172.20.0.9']
        mockHostResolver.return_value.resolve_ip.side_effect = ['sdw2', 'sdw3', 'sdw4']
        mockHostResolver.return_value.get_address.side_effect = ['172.20.0.6', '172.20.0.7', '172.20.0.8', '172.20.0.9']
        planner = Planner(logger=Mock(),
                          dburl=Mock(),
                          gpArray=gparray,
                          options=self.options)
        self.assertEqual(len(planner.target_hosts), 4)
        self.assertTrue(planner.target_hosts[3].status == HostStatus.NEW)
        self.assertTrue(planner.target_hosts[0].status == HostStatus.DECOMMISSIONED)

if __name__ == '__main__':
    run_tests()
