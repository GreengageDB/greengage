#
# Copyright (c) 2025-Present, Greengage Community
#

import os
import imp
import sys
import socket

from gppylib.test.unit.gp_unittest import *
from mock import *
from gppylib.db.dbconn import DbURL
from gppylib.gparray import Segment
from gppylib.gplog import *
from gppylib.system.configurationInterface import GpConfigurationProvider
from gppylib.system.environment import GpCoordinatorEnvironment

from ggrebalance_modules.test.config import initGparrayFromFile
from ggrebalance_modules.planner import ConfigurationEncoder, LogicalMove, Planner
from ggrebalance_modules.ggrebalance_sm import RebalanceSM
from ggrebalance_modules.rebalance_step import RebalanceStepMoveMirror
from ggrebalance_modules.rebalance_commons import Host, HostStatus, ValidationError

def rebalance_only(numsegs):
    def inner(func):
        def wrapper(self, *args, **kwargs):
            self.options.target_segment_count = numsegs
            ret = func(self, *args, **kwargs)
            self.options.target_segment_count = None
            return ret
        return wrapper
    return inner

def make_mock_cursor(rowcount=0, rows=None):
    cursor = MagicMock()
    cursor.rowcount = rowcount
    cursor.fetchone.return_value = rows[0] if rows else None
    cursor.__iter__ = Mock(return_value=iter(rows or []))
    return cursor

def mock_queryRow(conn, query):
    if "COUNT(1)" in query or "COUNT(*)" in query:
        return [0]
    return ["dummy"]

def mock_query(conn, query_string):
    return make_mock_cursor(rowcount=0)

class TestRebalanceUtilityCLI(GpTestCase):
    def setUp(self):
        ggrebalance_file = os.path.abspath(
            os.path.dirname(__file__) + "/../../ggrebalance")
        self.subject = imp.load_source('ggrebalance', ggrebalance_file)
        self.old_sys_argv = sys.argv
        sys.argv = []
        self.options, self.args, self.parser = self.subject.parseargs()

        self.subject.logger = Mock(
            spec=['log', 'warn', 'info', 'debug', 'error', 'warning', 'fatal', 'exception'])

        self.subject.check_running_gputils = Mock(return_value=False)
        self.subject.remove_pid_file = Mock()

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

    def test_zero_target_segment_count_is_rejected_before_connecting(self):
        options, args = self.parser.parse_args(
            ['--non-interactive-mode', '-x', '0', '--skip-rebalance'])
        with self.assertRaises(SystemExit) as exit_context:
            self.subject.main(options, args, self.parser)
        self.assertEqual(exit_context.exception.code, 1)
        self.subject.logger.error.assert_called_once_with(
            'Invalid value of --target-segment-count. It must be >= 1 and <= 4096')
        self.subject.dbconn.connect.assert_not_called()

    # The tests below set the options directly instead of parsing a command
    # line, because 'open' is mocked in setUp(), and optparse can't be
    # instantiated again. The attribute name is taken from the parser to check
    # that validate_options() looks at the attribute '--seed' is stored to.
    def set_seed(self, value):
        setattr(self.options, self.parser.get_option('--seed').dest, value)

    def check_seed_is_incompatible_with(self, option, seed=1):
        setattr(self.options, option, True)
        self.set_seed(seed)
        with self.assertRaises(SystemExit):
            self.subject.validate_options(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            f"Can't use together options '--{option.replace('_', '-')}' and '--seed'")

    def test_seed_is_incompatible_with_rollback(self):
        self.check_seed_is_incompatible_with('rollback_required')

    def test_seed_is_incompatible_with_clean(self):
        self.check_seed_is_incompatible_with('clean_required')

    # 0 is a valid seed, so it should be treated as a set option too
    def test_zero_seed_is_incompatible_with_rollback(self):
        self.check_seed_is_incompatible_with('rollback_required', seed=0)

    def test_zero_seed_is_incompatible_with_clean(self):
        self.check_seed_is_incompatible_with('clean_required', seed=0)

    def test_zero_seed_is_accepted(self):
        self.set_seed(0)
        self.subject.validate_options(self.options, self.args, self.parser)

    def test_negative_seed_is_rejected(self):
        self.set_seed(-1)
        with self.assertRaises(SystemExit):
            self.subject.validate_options(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call('Invalid value of --seed. Must be >= 0')

    @patch('gppylib.db.dbconn.query', side_effect=mock_query)
    @patch('gppylib.db.dbconn.execSQL')
    @patch('gppylib.db.dbconn.connect', return_value=MagicMock())
    @patch('gppylib.db.dbconn.queryRow', side_effect=mock_queryRow)
    @patch('ggrebalance.GpArray.initFromCatalog',
            return_value=initGparrayFromFile("balanced_grouped_6"))
    @patch('os.path.exists',
            side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @rebalance_only(numsegs=6)
    def test_already_balanced_grouped(self,
                                      mockOsPath,
                                      mockArray,
                                      mockQueryRow,
                                      mockConnect,
                                      mockExecSQL,
                                      mockQuery):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.info.assert_any_call(
            "Cluster is already balanced, no segment moves will be held.")
    
    @patch('gppylib.db.dbconn.execSQL')
    @patch('gppylib.db.dbconn.connect', return_value=MagicMock())
    @patch('gppylib.db.dbconn.queryRow', side_effect=lambda conn, query: [1])
    @patch('os.path.exists',
            side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @rebalance_only(numsegs=6)
    def test_segment_down(self,
                          mockOsPath,
                          mockQueryRow,
                          mockConnect,
                          mockExecSQL):
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            "ggrebalance failed: Detected some primary segments are down, please recover manually \n\nExiting...")

    @patch('gppylib.db.dbconn.query',    side_effect=mock_query)
    @patch('gppylib.db.dbconn.execSQL')
    @patch('gppylib.db.dbconn.connect',  return_value=MagicMock())
    @patch('gppylib.db.dbconn.queryRow', side_effect=mock_queryRow)
    @patch('ggrebalance.check_running_gputils')
    @patch('ggrebalance.GpArray.initFromCatalog',
            return_value=initGparrayFromFile("balanced_spread_24"))
    @patch('os.path.exists',
            side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('ggrebalance.check_down_segments')
    @rebalance_only(numsegs=24)
    def test_invalid_target_datadir(self,
                                    mockCheckDown,
                                    mockOsPath,
                                    mockArray,
                                    mockCheckRunning,
                                    mockQueryRow,
                                    mockConnect,
                                    mockExecSQL,
                                    mockQuery):
        self.options.target_hosts   = "sdw1, sdw2, sdw3"
        self.options.target_datadirs = '/data/primary/gpseg{content}'
        with self.assertRaises(SystemExit):
            self.subject.main(self.options, self.args, self.parser)
        self.subject.logger.error.assert_any_call(
            'ggrebalance failed: --target-datadirs should have format: '
            '"/data/primary/gpseg{content}, /data/mirror/gpseg{content}". '
            'Available templated parameters: {hostname}, {content} \n\nExiting...')
    
    @patch('gppylib.db.dbconn.query',    side_effect=mock_query)
    @patch('gppylib.db.dbconn.execSQL')
    @patch('gppylib.db.dbconn.connect',  return_value=MagicMock())
    @patch('gppylib.db.dbconn.queryRow', side_effect=mock_queryRow)
    @patch('ggrebalance.check_running_gputils')
    @patch('ggrebalance.GpArray.initFromCatalog',
            return_value=initGparrayFromFile("role_mismatch"))
    @patch('os.path.exists',
            side_effect=lambda path: path not in ['/tmp/dirdoesnotexist/gparraydump'])
    @patch('ggrebalance.check_down_segments')
    @rebalance_only(numsegs=4)
    def test_role_mistmatch(self,
                            mockCheckDown,
                            mockOsPath,
                            mockArray,
                            mockCheckRunning,
                            mockQueryRow,
                            mockConnect,
                            mockExecSQL,
                            mockQuery):
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
    
    @patch('ggrebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value='sdw1')
    def test_add_hosts_existing(self, MockResolver):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.add_hosts = "sdw1"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--add-hosts: Host 'sdw1' already exists in cluster as 'sdw1'", str(ctx.exception))
    
    @patch('ggrebalance_modules.planner.HostResolver.resolve_ip', return_value='sdw1')
    @patch('ggrebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value='sdw1')
    def test_add_hosts_existing_ip(self, MockResolver, mock_ip):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.add_hosts = "172.20.0.6"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--add-hosts: Host '172.20.0.6' already exists in cluster as 'sdw1'", str(ctx.exception))
    
    @patch('ggrebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value=None)
    def test_remove_hosts_unexisting(self, MockResolver):
        gparray = initGparrayFromFile('balanced_grouped_6')
        self.options.remove_hosts = "sdw3"
        with self.assertRaises(ValidationError) as ctx:
            planner = Planner(logger=Mock(),
                            dburl=Mock(),
                            gpArray=gparray,
                            options=self.options)
        self.assertIn("--remove-hosts: Host 'sdw3' does not exist in cluster", str(ctx.exception))
    
    @patch('ggrebalance_modules.planner.HostResolver.resolve_ip', return_value='sdw3')
    @patch('ggrebalance_modules.rebalance_commons.HostResolver.find_matching_hostname', return_value=None)
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
        self.assertTrue(planner.target_hosts == [Host('sdw1', 'sdw1'), Host('sdw2', 'sdw2')])
    
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
    
    @patch('ggrebalance_modules.planner.HostResolver')
    def test_target_hosts_ip(self, mockHostResolver):
        gparray = initGparrayFromFile('unbalanced_9_ip')
        self.options.target_hosts = "172.20.0.7, 172.20.0.8, 172.20.0.9"
        mockHostResolver.return_value.resolve_hostname.side_effect = ['172.20.0.6', '172.20.0.7', '172.20.0.8', '172.20.0.9']
        mockHostResolver.return_value.resolve_ip.side_effect = ['sdw2', 'sdw3', 'sdw4']
        mockHostResolver.return_value.find_matching_hostname.return_value = None
        mockHostResolver.return_value.get_address.side_effect = ['172.20.0.6', '172.20.0.7', '172.20.0.8', '172.20.0.9']
        planner = Planner(logger=Mock(),
                          dburl=Mock(),
                          gpArray=gparray,
                          options=self.options)
        self.assertEqual(len(planner.target_hosts), 4)
        self.assertTrue(planner.target_hosts[3].status == HostStatus.NEW)
        self.assertTrue(planner.target_hosts[0].status == HostStatus.DECOMMISSIONED)

    @patch('ggrebalance_modules.planner.HostResolver.resolve_hostname')
    @patch('ggrebalance_modules.planner.LNS.solve')
    def test_multiple_addresses_only_rejected_for_move_destination(self, solve, resolve_hostname):
        array = initGparrayFromFile('balanced_grouped_6')
        array.segmentPairs[0].primaryDB.setSegmentAddress('sdw1-2')
        self.options.target_hosts = 'sdw1,sdw3'
        self.options.seed = 1
        self.options.skip_resource_estimation = True
        planner = Planner(Mock(), Mock(), array, self.options)

        # Replace sdw2 with sdw3, keeping all segments on sdw1 in place.
        solve.return_value = ([(0, 1)] * 3 + [(1, 0)] * 3, None)
        moves = planner.form_moves()
        self.assertEqual([(m.srcHost.hostname, m.dstHost.hostname) for m in moves],
                         [('sdw2', 'sdw3')] * 6)

        # Reverse the placement so moves now land on sdw1.
        solve.return_value = ([(1, 0)] * 3 + [(0, 1)] * 3, None)
        with self.assertRaisesRegex(ValidationError, 'sdw1.*sdw1, sdw1-2'):
            planner.form_moves()

    # 'balanced_grouped_6' cluster consists of 'cdw', 'sdw1' and 'sdw2'. The names
    # ending with '.example.com' are the other names of the same machines, and
    # 'sdw3.example.com' is a machine, which is not a part of the cluster.
    HOST_ADDRESSES = {
        'cdw': '172.20.0.5',
        'sdw1': '172.20.0.6',
        'sdw1.example.com': '172.20.0.6',
        'sdw2': '172.20.0.7',
        'sdw2.example.com': '172.20.0.7',
        'sdw3.example.com': '172.20.0.8',
    }

    @staticmethod
    def fake_getaddrinfo(hostname, *args):
        if hostname not in TestHostsOptions.HOST_ADDRESSES:
            raise socket.gaierror(f'Name or service not known: {hostname}')
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (TestHostsOptions.HOST_ADDRESSES[hostname], 0))]

    def plan_hosts(self) -> Planner:
        with patch('socket.getaddrinfo', side_effect=TestHostsOptions.fake_getaddrinfo):
            return Planner(logger=Mock(),
                           dburl=Mock(),
                           gpArray=initGparrayFromFile('balanced_grouped_6'),
                           options=self.options)

    def test_remove_hosts_alias(self):
        self.options.remove_hosts = "sdw2.example.com"
        planner = self.plan_hosts()
        statuses = {host.hostname: host.status for host in planner.target_hosts}
        self.assertEqual(statuses, {'sdw1': HostStatus.ACTIVE, 'sdw2': HostStatus.DECOMMISSIONED})
        self.assertTrue(planner.host_set_changed)

    def test_target_hosts_alias(self):
        self.options.target_hosts = "sdw1, sdw2.example.com"
        planner = self.plan_hosts()
        statuses = {host.hostname: host.status for host in planner.target_hosts}
        self.assertEqual(statuses, {'sdw1': HostStatus.ACTIVE, 'sdw2': HostStatus.ACTIVE})
        self.assertFalse(planner.host_set_changed)

    def test_target_hosts_alias_and_new_host(self):
        self.options.target_hosts = "sdw1.example.com, sdw3.example.com"
        planner = self.plan_hosts()
        statuses = {host.hostname: host.status for host in planner.target_hosts}
        self.assertEqual(statuses, {'sdw1': HostStatus.ACTIVE,
                                    'sdw2': HostStatus.DECOMMISSIONED,
                                    'sdw3.example.com': HostStatus.NEW})
        self.assertTrue(planner.host_set_changed)

    def test_target_hosts_same_host_twice(self):
        self.options.target_hosts = "sdw1, sdw2, sdw2.example.com"
        with self.assertRaises(ValidationError) as ctx:
            self.plan_hosts()
        self.assertIn("--target-hosts: Hosts 'sdw2' and 'sdw2.example.com' refer to the same host 'sdw2'", str(ctx.exception))

    def test_remove_hosts_same_host_twice(self):
        self.options.remove_hosts = "sdw2.example.com, sdw2"
        with self.assertRaises(ValidationError) as ctx:
            self.plan_hosts()
        self.assertIn("--remove-hosts: Hosts 'sdw2.example.com' and 'sdw2' refer to the same host 'sdw2'", str(ctx.exception))

    def test_add_hosts_alias(self):
        self.options.add_hosts = "sdw2.example.com"
        with self.assertRaises(ValidationError) as ctx:
            self.plan_hosts()
        self.assertIn("--add-hosts: Host 'sdw2.example.com' already exists in cluster as 'sdw2'", str(ctx.exception))


class TestDestinationAddresses(GpTestCase):
    def setUp(self):
        self.array = initGparrayFromFile('balanced_grouped_6')
        self.resolver = Mock()
        self.resolver.get_address.return_value = '192.168.1.30'
        for segment in self.array.getDbList():
            if segment.getSegmentHostName() == 'sdw1':
                segment.setSegmentAddress('10.20.0.30')

    def test_existing_host_uses_catalog_address_and_encodes(self):
        hosts, changed = Planner.get_target_hosts(self.array, resolver=self.resolver)
        self.assertFalse(changed)
        self.assertEqual('10.20.0.30', next(h.address for h in hosts if h.hostname == 'sdw1'))
        self.resolver.get_address.assert_not_called()
        config, mapping = ConfigurationEncoder.encode_configuration(
            self.array, hosts, 'grouped', self.resolver)
        self.assertEqual(len(hosts), len(mapping))
        host_ids = {host.hostname: host_id for host, host_id in mapping.items()}
        self.assertEqual([host_ids['sdw1']] * 3 + [host_ids['sdw2']] * 3,
                         config.initial_primary_mapping)
        self.assertEqual([host_ids['sdw2']] * 3 + [host_ids['sdw1']] * 3,
                         config.initial_mirror_mapping)
        self.resolver.get_address.assert_not_called()

    def test_intermediate_host_skips_multiple_addresses(self):
        self.array.segmentPairs[0].primaryDB.setSegmentAddress('10.21.0.30')
        planner = Mock()
        planner.target_hosts, _ = Planner.get_target_hosts(
            self.array, add_hostname_list=['sdw3', 'sdw4'], resolver=self.resolver)
        primary_move, mirror_move = Mock(), Mock(segment_size=None)
        primary_move.dstHost.hostname = 'sdw2'
        mirror_move.dstHost.hostname = 'sdw3'
        host = Planner.select_intermediate_host(planner, primary_move, mirror_move, {})
        self.assertEqual(host.hostname, 'sdw4')

    def test_multiple_addresses_on_removed_host_are_allowed(self):
        self.array.segmentPairs[0].primaryDB.setSegmentAddress('10.21.0.30')
        hosts, changed = Planner.get_target_hosts(
            self.array, remove_hostname_list=['sdw1'], resolver=self.resolver)
        self.assertTrue(changed)
        self.assertEqual(HostStatus.DECOMMISSIONED,
                         next(h.status for h in hosts if h.hostname == 'sdw1'))
        ConfigurationEncoder.encode_configuration(self.array, hosts, 'grouped', self.resolver)

    def test_new_host_uses_dns(self):
        hosts, changed = Planner.get_target_hosts(
            self.array, add_hostname_list=['sdw3'], resolver=self.resolver)
        self.assertTrue(changed)
        self.assertEqual('192.168.1.30', next(h.address for h in hosts if h.hostname == 'sdw3'))
        self.resolver.get_address.assert_called_once_with('sdw3')

    def test_new_host_keeps_hostname_on_hostname_based_cluster(self):
        array = initGparrayFromFile('balanced_grouped_6')
        for options in ({'add_hostname_list': ['sdw3']},
                        {'target_hostname_list': ['sdw1', 'sdw2', 'sdw3']}):
            with self.subTest(options=options):
                hosts, _ = Planner.get_target_hosts(array, resolver=self.resolver, **options)
                self.assertEqual('sdw3', next(h.address for h in hosts if h.hostname == 'sdw3'))


class TestMirrorMoveConfig(GpTestCase):
    def setUp(self):
        self.subject = RebalanceSM(Mock(), Mock(), Mock(), Mock(), Mock())
        self.segment = Segment.initFromString(
            '7|2|m|m|s|u|sdw3|172.19.0.5|21500|/data/mirror/gpseg2')
        self.move = LogicalMove(
            seg=self.segment,
            srcHost=Host('sdw3', '172.19.0.5'),
            dstHost=Host('sdw1', '172.19.0.3'),
            target_port=21501,
            target_datadir='/new/mirror/gpseg2')
        self.step = RebalanceStepMoveMirror(self.move)
        self.catalog = Mock()
        self.catalog.getDbList.return_value = [self.segment]

    def assert_config(self, expected):
        with patch('ggrebalance_modules.ggrebalance_sm.GpArray.initFromCatalog',
                   return_value=self.catalog), \
             patch('ggrebalance_modules.ggrebalance_sm.open', mock_open()) as output:
            self.subject.create_config_file([self.step])
        output().write.assert_called_once_with(expected)

    def update_catalog(self):
        self.catalog.getDbList.return_value = [Segment.initFromString(
            '7|2|m|m|s|u|sdw1|172.19.0.3|21501|/new/mirror/gpseg2')]

    def test_normal_move_uses_addresses(self):
        self.assert_config('sdw3|172.19.0.5|21500|/data/mirror/gpseg2 '
                           'sdw1|172.19.0.3|21501|/new/mirror/gpseg2\n')

    def test_source_address_comes_from_catalog_row_for_dbid(self):
        other = self.segment.copy()
        other.setSegmentDbId(8)
        current = self.segment.copy()
        current.setSegmentAddress('172.19.1.5')
        self.catalog.getDbList.return_value = [other, current]
        self.assert_config('sdw3|172.19.1.5|21500|/data/mirror/gpseg2 '
                           'sdw1|172.19.0.3|21501|/new/mirror/gpseg2\n')

    def test_unrelated_catalog_location_is_rejected(self):
        current = self.segment.copy()
        current.setSegmentDataDirectory('/other/mirror/gpseg2')
        self.catalog.getDbList.return_value = [current]
        with patch('ggrebalance_modules.ggrebalance_sm.GpArray.initFromCatalog', return_value=self.catalog), \
             patch('ggrebalance_modules.ggrebalance_sm.open', mock_open()) as output:
            with self.assertRaisesRegex(ValidationError, 'Unexpected catalog location for dbid 7'):
                self.subject.create_config_file([self.step])
        output().write.assert_not_called()

    def test_partial_config_removed_on_validation_failure(self):
        unexpected = self.segment.copy()
        unexpected.setSegmentDbId(8)
        second_move = LogicalMove(seg=unexpected, srcHost=self.move.srcHost,
                                  dstHost=self.move.dstHost, target_port=self.move.target_port,
                                  target_datadir=self.move.target_datadir)
        changed = unexpected.copy()
        changed.setSegmentDataDirectory('/unexpected')
        self.catalog.getDbList.return_value = [self.segment, changed]
        filename = f'/tmp/ggrebalance_move_config_pid{os.getpid()}'
        with patch('ggrebalance_modules.ggrebalance_sm.GpArray.initFromCatalog', return_value=self.catalog):
            with self.assertRaisesRegex(ValidationError, 'Unexpected catalog location for dbid 8'):
                self.subject.create_config_file([self.step, RebalanceStepMoveMirror(second_move)])
        self.assertFalse(os.path.exists(filename))

    def test_retry_after_catalog_update_uses_destination_address(self):
        self.update_catalog()
        self.assert_config('sdw1|172.19.0.3|21501|/new/mirror/gpseg2 '
                           'sdw1|172.19.0.3|21501|/new/mirror/gpseg2\n')

    def test_rollback_after_catalog_update_uses_original_address(self):
        self.update_catalog()
        self.step.setStatus(self.step.Status.PLANNED, rollback=True)
        self.assert_config('sdw1|172.19.0.3|21501|/new/mirror/gpseg2 '
                           'sdw3|172.19.0.5|21500|/data/mirror/gpseg2\n')

    def test_pre_upgrade_retry_uses_catalog_hostname_as_address(self):
        self.update_catalog()
        self.catalog.getDbList.return_value[0].setSegmentAddress('sdw1')
        self.assert_config('sdw1|sdw1|21501|/new/mirror/gpseg2 '
                           'sdw1|sdw1|21501|/new/mirror/gpseg2\n')

    def test_pre_upgrade_rollback_uses_catalog_hostname_as_address(self):
        self.update_catalog()
        self.catalog.getDbList.return_value[0].setSegmentAddress('sdw1')
        self.step.setStatus(self.step.Status.PLANNED, rollback=True)
        self.assert_config('sdw1|sdw1|21501|/new/mirror/gpseg2 '
                           'sdw3|172.19.0.5|21500|/data/mirror/gpseg2\n')

    def test_rollback_before_catalog_update_uses_original_address(self):
        self.step.setStatus(self.step.Status.PLANNED, rollback=True)
        self.assert_config('sdw3|172.19.0.5|21500|/data/mirror/gpseg2 '
                           'sdw3|172.19.0.5|21500|/data/mirror/gpseg2\n')

    def test_hostname_equals_address(self):
        self.segment.setSegmentAddress('sdw3')
        self.move.dstHost.address = 'sdw1'
        self.assert_config('sdw3|sdw3|21500|/data/mirror/gpseg2 '
                           'sdw1|sdw1|21501|/new/mirror/gpseg2\n')


if __name__ == '__main__':
    run_tests()
