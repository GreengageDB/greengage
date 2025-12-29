from gppylib.test.unit.gp_unittest import *
from mock import *

from gprebalance_modules.planner import ResourceEstimator, ResourceError, LogicalMove, Planner, PortAllocator, PlanningError
from gppylib.db.dbconn import DbURL
from gppylib import gparray
from gprebalance_modules.test.config import initGparrayFromFile
from gprebalance_modules.rebalance_commons import (
    SegmentSize, 
    DiskSpaceChecker, 
    DiskSpaceInfo,
    Host,
    HostStatus
)

def check_query(conn, query):
    if  "SELECT COUNT(1) FROM pg_namespace WHERE nspname =" in query:
        return [0]
    return None

class TestDiskSpaceChecker(GpTestCase):
    """Test cases for DiskSpaceChecker"""
    
    def setUp(self):
        self.logger = Mock()
        self.checker = DiskSpaceChecker(self.logger, batch_size=4)
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskUsage')
    def test_get_disk_usage_success(self, mock_disk_usage_class, mock_pool_class):
        """Test successful disk usage retrieval"""
        # Setup mock pool
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Create mock commands
        cmd1 = Mock()
        cmd1.was_successful.return_value = True
        cmd1.directory = '/data1/primary/gpseg0'
        cmd1.kbytes_used.return_value = 1048576
        
        cmd2 = Mock()
        cmd2.was_successful.return_value = True
        cmd2.directory = '/data1/primary/gpseg1'
        cmd2.kbytes_used.return_value = 2097152
        
        mock_pool.getCompletedItems.return_value = [cmd1, cmd2]
        
        # Execute
        directories = ['/data1/primary/gpseg0', '/data1/primary/gpseg1']
        result = self.checker.get_disk_usage('sdw1', directories)
        
        # Verify results
        self.assertEqual(len(result), 2)
        self.assertEqual(result['/data1/primary/gpseg0'], 1048576)
        self.assertEqual(result['/data1/primary/gpseg1'], 2097152)
        
        # Verify pool usage
        self.assertEqual(mock_pool.addCommand.call_count, 2)
        mock_pool.join.assert_called_once()
        mock_pool.haltWork.assert_called_once()
        mock_pool.joinWorkers.assert_called_once()
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    def test_get_disk_usage_empty_list(self, mock_pool_class):
        """Test disk usage with empty directory list"""
        result = self.checker.get_disk_usage('sdw1', [])
        
        self.assertEqual(result, {})
        mock_pool_class.assert_not_called()
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskUsage')
    def test_get_disk_usage_command_failure(self, mock_disk_usage_class, mock_pool_class):
        """Test disk usage command failure"""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        cmd = Mock()
        cmd.was_successful.return_value = False
        cmd.get_results.return_value.stderr = "Permission denied"
        
        mock_pool.getCompletedItems.return_value = [cmd]
        
        with self.assertRaises(Exception) as context:
            self.checker.get_disk_usage('sdw1', ['/data1/primary/gpseg0'])
        
        self.assertIn("Unable to check disk usage", str(context.exception))
    
    @patch('gprebalance_modules.rebalance_commons.WorkerPool')
    @patch('gprebalance_modules.rebalance_commons.DiskFree')
    @patch('gprebalance_modules.rebalance_commons.pickle')
    @patch('gprebalance_modules.rebalance_commons.base64')
    def test_get_available_space_success(self, mock_base64, mock_pickle, 
                                         mock_disk_free_class, mock_pool_class):
        """Test successful available space retrieval"""
        from gppylib.operations.validate_disk_space import FileSystem
        
        # Setup mock pool
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        # Mock FileSystem objects
        fs1 = Mock(spec=FileSystem)
        fs1.name = '/dev/sdb1'
        fs1.disk_free = 10485760
        fs1.directories = ['/data1/primary/gpseg0']
        
        fs2 = Mock(spec=FileSystem)
        fs2.name = '/dev/sdb1'
        fs2.disk_free = 10485760
        fs2.directories = ['/data1/primary/gpseg1']
        
        mock_pickle.loads.return_value = [fs1, fs2]
        mock_base64.urlsafe_b64decode.return_value = b'pickled_data'
        
        cmd = Mock()
        cmd.was_successful.return_value = True
        cmd.get_results.return_value.stdout = 'encoded_data'
        
        mock_pool.getCompletedItems.return_value = [cmd]
        
        # Execute
        directories = ['/data1/primary/gpseg0', '/data1/primary/gpseg1']
        result = self.checker.get_available_space('sdw1', directories)
        
        # Verify
        self.assertEqual(len(result), 2)
        self.assertIn('/data1/primary/gpseg0', result)
        self.assertIn('/data1/primary/gpseg1', result)
        
        self.assertEqual(result['/data1/primary/gpseg0'].filesystem, '/dev/sdb1')
        self.assertEqual(result['/data1/primary/gpseg0'].available_kb, 10485760)
        self.assertEqual(result['/data1/primary/gpseg0'].available_gb, 10.0)
    
    @patch('gprebalance_modules.rebalance_commons.DiskFree')
    def test_get_available_space_command_failure(self, mock_disk_free_class):
        """Test available space command failure"""
        
        cmd = Mock()
        mock_disk_free_class.return_value = cmd
        cmd.was_successful.return_value = False
        cmd.get_results.return_value.stderr = "No such file or directory"
                
        with self.assertRaises(Exception) as context:
            self.checker.get_available_space('sdw1', ['/data1/primary/gpseg0'])
        
        self.assertIn("Failed to check disk free", str(context.exception))
    
class TestResourceEstimator(GpTestCase):
    """Test cases for ResourceEstimator using real GpArray configuration"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.logger = Mock()
        self.logger.info = Mock()
        self.logger.debug = Mock()
        self.logger.warning = Mock()
        self.logger.error = Mock()
        
        self.conn = Mock()
        self.dburl = Mock(spec=DbURL)
        
        # Load real GpArray configuration
        self.gparray = initGparrayFromFile('unbalanced_9_ip')
        
        # Options for planner
        self.options = Mock()
        self.options.target_segment_count = 9
        self.options.target_hosts = None
        self.options.add_hosts = None
        self.options.remove_hosts = None
        self.options.target_datadirs = None
        self.options.target_hosts_file = None
        self.options.add_hosts_file = None
        self.options.remove_hosts_file = None
        self.options.target_datadirs_file = None
        self.options.mirror_mode = 'grouped'
        self.options.skip_rebalance = False
        self.options.skip_resource_estimation = False
        self.options.batch_size = 16
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_segment_sizes_from_unbalanced_cluster(self, mock_dbconn):
        """Test segment size estimation from unbalanced cluster"""
        # Create a realistic move: move primary seg0 from sdw1 to sdw2
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        self.assertIsNotNone(seg0)
        self.assertEqual(seg0.hostname, 'sdw1')
        self.assertEqual(seg0.address, '172.20.0.6')
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker to return segment size
        estimator.disk_checker.get_disk_usage = Mock(return_value={
            '/data/primary0': 2097152  # 2GB
        })
        
        # Mock tablespace query (no tablespaces)
        mock_dbconn.query.return_value = []
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify segment size was set
        self.assertIsNotNone(moves[0].segment_size)
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 2097152)
        self.assertEqual(moves[0].segment_size.total_size_kb, 2097152)
        
        # Verify disk usage was called with correct parameters
        estimator.disk_checker.get_disk_usage.assert_called_once_with(
            '172.20.0.6',
            ['/data/primary0']
        )
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_multiple_segments_same_host(self, mock_dbconn):
        """
        Test estimating multiple segments from same source host
        """
        # Get primaries from sdw1: seg0, seg1, seg2
        segs_from_sdw1 = []
        for seg in self.gparray.getSegDbList():
            if seg.getSegmentHostName() == 'sdw1' and seg.isSegmentPrimary():
                segs_from_sdw1.append(seg)
        
        self.assertEqual(len(segs_from_sdw1), 3)
        
        # Create moves for all three segments
        moves = [
            LogicalMove(
                seg=segs_from_sdw1[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            ),
            LogicalMove(
                seg=segs_from_sdw1[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=None
            ),
            LogicalMove(
                seg=segs_from_sdw1[2],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary2',
                target_port=7002,
                segment_size=None
            )
        ]
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker to return sizes for all segments
        estimator.disk_checker.get_disk_usage = Mock(return_value={
            '/data/primary0': 1048576,  # 1GB
            '/data/primary1': 2097152,  # 2GB
            '/data/primary2': 1572864   # 1.5GB
        })
        
        # Mock tablespace query
        mock_dbconn.query.return_value = []
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify all segment sizes were set
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 1048576)
        self.assertEqual(moves[1].segment_size.datadir_size_kb, 2097152)
        self.assertEqual(moves[2].segment_size.datadir_size_kb, 1572864)
        
        # Verify single call to get_disk_usage with all directories
        estimator.disk_checker.get_disk_usage.assert_called_once()
        call_args = estimator.disk_checker.get_disk_usage.call_args[0]
        self.assertEqual(call_args[0], '172.20.0.6')
        self.assertEqual(set(call_args[1]), {'/data/primary0', '/data/primary1', '/data/primary2'})
    
    @patch('gprebalance_modules.planner.dbconn')
    def test_estimate_with_tablespaces(self, mock_dbconn):
        # Get primary seg0 from sdw1
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=None
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker for datadir and tablespaces
        call_count = [0]
        def mock_disk_usage_side_effect(host, dirs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: datadir
                return {'/data/primary0': 2097152}  # 2GB
            else:
                # Second call: tablespaces
                return {
                    '/tablespace1/2': 524288,   # 512MB
                    '/tablespace2/2': 1048576   # 1GB
                }
        
        estimator.disk_checker.get_disk_usage = Mock(side_effect=mock_disk_usage_side_effect)
        
        # Mock tablespace query to return tablespace locations
        mock_dbconn.query.return_value = [
            (2, '/tablespace1/2'),
            (2, '/tablespace2/2')
        ]
        
        estimator._estimate_segment_sizes(moves)
        
        # Verify segment size includes tablespaces
        self.assertIsNotNone(moves[0].segment_size)
        self.assertEqual(moves[0].segment_size.datadir_size_kb, 2097152)
        self.assertIsNotNone(moves[0].segment_size.tablespace_usage)
        self.assertEqual(moves[0].segment_size.tablespace_usage['/tablespace1/2'], 524288)
        self.assertEqual(moves[0].segment_size.tablespace_usage['/tablespace2/2'], 1048576)
        
        # Total should be datadir + tablespaces
        expected_total = 2097152 + 524288 + 1048576
        self.assertEqual(moves[0].segment_size.total_size_kb, expected_total)
    
    def test_validate_target_space_sufficient(self):
        """Test validation passes when sufficient space is available"""
        # Get primary seg0
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=2097152)  # 2GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker - 20GB available
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                )
            }
        })
        
        # Should not raise exception
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
        
        # Verify disk space check was called
        estimator.disk_checker.check_batch_available_space.assert_called_once()
        call_args = estimator.disk_checker.check_batch_available_space.call_args[0][0]
        self.assertIn('172.20.0.7', call_args)
        self.assertIn('/data/primary0', call_args['172.20.0.7'])
    
    def test_validate_target_space_insufficient(self):
        """Test validation fails when insufficient space for datadir"""
        # Get primary seg0
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=10485760)  # 10GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock disk checker - only 2GB available
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=2097152,  # 2GB available
                    directory='/data/primary0'
                )
            }
        })
        
        # Should raise ResourceError
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        self.assertIn("sdw2", error_msg)
        self.assertIn("/data/primary0", error_msg)
    
    def test_validate_multiple_moves_same_filesystem_insufficient(self):
        """Test validation aggregates space requirements on same filesystem"""
        # Get primaries from sdw1
        segs_from_sdw1 = []
        for seg in self.gparray.getDbList():
            if seg.hostname == 'sdw1' and seg.isSegmentPrimary() and seg.content < 2:
                segs_from_sdw1.append(seg)
        
        moves = [
            LogicalMove(
                seg=segs_from_sdw1[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            ),
            LogicalMove(
                seg=segs_from_sdw1[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Total needed: (5GB + 5GB) * 1.1 = 11GB
        # Available: only 8GB (insufficient)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=8388608,  # 8GB
                    directory='/data/primary0'
                ),
                '/data/primary1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem
                    available_kb=8388608,  # 8GB (same)
                    directory='/data/primary1'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        # Both directories should be mentioned
        self.assertIn("/data/primary0", error_msg)
        self.assertIn("/data/primary1", error_msg)
        # Should show aggregated requirement (~11GB) vs available (8GB)
        self.assertIn("11", error_msg)
        self.assertIn("8", error_msg)
    
    def test_validate_multiple_moves_same_filesystem_sufficient(self):
        """Test validation passes when aggregated space is sufficient"""
        # Get primaries from sdw1
        segs_from_sdw1 = []
        for seg in self.gparray.getDbList():
            if seg.hostname == 'sdw1' and seg.isSegmentPrimary() and seg.content < 2:
                segs_from_sdw1.append(seg)
        
        moves = [
            LogicalMove(
                seg=segs_from_sdw1[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            ),
            LogicalMove(
                seg=segs_from_sdw1[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Total needed: (5GB + 5GB) * 1.1 = 11GB
        # Available: 15GB (sufficient)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=15728640,  # 15GB
                    directory='/data/primary0'
                ),
                '/data/primary1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem
                    available_kb=15728640,  # 15GB (same)
                    directory='/data/primary1'
                )
            }
        })
        
        # Should not raise
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
    
    def test_validate_target_space_no_space_info(self):
        """Test validation fails when no space info is available"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=2097152)
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Return empty space info
        estimator.disk_checker.check_batch_available_space = Mock(return_value={})
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        self.assertIn("No disk space information for host sdw2", str(context.exception))
    
    def test_validate_tablespace_space_sufficient(self):
        """Test validation succeeds when tablespace has sufficient space on separate filesystem"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=2097152,  # 2GB
                    tablespace_usage={'/tablespace1/2': 1048576}  # 1GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock sufficient space for both datadir and tablespace on different filesystems
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                ),
                '/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdc1',  # Different filesystem
                    available_kb=10485760,  # 10GB
                    directory='/tablespace1'
                )
            }
        })
        
        # Should not raise exception
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
    
    def test_validate_tablespace_space_insufficient(self):
        """Test validation fails when tablespace has insufficient space"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=2097152,  # 2GB
                    tablespace_usage={'/tablespace1/2': 10485760}  # 10GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock sufficient space for datadir but insufficient for tablespace
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                ),
                '/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdc1',
                    available_kb=1048576,  # Only 1GB available
                    directory='/tablespace1'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        self.assertIn("/tablespace1", error_msg)
    
    def test_validate_multiple_tablespaces_different_filesystems(self):
        """Test validation with multiple tablespaces on different filesystems"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=2097152,  # 2GB
                    tablespace_usage={
                        '/tablespace1/2': 3145728,  # 3GB
                        '/tablespace2/2': 4194304   # 4GB
                    }
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Mock space - datadir OK, tablespace1 OK, tablespace2 insufficient
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                ),
                '/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdc1',
                    available_kb=10485760,  # 10GB - sufficient
                    directory='/tablespace1'
                ),
                '/tablespace2': DiskSpaceInfo(
                    filesystem='/dev/sdd1',
                    available_kb=2097152,  # Only 2GB - insufficient
                    directory='/tablespace2'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        # Only tablespace2 should be in error
        self.assertIn("/tablespace2", error_msg)
    
    def test_validate_tablespace_same_filesystem_as_datadir_insufficient(self):
        """Test when tablespace and datadir share the same filesystem - insufficient space"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=5242880,  # 5GB
                    tablespace_usage={'/data/tablespace1/2': 5242880}  # 5GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Both datadir and tablespace on same filesystem /dev/sdb1
        # Total needed: (5GB + 5GB) * 1.1 = 11GB
        # Available: only 8GB (insufficient)
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=8388608,  # 8GB
                    directory='/data/primary0'
                ),
                '/data/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem!
                    available_kb=8388608,  # 8GB (same as above - shared filesystem!)
                    directory='/data/tablespace1'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        # Should show BOTH directories since they're on the same filesystem
        self.assertIn("/data/primary0", error_msg)
        self.assertIn("/data/tablespace1", error_msg)
        # Should show it needs ~11GB but only has 8GB
        self.assertIn("11", error_msg)
        self.assertIn("8", error_msg)
    
    def test_validate_tablespace_same_filesystem_as_datadir_sufficient(self):
        """Test when tablespace and datadir share filesystem with sufficient space"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=5242880,  # 5GB
                    tablespace_usage={'/data/tablespace1/2': 5242880}  # 5GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Both on same filesystem, need 11GB, have 20GB - should pass
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=20971520,  # 20GB
                    directory='/data/primary0'
                ),
                '/data/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem
                    available_kb=20971520,  # 20GB (same)
                    directory='/data/tablespace1'
                )
            }
        })
        
        # Should not raise exception
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
    
    def test_validate_complex_overlapping_filesystems(self):
        """Test multiple segments with overlapping filesystem usage"""
        segs = [seg for seg in self.gparray.getDbList() 
                if seg.isSegmentPrimary() and seg.content < 2]
        
        moves = [
            # Seg 0: datadir on /dev/sdb1, tablespace on /dev/sdc1
            LogicalMove(
                seg=segs[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=3145728,  # 3GB
                    tablespace_usage={'/tablespace1/2': 2097152}  # 2GB
                )
            ),
            # Seg 1: datadir on /dev/sdb1 (SAME as seg0 datadir), 
            #        tablespace on /dev/sdb1 (SAME filesystem!)
            LogicalMove(
                seg=segs[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=SegmentSize(
                    datadir_size_kb=4194304,  # 4GB
                    tablespace_usage={'/data/tblspace2/3': 3145728}  # 3GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # /dev/sdb1: needs (3GB + 4GB + 3GB) * 1.1 = 11GB, has 12GB - PASS
        # /dev/sdc1: needs 2GB * 1.1 = 2.2GB, has 5GB - PASS
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=12582912,  # 12GB
                    directory='/data/primary0'
                ),
                '/data/primary1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same as primary0
                    available_kb=12582912,
                    directory='/data/primary1'
                ),
                '/data/tblspace2': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem!
                    available_kb=12582912,
                    directory='/data/tblspace2'
                ),
                '/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdc1',  # Different filesystem
                    available_kb=5242880,  # 5GB
                    directory='/tablespace1'
                )
            }
        })
        
        # Should not raise
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised when space is sufficient")
    
    def test_validate_complex_overlapping_filesystems_insufficient(self):
        """Test multiple segments with overlapping filesystems - insufficient space"""
        segs = [seg for seg in self.gparray.getDbList() 
                if seg.isSegmentPrimary() and seg.content < 2]
        
        moves = [
            # Seg 0: datadir on /dev/sdb1, tablespace on /dev/sdc1
            LogicalMove(
                seg=segs[0],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(
                    datadir_size_kb=3145728,  # 3GB
                    tablespace_usage={'/tablespace1/2': 2097152}  # 2GB
                )
            ),
            # Seg 1: datadir on /dev/sdb1, tablespace on /dev/sdb1 (SAME filesystem!)
            LogicalMove(
                seg=segs[1],
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary1',
                target_port=7001,
                segment_size=SegmentSize(
                    datadir_size_kb=4194304,  # 4GB
                    tablespace_usage={'/data/tblspace2/3': 3145728}  # 3GB
                )
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # /dev/sdb1: needs (3GB + 4GB + 3GB) * 1.1 = 11GB, has only 9GB - FAIL
        # /dev/sdc1: needs 2GB * 1.1 = 2.2GB, has 5GB - PASS
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=9437184,  # 9GB - insufficient!
                    directory='/data/primary0'
                ),
                '/data/primary1': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same as primary0
                    available_kb=9437184,
                    directory='/data/primary1'
                ),
                '/data/tblspace2': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem!
                    available_kb=9437184,
                    directory='/data/tblspace2'
                ),
                '/tablespace1': DiskSpaceInfo(
                    filesystem='/dev/sdc1',  # Different filesystem
                    available_kb=5242880,  # 5GB - sufficient
                    directory='/tablespace1'
                )
            }
        })
        
        with self.assertRaises(ResourceError) as context:
            estimator._validate_target_space(moves)
        
        error_msg = str(context.exception)
        self.assertIn("Insufficient disk space", error_msg)
        # Should show all three directories on /dev/sdb1
        self.assertIn("/data/primary0", error_msg)
        self.assertIn("/data/primary1", error_msg)
        self.assertIn("/data/tblspace2", error_msg)
        # Should show aggregated requirement (~11GB) vs available (9GB)
        self.assertIn("11", error_msg)
        self.assertIn("9", error_msg)
    
    def test_validate_no_double_counting_same_segment(self):
        """Test that the same segment is not double-counted on same filesystem"""
        seg0 = None
        for seg in self.gparray.getDbList():
            if seg.content == 0 and seg.isSegmentPrimary():
                seg0 = seg
                break
        
        # Create two moves for the same segment (shouldn't happen in reality, 
        # but tests deduplication logic)
        moves = [
            LogicalMove(
                seg=seg0,
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0',
                target_port=7000,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            ),
            LogicalMove(
                seg=seg0,  # Same segment!
                srcHost=Host('sdw1', '172.20.0.6', status=HostStatus.ACTIVE),
                dstHost=Host('sdw2', '172.20.0.7', status=HostStatus.ACTIVE),
                target_datadir='/data/primary0_copy',
                target_port=7001,
                segment_size=SegmentSize(datadir_size_kb=5242880)  # 5GB
            )
        ]
        
        estimator = ResourceEstimator(self.logger, self.conn, self.gparray)
        
        # Both on same filesystem
        # Should only count once: 5GB * 1.1 = 5.5GB, not 11GB
        estimator.disk_checker.check_batch_available_space = Mock(return_value={
            '172.20.0.7': {
                '/data/primary0': DiskSpaceInfo(
                    filesystem='/dev/sdb1',
                    available_kb=6291456,  # 6GB
                    directory='/data/primary0'
                ),
                '/data/primary0_copy': DiskSpaceInfo(
                    filesystem='/dev/sdb1',  # Same filesystem
                    available_kb=6291456,
                    directory='/data/primary0_copy'
                )
            }
        })
        
        # Should NOT raise - only needs 5.5GB, has 6GB
        try:
            estimator._validate_target_space(moves)
        except ResourceError:
            self.fail("ResourceError raised - segment was double-counted!")
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    @patch('gprebalance_modules.planner.DiskSpaceChecker')
    @patch('gprebalance_modules.planner.HostResolver.resolve_hostname')
    @patch('gprebalance_modules.planner.HostResolver.get_address')
    @patch('gprebalance_modules.planner.GreedySolver')
    @patch('gprebalance_modules.rebalance_schema.dbconn.queryRow', side_effect=check_query)
    @patch('gprebalance_modules.planner.dbconn')
    def test_planner_with_resource_estimation(self, mock_dbconn, mock_schema, mock_solver, 
                                               mock_get_address, mock_resolve, mock_disk_check, mock_port):
        """Test Planner integration with resource estimation"""
        # Setup resolver mocks
        mock_resolve.return_value = None
        def address_side_effect(hostname):
            addr_map = {
                'sdw1': '172.20.0.6',
                'sdw2': '172.20.0.7',
                'sdw3': '172.20.0.8'
            }
            return addr_map.get(hostname, hostname)
        mock_get_address.side_effect = address_side_effect
        
        # Setup solver mock
        mock_solver_instance = Mock()
        mock_solver.return_value = mock_solver_instance
        
        # Mock solution: move segments to balance cluster
        solution = {0: (0, 1),
                    1: (0, 1),
                    2: (0, 1),
                    3: (2, 0),
                    4: (2, 0),
                    5: (1, 2),
                    6: (1, 2),
                    7: (1, 2),
                    8: (2, 0)}

        mock_solver_instance.solve.return_value = (solution, {})
        
        self.options.target_datadirs="/data/primary{content}, /data/mirror{content}"

        mock_port.return_value._is_port_available.return_value = True
        # Create planner
        planner = Planner(
            logger=self.logger,
            dburl=self.dburl,
            gpArray=self.gparray,
            options=self.options
        )
        
        # Mock disk usage - all segments are 2GB
        mock_disk_check.return_value.get_disk_usage.return_value = {
            '/data/primary0': 2097152,
            '/data/primary1': 2097152,
            '/data/primary2': 2097152,
            '/data/mirror3': 2097152,
            '/data/mirror4': 2097152,
            '/data/mirror5': 2097152,
            '/data/mirror6': 2097152,
            '/data/mirror7': 2097152,
            '/data/mirror8': 2097152,
        }
            
        # Mock available space - on all targets where we move segments to
        mock_disk_check.return_value.check_batch_available_space.return_value = {
            '172.20.0.7': {
                '/data/primary8': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/primary8'),
                '/data/mirror5': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror5'),
                '/data/mirror6': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror6'),
                '/data/mirror7': DiskSpaceInfo('/dev/sdb1', 52428800, '/data/mirror7'),
            },
            '172.20.0.8': {
                '/data/mirror0': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror0'),
                '/data/mirror1': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror1'),
                '/data/mirror2': DiskSpaceInfo('/dev/sdc1', 52428800, '/data/mirror2'),
            },
        }
        
        mock_dbconn.connect.return_value = self.conn
        mock_dbconn.query.return_value = []
        
        # Execute planning
        plan = planner.plan()
        
        # Verify moves were created
        self.assertIsNotNone(plan.getMoves())
        
        # Verify resource estimation was performed
        for move in plan.getMoves():
            self.assertIsNotNone(move.segment_size, 
                                f"Segment size not set for move: {move}")
    
    @patch('gprebalance_modules.planner.dbconn')
    @patch('gprebalance_modules.rebalance_schema.dbconn.queryRow', side_effect=check_query)
    @patch('gprebalance_modules.planner.HostResolver.resolve_hostname')
    @patch('gprebalance_modules.planner.HostResolver.get_address')
    def test_planner_skips_resource_estimation_when_requested(self, mock_get_address, 
                                                              mock_resolve, mock_schema, mock_conn):
        """Test Planner skips resource estimation when skip_resource_estimation=True"""
        mock_resolve.return_value = None
        def address_side_effect(hostname):
            addr_map = {
                'sdw1': '172.20.0.6',
                'sdw2': '172.20.0.7',
                'sdw3': '172.20.0.8'
            }
            return addr_map.get(hostname, hostname)
        mock_get_address.side_effect = address_side_effect
        
        # Enable skip flag
        self.options.skip_resource_estimation = True
        
        planner = Planner(
            logger=self.logger,
            dburl=self.dburl,
            gpArray=self.gparray,
            options=self.options
        ).plan()
        
        # Verify warning was logged
        self.logger.warning.assert_any_call("Skipping resource estimation")

class TestPortAllocator(GpTestCase):
    """
    Unit tests for PortAllocator class
    """
    
    def setUp(self):
        self.logger = Mock()
        
    def _create_mock_segment(self, dbid, content, hostname, port, role='p', preferred_role='p'):
        """
        Helper to create mock segment
        """
        seg = Mock(spec=gparray.Segment)
        seg.dbid = dbid
        seg.content = content
        seg.hostname = hostname
        seg.datadir = f'/data{content}/seg{content}'
        seg.port = port
        seg.role = role
        seg.preferred_role = preferred_role
        
        seg.getSegmentDbId.return_value = dbid
        seg.getSegmentContentId.return_value = content
        seg.getSegmentHostName.return_value = hostname
        seg.getSegmentDataDirectory.return_value = seg.datadir
        seg.getSegmentPort.return_value = port
        seg.isSegmentPrimary.return_value = (role == 'p')
        seg.isSegmentMirror.return_value = (role == 'm')
        
        return seg
    
    def _create_mock_gparray(self, segments):
        """
        Helper to create mock GpArray
        """
        array = Mock(spec=gparray.GpArray)
        array.getSegDbList.return_value = segments
        return array
    
    def test_initialization_simple(self):
        """
        Test basic initialization with simple segment configuration
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 0, 'host2', 6000, 'm'),
            self._create_mock_segment(3, 1, 'host1', 6001, 'p'),
            self._create_mock_segment(4, 1, 'host2', 6001, 'm'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger)
        
        # Verify existing ports tracked
        self.assertIn(6000, allocator.existing_ports_by_host['host1'])
        self.assertIn(6001, allocator.existing_ports_by_host['host1'])
        self.assertIn(6000, allocator.existing_ports_by_host['host2'])
        self.assertIn(6001, allocator.existing_ports_by_host['host2'])
        
        # Verify base ports detected
        self.assertEqual(allocator.base_ports_by_host['host1'], (6000, None))
        self.assertEqual(allocator.base_ports_by_host['host2'], (None, 6000))
    
    def test_initialization_with_primaries_and_mirrors(self):
        """
        Test initialization with both primaries and mirrors on same host
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 1, 'host1', 6001, 'p'),
            self._create_mock_segment(3, 0, 'host2', 7000, 'm'),
            self._create_mock_segment(4, 1, 'host2', 7001, 'm'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger)
        
        # Host1 has primaries
        primary_ports, mirror_ports = allocator.existing_ports_by_role['host1']
        self.assertEqual(primary_ports, {6000, 6001})
        self.assertEqual(mirror_ports, set())
        
        # Host2 has mirrors
        primary_ports, mirror_ports = allocator.existing_ports_by_role['host2']
        self.assertEqual(primary_ports, set())
        self.assertEqual(mirror_ports, {7000, 7001})
        
        # Base ports
        self.assertEqual(allocator.base_ports_by_host['host1'], (6000, None))
        self.assertEqual(allocator.base_ports_by_host['host2'], (None, 7000))
    
    def test_allocate_port_existing_host_port_available(self):
        """
        Test allocating port on existing host when current port is available
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 1, 'host1', 6001, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host = Host(hostname='host2', address='10.0.0.2', status=HostStatus.ACTIVE)
        
        # Allocate port 7000 on host2 (doesn't exist in cluster yet)
        allocated_port = allocator.allocate_port(host, current_port=7000, is_mirror=False)
        
        self.assertEqual(allocated_port, 7000)
        self.assertIn(7000, allocator.planned_ports_by_host['host2'])
    
    def test_allocate_port_existing_host_port_conflict(self):
        """
        Test allocating port on existing host when current port conflicts
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 1, 'host1', 6001, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.ACTIVE)
        
        # Try to allocate port 6000 which is already used
        allocated_port = allocator.allocate_port(host, current_port=6000, is_mirror=False)
        
        # Should get next available port
        self.assertNotEqual(allocated_port, 6000)
        self.assertGreaterEqual(allocated_port, 6000)
        self.assertIn(allocated_port, allocator.planned_ports_by_host['host1'])
    
    def test_allocate_port_new_host_first_primary(self):
        """
        Test allocating first primary port on new host
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        new_host = Host(hostname='host2', address='10.0.0.2', status=HostStatus.NEW)
        
        # Allocate first primary on new host
        allocated_port = allocator.allocate_port(new_host, current_port=6000, is_mirror=False)
        
        self.assertEqual(allocated_port, 6000)
        self.assertEqual(allocator.base_ports_by_host['host2'], (6000, None))
        self.assertIn(6000, allocator.planned_ports_by_host['host2'])
    
    def test_allocate_port_new_host_first_mirror(self):
        """
        Test allocating first mirror port on new host
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        new_host = Host(hostname='host2', address='10.0.0.2', status=HostStatus.NEW)
        
        # Allocate first mirror on new host
        allocated_port = allocator.allocate_port(new_host, current_port=7000, is_mirror=True)
        
        self.assertEqual(allocated_port, 7000)
        self.assertEqual(allocator.base_ports_by_host['host2'], (None, 7000))
        self.assertIn(7000, allocator.planned_ports_by_host['host2'])
    
    def test_allocate_port_new_host_subsequent_primaries(self):
        """
        Test allocating subsequent primary ports on new host follow pattern
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        new_host = Host(hostname='host2', address='10.0.0.2', status=HostStatus.NEW)
        
        # Allocate first primary
        port1 = allocator.allocate_port(new_host, current_port=6000, is_mirror=False)
        self.assertEqual(port1, 6000)
        
        # Allocate second primary - should follow pattern
        port2 = allocator.allocate_port(new_host, current_port=6001, is_mirror=False)
        self.assertEqual(port2, 6001)
        
        # Allocate third primary
        port3 = allocator.allocate_port(new_host, current_port=6002, is_mirror=False)
        self.assertEqual(port3, 6002)
    
    def test_allocate_port_new_host_mirrors_separate_from_primaries(self):
        """
        Test that mirrors and primaries maintain separate port ranges on new host
        """
        segments = []
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        new_host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        
        # Allocate primaries starting at 6000
        port_p1 = allocator.allocate_port(new_host, current_port=6000, is_mirror=False)
        port_p2 = allocator.allocate_port(new_host, current_port=6001, is_mirror=False)
        
        # Allocate mirrors starting at 7000
        port_m1 = allocator.allocate_port(new_host, current_port=7000, is_mirror=True)
        port_m2 = allocator.allocate_port(new_host, current_port=7001, is_mirror=True)
        
        self.assertEqual(port_p1, 6000)
        self.assertEqual(port_p2, 6001)
        self.assertEqual(port_m1, 7000)
        self.assertEqual(port_m2, 7001)
        
        # Verify base ports
        self.assertEqual(allocator.base_ports_by_host['host1'], (6000, 7000))
    
    def test_is_port_available(self):
        """
        Test port availability checking
        """
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger)
        
        # Port 6000 is used
        self.assertFalse(allocator._is_port_available('host1', 6000))
        
        # Port 6001 is free
        self.assertTrue(allocator._is_port_available('host1', 6001))
        
        # Mark 6001 as planned
        allocator.planned_ports_by_host['host1'].add(6001)
        self.assertFalse(allocator._is_port_available('host1', 6001))
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    def test_check_port_on_host_available(self, mock_port_is_available):
        """Test actual port verification on host when port is available"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=True)
        
        # Mock PortIsAvailable command
        mock_cmd = Mock()
        mock_cmd.is_port_available.return_value = True
        mock_port_is_available.return_value = mock_cmd
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        result = allocator._check_port_on_host(host, 6000)
        
        self.assertTrue(result)
        mock_cmd.run.assert_called_once()
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    def test_check_port_on_host_in_use(self, mock_port_is_available):
        """Test actual port verification when port is in use"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=True)
        
        # Mock PortIsAvailable command
        mock_cmd = Mock()
        mock_cmd.is_port_available.return_value = False
        mock_port_is_available.return_value = mock_cmd
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        result = allocator._check_port_on_host(host, 6000)
        
        self.assertFalse(result)
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    def test_check_port_on_host_verification_disabled(self, mock_port_is_available):
        """Test that verification is skipped when disabled"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        result = allocator._check_port_on_host(host, 6000)
        
        # Should return True without checking
        self.assertTrue(result)
        mock_port_is_available.assert_not_called()
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    def test_verify_and_allocate_port_preferred_available(self, mock_port_is_available):
        """Test verify_and_allocate when preferred port is available"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=True)
        
        # Mock port as available
        mock_cmd = Mock()
        mock_cmd.is_port_available.return_value = True
        mock_port_is_available.return_value = mock_cmd
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        allocated = allocator._verify_and_allocate_port(host, 6000)
        
        self.assertEqual(allocated, 6000)
    
    @patch('gprebalance_modules.planner.PortIsAvailable')
    def test_verify_and_allocate_port_preferred_in_use(self, mock_port_is_available):
        """
        Test verify_and_allocate when preferred port is in use
        """
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=True)
        
        # Mock first port as in use, second as available
        def mock_is_available_side_effect():
            call_count = [0]
            def side_effect():
                call_count[0] += 1
                return call_count[0] > 1
            return side_effect
        
        mock_cmd = Mock()
        mock_cmd.is_port_available.side_effect = mock_is_available_side_effect()
        mock_port_is_available.return_value = mock_cmd
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        allocated = allocator._verify_and_allocate_port(host, 6000)
        
        # Should get next port
        self.assertEqual(allocated, 6001)
        self.assertIn(6000, allocator.existing_ports_by_host['host1'])  # Marked as used
    
    def test_find_next_available_port_with_base(self):
        """Test finding next available port with established base"""
        segments = [
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 1, 'host1', 6001, 'p'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.ACTIVE)
        
        # Find next primary port (base is 6000)
        next_port = allocator._find_next_available_port(host, 6000, is_mirror=False)
        
        # Should skip 6000, 6001 and return 6002
        self.assertEqual(next_port, 6002)
    
    def test_find_verified_port_max_attempts(self):
        """Test that finding port fails after max attempts"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        
        # Fill up many ports
        for i in range(1000):
            allocator.existing_ports_by_host['host1'].add(6000 + i)
        
        # Should raise error after max attempts
        with self.assertRaises(PlanningError) as ctx:
            allocator._find_verified_port(host, 6000)
        
        self.assertIn("Cannot find available port", str(ctx.exception))
        self.assertIn("after 1000 attempts", str(ctx.exception))
    
    def test_complex_scenario_mixed_roles(self):
        """
        Test complex scenario with primaries and mirrors on same host
        """
        segments = [
            # Host1: primaries on 6000-6002
            self._create_mock_segment(1, 0, 'host1', 6000, 'p'),
            self._create_mock_segment(2, 1, 'host1', 6001, 'p'),
            self._create_mock_segment(3, 2, 'host1', 6002, 'p'),
            # Host1: mirrors on 7000-7001
            self._create_mock_segment(4, 3, 'host1', 7000, 'm'),
            self._create_mock_segment(5, 4, 'host1', 7001, 'm'),
        ]
        
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        host1 = Host(hostname='host1', address='10.0.0.1', status=HostStatus.ACTIVE)
        
        # Allocate new primary (should get 6003)
        port_p = allocator.allocate_port(host1, current_port=6003, is_mirror=False)
        self.assertEqual(port_p, 6003)
        
        # Allocate new mirror (should get 7002)
        port_m = allocator.allocate_port(host1, current_port=7002, is_mirror=True)
        self.assertEqual(port_m, 7002)
        
        # Verify role separation maintained
        primary_ports, mirror_ports = allocator.existing_ports_by_role['host1']
        self.assertIn(6003, primary_ports)
        self.assertIn(7002, mirror_ports)
    
    def test_empty_gparray(self):
        """Test initialization with empty gparray"""
        segments = []
        gparray_mock = self._create_mock_gparray(segments)
        allocator = PortAllocator(gparray_mock, self.logger)
        
        self.assertEqual(len(allocator.existing_ports_by_host), 0)
        self.assertEqual(len(allocator.base_ports_by_host), 0)
        
        # Should still be able to allocate on new host
        new_host = Host(hostname='host1', address='10.0.0.1', status=HostStatus.NEW)
        port = allocator.allocate_port(new_host, current_port=6000, is_mirror=False)
        self.assertEqual(port, 6000)

class TestPortAllocatorIntegration(GpTestCase):
    """Integration tests simulating realistic rebalance scenarios"""
    
    def setUp(self):
        self.logger = Mock()
    
    def _create_segment(self, dbid, content, hostname, port, role='p'):
        """Helper to create mock segment"""
        seg = Mock(spec=gparray.Segment)
        seg.dbid = dbid
        seg.content = content
        seg.hostname = hostname
        seg.datadir = f'/data{content}/seg{content}'
        seg.port = port
        seg.role = role
        seg.preferred_role = role
        
        seg.getSegmentDbId.return_value = dbid
        seg.getSegmentContentId.return_value = content
        seg.getSegmentHostName.return_value = hostname
        seg.getSegmentDataDirectory.return_value = seg.datadir
        seg.getSegmentPort.return_value = port
        seg.isSegmentPrimary.return_value = (role == 'p')
        seg.isSegmentMirror.return_value = (role == 'm')
        
        return seg
    
    def test_expansion_scenario(self):
        """Test port allocation during cluster expansion (add new host)"""
        # Initial: 2 hosts with 2 primaries each
        segments = [
            self._create_segment(1, 0, 'host1', 6000, 'p'),
            self._create_segment(2, 1, 'host1', 6001, 'p'),
            self._create_segment(3, 0, 'host2', 7000, 'm'),
            self._create_segment(4, 1, 'host2', 7001, 'm'),
        ]
        
        gparray_mock = Mock(spec=gparray.GpArray)
        gparray_mock.getSegDbList.return_value = segments
        
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        # Add new host3 - move some mirrors there
        new_host = Host(hostname='host3', address='10.0.0.3', status=HostStatus.NEW)
        
        # Move mirror of content 0 to new host
        port1 = allocator.allocate_port(new_host, current_port=7000, is_mirror=True)
        self.assertEqual(port1, 7000)  # Establishes base
        
        # Move mirror of content 1 to new host
        port2 = allocator.allocate_port(new_host, current_port=7001, is_mirror=True)
        self.assertEqual(port2, 7001)  # Follows pattern
    
    def test_shrink_scenario(self):
        """Test port allocation during cluster shrink (remove host)"""
        # Initial: 3 hosts
        segments = [
            self._create_segment(1, 0, 'host1', 6000, 'p'),
            self._create_segment(2, 1, 'host2', 6000, 'p'),
            self._create_segment(3, 2, 'host3', 6000, 'p'),
            self._create_segment(4, 0, 'host2', 7000, 'm'),
            self._create_segment(5, 1, 'host3', 7000, 'm'),
            self._create_segment(6, 2, 'host1', 7000, 'm'),
        ]
        
        gparray_mock = Mock(spec=gparray.GpArray)
        gparray_mock.getSegDbList.return_value = segments
        
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        # Remove host3 - redistribute its segments
        host1 = Host(hostname='host1', address='10.0.0.1', status=HostStatus.ACTIVE)
        host2 = Host(hostname='host2', address='10.0.0.2', status=HostStatus.ACTIVE)
        
        # Move content 2 primary from host3 to host1
        port_p = allocator.allocate_port(host1, current_port=6000, is_mirror=False)
        self.assertEqual(port_p, 6001)  # 6000 in use, get next
        
        # Move content 2 mirror from host1 to host2
        port_m = allocator.allocate_port(host2, current_port=7000, is_mirror=True)
        self.assertEqual(port_m, 7001)  # 7000 in use, get next
    
    def test_rebalance_after_failure(self):
        """Test rebalance scenario after segment failure and recovery"""
        # Scenario: segment recovered with different port, need to rebalance
        segments = [
            self._create_segment(1, 0, 'host1', 6000, 'p'),
            self._create_segment(2, 1, 'host1', 6001, 'p'),
            self._create_segment(3, 2, 'host1', 6002, 'p'),
            self._create_segment(4, 0, 'host2', 7000, 'm'),
            self._create_segment(5, 1, 'host2', 7001, 'm'),
            self._create_segment(6, 2, 'host2', 7999, 'm'),  # Recovered with odd port
        ]
        
        gparray_mock = Mock(spec=gparray.GpArray)
        gparray_mock.getSegDbList.return_value = segments
        
        allocator = PortAllocator(gparray_mock, self.logger, verify_ports=False)
        
        # Rebalance: move content 2 mirror back to proper port
        host2 = Host(hostname='host2', address='10.0.0.2', status=HostStatus.ACTIVE)
        
        # Try to use 7002 (following pattern)
        port = allocator.allocate_port(host2, current_port=7002, is_mirror=True)
        self.assertEqual(port, 7002)  # Should be available

if __name__ == '__main__':
    run_tests()
