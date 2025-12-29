import os
import socket
import tempfile

from gppylib.test.unit.gp_unittest import *
from mock import *
from gprebalance_modules.rebalance_commons import HostResolver, TemplateParser, ValidationError, is_ip_address

class TestHostResolver(GpTestCase):
    """
    Test suite for HostResolver class
    """
    
    def setUp(self):
        self.resolver = HostResolver()
    
    def tearDown(self):
        self.resolver = None
    
    def test_is_ip_address_valid_ipv4(self):
        self.assertTrue(is_ip_address('192.168.1.1'))
        self.assertTrue(is_ip_address('10.0.0.1'))
        self.assertTrue(is_ip_address('255.255.255.255'))
        self.assertTrue(is_ip_address('0.0.0.0'))
    
    def test_is_ip_address_valid_ipv6(self):
        self.assertTrue(is_ip_address('2001:0db8:85a3::8a2e:0370:7334'))
        self.assertTrue(is_ip_address('::1'))
        self.assertTrue(is_ip_address('fe80::1'))
        self.assertTrue(is_ip_address('::'))
    
    def test_is_ip_address_invalid(self):
        self.assertFalse(is_ip_address('hostname'))
        self.assertFalse(is_ip_address('not-an-ip'))
        self.assertFalse(is_ip_address('999.999.999.999'))
        self.assertFalse(is_ip_address('192.168.1'))
        self.assertFalse(is_ip_address(''))
        self.assertFalse(is_ip_address('192.168.1.1.1'))
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_success_ipv4(self, mock_getaddrinfo):
        # Mock socket.getaddrinfo to return IPv4 address
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        result = self.resolver.resolve_hostname('testhost')
        
        self.assertEqual(result, '192.168.1.10')
        mock_getaddrinfo.assert_called_once_with(
            'testhost', None, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        # Check caching - now returns a set
        self.assertEqual(self.resolver._hostname_to_ips['testhost'], {'192.168.1.10'})
        # Check reverse mapping
        self.assertEqual(self.resolver._ip_to_hostname['192.168.1.10'], 'testhost')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_success_ipv6(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('2001:db8::1', 0, 0, 0))
        ]
        
        result = self.resolver.resolve_hostname('testhost6')
        
        self.assertEqual(result, '2001:db8::1')
        self.assertEqual(self.resolver._hostname_to_ips['testhost6'], {'2001:db8::1'})
        self.assertEqual(self.resolver._ip_to_hostname['2001:db8::1'], 'testhost6')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_success_ipv6_with_scope(self, mock_getaddrinfo):
        # Test IPv6 with scope ID normalization
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('fe80::1%eth0', 0, 0, 0))
        ]
        
        result = self.resolver.resolve_hostname('testhost6')
        
        # Should normalize by removing scope ID
        self.assertEqual(result, 'fe80::1')
        self.assertEqual(self.resolver._hostname_to_ips['testhost6'], {'fe80::1'})
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_multiple_ips(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.10', 0)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('fe80::1', 0, 0, 0))
        ]
        
        result = self.resolver.resolve_hostname('multihost')
        
        # Should return first IP when sorted
        self.assertIn(result, {'10.0.0.10', '192.168.1.10', 'fe80::1'})
        # Should cache all IPs
        self.assertEqual(self.resolver._hostname_to_ips['multihost'], 
                        {'192.168.1.10', '10.0.0.10', 'fe80::1'})
        # All IPs should have reverse mappings
        self.assertEqual(self.resolver._ip_to_hostname['192.168.1.10'], 'multihost')
        self.assertEqual(self.resolver._ip_to_hostname['10.0.0.10'], 'multihost')
        self.assertEqual(self.resolver._ip_to_hostname['fe80::1'], 'multihost')
    
    @patch('socket.getaddrinfo')
    def test_resolve_hostname_cached(self, mock_getaddrinfo):
        self.resolver._hostname_to_ips['cached'] = {'192.168.1.100'}
        
        result = self.resolver.resolve_hostname('cached')
        
        self.assertEqual(result, '192.168.1.100')
        mock_getaddrinfo.assert_not_called()
        
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_success(self, mock_hostname_class):
        # Mock the Hostname command
        mock_cmd = MagicMock()
        mock_cmd.was_successful.return_value = True
        mock_cmd.get_hostname.return_value = 'testhost'
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('192.168.1.10')
        
        self.assertEqual(result, 'testhost')
        mock_hostname_class.assert_called_once()
        mock_cmd.run.assert_called_once()
        # Check caching - reverse mapping
        self.assertEqual(self.resolver._ip_to_hostname['192.168.1.10'], 'testhost')
        # Check forward mapping was also created
        self.assertIn('192.168.1.10', self.resolver._hostname_to_ips['testhost'])
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_cached(self, mock_hostname_class):
        self.resolver._ip_to_hostname['192.168.1.100'] = 'cached'
        
        result = self.resolver.resolve_ip('192.168.1.100')
        
        self.assertEqual(result, 'cached')
        mock_hostname_class.assert_not_called()
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_invalid_ip(self, mock_hostname_class):
        result = self.resolver.resolve_ip('not-an-ip')
        
        self.assertIsNone(result)
        mock_hostname_class.assert_not_called()
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_command_failure(self, mock_hostname_class):
        mock_cmd = MagicMock()
        mock_cmd.was_successful.return_value = False
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('192.168.1.99')
        
        self.assertIsNone(result)
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_exception(self, mock_hostname_class):
        mock_cmd = MagicMock()
        mock_cmd.run.side_effect = Exception('Resolution failed')
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('192.168.1.99')
        
        self.assertIsNone(result)
    
    @patch('gprebalance_modules.rebalance_commons.Hostname')
    def test_resolve_ip_ipv6(self, mock_hostname_class):
        mock_cmd = MagicMock()
        mock_cmd.was_successful.return_value = True
        mock_cmd.get_hostname.return_value = 'testhost6'
        mock_hostname_class.return_value = mock_cmd
        
        result = self.resolver.resolve_ip('2001:db8::1')
        
        self.assertEqual(result, 'testhost6')
        self.assertEqual(self.resolver._ip_to_hostname['2001:db8::1'], 'testhost6')
        
    def test_hosts_match_identical_hostnames(self):
        self.assertTrue(self.resolver.hosts_match('testhost', 'testhost'))
    
    def test_hosts_match_identical_ips(self):
        self.assertTrue(self.resolver.hosts_match('192.168.1.10', '192.168.1.10'))
    
    def test_hosts_match_ipv6_normalization(self):
        # IPv6 addresses with scope IDs should match
        self.assertTrue(self.resolver.hosts_match('fe80::1%eth0', 'fe80::1%eth1'))
        self.assertTrue(self.resolver.hosts_match('fe80::1%eth0', 'fe80::1'))
    
    def test_hosts_match_different_ips(self):
        self.assertFalse(self.resolver.hosts_match('192.168.1.10', '192.168.1.11'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_ip_to_hostname(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # IP matches hostname's resolved IP
        self.assertTrue(self.resolver.hosts_match('192.168.1.10', 'testhost'))
        # Should work in reverse too
        self.assertTrue(self.resolver.hosts_match('testhost', '192.168.1.10'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_ip_to_hostname_multiple_ips(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.10', 0))
        ]
        
        # Should match any of the IPs
        self.assertTrue(self.resolver.hosts_match('192.168.1.10', 'testhost'))
        self.assertTrue(self.resolver.hosts_match('10.0.0.10', 'testhost'))
        self.assertFalse(self.resolver.hosts_match('192.168.1.99', 'testhost'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_ip_to_hostname_no_match(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # IP doesn't match hostname's resolved IP
        self.assertFalse(self.resolver.hosts_match('192.168.1.99', 'testhost'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_hostname_to_hostname_shared_ip(self, mock_getaddrinfo):
        # Two hostnames sharing an IP address
        def getaddrinfo_side_effect(hostname, *args):
            if hostname == 'host1':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))]
            elif hostname == 'host2':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))]
            return []
        
        mock_getaddrinfo.side_effect = getaddrinfo_side_effect
        
        # Should match because they share an IP
        self.assertTrue(self.resolver.hosts_match('host1', 'host2'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_hostname_to_hostname_no_shared_ip(self, mock_getaddrinfo):
        def getaddrinfo_side_effect(hostname, *args):
            if hostname == 'host1':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))]
            elif hostname == 'host2':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))]
            return []
        
        mock_getaddrinfo.side_effect = getaddrinfo_side_effect
        
        # Should not match - no shared IPs
        self.assertFalse(self.resolver.hosts_match('host1', 'host2'))
    
    def test_hosts_match_different_hostnames_no_resolution(self):
        # Without resolution data, different hostnames don't match
        self.assertFalse(self.resolver.hosts_match('host1', 'host2'))
    
    @patch('socket.getaddrinfo')
    def test_hosts_match_resolution_failure(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror('Resolution failed')
        
        # Should return False when resolution fails
        self.assertFalse(self.resolver.hosts_match('192.168.1.10', 'unknown'))
    
    def test_hosts_match_ipv6(self):
        self.assertTrue(self.resolver.hosts_match('2001:db8::1', '2001:db8::1'))
        self.assertFalse(self.resolver.hosts_match('2001:db8::1', '2001:db8::2'))
        
    def test_find_matching_hostname_exact_match(self):
        existing_hosts = ['host1', 'host2', 'host3']
        
        result = self.resolver.find_matching_hostname('host2', existing_hosts)
        
        self.assertEqual(result, 'host2')
    
    @patch('socket.getaddrinfo')
    def test_find_matching_hostname_ip_match(self, mock_getaddrinfo):
        def getaddrinfo_side_effect(hostname, *args):
            if hostname == 'host1':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))]
            elif hostname == 'host2':
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))]
            return []
        
        mock_getaddrinfo.side_effect = getaddrinfo_side_effect
        
        existing_hosts = ['host1', 'host2']
        
        result = self.resolver.find_matching_hostname('192.168.1.10', existing_hosts)
        
        # Should match host1
        self.assertEqual(result, 'host1')
    
    def test_find_matching_hostname_no_match(self):
        existing_hosts = ['host1', 'host2']
        
        result = self.resolver.find_matching_hostname('nonexistent', existing_hosts)
        
        self.assertIsNone(result)
    
    def test_find_matching_hostname_empty_list(self):
        result = self.resolver.find_matching_hostname('testhost', [])
        
        self.assertIsNone(result)
    
    def test_get_all_addresses(self):
        self.resolver._hostname_to_ips['multihost'] = {'192.168.1.10', '10.0.0.10'}
        
        result = self.resolver.get_all_addresses('multihost')
        
        self.assertEqual(result, {'192.168.1.10', '10.0.0.10'})
    
    def test_get_all_addresses_empty(self):
        result = self.resolver.get_all_addresses('unknown')
        
        self.assertEqual(result, set())
    
    def test_get_address_returns_first_sorted(self):
        self.resolver._hostname_to_ips['multihost'] = {'192.168.1.10', '10.0.0.10', 'fe80::1'}
        
        result = self.resolver.get_address('multihost')
        
        # Should return first when sorted
        sorted_ips = sorted(['192.168.1.10', '10.0.0.10', 'fe80::1'])
        self.assertEqual(result, sorted_ips[0])
    
    def test_get_hostname(self):
        self.resolver._ip_to_hostname['192.168.1.10'] = 'testhost'
        
        result = self.resolver.get_hostname('192.168.1.10')
        
        self.assertEqual(result, 'testhost')
    
    def test_get_hostname_not_found(self):
        result = self.resolver.get_hostname('192.168.1.99')
        
        # Should return IP itself if not found
        self.assertEqual(result, '192.168.1.99')

    @patch('socket.getaddrinfo')
    def test_caching_integration(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))
        ]
        
        # First call should hit the network
        result1 = self.resolver.resolve_hostname('testhost')
        self.assertEqual(result1, '192.168.1.10')
        self.assertEqual(mock_getaddrinfo.call_count, 1)
        
        # Second call should use cache
        result2 = self.resolver.resolve_hostname('testhost')
        self.assertEqual(result2, '192.168.1.10')
        self.assertEqual(mock_getaddrinfo.call_count, 1)  # Still 1, not called again
        
        # Third call to get from cache directly
        result3 = self.resolver.get_address('testhost')
        self.assertEqual(result3, '192.168.1.10')
        
        # get_all_addresses should also work
        all_ips = self.resolver.get_all_addresses('testhost')
        self.assertEqual(all_ips, {'192.168.1.10'})
    
    @patch('socket.getaddrinfo')
    def test_multiple_hosts(self, mock_getaddrinfo):
        def getaddrinfo_side_effect(hostname, *args):
            mapping = {
                'host1': [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.10', 0))],
                'host2': [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.11', 0))],
                'host3': [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.12', 0))]
            }
            return mapping.get(hostname, [])
        
        mock_getaddrinfo.side_effect = getaddrinfo_side_effect
        
        existing_hosts = ['host1', 'host2', 'host3']
        
        # Test various scenarios
        self.assertEqual(self.resolver.find_matching_hostname('host1', existing_hosts), 'host1')
        self.assertEqual(self.resolver.find_matching_hostname('192.168.1.11', existing_hosts), 'host2')
        self.assertIsNone(self.resolver.find_matching_hostname('192.168.1.99', existing_hosts))
        
        # Test hosts_match
        self.assertTrue(self.resolver.hosts_match('host1', '192.168.1.10'))
        self.assertFalse(self.resolver.hosts_match('host1', '192.168.1.11'))
        
        # Test get_all_addresses
        self.assertEqual(self.resolver.get_all_addresses('host2'), {'192.168.1.11'})


class TestTemplateParser(GpTestCase):
    """Test suite for TemplateParser"""
    
    def test_parse_basic_input(self):
        """Test basic comma-separated input"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/primary, /data/mirror"
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_quoted_input_double_quotes(self):
        """Test input with double quotes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            '"/data/primary", "/data/mirror"'
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_quoted_input_single_quotes(self):
        """Test input with single quotes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "'/data/primary', '/data/mirror'"
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_with_content_placeholder(self):
        """Test input that already has {content}"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/primary/gpseg{content}, /data/mirror/gpseg{content}"
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_with_hostname_placeholder(self):
        """Test input with {hostname} placeholder"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/{hostname}/primary, /data/{hostname}/mirror"
        )
        self.assertEqual(primary, "/data/{hostname}/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/{hostname}/mirror/gpseg{content}")
    
    def test_parse_with_both_placeholders(self):
        """Test input with both {hostname} and {content} placeholders"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/{hostname}/primary/gpseg{content}, /data/{hostname}/mirror/gpseg{content}"
        )
        self.assertEqual(primary, "/data/{hostname}/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/{hostname}/mirror/gpseg{content}")
    
    def test_parse_with_trailing_slashes(self):
        """Test input with trailing slashes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/primary/, /data/mirror/"
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_with_extra_whitespace(self):
        """Test input with extra whitespace"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "  /data/primary  ,  /data/mirror  "
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_identical_templates_error(self):
        """Test that identical templates raise error"""
        with self.assertRaisesRegex(ValidationError, "cannot be identical"):
            TemplateParser.parse_datadirs_input(
                "/data/primary, /data/primary"
            )
    
    def test_identical_after_normalization_error(self):
        """Test that templates identical after normalization raise error"""
        with self.assertRaisesRegex(ValidationError, "cannot be identical"):
            TemplateParser.parse_datadirs_input(
                "/data/segments, /data/segments/"
            )
    
    def test_non_absolute_path_error(self):
        """Test that relative paths raise error"""
        with self.assertRaisesRegex(ValidationError, "must be absolute"):
            TemplateParser.parse_datadirs_input(
                "data/primary, data/mirror"
            )
    
    def test_non_absolute_second_path_error(self):
        """Test that relative path in second position raises error"""
        with self.assertRaisesRegex(ValidationError, "must be absolute"):
            TemplateParser.parse_datadirs_input(
                "/data/primary, data/mirror"
            )
    
    def test_empty_path_error(self):
        """Test that empty paths raise error"""
        with self.assertRaisesRegex(ValidationError, "cannot be empty"):
            TemplateParser.parse_datadirs_input(
                '"", /data/mirror'
            )
    
    def test_empty_after_quotes_error(self):
        """Test that empty path after quote removal raises error"""
        with self.assertRaisesRegex(ValidationError, "cannot be empty"):
            TemplateParser.parse_datadirs_input(
                '  ""  , /data/mirror'
            )
    
    def test_invalid_placeholder_error(self):
        """Test that invalid placeholders raise error"""
        with self.assertRaisesRegex(ValidationError, "Invalid placeholder"):
            TemplateParser.parse_datadirs_input(
                "/data/{invalid}/primary, /data/mirror"
            )
    
    def test_multiple_invalid_placeholders_error(self):
        """Test that multiple invalid placeholders are caught"""
        with self.assertRaisesRegex(ValidationError, "Invalid placeholder"):
            TemplateParser.parse_datadirs_input(
                "/data/{foo}/{bar}/primary, /data/mirror"
            )
    
    def test_wrong_format_missing_second_dir_error(self):
        """Test that missing second directory raises error"""
        with self.assertRaisesRegex(ValidationError, "should have format"):
            TemplateParser.parse_datadirs_input(
                "/data/primary"
            )
    
    def test_wrong_format_too_many_dirs_error(self):
        """Test that too many directories raise error"""
        with self.assertRaisesRegex(ValidationError, "should have format"):
            TemplateParser.parse_datadirs_input(
                "/data/primary, /data/mirror, /data/extra"
            )
    
    def test_instantiate_template_both_placeholders(self):
        """Test template instantiation with both placeholders"""
        template = "/data/{hostname}/gpseg{content}"
        result = TemplateParser.instantiate_template(
            template, hostname="sdw1", content=0
        )
        self.assertEqual(result, "/data/sdw1/gpseg0")
    
    def test_instantiate_template_content_only(self):
        """Test template instantiation with content only"""
        template = "/data/primary/gpseg{content}"
        result = TemplateParser.instantiate_template(
            template, content=5
        )
        self.assertEqual(result, "/data/primary/gpseg5")
    
    def test_instantiate_template_hostname_only(self):
        """Test template instantiation with hostname only"""
        template = "/data/{hostname}/segments/gpseg{content}"
        result = TemplateParser.instantiate_template(
            template, hostname="sdw2"
        )
        self.assertEqual(result, "/data/sdw2/segments/gpseg{content}")
    
    def test_instantiate_template_no_substitution(self):
        """Test template instantiation with no substitution"""
        template = "/data/{hostname}/gpseg{content}"
        result = TemplateParser.instantiate_template(template)
        self.assertEqual(result, "/data/{hostname}/gpseg{content}")
    
    def test_instantiate_template_large_content_id(self):
        """Test template instantiation with large content ID"""
        template = "/data/primary/gpseg{content}"
        result = TemplateParser.instantiate_template(
            template, content=12345
        )
        self.assertEqual(result, "/data/primary/gpseg12345")
    
    def test_extract_parent_directory_basic(self):
        """Test parent directory extraction"""
        result = TemplateParser.extract_parent_directory(
            "/data/primary/gpseg0"
        )
        self.assertEqual(result, "/data/primary")
    
    def test_extract_parent_directory_with_trailing_slash(self):
        """Test parent directory extraction with trailing slash"""
        result = TemplateParser.extract_parent_directory(
            "/data/primary/gpseg0/"
        )
        self.assertEqual(result, "/data/primary")
    
    def test_extract_parent_directory_deep_path(self):
        """Test parent directory extraction with deep path"""
        result = TemplateParser.extract_parent_directory(
            "/mnt/disk1/data/segments/primary/gpseg0"
        )
        self.assertEqual(result, "/mnt/disk1/data/segments/primary")
    
    def test_extract_parent_directory_custom_naming(self):
        """Test parent directory extraction with custom segment naming"""
        result = TemplateParser.extract_parent_directory(
            "/data/primary/seg_data_0"
        )
        self.assertEqual(result, "/data/primary")
    
    def test_parse_comma_in_double_quotes(self):
        """Test paths with commas inside double quotes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            '"/di,r1/primary/", "/dir2/primary"'
        )
        self.assertEqual(primary, "/di,r1/primary/gpseg{content}")
        self.assertEqual(mirror, "/dir2/primary/gpseg{content}")

    def test_parse_comma_in_single_quotes(self):
        """Test paths with commas inside single quotes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "'/di,r1/primary/', '/dir2/primary'"
        )
        self.assertEqual(primary, "/di,r1/primary/gpseg{content}")
        self.assertEqual(mirror, "/dir2/primary/gpseg{content}")
    
    def test_parse_multiple_commas_in_quotes(self):
        """Test paths with multiple commas inside quotes"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            '"/path,with,many,commas/primary", "/simple/mirror"'
        )
        self.assertEqual(primary, "/path,with,many,commas/primary/gpseg{content}")
        self.assertEqual(mirror, "/simple/mirror/gpseg{content}")

class TestTemplateParserFile(GpTestCase):
    """Test suite for TemplateParser file parsing"""
    
    def setUp(self):
        """Create temporary directory for test files"""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary directory"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def _create_test_file(self, filename, content):
        """Helper to create test file"""
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath
    
    def test_parse_file_basic(self):
        """Test parsing from file with basic paths"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/primary\n/data/mirror\n"
        )
        
        primary, mirror = TemplateParser.parse_datadirs_file(filepath)
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_file_with_quotes(self):
        """Test parsing file with quoted paths"""
        filepath = self._create_test_file(
            "datadirs.txt",
            '"/data/primary"\n"/data/mirror"\n'
        )
        
        primary, mirror = TemplateParser.parse_datadirs_file(filepath)
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_file_with_blank_lines(self):
        """Test parsing file with blank lines"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "\n/data/primary\n\n/data/mirror\n\n"
        )
        
        primary, mirror = TemplateParser.parse_datadirs_file(filepath)
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_file_with_placeholders(self):
        """Test parsing file with template placeholders"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/{hostname}/primary/gpseg{content}\n"
            "/data/{hostname}/mirror/gpseg{content}\n"
        )
        
        primary, mirror = TemplateParser.parse_datadirs_file(filepath)
        self.assertEqual(primary, "/data/{hostname}/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/{hostname}/mirror/gpseg{content}")
    
    def test_parse_file_with_whitespace(self):
        """Test parsing file with extra whitespace"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "  /data/primary  \n  /data/mirror  \n"
        )
        
        primary, mirror = TemplateParser.parse_datadirs_file(filepath)
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_file_wrong_line_count_one_line(self):
        """Test file with only one line"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/primary\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "exactly 2 lines"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_wrong_line_count_three_lines(self):
        """Test file with three lines"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/primary\n/data/mirror\n/data/extra\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "exactly 2 lines"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_empty_file(self):
        """Test empty file"""
        filepath = self._create_test_file("datadirs.txt", "")
        
        with self.assertRaisesRegex(ValidationError, "exactly 2 lines"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_only_blank_lines(self):
        """Test file with only blank lines"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "\n\n\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "exactly 2 lines"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_not_exists(self):
        """Test non-existent file"""
        with self.assertRaises(FileNotFoundError):
            TemplateParser.parse_datadirs_file("/nonexistent/file.txt")
    
    def test_parse_file_identical_paths(self):
        """Test file with identical paths"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/segments\n/data/segments\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "cannot be identical"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_invalid_placeholder(self):
        """Test file with invalid placeholder"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "/data/{invalid}/primary\n/data/mirror\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "Invalid placeholder"):
            TemplateParser.parse_datadirs_file(filepath)
    
    def test_parse_file_non_absolute_path(self):
        """Test file with relative path"""
        filepath = self._create_test_file(
            "datadirs.txt",
            "data/primary\n/data/mirror\n"
        )
        
        with self.assertRaisesRegex(ValidationError, "must be absolute"):
            TemplateParser.parse_datadirs_file(filepath)

class TestTemplateParserEdgeCases(unittest.TestCase):
    """Test edge cases and corner scenarios"""
    
    def test_parse_windows_style_path_error(self):
        """Test that Windows-style paths are rejected"""
        with self.assertRaisesRegex(ValidationError, "must be absolute"):
            TemplateParser.parse_datadirs_input(
                "C:\\data\\primary, C:\\data\\mirror"
            )
    
    def test_parse_mixed_quote_types(self):
        """Test mixed quote types (one double, one single)"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            '"/data/primary", \'/data/mirror\''
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_only_first_quoted(self):
        """Test only first path quoted"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            '"/data/primary", /data/mirror'
        )
        self.assertEqual(primary, "/data/primary/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror/gpseg{content}")
    
    def test_parse_special_characters_in_path(self):
        """Test paths with special characters"""
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/primary-data_01, /data/mirror-data_01"
        )
        self.assertEqual(primary, "/data/primary-data_01/gpseg{content}")
        self.assertEqual(mirror, "/data/mirror-data_01/gpseg{content}")
    
    def test_parse_very_long_path(self):
        """Test very long paths"""
        long_path = "/data/" + "a" * 200
        primary, mirror = TemplateParser.parse_datadirs_input(
            f"{long_path}/primary, {long_path}/mirror"
        )
        self.assertEqual(primary, f"{long_path}/primary/gpseg{{content}}")
        self.assertEqual(mirror, f"{long_path}/mirror/gpseg{{content}}")
    
    def test_instantiate_template_zero_content(self):
        """Test instantiation with content=0"""
        template = "/data/primary/gpseg{content}"
        result = TemplateParser.instantiate_template(template, content=0)
        self.assertEqual(result, "/data/primary/gpseg0")
    
    def test_instantiate_multiple_hostname_placeholders(self):
        """Test instantiation with multiple hostname placeholders"""
        template = "/data/{hostname}/primary/{hostname}/gpseg{content}"
        result = TemplateParser.instantiate_template(
            template, hostname="sdw1", content=0
        )
        self.assertEqual(result, "/data/sdw1/primary/sdw1/gpseg0")
    
    def test_collision_same_parent_different_suffix(self):
        """Test that templates with same parent but different suffixes are valid"""
        # This should NOT raise an error - they resolve to different paths
        primary, mirror = TemplateParser.parse_datadirs_input(
            "/data/gpseg{content}, /data/gpmirror{content}"
        )
        self.assertEqual(primary, "/data/gpseg{content}")
        self.assertEqual(mirror, "/data/gpmirror{content}")

if __name__ == '__main__':
    run_tests()
