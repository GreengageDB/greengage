from mock import *
from .gp_unittest import *
import socket

from gppylib.utils import escape_string, validateHostnameAddress

class UtilsFunctionsTest(GpTestCase):
    def test_escape_string_can_handle_utf8(self):
        self.assertEqual('public."spiegelungssätze"', escape_string('public."spiegelungssätze"'))


class HostnameAddressValidationTest(GpTestCase):
    def test_forward_resolution_needs_no_ptr(self):
        for address, resolved_ip, family in [
                ('10.20.0.30', '10.20.0.30', socket.AF_INET),
                ('2001:0db8:0:0:0:0:0:30', '2001:db8::30', socket.AF_INET6)]:
            with self.subTest(address=address), \
                 patch('gppylib.utils.socket.getaddrinfo', return_value=[
                     (family, socket.SOCK_STREAM, 6, '', (resolved_ip, 0))]) as forward, \
                 patch('gppylib.utils.gethostbyaddr', side_effect=socket.herror('No PTR')) as reverse:
                self.assertTrue(validateHostnameAddress('sdw1', address))
                forward.assert_called_once_with('sdw1', None, socket.AF_UNSPEC, socket.SOCK_STREAM)
                reverse.assert_not_called()

    def test_alias_uses_only_reverse_resolution(self):
        for alias_ip, expected in [('10.20.0.30', True), ('10.20.0.31', False)]:
            with self.subTest(alias_ip=alias_ip), \
                 patch('gppylib.utils.socket.getaddrinfo') as forward, \
                 patch('gppylib.utils.gethostbyaddr', side_effect=[
                     ('sdw1', [], ['10.20.0.30']), ('sdw1-db', [], [alias_ip])]):
                self.assertEqual(expected, validateHostnameAddress('sdw1', 'sdw1-db'))
                forward.assert_not_called()

    def test_reverse_fallback_preserves_existing_pairs(self):
        with patch('gppylib.utils.socket.getaddrinfo', side_effect=socket.gaierror()), \
             patch('gppylib.utils.gethostbyaddr', return_value=('sdw1', [], ['10.20.0.30'])):
            self.assertTrue(validateHostnameAddress('sdw1', '10.20.0.30'))

    def test_mismatched_addresses_are_rejected(self):
        with patch('gppylib.utils.socket.getaddrinfo', return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.20.0.30', 0))]), \
             patch('gppylib.utils.gethostbyaddr', side_effect=[
                 ('sdw1', [], ['10.20.0.30']), ('sdw2', [], ['10.20.0.31'])]):
            self.assertFalse(validateHostnameAddress('sdw1', '10.20.0.31'))

    def test_unresolvable_pair_is_rejected(self):
        with patch('gppylib.utils.socket.getaddrinfo', side_effect=socket.gaierror()), \
             patch('gppylib.utils.gethostbyaddr', side_effect=socket.herror()):
            self.assertFalse(validateHostnameAddress('sdw1', '10.20.0.30'))
