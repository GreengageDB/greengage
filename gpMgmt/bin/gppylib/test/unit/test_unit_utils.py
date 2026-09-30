import unittest

from mock import patch, mock_open

from .gp_unittest import GpTestCase
import sys
from gppylib.utils import get_dist_info

class UtilsTestCase(GpTestCase):

    def setUp(self):
        if sys.version_info[0] == 2:
            self.open_patch = '__builtin__.open'
        else:
            self.open_patch = 'builtins.open'

    def test_get_dist_info_valid_real(self):
        valid_data_ubuntu24 = """
NAME="Ubuntu"
VERSION_ID="24.04"
VERSION="24.04.3 LTS (Noble Numbat)"
VERSION_CODENAME=noble
ID=ubuntu
ID_LIKE=debian
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
UBUNTU_CODENAME=noble
LOGO=ubuntu-logo
        """

        valid_data_ubuntu22 = """
PRETTY_NAME="Ubuntu 22.04.5 LTS"
NAME="Ubuntu"
VERSION_ID="22.04"
VERSION="22.04.5 LTS (Jammy Jellyfish)"
VERSION_CODENAME=jammy
ID=ubuntu
ID_LIKE=debian
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
UBUNTU_CODENAME=jammy
        """

        valid_data_rocky8 = """
NAME="Rocky Linux"
VERSION="8.9 (Green Obsidian)"
ID="rocky"
ID_LIKE="rhel centos fedora"
VERSION_ID="8.9"
PLATFORM_ID="platform:el8"
PRETTY_NAME="Rocky Linux 8.9 (Green Obsidian)"
ANSI_COLOR="0;32"
LOGO="fedora-logo-icon"
CPE_NAME="cpe:/o:rocky:rocky:8:GA"
HOME_URL="https://rockylinux.org/"
BUG_REPORT_URL="https://bugs.rockylinux.org/"
SUPPORT_END="2029-05-31"
ROCKY_SUPPORT_PRODUCT="Rocky-Linux-8"
ROCKY_SUPPORT_PRODUCT_VERSION="8.9"
REDHAT_SUPPORT_PRODUCT="Rocky Linux"
REDHAT_SUPPORT_PRODUCT_VERSION="8.9"
"""

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_ubuntu24):
            self.assertEqual(('debian', 24), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_ubuntu22):
            self.assertEqual(('debian', 22), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_rocky8):
            self.assertEqual(('rhel centos fedora', 8), get_dist_info())

    def test_get_dist_valid_edge_cases(self):
        no_id_like = """
VERSION_ID="24.04"
ID=ubuntu
        """

        no_id_version_id = """
VERSION="22.04.5 LTS (Jammy Jellyfish)"
ID_LIKE=debian
        """

        with patch(self.open_patch, new_callable=mock_open, read_data=no_id_like):
            self.assertEqual(('ubuntu', 24), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=no_id_version_id):
            self.assertEqual(('debian', 22), get_dist_info())

    def test_get_dist_invalid(self):

        invalid_id_line = "ID_LIKE=123=123"
        no_number_in_version = "VERSION_ID=test"

        # empty file
        with patch(self.open_patch, new_callable=mock_open, read_data=""):
            self.assertEqual((None, None), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=invalid_id_line):
            self.assertEqual((None, None), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=no_number_in_version):
            self.assertEqual((None, None), get_dist_info())



if __name__ == '__main__':
    unittest.main()
