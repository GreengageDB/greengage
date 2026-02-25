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

        valid_data_centos7 = """
NAME="CentOS Linux"
VERSION="7 (Core)"
ID="centos"
ID_LIKE="rhel fedora"
VERSION_ID="7"
PRETTY_NAME="CentOS Linux 7 (Core)"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:centos:centos:7"
HOME_URL="https://www.centos.org/"
BUG_REPORT_URL="https://bugs.centos.org/"

CENTOS_MANTISBT_PROJECT="CentOS-7"
CENTOS_MANTISBT_PROJECT_VERSION="7"
REDHAT_SUPPORT_PRODUCT="centos"
REDHAT_SUPPORT_PRODUCT_VERSION="7"
"""

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_ubuntu24):
            self.assertEqual(('debian', 24), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_ubuntu22):
            self.assertEqual(('debian', 22), get_dist_info())

        with patch(self.open_patch, new_callable=mock_open, read_data=valid_data_centos7):
            self.assertEqual(('rhel fedora', 7), get_dist_info())

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
