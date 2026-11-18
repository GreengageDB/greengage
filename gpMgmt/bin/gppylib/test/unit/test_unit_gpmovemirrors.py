# Copyright (c) 2026-Present, Greengage Community

import ast
from pathlib import Path
from tempfile import TemporaryDirectory
import types

from mock import Mock, patch
from gppylib.gparray import Segment
from gppylib.programs.clsRecoverSegment_triples import RecoveryTripletsUserConfigFile
from gppylib.test.unit.gp_unittest import GpTestCase, run_tests


class MoveMirrorsConfigTest(GpTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        filename = str(Path(__file__).resolve().parents[3] / 'gpmovemirrors')
        with open(filename) as source:
            tree = ast.parse(source.read(), filename)
        # The script's final try block runs the CLI on import. Load only its
        # definitions and globals so tests cannot start a mirror move.
        assert isinstance(tree.body[-1], ast.Try)
        tree.body.pop()
        cls.subject = types.ModuleType('gpmovemirrors')
        cls.subject.__file__ = filename
        exec(compile(tree, filename, 'exec'), cls.subject.__dict__)

    def setUp(self):
        tempdir = TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.moves = Path(tempdir.name) / 'moves'
        self.backout = Path(tempdir.name) / 'backout'
        self.config = self.subject.Configuration()
        self.segment = Segment.initFromString(
            '7|2|m|m|s|u|sdw3|172.19.0.5|21500|/data/mirror/gpseg2')
        self.catalog = Mock()
        self.catalog.getDbList.return_value = [self.segment]
        self.legacy = ('172.19.0.5|21500|/data/mirror/gpseg2 '
                       '172.19.0.3|21501|/new/mirror/gpseg2\n')
        self.with_hostnames = (
            'sdw3|172.19.0.5|21500|/data/mirror/gpseg2 '
            'sdw1|172.19.0.3|21501|/new/mirror/gpseg2\n')

    def read_config(self, contents, valid_pair=True):
        self.moves.write_text(contents)
        with patch.object(self.subject, 'gpArrayInstance', self.catalog, create=True), \
             patch.object(self.subject.utils, 'validateHostnameAddress', return_value=valid_pair) as validate:
            self.config.read_input_file(str(self.moves))
        return validate

    def test_legacy_input_and_backout(self):
        validate = self.read_config(self.legacy)
        validate.assert_not_called()
        old, new = self.legacy.split()
        self.config.write_output_file(self.backout, oldToNew=False)
        self.assertEqual(f'{new} {old}\n', self.backout.read_text())

    def test_four_field_move_and_backout_reach_recovery_with_both_fields(self):
        validate = self.read_config(self.with_hostnames)
        validate.assert_called_once_with('sdw1', '172.19.0.3')
        self.config.write_output_file(self.backout, oldToNew=False)
        old, new = self.with_hostnames.split()
        self.assertEqual(f'{new} {old}\n', self.backout.read_text())
        for path, source, target in [(self.config.inputFile, old, new), (self.backout, new, old)]:
            row, = RecoveryTripletsUserConfigFile._parseConfigFile(path)
            self.assertEqual([row['failedHostname'], row['failedAddress']], source.split('|')[:2])
            self.assertEqual([row['newHostname'], row['newAddress']], target.split('|')[:2])

    def test_backout_preserves_literal_ipv6_addresses(self):
        line = self.with_hostnames.replace('172.19.0.5', '2001:db8::5').replace('172.19.0.3', '2001:db8::3')
        self.segment.setSegmentAddress('2001:db8::5')
        self.read_config(line)
        old, new = line.split()
        self.config.write_output_file(self.backout, oldToNew=False)
        self.assertEqual(f'{new} {old}\n', self.backout.read_text())

    def test_rejects_mixed_field_counts(self):
        line = self.with_hostnames.split()[0] + ' ' + self.legacy.split()[1]
        with self.assertRaisesRegex(Exception, 'expected 3 or 4 parts'):
            self.config._getParsedRow(1, line)

    def test_rejects_invalid_field_counts(self):
        with self.assertRaisesRegex(Exception, 'expected 3 or 4 parts'):
            self.config._getParsedRow(1, 'sdw3|21500 sdw1|21501')

    def test_rejects_empty_hostname(self):
        with self.assertRaisesRegex(Exception, 'Invalid hostname'):
            self.config._getParsedRow(1, self.with_hostnames.replace('sdw1|', '|'))

    def test_rejects_old_hostname_mismatch(self):
        with self.assertRaisesRegex(Exception, 'Old mirror hostname does not match'):
            self.read_config(self.with_hostnames.replace('sdw3|', 'wrong|'))

    def test_rejects_destination_hostname_address_mismatch(self):
        with self.assertRaisesRegex(Exception, 'Destination hostname and address'):
            self.read_config(self.with_hostnames, valid_pair=False)

    def test_same_host_and_directory_via_another_address_stays_in_place(self):
        self.read_config('sdw3|172.19.0.5|21500|/data/mirror/gpseg2 '
                         'sdw3|172.19.1.5|21501|/data/mirror/gpseg2')
        self.assertTrue(self.config.oldMirrorList[0].inPlace)


if __name__ == '__main__':
    run_tests()
