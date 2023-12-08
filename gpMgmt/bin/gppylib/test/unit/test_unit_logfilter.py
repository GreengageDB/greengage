#!/usr/bin/env python

import unittest
import yaml
from datetime import datetime
import gppylib.logfilter as lf

class GppylibLogFilterTestCase(unittest.TestCase):
    def test_parse_log_name(self):
        testData = [
            ('gp_era', lf.LogNameInfo('gp_era', None, True)),
            ('startup.log', lf.LogNameInfo('startup.log', None, True)),
            ('gpdb-2023-10-05_090100.csv', lf.LogNameInfo('gpdb-2023-10-05_090100.csv', datetime(2023, 10, 5, 9, 1), True)),
            ('gpdb-2023-10-05_09050.csv', lf.LogNameInfo('gpdb-2023-10-05_09050.csv', datetime(2023, 10, 5, 9, 5), True)),
            ('gpdb-2023-10-05.csv', lf.LogNameInfo('gpdb-2023-10-05.csv', datetime(2023, 10, 5, 0, 0), True)),
            ('gpdb-2023-10-05_09010.csv', lf.LogNameInfo('gpdb-2023-10-05_09010.csv', datetime(2023, 10, 5, 9, 1), True)),
            ('gpdb-23-10-05_09011.csv', lf.LogNameInfo('gpdb-23-10-05_09011.csv', None, True)), # invalid format %Y should have 4 digits
            ('gpdb-2023-1-05_2000.csv', lf.LogNameInfo('gpdb-2023-1-05_2000.csv', datetime(2023, 1, 5, 20, 0), True)),
            ('gpdb-2023-1-5_0000.csv', lf.LogNameInfo('gpdb-2023-1-5_0000.csv', datetime(2023, 1, 5, 0, 0), True)),
        ]

        for testTuple in testData:
            got = lf._parseLogFileName(testTuple[0])
            expected = testTuple[1]
            self.assertEqual(got, expected, '\nEXPECTED:\n%s\nGOT:\n%s' % (yaml.dump(got), yaml.dump(expected)))

    def test_getOrderedLogNameInfoArrByNameTS(self):
        # only file names without timestamp
        fNamesWithoutTS = ['b', 'c', 'a']
        expectedOrderWithoutTS = [
            lf.LogNameInfo('a'), lf.LogNameInfo('b'), lf.LogNameInfo('c'),
        ]
        self.assertEqual(lf._getOrderedLogNameInfoArrByNameTS(fNamesWithoutTS), expectedOrderWithoutTS)

        # only fiile names with timestamp
        fNamesWithTS = ['gpdb-2023-10-05_090100.csv', 'gpdb-2023-01-05_090100.csv', 'gpdb-2023-2-05_080000.csv']
        expectedOrderWithTS = [
            lf.LogNameInfo('gpdb-2023-01-05_090100.csv', datetime(2023, 1, 5, 9, 1), True),
            lf.LogNameInfo('gpdb-2023-2-05_080000.csv', datetime(2023, 2, 5, 8, 0), True),
            lf.LogNameInfo('gpdb-2023-10-05_090100.csv', datetime(2023, 10, 5, 9, 1), True),
        ]
        self.assertEqual(lf._getOrderedLogNameInfoArrByNameTS(fNamesWithTS), expectedOrderWithTS)

        # combination of file names with and w/o timestamps
        self.assertEqual(
            lf._getOrderedLogNameInfoArrByNameTS(fNamesWithoutTS + fNamesWithTS),
            expectedOrderWithTS + expectedOrderWithoutTS
        )

    def test_getLogInfoArrayByNamesOrderedAndMarkedInTSRange_corner(self):
        fnToTest = lf.getLogInfoArrayByNamesOrderedAndMarkedInTSRange

        ########################
        # empty input
        self.assertEqual([], fnToTest([], None, None))

        ########################
        # test `begin` date: there would be always one file which is not skipped even begin date is
        # greater than it's time stamp
        testArr = ['gpdb-2023-10-05_000000.csv']
        expectedArr = [lf.LogNameInfo('gpdb-2023-10-05_000000.csv', datetime(2023, 10, 5), True)]
        self.assertEqual(expectedArr, fnToTest(testArr, None, None))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 10, 5), None))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 10, 6), None))

        ########################
        # test `end`` date
        self.assertEqual(expectedArr, fnToTest(testArr, None, datetime(2023, 10, 6)))

        expectedArr[0].belongsToTimeRangeFilter = False
        self.assertEqual(expectedArr, fnToTest(testArr, None, datetime(2023, 10, 5)))
        self.assertEqual(expectedArr, fnToTest(testArr, None, datetime(2023, 10, 4)))

        ########################
        # test file that doesn't match time stamp pattern it would be always not skipped
        testArr = ['status.log']
        expectedArr = [lf.LogNameInfo('status.log', None, True)]
        self.assertEqual(expectedArr, fnToTest(testArr, None, None))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 10, 5), None))
        self.assertEqual(expectedArr, fnToTest(testArr, None, datetime(2023, 10, 5)))

        ########################
        # test complex queries
        testArr = [
            'gp_era',
            'startup.log',
            'gpdb-2023-10-05_090100.csv',
            'gpdb-2023-10-05_09050.csv',
            'gpdb-2023-10-05.csv',
            'gpdb-2023-10-5.csv',
            'gpdb-2023-10-05_09010.csv',
            'gpdb-23-10-05_09011.csv',
            'gpdb-2023-1-05_2000.csv',
            'gpdb-2023-1-5_0000.csv',
            'gpdb-9999-12-31_235959.csv'
        ]

        # ordered by the time stamp and after by name (file names without timestamp are considered as
        # datetime.max)
        expectedArr = [
            lf.LogNameInfo('gpdb-2023-1-5_0000.csv', datetime(2023, 1, 5, 0, 0), True), # format changed - first number of day is skipped and time doesn't contain seconds
            lf.LogNameInfo('gpdb-2023-1-05_2000.csv', datetime(2023, 1, 5, 20, 0), True), # format changed - time doesn't contain seconds
            lf.LogNameInfo('gpdb-2023-10-05.csv', datetime(2023, 10, 5, 0, 0), True), # format changed - no time prefix
            lf.LogNameInfo('gpdb-2023-10-5.csv', datetime(2023, 10, 5, 0, 0), True), # format changed - first number of day is skipped
            lf.LogNameInfo('gpdb-2023-10-05_09010.csv', datetime(2023, 10, 5, 9, 1), True), # format changed - last second is skipped
            lf.LogNameInfo('gpdb-2023-10-05_090100.csv', datetime(2023, 10, 5, 9, 1), True),
            lf.LogNameInfo('gpdb-2023-10-05_09050.csv', datetime(2023, 10, 5, 9, 5), True), # format changed - last second skipped
            lf.LogNameInfo('gpdb-9999-12-31_235959.csv', datetime(9999, 12, 31, 23, 59, 59), True), # timestamp is around datetime.max
            # files without timestamps
            lf.LogNameInfo('gp_era', None, True),
            lf.LogNameInfo('gpdb-23-10-05_09011.csv', None, True),
            lf.LogNameInfo('startup.log', None, True),
        ]

        # all matches
        self.assertEqual(expectedArr, fnToTest(testArr, datetime.min, datetime.max))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime.min, datetime.max))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 1, 5, 10), datetime.max))

        # first element doesn't match the `begin` date
        expectedArr[0].belongsToTimeRangeFilter = False
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 1, 5, 20), datetime.max))
        self.assertEqual(expectedArr, fnToTest(testArr, datetime(2023, 1, 5, 21), datetime.max))

        # `end` date is added
        expectedArr[-4].belongsToTimeRangeFilter = False
        expectedArr[-5].belongsToTimeRangeFilter = False
        expectedArr[-6].belongsToTimeRangeFilter = False
        expectedArr[-7].belongsToTimeRangeFilter = False
        self.assertEqual(expectedArr,
            fnToTest(testArr, datetime(2023, 1, 5, 20), datetime(2023, 10, 5, 9, 1))
        )
