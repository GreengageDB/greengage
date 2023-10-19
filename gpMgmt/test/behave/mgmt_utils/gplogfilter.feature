@gplogfilter
Feature: gplogfilter tests
    Scenario: invalid begin and end arguments
        When the user runs "gplogfilter -b "2023-10-05 09:01" -e "2023-10-05 09:01""
        Then gplogfilter should print "gplogfilter: \(IOError\) "Invalid arguments: "begin" date \(2023-10-05 09:01:00\) is >= "end" date \(2023-10-05 09:01:00\)"" error message
        When the user runs "gplogfilter -b "2023-10-05 09:01:01" -e "2023-10-05 09:00:00""
        Then gplogfilter should print "gplogfilter: \(IOError\) "Invalid arguments: "begin" date \(2023-10-05 09:01:01\) is >= "end" date \(2023-10-05 09:00:00\)"" error message

    Scenario: time range covers all files
        Given log directory with files
            | name                       | start_time          |
            | status.log                 | 2023-01-01 00:00:00 |
            | gpdb-2023-1-1_000000.csv   | 2023-01-01 00:00:00 |
            | gpdb-2022-12-31_235959.csv | 2022-12-31 23:59:59 |
        When under changed DATA_DIR user runs "gplogfilter"
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2022-12-31_235959.csv | 10    |
            | gpdb-2023-1-1_000000.csv   | 10    |
            | status.log                 | 10    |
        When under changed DATA_DIR user runs "gplogfilter -b "1990-1-1" -e "9999-12-31""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2022-12-31_235959.csv | 10    |
            | gpdb-2023-1-1_000000.csv   | 10    |
            | status.log                 | 10    |
        When under changed DATA_DIR user runs "gplogfilter -b "2022-12-31""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2022-12-31_235959.csv | 10    |
            | gpdb-2023-1-1_000000.csv   | 10    |
            | status.log                 | 10    |
        When under changed DATA_DIR user runs "gplogfilter -e "2024-1-1""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2022-12-31_235959.csv | 10    |
            | gpdb-2023-1-1_000000.csv   | 10    |
            | status.log                 | 10    |

    Scenario: time range doesn't covers all files
        Given log directory with files
            | name                       | start_time          |
            | status.log                 | 2023-01-05 00:00:00 |
            | gpdb-2023-1-5_0000.csv     | 2023-01-05 00:00:00 |
            | gpdb-2023-1-05_2000.csv    | 2023-01-05 20:00:00 |
            | gpdb-2023-10-05_09010.csv  | 2023-10-05 09:01:00 |
            | gpdb-2023-10-05_090100.csv | 2023-10-05 09:04:00 |
            | gpdb-2023-10-05_09050.csv  | 2023-10-05 09:05:00 |
        When under changed DATA_DIR user runs "gplogfilter -b "2023-10-05 09:01""
        # status.log has timestamps ranging from 00:00:00 to 00:01:30, so 0 log lines match the filter
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2023-10-05_09010.csv  | 10    |
            | gpdb-2023-10-05_090100.csv | 10    |
            | gpdb-2023-10-05_09050.csv  | 10    |
            | status.log                 | 0     |
        When under changed DATA_DIR user runs "gplogfilter -e "2023-01-05 20:00""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2023-1-5_0000.csv     | 10    |
            | status.log                 | 10    |
        When under changed DATA_DIR user runs "gplogfilter -b "2023-10-05" -e "2023-10-05 09:05""
        # gpdb-2023-10-05_090100.csv has timestamps ranging from 09:04:00 to 09:05:30, so 6 log lines match the filter (from 09:04:00 to 09:04:50)
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       | count |
            | gpdb-2023-1-05_2000.csv    | 0     |
            | gpdb-2023-10-05_09010.csv  | 10    |
            | gpdb-2023-10-05_090100.csv | 6     |
            | status.log                 | 0     |
