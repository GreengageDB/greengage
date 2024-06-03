@gplogfilter
Feature: gplogfilter tests
    Scenario: invalid begin and end arguments
        When the user runs "gplogfilter -b "2023-10-05 09:01" -e "2023-10-05 09:01""
        Then gplogfilter should print "gplogfilter: \(IOError\) "Invalid arguments: "begin" date \(2023-10-05 09:01:00\) is >= "end" date \(2023-10-05 09:01:00\)"" error message
        When the user runs "gplogfilter -b "2023-10-05 09:01:01" -e "2023-10-05 09:00:00""
        Then gplogfilter should print "gplogfilter: \(IOError\) "Invalid arguments: "begin" date \(2023-10-05 09:01:01\) is >= "end" date \(2023-10-05 09:00:00\)"" error message

    Scenario: time range covers all files
        Given log directory with files
            | name                       |
            | status.log                 |
            | gpdb-2023-1-1_000000.csv   |
            | gpdb-2022-12-31_235959.csv |
        When under changed DATA_DIR user runs "gplogfilter"
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2022-12-31_235959.csv |
            | gpdb-2023-1-1_000000.csv   |
            | status.log                 |
        When under changed DATA_DIR user runs "gplogfilter -b "1990-1-1" -e "9999-12-31""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2022-12-31_235959.csv |
            | gpdb-2023-1-1_000000.csv   |
            | status.log                 |
        When under changed DATA_DIR user runs "gplogfilter -b "2022-12-31""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2022-12-31_235959.csv |
            | gpdb-2023-1-1_000000.csv   |
            | status.log                 |
        When under changed DATA_DIR user runs "gplogfilter -e "2024-1-1""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2022-12-31_235959.csv |
            | gpdb-2023-1-1_000000.csv   |
            | status.log                 |

    Scenario: time range doesn't covers all files
        Given log directory with files
            | name                       |
            | status.log                 |
            | gpdb-2023-1-5_0000.csv     |
            | gpdb-2023-1-05_2000.csv    |
            | gpdb-2023-10-05_09010.csv  |
            | gpdb-2023-10-05_090100.csv |
            | gpdb-2023-10-05_09050.csv  |
        When under changed DATA_DIR user runs "gplogfilter -b "2023-10-05 09:01""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2023-10-05_09010.csv  |
            | gpdb-2023-10-05_090100.csv |
            | gpdb-2023-10-05_09050.csv  |
            | status.log                 |
        When under changed DATA_DIR user runs "gplogfilter -e "2023-01-05 20:00""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2023-1-5_0000.csv     |
            | status.log                 |
        When under changed DATA_DIR user runs "gplogfilter -b "2023-10-05" -e "2023-10-05 09:05""
        Then gplogiflter shouldn't skip next files from directory (files ordered by time stamp and by name)
            | name                       |
            | gpdb-2023-1-05_2000.csv    |
            | gpdb-2023-10-05_09010.csv  |
            | gpdb-2023-10-05_090100.csv |
            | status.log                 |
