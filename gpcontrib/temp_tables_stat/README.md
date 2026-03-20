# temp_tables_stat

Sometimes users can create temporary tables which take significant disk space
and we need to know how much the space is being used in each session.
This extension is useful to find out how much disk space is occupied by
temporary tables and which user created them in which session.
The extension tracks the creation and deletion of temporary table files on
segments. The list of files is stored in the shared memory of each segment.
The extension adds the `tts_get_seg_files` function to get the tracked files,
their sizes, user and session where they were created from segments.

Example

```
SELECT * FROM tts_get_seg_files();
```

## How to create the extension

Add temp_tables_stat to shared_preload_libraries and restart the cluster.

```
gpconfig -c shared_preload_libraries -v \
  "$(psql -At -c \
    "SELECT array_to_string( \
        array_append( \
          string_to_array( \
            current_setting('shared_preload_libraries'), \
            ','), \
          'temp_tables_stat'), \
        ',')" \
    postgres)"
gpstop -ra
```

Create the extension in your database.

```
CREATE EXTENSION temp_tables_stat;
```
