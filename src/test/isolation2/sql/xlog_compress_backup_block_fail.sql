-- XLogCompressBackupBlock() used to elog(ERROR) when ZSTD compression of a
-- full-page image failed. That ERROR happens inside a critical section
-- during WAL record assembly, so it gets promoted to PANIC and crashes the
-- segment. Verify that a compression failure is now handled gracefully:
-- the segment stays up and falls back to storing the page uncompressed.
SET wal_compression = on;

-- Force every call to XLogCompressBackupBlock() to fail, on every segment.
SELECT gp_inject_fault_infinite('xlog_compress_backup_block', 'skip', dbid)
    FROM gp_segment_configuration WHERE content > -1 AND role = 'p';

CREATE TABLE xlog_compress_backup_block_fail(a int, b text) DISTRIBUTED BY (a);

INSERT INTO xlog_compress_backup_block_fail
    SELECT i, repeat('x', 200) FROM generate_series(1, 3000) i;

SELECT gp_inject_fault('xlog_compress_backup_block', 'reset', dbid)
    FROM gp_segment_configuration WHERE content > -1 AND role = 'p';

-- Segments must still be up, and every row must have made it in.
SELECT count(*) FROM xlog_compress_backup_block_fail;

DROP TABLE xlog_compress_backup_block_fail;
