-- ZSTD_compressCCtx() reporting ZSTD_error_dstSize_tooSmall is a routine
-- outcome (the page just didn't compress small enough to fit), not a real
-- failure -- it is expected to happen regularly on ordinary data, not only
-- on pathological input. Verify XLogCompressBackupBlock() still handles it
-- gracefully: no crash, and the page falls back to being stored
-- uncompressed.
SET wal_compression = on;

-- Force ZSTD_compressCCtx() to genuinely report dstSize_tooSmall on every
-- call, on every segment, by shrinking its output buffer.
SELECT gp_inject_fault_infinite('xlog_compress_backup_block_dstsize_too_small', 'skip', dbid)
    FROM gp_segment_configuration WHERE content > -1 AND role = 'p';

CREATE TABLE xlog_compress_backup_block_dstsize_too_small(a int, b text) DISTRIBUTED BY (a);

INSERT INTO xlog_compress_backup_block_dstsize_too_small
    SELECT i, repeat('x', 200) FROM generate_series(1, 3000) i;

SELECT gp_inject_fault('xlog_compress_backup_block_dstsize_too_small', 'reset', dbid)
    FROM gp_segment_configuration WHERE content > -1 AND role = 'p';

-- Segments must still be up, and every row must have made it in.
SELECT count(*) FROM xlog_compress_backup_block_dstsize_too_small;

DROP TABLE xlog_compress_backup_block_dstsize_too_small;
