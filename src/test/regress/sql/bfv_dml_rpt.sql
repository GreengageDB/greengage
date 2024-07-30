-- Check, that SELECT over CTE, which contains DML, works without an error
DO $$
DECLARE
    res INT;
BEGIN
    CREATE TEMPORARY TABLE t_test(a INT) DISTRIBUTED REPLICATED;

    WITH cte AS (
        INSERT INTO t_test(a) VALUES (1), (2), (3) RETURNING a
    ) SELECT sum(a) FROM cte INTO res;
    RAISE NOTICE '%', res;

    WITH cte AS (
        UPDATE t_test SET a = a+1 RETURNING a
    ) SELECT sum(a) FROM cte INTO res;
    RAISE NOTICE '%', res;

    WITH cte AS (
        DELETE FROM t_test WHERE a < 4 RETURNING a
    ) SELECT sum(a) FROM cte INTO res;
    RAISE NOTICE '%', res;

    DROP TABLE t_test;
END;
$$;

-- Check, that INSERT produce the error with correct explanation
DO $$
DECLARE
    res INT;
BEGIN
    CREATE TEMPORARY TABLE t_test(a INT) DISTRIBUTED REPLICATED;
    INSERT INTO t_test(a) VALUES (1), (2), (3) RETURNING a INTO res;
    RAISE NOTICE '%', res;
    DROP TABLE t_test;
END;
$$;

-- Check, that INSERT will be executed without an error
DO $$
DECLARE
    res INT;
BEGIN
    CREATE TEMPORARY TABLE t_test(a INT) DISTRIBUTED REPLICATED;
    INSERT INTO t_test(a) VALUES (1) RETURNING a INTO res;
    RAISE NOTICE '%', res;
    DROP TABLE t_test;
END;
$$;

-- Check correct work at the utility mode
CREATE TABLE t_test(a int) DISTRIBUTED REPLICATED;
INSERT INTO t_test(a) VALUES (1), (2), (3);
\! PGOPTIONS='-c gp_session_role=utility' psql -p 6002 -d regression -c 'UPDATE t_test SET a = 1 WHERE a = 2;'
\! PGOPTIONS='-c gp_session_role=utility' psql -p 6002 -d regression -c 'SELECT * FROM t_test ORDER BY 1;'
DROP TABLE t_test;
