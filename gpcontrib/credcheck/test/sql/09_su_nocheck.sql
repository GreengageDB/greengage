LOAD 'credcheck';

SET credcheck.password_min_upper To 4;
CREATE USER aaa PASSWORD 'DuMmY4P';
-- must return an error
ALTER ROLE aaa PASSWORD 'DummY2';
SET credcheck.superuser_nocheck TO on;
-- no error
ALTER ROLE aaa PASSWORD 'DummY2';
DROP ROLE aaa;
