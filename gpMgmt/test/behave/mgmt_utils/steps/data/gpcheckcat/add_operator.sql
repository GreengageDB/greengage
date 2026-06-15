-- Create a function that will be added as an operator
CREATE FUNCTION my_pk_schema.is_zero(int) RETURNS TABLE(f1 boolean)
AS $$ select $1 = 0 $$  LANGUAGE SQL;

-- Use a prefix operator (RIGHTARG): PostgreSQL 14 removed support for
-- postfix (LEFTARG-only) operators. A prefix operator yields an equivalent
-- pg_operator entry with the non-oid primary key that this test relies on.
CREATE OPERATOR my_pk_schema.!# (PROCEDURE = my_pk_schema.is_zero,RIGHTARG = integer);
