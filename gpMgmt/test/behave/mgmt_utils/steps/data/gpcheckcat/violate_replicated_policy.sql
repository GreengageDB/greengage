CREATE FUNCTION fn_vol()
        RETURNS int
        LANGUAGE plpgsql
        IMMUTABLE
AS $$
BEGIN
	RETURN (random() * 1000)::int;
END;
$$
EXECUTE ON ANY;

CREATE FUNCTION fn_val()
        RETURNS int
        LANGUAGE plpgsql
        IMMUTABLE
AS $$
BEGIN
	RETURN 42;
END;
$$
EXECUTE ON ANY;

CREATE TABLE dist_replicated_vol1(
    a INT,
    b FLOAT,
    c TEXT DEFAULT 'Lorem Ipsum',
    d TEXT DEFAULT NULL,
    e_vol INT DEFAULT fn_vol()
) DISTRIBUTED REPLICATED;

CREATE TABLE dist_replicated_vol2(
    a_vol INT DEFAULT (42*fn_vol())+42,
    b_vol INT DEFAULT fn_val() + 42,
    c_vol FLOAT
) DISTRIBUTED REPLICATED;

CREATE TABLE dist_replicated_non_vol(
    a FLOAT,
    b FLOAT,
    c INT DEFAULT fn_val()
) DISTRIBUTED REPLICATED;

-- Replace already used function to break policy
ALTER FUNCTION fn_vol() VOLATILE;
