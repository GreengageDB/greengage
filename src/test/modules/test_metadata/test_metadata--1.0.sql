-- test_metadata--1.0.sql
CREATE FUNCTION test_create_metadata_queue()
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_delete_metadata_queue(IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_send_metadata(IN len int4, IN gp_id int4, IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_send_empty_metadata(IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_check_metadata(IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_count_metadata(IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_clean_metadata(IN queue_id int4)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
