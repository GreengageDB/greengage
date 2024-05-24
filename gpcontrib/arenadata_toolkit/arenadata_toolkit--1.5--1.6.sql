/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.5--1.6.sql */

CREATE FUNCTION arenadata_toolkit.adb_hba_file_rules() RETURNS TABLE (
    line_number int4,
    type text,
    database text[],
    user_name text[],
    address text,
    netmask text,
    auth_method text,
    options text[],
    error text
) AS '$libdir/arenadata_toolkit', 'adb_hba_file_rules' LANGUAGE C VOLATILE STRICT;

CREATE VIEW arenadata_toolkit.adb_hba_file_rules_view AS
    SELECT * FROM arenadata_toolkit.adb_hba_file_rules();

REVOKE ALL on arenadata_toolkit.adb_hba_file_rules_view FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION arenadata_toolkit.adb_hba_file_rules() FROM PUBLIC;
