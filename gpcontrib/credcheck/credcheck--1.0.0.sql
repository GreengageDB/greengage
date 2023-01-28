-- credcheck extension for PostgreSQL
-- Copyright (c) 2021-2023 MigOps Inc - All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION credcheck" to load this file. \quit

CREATE SCHEMA credcheck;

----
-- Table used to store the password reuse historic
----
CREATE TABLE credcheck.pg_auth_history
(
	rolename name NOT NULL,
	password_date timestamp without time zone NOT NULL,
	password_text text NOT NULL,
	PRIMARY KEY (rolename, password_text)
);
CREATE INDEX ON credcheck.pg_auth_history(password_date, rolename);

-- Include the table into pg_dump
SELECT pg_catalog.pg_extension_config_dump('credcheck.pg_auth_history', '');
