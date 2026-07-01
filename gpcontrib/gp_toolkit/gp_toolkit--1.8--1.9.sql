/* gpcontrib/gp_toolkit/gp_toolkit--1.8--1.9.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.9" to load this file. \quit

GRANT SELECT ON gp_toolkit.gp_resgroup_status_per_segment TO public;
