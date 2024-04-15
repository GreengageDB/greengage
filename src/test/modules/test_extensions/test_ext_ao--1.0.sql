/* src/test/modules/test_extensions/test_ext_ao--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext_ao" to load this file. \quit

create table test_ext_ao_table(i int) with (appendonly=true);
