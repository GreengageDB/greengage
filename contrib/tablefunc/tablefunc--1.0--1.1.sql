/* contrib/tablefunc/tablefunc--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION tablefunc UPDATE TO '1.1'" to load this file. \quit

ALTER function crosstab(text) EXECUTE ON INITPLAN;
ALTER function crosstab2(text) EXECUTE ON INITPLAN;
ALTER function crosstab3(text) EXECUTE ON INITPLAN;
ALTER function crosstab4(text) EXECUTE ON INITPLAN;
ALTER function crosstab(text,integer) EXECUTE ON INITPLAN;
ALTER function crosstab(text,text) EXECUTE ON INITPLAN;
ALTER function connectby(text,text,text,text,integer,text) EXECUTE ON INITPLAN;
ALTER function connectby(text,text,text,text,integer) EXECUTE ON INITPLAN;
ALTER function connectby(text,text,text,text,text,integer,text) EXECUTE ON INITPLAN;
ALTER function connectby(text,text,text,text,text,integer) EXECUTE ON INITPLAN;
