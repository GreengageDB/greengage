/* gpcontrib/gp_rebalance_numsegments/gp_rebalance_numsegments--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_rebalance_numsegments" to load this file. \quit

-- This function set the numsegments when creating tables during rebalance operation.
-- This form accepts an integer argument: [1, gp_num_contents_in_cluster].
CREATE FUNCTION gp_toolkit.gp_set_rebalance_numsegments(integer) RETURNS int
AS '$libdir/gp_rebalance_numsegments','gp_set_rebalance_numsegments'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gp_toolkit.gp_set_rebalance_numsegments(integer) FROM public;

-- This function reset the default numsegments.
-- This function resets numsegments directly to default value INT_MAX
CREATE FUNCTION gp_toolkit.gp_reset_rebalance_numsegments() RETURNS void
AS '$libdir/gp_rebalance_numsegments','gp_reset_rebalance_numsegments'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gp_toolkit.gp_reset_rebalance_numsegments() FROM public;

-- This function get the default numsegments when creating tables.
CREATE FUNCTION gp_toolkit.gp_get_rebalance_numsegments() RETURNS int
AS '$libdir/gp_rebalance_numsegments','gp_get_rebalance_numsegments'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gp_toolkit.gp_get_rebalance_numsegments() FROM public;

-- This function checks if the rebalance numsegments is set.
CREATE FUNCTION gp_toolkit.gp_rebalance_numsegments_is_set() RETURNS int
AS '$libdir/gp_rebalance_numsegments','gp_rebalance_numsegments_is_set'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gp_toolkit.gp_rebalance_numsegments_is_set() FROM public;
