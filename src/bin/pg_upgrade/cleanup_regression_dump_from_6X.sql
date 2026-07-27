-- Tables partitioned by multiple columns
\c regression
DROP TABLE bfv_partition.t26002_t1;
DROP TABLE dpe_malp.malp;
DROP TABLE partition_pruning.pt_complex;
DROP TABLE public.dml_ao_pt_p;
DROP TABLE public.dml_co_pt_p;
DROP TABLE public.dml_heap_pt_p;
DROP TABLE public.equal_operator_not_in_search_path_table_multi_key;
DROP TABLE public.mpp18162c;
DROP TABLE public.mpp18162a;
DROP TABLE public.mpp18162b;
DROP TABLE public.mpp18162f;
DROP TABLE public.mpp18179;
DROP TABLE public.mpp18162d;
DROP TABLE public.mpp18162e;
DROP TABLE public.mpp5878;
DROP TABLE public.mpp5878a;


-- plpythonu dependent functions
\c isolation2test
DROP FUNCTION public.exec_cmd_on_segments(cmd text);
DROP FUNCTION public.pg_controldata_redo_lsn(datadir text);
DROP FUNCTION public.pg_ctl(datadir text, command text, command_mode text);
DROP FUNCTION public.pg_ctl_start(datadir text, port integer);

\c mapred_regression
DROP FUNCTION public.execute(cmd text);
DROP FUNCTION public.mapreduce(file text);
DROP FUNCTION public.mapreduce(file text, keys text);
DROP FUNCTION public.python_path();
DROP FUNCTION public.python_version();

\c regression
DROP FUNCTION bfv_catalog.count_operator(query text, operator text);
DROP FUNCTION bfv_legacy.count_operator(explain_query text, op_name text);
DROP FUNCTION bfv_legacy.nonzero_width(explain_query text);
DROP FUNCTION memconsumption.has_account_type(query text, search_text text);
DROP FUNCTION memconsumption.sum_owner_consumption(query text, owner text);
DROP FUNCTION partition_pruning.get_selected_parts(explain_query text);
DROP FUNCTION public.change_file_permission_readonly(path text);
DROP FUNCTION public.check_workfile_compressed(explain_query text, is_comp_buff_limit boolean);
DROP FUNCTION public.count_operator(query text, operator text);
DROP FUNCTION public.db_dirs(dboid oid);
DROP FUNCTION public.dml_fn2(x integer);
DROP FUNCTION public.get_temp_file_num();
DROP FUNCTION public.gp_tablespace_watch_log(dbid integer, message text);
DROP FUNCTION public.gp_tablespace_watch_match(dbid integer, name text, patstr text);
DROP FUNCTION public.gp_tablespace_watch_start(dbid integer, name text, location text);
DROP FUNCTION public.gp_tablespace_watch_stop();
DROP FUNCTION public.insert_correct();
DROP FUNCTION public.isspilling(explain_query text);
DROP FUNCTION public.list_tablespace_dbid_dirs(expected_number_of_tablespaces integer, tablespace_location_directory text);
DROP FUNCTION public.remove_tablespace_location_directory(tablespace_location_dir text);
DROP FUNCTION public.run_all_in_one();
DROP FUNCTION public.save_keepalives_data(result_table text);
DROP FUNCTION public.setup_tablespace_location_dir_for_test(tablespace_location_dir text);
DROP FUNCTION public.stat_table_segfile_size(datname text, tabname text);
DROP FUNCTION public.test_bigint_python();
DROP FUNCTION qp_misc_rio.func_array_argument_plpythonu(arg double precision[]);
DROP FUNCTION qp_misc_rio.func_plpythonu(n integer);
DROP FUNCTION qp_misc_rio.func_plpythonu2(x integer);
DROP FUNCTION qp_misc_rio.t18_pytest();
DROP FUNCTION qp_query_execution.qx_count_operator(query text, planner_operator text, optimizer_operator text);
DROP FUNCTION qp_with_functional_inlining.cte_func3();
DROP FUNCTION qp_with_functional_noinlining.cte_func3();
DROP FUNCTION sort_schema.has_sortmethod(explain_analyze_query text);


-- Views with removed functions
\c regression
DROP VIEW mpp7164.partagain;
DROP VIEW mpp7164.partrank;


-- Views with removed columns
\c regression
DROP MATERIALIZED VIEW public.table_relfilenode CASCADE;


-- Views with removed relations
\c regression
DROP VIEW "mpp7164"."partlist";
DROP VIEW "public"."redundantly_named_part";


-- Disallowed OPERATOR =>
\c regression
DROP OPERATOR => (int8, none);


-- Views with changed functions
\c mapred_regression
DROP VIEW public.env;


-- EXECUTE ON MASTER functions returning scalar values
\c regression_sort
DROP EXTENSION orafce CASCADE; -- Drop the whole extension, as it is not upgradable


-- Removed "abstime" data type
\c regression
DROP TABLE gpdist_legacy_opclasses.all_legacy_types;


-- Tables with OIDS
\c regression
ALTER TABLE bfv_dml.tabwithoids set without oids;
ALTER TABLE public.emp set without oids;
ALTER TABLE public.stud_emp set without oids;
ALTER TABLE public.tenk1 set without oids;
ALTER TABLE public.tt7 set without oids;
ALTER TABLE qp_dml_oids.dml_ao set without oids;
ALTER TABLE qp_dml_oids.dml_heap_check_r set without oids;
ALTER TABLE qp_dml_oids.dml_heap_p set without oids;
ALTER TABLE qp_dml_oids.dml_heap_r set without oids;
ALTER TABLE qp_dml_oids.dml_heap_with_oids set without oids;
ALTER TABLE sort_schema.gpsort_alltypes set without oids;


-- Invalid "unknown" columns
\c regression
DROP TABLE public.aocs_unknown;
DROP MATERIALIZED VIEW public.mv_unspecified_types;
DROP TABLE public.test_issue_12936;


-- Missing support function for partitions
\c regression
DROP SCHEMA equal_operator_not_in_search_path_schema CASCADE;


-- Incompatible GUC settings
\c regression
ALTER DATABASE dsp1 RESET gp_default_storage_options;
ALTER DATABASE dsp2 RESET gp_default_storage_options;


-- The way how tsquery is displayed was chagned between versions, so is may differ between dumps.
-- See "tsqueryout" and "infix" functions for more details.
\c regression
DROP TABLE public.test_tsquery;
