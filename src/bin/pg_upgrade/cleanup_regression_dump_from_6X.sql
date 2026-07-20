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


-- Drop ALL partitioned tables, as the they are dumped differently based on the version of the source cluster.
\c isolation2test
DROP TABLE public.reindex_dropindex_crtab_part_aoco_btree CASCADE;
DROP TABLE public.a_partition_table_for_analyze_cancellation CASCADE;
DROP TABLE public.reindex_serialize_ins_tab_heap_part CASCADE;
DROP TABLE public.reindex_crtabforadd_part_heap_btree CASCADE;
DROP TABLE public.reindex_crtab_part_heap_btree CASCADE;
DROP TABLE public.reindex_serialize_tab_ao_part CASCADE;
DROP TABLE public.reindex_crtab_part_ao_btree CASCADE;
DROP TABLE public.reindex_crtab_part_aoco_btree CASCADE;
DROP TABLE public.reindex_dropindex_crtab_part_ao_btree CASCADE;
DROP TABLE public.ao_vacuum_drop_column CASCADE;
DROP TABLE public.reindex_dropindex_crtab_part_heap_btree CASCADE;
DROP TABLE public.reindex_ao_gist CASCADE;
DROP TABLE public.reindex_crtabforalter_part_heap_btree CASCADE;
DROP TABLE public.part_tbl_upd_del CASCADE;
DROP TABLE public.reindex_serialize_tab_heap_part CASCADE;
DROP TABLE public.reindex_crtabforalter_part_ao_btree CASCADE;
DROP TABLE public.pg_partitions_ddl_tab CASCADE;
DROP TABLE public.ao_vacuum_drop_row CASCADE;
DROP TABLE public.reindex_crtabforalter_part_aoco_btree CASCADE;
DROP TABLE public.utility_part_root CASCADE;
DROP TABLE public.reindex_crtabforadd_part_aoco_btree CASCADE;
DROP TABLE public.reindex_aoco_gist CASCADE;
DROP TABLE public.reindex_crtabforadd_part_ao_btree CASCADE;
DROP TABLE public.reindex_heap_gist CASCADE;
DROP TABLE public.lineitem CASCADE;

\c incrementalanalyze
DROP TABLE public.hll_part CASCADE;
DROP TABLE public.foo CASCADE;
DROP TABLE public.incr_analyze_test CASCADE;
DROP TABLE public.hll_part_def CASCADE;

\c regression
DROP TABLE "partition_pruning"."date_parts";
DROP TABLE "public"."constraint_pt3";
DROP TABLE dpe_multi.fact1 CASCADE;
DROP TABLE olap_window_seq.window_part_sales CASCADE;
DROP TABLE partition_ddl2.mpp3256 CASCADE;
DROP TABLE public.mpp6589b CASCADE;
DROP TABLE stat_heap4.stat_part_heap_t4 CASCADE;
DROP TABLE stat_co6.stat_part_co_t6 CASCADE;
DROP TABLE qp_query_execution.a_p CASCADE;
DROP TABLE public.test_part_relops_tmpl CASCADE;
DROP TABLE public.reorg_leaf CASCADE;
DROP TABLE bfv_partition.part_acl_test CASCADE;
DROP TABLE public.mpp7635_aoi_table2 CASCADE;
DROP TABLE alter_ao_part_exch_column.ao_part CASCADE;
DROP TABLE dpe_single.pt CASCADE;
DROP TABLE public.pt_ao_tab_rng CASCADE;
DROP TABLE alter_ao_part_tables_row.sto_altap3 CASCADE;
DROP TABLE public.dml_ao_check_s CASCADE;
DROP TABLE public.dml_heap_pt_s CASCADE;
DROP TABLE public.pt_ao_tab CASCADE;
DROP TABLE gpexplain.bitmap_btree_test CASCADE;
DROP TABLE partition_pruning.foo CASCADE;
DROP TABLE public.co_cr_sub_partzlib8192_1 CASCADE;
DROP TABLE public.co_cr_sub_partzlib8192_1_2 CASCADE;
DROP TABLE public.dml_co_pt_r CASCADE;
DROP TABLE partition_pruning.pt CASCADE;
DROP TABLE bfv_partition.pt CASCADE;
DROP TABLE partition_pruning.pt_lt_tab_df CASCADE;
DROP TABLE public.colalias_dml_decimal CASCADE;
DROP TABLE public.pt_co_tab_rng CASCADE;
DROP TABLE stat_heap6.stat_part_heap_t6 CASCADE;
DROP TABLE stat_ao5.stat_part_ao_t5 CASCADE;
DROP TABLE public.portals_updatable_rank CASCADE;
DROP TABLE partition_ddl2.mpp3080_float4 CASCADE;
DROP TABLE partition_ddl2.mpp3080_floatreal CASCADE;
DROP TABLE partition_ddl2.mpp3244 CASCADE;
DROP TABLE partition_ddl2.mpp3304_customer CASCADE;
DROP TABLE partition_pruning.partprune_foo CASCADE;
DROP TABLE public.rewrite_optimization_aoco_parent CASCADE;
DROP TABLE public.co_wt_sub_partrle_type8192_1_uncompr CASCADE;
DROP TABLE public.constraint_pt2 CASCADE;
DROP TABLE public.unnamed_index_multi_part_table CASCADE;
DROP TABLE public.partsupp_1 CASCADE;
DROP TABLE public.part_tbl CASCADE;
DROP TABLE stat_ao1.stat_part_ao_t1 CASCADE;
DROP TABLE public.test_partitioned_table_never_decrements_parruleord_to_zero CASCADE;
DROP TABLE partition_ddl2.mpp3045_hhh CASCADE;
DROP TABLE public.mpp10480 CASCADE;
DROP TABLE upgrade_cornercases.part CASCADE;
DROP TABLE stat_heap8.stat_part_heap_t8 CASCADE;
DROP TABLE partition_ddl2.mpp3216_rename CASCADE;
DROP TABLE public.ao_wt_sub_partzlib8192_5_2 CASCADE;
DROP TABLE public.aopart_lineitem CASCADE;
DROP TABLE stat_heap3.stat_part_heap_t3 CASCADE;
DROP TABLE stat_heap2.stat_part_heap_t2 CASCADE;
DROP TABLE qp_query_execution.bar_p CASCADE;
DROP TABLE alter_ao_part_tables_row.sto_alt_ao_part CASCADE;
DROP TABLE public.subpartition_aoco_leaf CASCADE;
DROP TABLE public.rewrite_optimization_heap_parent CASCADE;
DROP TABLE gpexplain.bitmap_gist_test CASCADE;
DROP TABLE partition_pruning.bar CASCADE;
DROP TABLE public.dml_co_pt_s CASCADE;
DROP TABLE stat_heap1.stat_part_heap_t1 CASCADE;
DROP TABLE public.aopart_region CASCADE;
DROP TABLE public.co_cr_sub_partzlib8192_1_uncompr CASCADE;
DROP TABLE public.test_split_part CASCADE;
DROP TABLE public.dml_heap_pt_r CASCADE;
DROP TABLE bfv_partition.timestamp_month_rangep_startincl CASCADE;
DROP TABLE partition_ddl2.mpp3059c CASCADE;
DROP TABLE public.copy_on_segment_check_distkey_subpartition CASCADE;
DROP TABLE public.idxpart CASCADE;
DROP TABLE dpe_bugs.pat CASCADE;
DROP TABLE public.aopart_part CASCADE;
DROP TABLE public.mpp17707 CASCADE;
DROP TABLE public.multivarblock_parttab CASCADE;
DROP TABLE qp_orca_fallback.homer CASCADE;
DROP TABLE partition_ddl2.mpp3080_floatdouble CASCADE;
DROP TABLE public.co_wt_sub_partrle_type8192_1 CASCADE;
DROP TABLE public.oid_check_ao_pt1 CASCADE;
DROP TABLE public.oid_check_co_pt1 CASCADE;
DROP TABLE bfv_statistic.t25289_t4 CASCADE;
DROP TABLE partition_ddl2.mpp3282_partsupp CASCADE;
DROP TABLE public.volatilefn_dml_int8 CASCADE;
DROP TABLE public.tbl_default_distribution CASCADE;
DROP TABLE public.pt_tab_encode CASCADE;
DROP TABLE public.distrib_part_test CASCADE;
DROP TABLE public.dml_heap_check_s CASCADE;
DROP TABLE public.mpp6589a CASCADE;
DROP TABLE public.pt_indx_tab CASCADE;
DROP TABLE public.pt_heap_tab_rng CASCADE;
DROP TABLE alter_ao_part_tables_splitpartition_column.sto_alt_uao_part_splitpartition CASCADE;
DROP TABLE qp_query_execution.abbp CASCADE;
DROP TABLE alter_ao_part_tables_column.sto_alt_ao_part CASCADE;
DROP TABLE public.foo1 CASCADE;
DROP TABLE public.update_gp_foo1 CASCADE;
DROP TABLE partition_ddl2.mpp3059d CASCADE;
DROP TABLE public.orders CASCADE;
DROP TABLE stat_ao2.stat_part_ao_t2 CASCADE;
DROP TABLE qp_query_execution.lossmithe_colstor CASCADE;
DROP TABLE public.pxn CASCADE;
DROP TABLE public.aopart_partsupp CASCADE;
DROP TABLE public.co_wt_sub_partrle_type8192_1_2_uncompr CASCADE;
DROP TABLE public.t_ao_alias_31345_partsupp CASCADE;
DROP TABLE public.sto_ao_ao CASCADE;
DROP TABLE public.at CASCADE;
DROP TABLE public.multi_segfile_parttab CASCADE;
DROP TABLE bfv_partition.timestamp_month_listp CASCADE;
DROP TABLE partition_ddl2.mpp3080_numeric CASCADE;
DROP TABLE partition_pruning.pt_lt_tab CASCADE;
DROP TABLE public.ao_wt_sub_partzlib8192_5_2_uncompr CASCADE;
DROP TABLE public.mpp10223 CASCADE;
DROP TABLE alter_ao_part_exch_row.ao_part CASCADE;
DROP TABLE partition_ddl2.mpp3080_float8 CASCADE;
DROP TABLE public.aopart_customer CASCADE;
DROP TABLE public.copy_on_segment_partion_dist_randomly CASCADE;
DROP TABLE public.issue_14186 CASCADE;
DROP TABLE public.pfoo CASCADE;
DROP TABLE alter_ao_part_tables_splitpartition_row.sto_alt_uao_part_splitpartition CASCADE;
DROP TABLE partition_ddl2.parttab CASCADE;
DROP TABLE public.co_wt_sub_partrle_type8192_1_2 CASCADE;
DROP TABLE stat_heap9.stat_part_heap_t9 CASCADE;
DROP TABLE gpexplain.bitmap_bitmap_test CASCADE;
DROP TABLE stat_ao6.stat_part_ao_t6 CASCADE;
DROP TABLE alter_ao_part_tables_column.sto_altap3 CASCADE;
DROP TABLE partition_ddl2.mpp3059a_rename CASCADE;
DROP TABLE stat_heap5.stat_part_heap_t5 CASCADE;
DROP TABLE stat_co7.stat_part_co_t7 CASCADE;
DROP TABLE public.part_tab CASCADE;
DROP TABLE partition_ddl2.mpp3059 CASCADE;
DROP TABLE partition_pruning.part_left CASCADE;
DROP TABLE public.mpp3033b CASCADE;
DROP TABLE public.test_switch_generic CASCADE;
DROP TABLE partition_ddl2.mpp3238_supplier CASCADE;
DROP TABLE public.ao_wt_sub_partzlib8192_5 CASCADE;
DROP TABLE public.dml_heap_check_r CASCADE;
DROP TABLE stat_co2.stat_part_co_t2 CASCADE;
DROP TABLE stat_ao7.stat_part_ao_t7 CASCADE;
DROP TABLE partition_ddl2.mpp3080_numericbig CASCADE;
DROP TABLE public.dcl_messaging_test CASCADE;
DROP TABLE public.mpp14613_range CASCADE;
DROP TABLE public.mpp17740 CASCADE;
DROP TABLE stat_co5.stat_part_co_t5 CASCADE;
DROP TABLE public.vacuum_gp_pt CASCADE;
DROP TABLE mpp7164.mpp7164r CASCADE;
DROP TABLE public.copy_on_segment_check_distkey_partioned CASCADE;
DROP TABLE bfv_partition.grant_test CASCADE;
DROP TABLE public.pt_xchg CASCADE;
DROP TABLE dpe_malp.apart CASCADE;
DROP TABLE public.dml_union_s CASCADE;
DROP TABLE bfv_partition.bar CASCADE;
DROP TABLE partition_ddl2.mpp3244a CASCADE;
DROP TABLE public.copy_from_partition_table CASCADE;
DROP TABLE public.dml_ao_pt_r CASCADE;
DROP TABLE public.mpp3033a CASCADE;
DROP TABLE stat_ao3.stat_part_ao_t3 CASCADE;
DROP TABLE partition_with_user_defined_function.some_partitioned_table CASCADE;
DROP TABLE public.bfv_subquery_ CASCADE;
DROP TABLE public.constraint_pt1 CASCADE;
DROP TABLE public.mpp7232a CASCADE;
DROP TABLE partition_ddl2.mpp3377_sales CASCADE;
DROP TABLE public.alter_dist_key_for_aoco_partition_table CASCADE;
DROP TABLE qp_orca_fallback.partition_key_dropped CASCADE;
DROP TABLE public.t_ao_alias_31345 CASCADE;
DROP TABLE partition_ddl2.mpp3250 CASCADE;
DROP TABLE public.dis_tupdesc CASCADE;
DROP TABLE public.mixed_ao_part CASCADE;
DROP TABLE public.aopart_nation CASCADE;
DROP TABLE public.mpp6589i CASCADE;
DROP TABLE public.oid_check_pt1 CASCADE;
DROP TABLE qp_query_execution.foo_p CASCADE;
DROP TABLE partition_ddl2.mpp3080_int8 CASCADE;
DROP TABLE stat_co4.stat_part_co_t4 CASCADE;
DROP TABLE bfv_partition.timestamp_month_rangep_startexcl CASCADE;
DROP TABLE public.aopart_supplier CASCADE;
DROP TABLE public.pt_with_multikey_index CASCADE;
DROP TABLE public.pt_heap_tab CASCADE;
DROP TABLE public.deep_part CASCADE;
DROP TABLE public.dml_ao_check_r CASCADE;
DROP TABLE dpe_single.pt1 CASCADE;
DROP TABLE public.mpp5992 CASCADE;
DROP TABLE public.alter_dist_key_for_ao_partition_table CASCADE;
DROP TABLE public.ao_wt_sub_partzlib8192_5_uncompr CASCADE;
DROP TABLE public.dml_ao_pt_s CASCADE;
DROP TABLE stat_co1.stat_part_co_t1 CASCADE;
DROP TABLE partition_ddl2.mpp3375a CASCADE;
DROP TABLE public.at_range CASCADE;
DROP TABLE public.dml_co_check_s CASCADE;
DROP TABLE stat_heap7.stat_part_heap_t7 CASCADE;
DROP TABLE memconsumption.bar_part CASCADE;
DROP TABLE public.unnamed_index_part_table CASCADE;
DROP TABLE public.two_level_pt CASCADE;
DROP TABLE public.co_cr_sub_partzlib8192_1_2_uncompr CASCADE;
DROP TABLE stat_ao4.stat_part_ao_t4 CASCADE;
DROP TABLE public.equal_operator_not_in_search_path_table CASCADE;
DROP TABLE gpexplain.bitmap_spgist_test CASCADE;
DROP TABLE stat_co3.stat_part_co_t3 CASCADE;
DROP TABLE public.subt_alter_part_tab_ao1 CASCADE;
DROP TABLE gpexplain.bitmap_gin_test CASCADE;
DROP TABLE partition_pruning.part_right CASCADE;
DROP TABLE public.at_list CASCADE;
DROP TABLE public.mpp7232b CASCADE;
DROP TABLE public.mpp7863 CASCADE;
DROP TABLE public.update_gp_foo CASCADE;
DROP TABLE public.pt_co_tab CASCADE;
DROP TABLE public.pnx CASCADE;
DROP TABLE partition_ddl2.mpp3059b CASCADE;
DROP TABLE partition_subquery.t1 CASCADE;
DROP TABLE public.aopart_orders CASCADE;
DROP TABLE public.dml_co_check_r CASCADE;
DROP TABLE public.mpp10223b CASCADE;
DROP TABLE public.some_partitioned_table_to_truncate CASCADE;
DROP TABLE partition_ddl2.mpp2564_transactions CASCADE;
DROP TABLE public.dtm_plpg_foo CASCADE;

\c dsp3
DROP TABLE public.dsp_partition1 CASCADE;

\c testanalyze
DROP TABLE public.no_eqop CASCADE;
