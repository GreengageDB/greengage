-- Tests for the WAL-based one-phase / two-phase commit decision: the QD
-- broadcasts PREPARE at COMMIT only if some QE reported a durable change to
-- a permanent relation (see MarkWalWriteForPermanentRel()); otherwise it
-- uses the cheaper one-phase commit.
--
-- Each case arms a 'skip' fault on the expected path ('before_xlog_xact_prepare'
-- fires on PREPARE, 'start_performDtxProtocolCommitOnePhase' on one-phase
-- commit) and an 'error' fault on the wrong path.  The 'status' check after
-- COMMIT must show the expected fault hit once; a broken decision fails the
-- COMMIT itself with the injected error instead of hanging the test.

create table dtx_phase_heap(a int) distributed by (a);
create table dtx_phase_ao(a int) with (appendonly=true) distributed by (a);
create table dtx_phase_aocs(a int) with (appendonly=true, orientation=column) distributed by (a);
create table dtx_phase_prune(a int) distributed by (a);
create unlogged table dtx_phase_unlogged(a int) distributed by (a);
create table dtx_phase_lock_only(a int) distributed by (a);
-- Fill heap pages completely so a later seqscan prunes them (pruning
-- requires page free space below BLCKSZ/10).
insert into dtx_phase_prune select i from generate_series(1, 30000) i;
delete from dtx_phase_prune;

-- Case 1: a multi-segment heap write takes the two-phase path.
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_heap select i from generate_series(1, 100) i;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 2: an append-optimized row write takes the two-phase path.  AO WAL
-- records carry no block references, so this guards the explicit mark in
-- xlog_ao_insert().
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_ao select i from generate_series(1, 100) i;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 3: same for a column-oriented append-optimized write.
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_aocs select i from generate_series(1, 100) i;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 4: a lock-only transaction takes the one-phase path.  LOCK TABLE
-- logs XLOG_STANDBY_LOCK under an assigned xid but registers no blocks.
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: lock table dtx_phase_heap in access exclusive mode;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 5: a read-only transaction stays one-phase even when its scans emit
-- block-carrying maintenance WAL (page pruning, hint-bit FPIs) -- those are
-- logged without an xid.
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: select count(*) from dtx_phase_prune;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 6: work rolled back to a savepoint still forces the two-phase path.
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: savepoint sp1;
1: insert into dtx_phase_heap select i from generate_series(101, 200) i;
1: rollback to savepoint sp1;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 7: unlogged-table writes take the one-phase path (data pages of
-- unlogged relations are not WAL-logged).
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_unlogged select i from generate_series(1, 100) i;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 8: same for a temp table.  It is created in session 1 so the tested
-- transaction on that connection sees it.
1: create temp table dtx_phase_temp(a int) distributed by (a);
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_temp select i from generate_series(1, 100) i;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 9: PREPARE is broadcast to every participant, including segments
-- that only saw the lock (the single row lands on one segment).
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content >= 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content >= 0;
1: begin;
1: lock table dtx_phase_lock_only in access exclusive mode;
1: insert into dtx_phase_heap values (1);
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content >= 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content >= 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content >= 0;

-- Case 10: COPY takes the two-phase path; it receives the QE reports
-- through its own dispatch path (cdbcopy.c).
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: copy dtx_phase_heap from program 'seq 201 300';
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 11: the flag resets between transactions on reused QE backends: a
-- write takes two-phase, the next read-only transaction stays one-phase.
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: insert into dtx_phase_heap select i from generate_series(301, 310) i;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: select count(*) from dtx_phase_heap;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 12: the report is sticky across statements: lock first, write in a
-- later statement -- still two-phase.
select gp_inject_fault('before_xlog_xact_prepare', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: lock table dtx_phase_lock_only in access exclusive mode;
1: insert into dtx_phase_heap select i from generate_series(311, 320) i;
1: commit;
select gp_inject_fault('before_xlog_xact_prepare', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 13: read, lock, read -- no durable write, stays one-phase.  The
-- reads run before the lock so hint-bit FPIs are logged without an xid.
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: begin;
1: select count(*) from dtx_phase_heap;
1: lock table dtx_phase_lock_only in access exclusive mode;
1: select count(*) from dtx_phase_lock_only;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

-- Case 14: a read after LOCK TABLE (which assigned an xid) still stays
-- one-phase.  With gp_disable_tuple_hints=on (the default) the QE defers
-- hint-bit dirtying for user tables with recent xmins, so reading the
-- just-populated table emits no XLOG_FPI_FOR_HINT that could pick up the
-- lock's xid; the table's catalog rows were already hinted by the earlier
-- statements.  autovacuum is disabled on it so nothing hints it in between.
create table dtx_phase_hint_target(a int) with (autovacuum_enabled = false) distributed by (a);
insert into dtx_phase_hint_target select i from generate_series(1, 100) i;
checkpoint;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'skip', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'error', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
1: set gp_disable_tuple_hints = on;
1: begin;
1: lock table dtx_phase_lock_only in access exclusive mode;
1: select count(*) from dtx_phase_hint_target;
1: commit;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'status', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('start_performDtxProtocolCommitOnePhase', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;
select gp_inject_fault('before_xlog_xact_prepare', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = 0;

1q:
drop table dtx_phase_heap;
drop table dtx_phase_ao;
drop table dtx_phase_aocs;
drop table dtx_phase_prune;
drop table dtx_phase_unlogged;
drop table dtx_phase_lock_only;
drop table dtx_phase_hint_target;
