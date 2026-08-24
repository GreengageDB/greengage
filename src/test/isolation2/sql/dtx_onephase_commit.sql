-- Tests for the WAL-based one-phase / two-phase commit decision.
--
-- At COMMIT the QD broadcasts PREPARE only when some QE reported a durable
-- change to a permanent relation: a WAL record carrying registered block
-- references logged under an assigned xid, or an append-optimized write
-- record (see MarkWalWriteForPermanentRel()).  A transaction whose QEs
-- emitted only lock / invalidation / read-only-maintenance WAL commits with
-- the cheaper one-phase protocol.
--
-- Observability: fault 'before_xlog_xact_prepare' fires on a QE iff it
-- executes PREPARE (two-phase path); 'start_performDtxProtocolCommitOnePhase'
-- fires on a QE iff it executes one-phase commit.  For a given COMMIT the two
-- are mutually exclusive and the broadcast completes before COMMIT returns,
-- so each case asserts synchronously, without waiting:
-- * a 'skip' fault on the expected path, checked with 'status' right after
--   the commit -- num times hit:'1' proves the path was taken;
-- * an 'error' fault on the wrong path -- if the decision logic is broken,
--   the COMMIT itself fails immediately with the injected error instead of
--   the test hanging on a wait.

create table dtx_phase_heap(a int) distributed by (a);
create table dtx_phase_ao(a int) with (appendonly=true) distributed by (a);
create table dtx_phase_aocs(a int) with (appendonly=true, orientation=column) distributed by (a);
create table dtx_phase_prune(a int) distributed by (a);
create unlogged table dtx_phase_unlogged(a int) distributed by (a);
create table dtx_phase_lock_only(a int) distributed by (a);
-- Enough rows to fill heap pages completely on every segment: opportunistic
-- pruning of a page requires its free space to be below BLCKSZ/10, which
-- holds for fully packed pages.
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

-- Case 2: a multi-segment append-optimized row write takes the two-phase
-- path.  AO write WAL records carry no block references (AO segfiles are not
-- buffer pages), so this guards the explicit MarkWalWriteForPermanentRel()
-- call in xlog_ao_insert() against relying on the pg_aoseg heap update alone.
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

-- Case 4: a transaction whose only WAL is standby-lock records takes the
-- one-phase path.  LOCK TABLE emits XLOG_STANDBY_LOCK under an assigned xid
-- but registers no blocks; under the old any-WAL-under-xid predicate this
-- forced a needless full two-phase commit.
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

-- Case 5: a read-only transaction stays on the one-phase path even when its
-- scans emit block-carrying maintenance WAL: opportunistic page pruning
-- (XLOG_HEAP2_CLEAN) and hint-bit full-page images (XLOG_FPI_FOR_HINT) are
-- logged without an xid and must not count as durable transaction work.
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

-- Case 6: the durable-work report is sticky across aborted subtransactions:
-- permanent work rolled back to a savepoint still forces the two-phase path.
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

-- Case 7: writes that touch only an unlogged table take the one-phase path:
-- unlogged relations WAL-log nothing but their init fork, so the transaction
-- produces no durable-work WAL on any QE.
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

-- Case 8: same for a temp table -- temp relations never WAL-log their data
-- pages at all.  The table is created in session 1 (outside the fault
-- window) so it is visible to the transaction dispatched over the same
-- connection.
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

-- Case 9: once any QE reports a durable write, the QD broadcasts PREPARE to
-- every participating segment, not only to the one that wrote.  The LOCK
-- touches every segment; the single inserted row lands on exactly one of
-- them, so the other segments only ever see the lock and still must PREPARE.
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

-- Case 10: COPY takes the two-phase path exactly like INSERT.  COPY receives
-- the QE durable-work reports through its own dispatch path (cdbcopy.c),
-- separate from ordinary query dispatch (cdbdisp_async.c).
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

-- Case 11: the durable-work flag resets between transactions on the same,
-- reused QE backends: a write transaction takes the two-phase path, and an
-- immediately following read-only transaction on the same connection takes
-- the one-phase path rather than inheriting the previous transaction's flag.
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

-- Case 12: the durable-work report is sticky across separately-dispatched
-- statements of one transaction: a lock-only statement followed by a real
-- write in a later statement still forces the two-phase path.
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

-- Case 13: a multi-statement transaction with no durable write at any point
-- (a read, then a lock, then another read) stays on the one-phase path.  The
-- reads are placed before the lock so that any hint-bit full-page images
-- they trigger are logged before an xid is assigned.
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

1q:
drop table dtx_phase_heap;
drop table dtx_phase_ao;
drop table dtx_phase_aocs;
drop table dtx_phase_prune;
drop table dtx_phase_unlogged;
drop table dtx_phase_lock_only;
