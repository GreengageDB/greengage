-- -----------------------------------------------------------------------------
-- TEST ONLY (needs an assert-enabled build): FINC_SEND_DISMISS emulates an
-- interconnect error unwinding out of SendChunkUDPIFC with the connection's
-- aggregation buffer unset (pBuff == NULL), followed by a swallower above
-- (elog_demote/elog_dismiss, e.g. a monitoring hook) resuming execution on
-- the poisoned state.
--
-- The very next send on the poisoned connection must fail with a clean
-- interconnect error (pBuff NULL guards). Without the guards it crashes the
-- segment in prepareXmit() (memcpy to NULL).
--
-- The dismiss_test table is intentionally left behind for the regression
-- database (src/test/regress/README: leave objects behind).
-- -----------------------------------------------------------------------------

CREATE TABLE dismiss_test AS
SELECT g AS id, repeat(chr(104 + g % 20), 100) AS txt
FROM generate_series(1, 20) g
DISTRIBUTED RANDOMLY;

SET gp_udpic_fault_inject_percent = 100;
SET gp_udpic_fault_inject_bitmap = 67108864;	-- FINC_SEND_DISMISS

SELECT id, txt FROM dismiss_test;

RESET gp_udpic_fault_inject_bitmap;
RESET gp_udpic_fault_inject_percent;
