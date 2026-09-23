#!/usr/bin/env python3
#
# Copyright (c) 2025-Present, Greengage Community
#

from enum import Enum
import pickle
from ggrebalance_modules.planner import *

class RebalanceStep:

    MAX_RETRY_COUNT = 2

    class Status(Enum):
        APPROVE_REQUIRED = 1
        PLANNED = 2
        IN_PROGRESS = 3
        ERROR = 4
        CANCELLED = 5
        DONE = 6

    def __init__(self, move: LogicalMove):
        self.move_order = -1
        self.move = move
        self.status = self.Status.PLANNED
        self.rollback = False
        self.retry_attempt_count = 0

    def __str__(self) -> str:
        rollback_label = ''
        if self.isRollback():
            rollback_label = '[ROLLBACK] '
        return (
            f"{rollback_label}Rebalance step with move_order: {self.getMoveOrder()}, status: {self.getStatus().name}"
        )

    def getMoveOrder(self) -> int:
        return self.move_order

    def setMoveOrder(self, move_order: int) -> None:
        self.move_order = move_order

    def getStatus(self) -> Status:
        return self.status

    def getMove(self) -> LogicalMove:
        return self.move

    def setStatus(self, status: Status, rollback: bool = False) -> None:
        self.status = status
        if self.rollback != rollback:
            self.retry_attempt_count = 0
        elif status == self.Status.IN_PROGRESS:
            self.retry_attempt_count += 1
        self.rollback = rollback

    def isRetryAllowed(self):
        return self.retry_attempt_count < self.MAX_RETRY_COUNT

    def serializeStep(self) -> bytes:
        return pickle.dumps(self)

    def isRollback(self) -> bool:
        return self.rollback

class RebalanceStepMoveMirror(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)

    def __str__(self):
        return (
            f"{super().__str__()}, type: mirror move:\n"
            f"{str(self.move)}"
        )

class RebalanceStepSwitchoverToMirror(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)
        self.status = self.Status.APPROVE_REQUIRED

    def __str__(self) -> str:
        return (
            f"{super().__str__()}, type: switchover from Primary to Mirror, DBID {str(self.move.seg.getSegmentDbId())}"
        )

class RebalanceStepSwitchoverToPrimary(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)
        self.status = self.Status.APPROVE_REQUIRED

    def __str__(self) -> str:
        return (
            f"{super().__str__()}, type: switchover from Mirror to Primary, DBID {str(self.move.seg.getSegmentDbId())}"
        )

def deserializeStep(input: bytes) -> RebalanceStep:
    return pickle.loads(input)
