#!/usr/bin/env python3

from enum import Enum
import pickle
from ggrebalance_modules.planner import *

class RebalanceStep:
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

    def __str__(self) -> str:
        rollback_label = ''
        if self.isRollback():
            rollback_label = '[ROLLBACK] '
        return (
            f"{rollback_label}Rebalance step with move_order: {self.getMoveOrder()}, status: {self.getStatus()}"
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
        self.rollback = rollback

    def serializeStep(self) -> bytes:
        return pickle.dumps(self)

    def isRollback(self) -> bool:
        return self.rollback

class RebalanceStepMoveMirror(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)

    def __str__(self):
        return (
            f"{super().__str__()}, type: RebalanceStepMoveMirror:\n"
            f"{str(self.move)}"
        )

class RebalanceStepSwitchoverToMirror(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)
        self.status = self.Status.APPROVE_REQUIRED

    def __str__(self) -> str:
        return (
            f"{super().__str__()}, type: RebalanceStepSwitchoverToMirror, DBID {str(self.move.seg.getSegmentDbId())}"
        )

class RebalanceStepSwitchoverToPrimary(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)
        self.status = self.Status.APPROVE_REQUIRED

    def __str__(self) -> str:
        return (
            f"{super().__str__()}, type: RebalanceStepSwitchoverToPrimary, DBID {str(self.move.seg.getSegmentDbId())}"
        )

def deserializeStep(input: bytes) -> RebalanceStep:
    return pickle.loads(input)
