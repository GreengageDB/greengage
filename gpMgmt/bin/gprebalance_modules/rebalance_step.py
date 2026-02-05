#!/usr/bin/env python3

from enum import Enum
import pickle
from gprebalance_modules.planner import *

class RebalanceStep:
    class Status(Enum):
        APPROVE_REQUIRED = 1
        PLANNED = 2
        IN_PROGRESS = 3
        ERROR = 4
        ROLLBACK_PLANNED = 5
        ROLLED_BACK = 6
        CANCELLED = 7
        DONE = 8

    def __init__(self, move: LogicalMove):
        self.move_order = -1
        self.move = move
        self.status = self.Status.PLANNED

    def __str__(self):
        return (
            f"Rebalance step with move_order: {self.getMoveOrder()}, status: {self.getStatus()}"
        )

    def getMoveOrder(self):
        return self.move_order

    def setMoveOrder(self, move_order: int):
        self.move_order = move_order

    def getStatus(self):
        return self.status

    def getMove(self):
        return self.move

    def setStatus(self, status: Status):
        self.status = status

    def serializeStep(self) -> bytes:
        return pickle.dumps(self)

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

    def __str__(self):
        return (
            f"{super().__str__()}, type: RebalanceStepSwitchoverToMirror, DBID {str(self.move.seg.getSegmentDbId())}"
        )

class RebalanceStepSwitchoverToPrimary(RebalanceStep):
    def __init__(self, move: LogicalMove):
        super().__init__(move)
        self.status = self.Status.APPROVE_REQUIRED

    def __str__(self):
        return (
            f"{super().__str__()}, type: RebalanceStepSwitchoverToPrimary, DBID {str(self.move.seg.getSegmentDbId())}"
        )

def deserializeStep(input: bytes) -> RebalanceStep:
    return pickle.loads(input)
