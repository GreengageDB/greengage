#!/usr/bin/env python3

import pickle

class Plan:
    def __init__(self):
        self.target_segment_count = 0

    def getTargetSegmentCount(self) -> int:
        return self.target_segment_count

    def setTargetSegmentCount(self, target_segment_count: int) -> None:
        self.target_segment_count = target_segment_count

    def serializePlan(self) -> bytes:
        return pickle.dumps(self)

def deserializePlan(input: bytes) -> Plan:
    return pickle.loads(input)
