#!/usr/bin/env python3

import os

GPMGMT_FAULT_POINT = 'GPMGMT_FAULT_POINT'

def inject_fault(fault_point):
    if GPMGMT_FAULT_POINT in os.environ and fault_point == os.environ[GPMGMT_FAULT_POINT]:
        raise Exception('Fault Injection %s' % os.environ[GPMGMT_FAULT_POINT]) 

# decorator for test purposes
def wrap_state_func_with_faults(func):
    def func_with_faults(*args):
        inject_fault(f'{func.__name__}_begin')
        func(*args)
        inject_fault(f'{func.__name__}_end')
    return func_with_faults
