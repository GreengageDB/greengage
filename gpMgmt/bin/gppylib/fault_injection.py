#!/usr/bin/env python3

import os
import threading
import time
import signal

GPMGMT_FAULT_POINT = 'GPMGMT_FAULT_POINT'
GPMGMT_FAULT_DELAY_MS = 'GPMGMT_FAULT_DELAY_MS'
GPMGMT_FAULT_TYPE = 'GPMGMT_FAULT_TYPE'
GPMGMT_FAULT_FILE_FLAG = 'GPMGMT_FAULT_FILE_FLAG'

GPMGMT_FAULT_TYPE_SYSPEND = 'suspend'

def inject_fault(fault_point):
    if GPMGMT_FAULT_POINT in os.environ and fault_point == os.environ[GPMGMT_FAULT_POINT]:
        if GPMGMT_FAULT_TYPE in os.environ and os.environ[GPMGMT_FAULT_TYPE] == GPMGMT_FAULT_TYPE_SYSPEND:
            while GPMGMT_FAULT_FILE_FLAG in os.environ and os.path.exists(os.environ[GPMGMT_FAULT_FILE_FLAG]):
                time.sleep(0.1)
            return
        if GPMGMT_FAULT_DELAY_MS in os.environ and int(os.environ[GPMGMT_FAULT_DELAY_MS]) > 0:
            delay_ms = int(os.environ[GPMGMT_FAULT_DELAY_MS])

            def raise_exception(delay: int):
                time.sleep(delay / 1000)
                print('Fault Injection %s' % os.environ[GPMGMT_FAULT_POINT])
                os.kill(os.getpid(), signal.SIGINT)

            thread = threading.Thread(target=raise_exception, kwargs={"delay": delay_ms})
            thread.daemon = True
            thread.start()
        else:
            raise Exception('Fault Injection %s' % os.environ[GPMGMT_FAULT_POINT])

def inject_fault_check(fault_point) -> bool:
    if GPMGMT_FAULT_POINT in os.environ and fault_point == os.environ[GPMGMT_FAULT_POINT]:
        return True
    else:
        return False

# decorator for test purposes
def wrap_state_func_with_faults(func):
    def func_with_faults(*args):
        inject_fault(f'{func.__name__}_begin')
        result = func(*args)
        inject_fault(f'{func.__name__}_end')
        return result
    return func_with_faults
