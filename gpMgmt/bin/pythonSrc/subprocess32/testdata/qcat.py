"""When ran as a script, simulates cat with no arguments."""

from __future__ import absolute_import
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        sys.stdout.write(line)
