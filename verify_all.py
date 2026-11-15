"""Unified verification entry point.

Runs the original behavior regression (business reference, frozen traces and
fault priority), the real-file disk comparison and the new structural checks
for the refactored module boundaries.
"""
import contextlib
import io

import checks
import structure_checks


def verify():
    checks.verify()
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        checks.disk()
    structure_checks.verify()
    print("UNIFIED_VERIFY_OK")


if __name__ == "__main__":
    verify()
