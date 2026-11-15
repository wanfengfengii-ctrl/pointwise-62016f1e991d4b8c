"""Generic cleanup helpers shared by the scheduler and the outer lifecycle.

This module contains no business rules, no segment scheduling and no file
ownership; it only runs supplied actions to completion and preserves the
priority of an already-active exception.
"""


def collect(actions):
    """Run every action, returning exceptions in the order they occurred."""
    errors = []
    for action in actions:
        if action is None:
            continue
        try:
            action()
        except BaseException as error:
            errors.append(error)
    return errors


def deliver(errors, failed):
    """Raise the first cleanup error only when no primary error is active."""
    if errors and not failed:
        raise errors[0]
