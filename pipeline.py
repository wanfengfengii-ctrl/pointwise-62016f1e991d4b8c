"""Outer assembly of Rules, Driver and order with lifecycle and error priority.

pipeline owns parameter validation, the original input iterator and the final
output lifetime.  It does not interpret business fields and implements no
sorting or merging loop; it merely wires the three collaborating ports.
"""
from cleanup import collect, deliver
from driver import Driver
from reduction import Rules
from sorter import order


def aggregate(records, workspace, rows_per_run=1000, fanin=8):
    if type(rows_per_run) is not int or rows_per_run < 1:
        raise ValueError("rows_per_run")
    if type(fanin) is not int or fanin < 2:
        raise ValueError("fanin")
    return _execute(records, workspace, rows_per_run, fanin)


def _closer(item):
    return item.close if item is not None else None


def _execute(records, workspace, rows_per_run, fanin):
    driver = Driver(workspace)
    rules = Rules()
    source = None
    ordered = None
    output = None
    failed = False
    try:
        source = iter(records)
        ordered = order(source, driver, rules, rows_per_run, fanin)
        output = rules.finish(ordered)
        for item in output:
            yield item
    except BaseException:
        failed = True
        raise
    finally:
        # Input first, then the terminable final merge, then everything the
        # driver still owns; each action runs even when an earlier one fails.
        errors = collect([_closer(source), _closer(output), _closer(ordered)])
        errors += driver.cleanup()
        deliver(errors, failed)
