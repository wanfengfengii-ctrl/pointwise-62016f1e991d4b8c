"""Bounded external sorting and fan-in merging over the driver port.

order implements only limit-based chunking, sorting, round batching, heap
merging and the associated pull order.  Cells are handled exclusively through
the Rules port; the driver port is the only way segments are touched.  It does
not interpret record or cell fields, does not finish output, and does not own
the original input's close or the outer lifecycle's cleanup.
"""
import heapq

from cleanup import collect, deliver


def order(records, driver, rules, rows_per_run, fanin):
    """Yield sorted, same-key-merged segment cells from ``records``."""
    runs, chunk = [], []
    for record in records:
        chunk.append(rules.lift(record))
        if len(chunk) == rows_per_run:
            chunk.sort(key=rules.key)
            runs.append(_save(driver, rules, _collapse(rules, chunk)))
            chunk = []
    if chunk:
        chunk.sort(key=rules.key)
        runs.append(_save(driver, rules, _collapse(rules, chunk)))
    while len(runs) > fanin:
        following = []
        for start in range(0, len(runs), fanin):
            part = runs[start:start + fanin]
            following.append(_save(driver, rules, _merge(driver, rules, part)))
            for name in part:
                driver.remove(name)
        runs = following
    yield from _merge(driver, rules, runs)


def _collapse(rules, cells):
    """Fold consecutive same-key cells; never mutates the provided cells."""
    pending = None
    for cell in cells:
        if pending is not None and rules.key(pending) == rules.key(cell):
            pending = rules.combine(pending, cell)
        else:
            if pending is not None:
                yield pending
            pending = cell
    if pending is not None:
        yield pending


def _merge(driver, rules, names):
    """Heap-merge one batch of segments, prefetching one cell per stream."""
    tokens, heap = [], []
    failed = False
    try:
        for index, name in enumerate(names):
            token = driver.open_read(name)
            tokens.append(token)
            cell = driver.read(token)
            if cell is not None:
                heapq.heappush(heap, (rules.key(cell), index, cell))
        pending = None
        while heap:
            _, index, cell = heapq.heappop(heap)
            following = driver.read(tokens[index])
            if following is not None:
                heapq.heappush(heap, (rules.key(following), index, following))
            if pending is not None and rules.key(pending) == rules.key(cell):
                pending = rules.combine(pending, cell)
            else:
                if pending is not None:
                    yield pending
                pending = cell
        if pending is not None:
            yield pending
    except BaseException:
        failed = True
        raise
    finally:
        deliver(collect([_closer(driver, token) for token in reversed(tokens)]), failed)


def _save(driver, rules, cells):
    """Write one segment from a cell stream; return its segment name."""
    name = driver.allocate()
    token = None
    failed = False
    try:
        token = driver.open_write(name)
        for cell in cells:
            driver.write(token, cell)
    except BaseException:
        failed = True
        raise
    finally:
        deliver(collect([getattr(cells, "close", None),
                         _closer(driver, token) if token is not None else None]),
                failed)
    return name


def _closer(driver, token):
    return lambda: driver.close(token)
