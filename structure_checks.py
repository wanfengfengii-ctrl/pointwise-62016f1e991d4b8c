"""Structural verification of the refactored responsibility boundaries.

These checks exercise the production collaborators directly with independent
doubles: Rules on cross-segment devices, order with opaque cells, a custom
rule set and a port-only segment double, and Driver against the fault-injecting
memory workspace.  Source and import inspection guards the module boundaries.
Nothing here is used by the production chain.
"""
import ast
import tempfile
from pathlib import Path

import driver as driver_module
import pipeline
import reduction
import sorter
import spill
import workspace

ROOT = Path(__file__).resolve().parent
PRODUCTION = ("reduction.py", "driver.py", "sorter.py", "pipeline.py", "spill.py")


# ---------------------------------------------------------------------------
# Rules: purity, cross-segment devices, None/empty devices, lazy finish
# ---------------------------------------------------------------------------
def rules_check():
    rules = reduction.Rules()

    # lift validates and returns a fresh cell without touching its input.
    for bad in (["a", "x", True], ["a"], ["a", 3, 1], [None, "x", 1],
                ["a", ["x"], 1], ["a", "x", 1.0], "abc"):
        try:
            rules.lift(bad)
        except ValueError as error:
            assert str(error) == "record"
        else:
            raise AssertionError("invalid record accepted: %r" % (bad,))
    record = ["a", "", 5]
    cell = rules.lift(record)
    assert cell == ["a", "", 1, 5] and cell is not record
    assert record == ["a", "", 5]

    # key orders a None device before any string, including the empty string.
    keys = [rules.key(["a", None, 1, 0]), rules.key(["a", "", 1, 0]),
            rules.key(["a", "d", 1, 0]), rules.key(["b", None, 1, 0])]
    assert keys == sorted(keys) and keys[0] < keys[1] < keys[2] < keys[3]

    # combine is pure and keeps both identity fields of the left cell.
    left, right = ["a", "d", 1, 2], ["a", "d", 3, 5]
    merged = rules.combine(left, right)
    assert merged == ["a", "d", 4, 7]
    assert left == ["a", "d", 1, 2] and right == ["a", "d", 3, 5]

    # Two segments, same device "d" repeated, mixed None and empty devices.
    # None sorts before "" and "d"; finish counts distinct devices (2) and
    # folds every row of the group.
    segments = [
        [["a", "d", 1, 2], ["a", None, 1, 4], ["a", "", 1, 0]],
        [["a", "d", 2, 5], ["a", None, 3, 6], ["a", "", 1, 1]],
    ]
    combined = sorted((rules.combine(first, second)
                       for first, second in zip(*segments)), key=rules.key)
    assert combined[0] == ["a", None, 4, 10]
    assert combined[1] == ["a", "", 2, 1]
    assert combined[2] == ["a", "d", 3, 7]

    # finish is lazy and consumes the merged stream exactly once.
    pulls = []

    def stream():
        for cell in combined:
            pulls.append(cell)
            yield cell

    result = rules.finish(stream())
    assert iter(result) is result and pulls == []
    output = list(result)
    assert output == [("a", 9, 18, 2)]
    assert len(pulls) == 3
    # Empty input produces no output and no spurious group.
    assert list(rules.finish(iter(()))) == []


# ---------------------------------------------------------------------------
# order: opaque cells, custom rules, port-only driver, single pass
# ---------------------------------------------------------------------------
class _Opaque:
    """Cell without item access, ordering or equality; only the rule reads it."""

    __slots__ = ("group", "value")

    def __init__(self, group, value):
        self.group = group
        self.value = value


class _TagRules:
    """Rules over opaque cells; finish must never be reached by order."""

    def lift(self, record):
        return _Opaque(record % 3, record)

    def key(self, cell):
        return (cell.group,)

    def combine(self, left, right):
        return _Opaque(left.group, left.value + right.value)

    def finish(self, cells):
        raise AssertionError("order must not call finish")


class _Input:
    def __init__(self, rows, log):
        self.rows, self.log, self.index, self.closed = rows, log, 0, False

    def __iter__(self):
        return self

    def __next__(self):
        self.log.append(("pull", self.index))
        if self.index == len(self.rows):
            raise StopIteration
        row = self.rows[self.index]
        self.index += 1
        return row

    def close(self):
        self.closed = True
        self.log.append(("input_close",))


class _PortDriver:
    """Implements exactly the documented driver port; holds no workspace."""

    def __init__(self, log, fanin):
        self.log = log
        self.fanin = fanin
        self.segments = {}
        self.lengths = {}
        self.live = {}
        self.owned = []
        self.serial = 0
        self.reads = {}
        self.peak = 0
        self.cleanup_calls = 0

    def allocate(self):
        self.serial += 1
        name = "s%04d" % self.serial
        self.owned.append(name)
        self.log.append(("allocate", name))
        return name

    def _track(self, token):
        self.live[token] = True
        self.peak = max(self.peak, len(self.live))

    def open_read(self, name):
        assert name in self.segments, name
        assert name not in self.reads, "segment reopened: " + name
        self.reads[name] = 0
        token = "read:" + name
        self._track(token)
        self.live[token] = iter(self.segments[name])
        self.log.append(("open_r", name))
        return token

    def open_write(self, name):
        assert name in self.owned and name not in self.segments
        self.segments[name] = []
        token = "write:" + name
        self._track(token)
        self.live[token] = self.segments[name]
        self.log.append(("open_w", name))
        return token

    def read(self, token):
        assert token in self.live, token
        self.reads[token[5:]] += 1
        try:
            cell = next(self.live[token])
        except StopIteration:
            cell = None
        self.log.append(("read", token[5:]))
        return cell

    def write(self, token, cell):
        assert token.startswith("write:") and token in self.live
        self.live[token].append(cell)
        self.log.append(("write", token[6:]))

    def close(self, token):
        assert token in self.live, token
        del self.live[token]
        self.log.append(("close", token.split(":", 1)[1]))

    def remove(self, name):
        self.lengths[name] = len(self.segments[name])
        self.owned.remove(name)
        self.segments.pop(name, None)
        self.log.append(("remove", name))

    def cleanup(self):
        self.cleanup_calls += 1
        return []


def order_check():
    rules = _TagRules()
    for rows_per_run in (1, 2, 3, 7):
        for fanin in (2, 3, 5):
            for n in (0, 1, 2, 5, 13, 40):
                log = []
                source = _Input(list(range(n)), log)
                driver = _PortDriver(log, fanin)
                cells = list(sorter.order(source, driver, rules, rows_per_run, fanin))
                # Sorted and fully merged by the custom key.
                keys = [rules.key(cell) for cell in cells]
                assert keys == sorted(keys)
                assert all(a[0] != b[0] for a, b in zip(keys, keys[1:]))
                totals = {}
                for value in range(n):
                    totals[value % 3] = totals.get(value % 3, 0) + value
                expected = [(group, total)
                            for group, total in sorted(totals.items())]
                assert [(cell.group, cell.value) for cell in cells] == expected
                # Each produced segment was read once, sequentially to EOF.
                for name, count in driver.reads.items():
                    length = driver.lengths.get(
                        name, len(driver.segments.get(name, [])))
                    assert count == length + 1, (name, count, length)
                # order never calls finish, never closes the input, never
                # invokes driver.cleanup, and keeps the handle bound.
                assert not source.closed
                assert driver.cleanup_calls == 0
                assert driver.peak <= fanin + 1, (driver.peak, fanin)
                # Bounded chunking: first save happens after exactly
                # rows_per_run pulls, before any further pull.
                if n > rows_per_run:
                    first_write = next(i for i, e in enumerate(log)
                                       if e[0] == "open_w")
                    pulls_before = sum(1 for e in log[:first_write]
                                       if e[0] == "pull")
                    assert pulls_before == rows_per_run, (pulls_before, n)
                    assert ("pull", rows_per_run) not in log[:first_write]

    # A tail batch holding a single segment is still rewritten.
    log = []
    source = _Input(list(range(3)), log)
    driver = _PortDriver(log, 2)
    list(sorter.order(source, driver, _TagRules(), 1, 2))
    assert driver.serial == 5  # 3 initial runs + 2 rewritten batches
    tail = [e for e in log if e[0] == "open_w"][-2:]
    assert len(tail) == 2
    # The terminal segments stay owned for the outer lifecycle.
    assert driver.owned, "final segments must remain for the outer owner"


# ---------------------------------------------------------------------------
# Driver: ownership release, reverse cleanup, error collection
# ---------------------------------------------------------------------------
def driver_check():
    ws = workspace.MemoryWorkspace(fail={"close": [1]})
    own = driver_module.Driver(ws)
    name = own.allocate()
    token = own.open_write(name)
    own.write(token, ["a", None, 1, 1])
    assert hash(token) == hash(token) and not isinstance(token, workspace.Handle)
    try:
        own.close(token)
    except OSError as error:
        assert str(error) == "close:1"
    else:
        raise AssertionError("injected close fault not raised")
    # Ownership was released despite the failing event: no retry, no live handle.
    start = len(ws.trace)
    assert own.cleanup() == []
    assert ws.trace[start:] == [["remove", name]]
    assert own.cleanup() == [] and ws.trace[start:] == [["remove", name]]
    assert ws.live == [] and ws.remaining() == []

    # A failing remove still releases ownership and is never retried.
    ws = workspace.MemoryWorkspace(fail={"remove": [1]})
    own = driver_module.Driver(ws)
    first, second = own.allocate(), own.allocate()
    try:
        own.remove(first)
    except OSError as error:
        assert str(error) == "remove:1"
    else:
        raise AssertionError("injected remove fault not raised")
    start = len(ws.trace)
    assert own.cleanup() == []
    assert ws.trace[start:] == [["remove", second]]
    assert ws.live == [] and ws.remaining() == []

    # Reverse cleanup over both registries, collecting errors in occurrence order.
    ws = workspace.MemoryWorkspace(fail={"close": [1, 3], "remove": [2]})
    own = driver_module.Driver(ws)
    segments = [own.allocate() for _ in range(3)]
    for index, name in enumerate(segments):
        own.write(own.open_write(name), ["g", None, 1, index])
    errors = own.cleanup()
    assert [str(error) for error in errors] == ["close:1", "close:3", "remove:2"]
    assert ws.live == [] and ws.remaining() == []
    assert own.cleanup() == []

    # Real files behave the same and leave no residue.
    with tempfile.TemporaryDirectory(prefix="warehouse-structure-") as root:
        real = workspace.Workspace(root)
        own = driver_module.Driver(real)
        name = own.allocate()
        writing = own.open_write(name)
        own.write(writing, ["g", "d", 1, 2])
        own.close(writing)
        reading = own.open_read(name)
        assert own.read(reading) == ["g", "d", 1, 2]
        assert own.read(reading) is None
        own.close(reading)
        assert own.cleanup() == []
        assert real.live == [] and real.remaining() == []
        assert list(Path(root).iterdir()) == []


# ---------------------------------------------------------------------------
# Source and import boundaries
# ---------------------------------------------------------------------------
def _parsed(name):
    return ast.parse((ROOT / name).read_text(encoding="utf-8"))


def _modules(tree):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "relative import"
            found.add(node.module.split(".")[0])
    return found


def boundary_check():
    trees = {name: _parsed(name) for name in PRODUCTION}
    modules = {name: _modules(tree) for name, tree in trees.items()}
    assert modules["reduction.py"] == set()
    assert modules["driver.py"] == set()
    assert modules["sorter.py"] <= {"heapq", "cleanup"}
    assert modules["pipeline.py"] == {"cleanup", "driver", "reduction", "sorter"}
    assert modules["spill.py"] == {"pipeline"}
    for name, imported in modules.items():
        assert "workspace" not in imported, name
        assert "checks" not in imported, name
        assert "structure_checks" not in imported, name
    assert "reduction" not in modules["sorter.py"]
    assert "sorter" not in modules["driver.py"]
    assert "driver" not in modules["reduction.py"]

    # No dynamic loading, evaluation or hidden callbacks; no test-data coupling.
    banned = ("eval", "exec", "__import__", "importlib")
    for name, tree in trees.items():
        calls = {node.func.id for node in ast.walk(tree)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        assert not (calls & set(banned)), (name, calls & set(banned))
        text = (ROOT / name).read_text(encoding="utf-8")
        for token in ("samples.json", "expected.json", "checks."):
            assert token not in text, (name, token)

    # Rules is implemented as a pure class without module-level mutable state.
    for statement in trees["reduction.py"].body:
        assert not isinstance(statement, (ast.Assign, ast.AnnAssign)), \
            "mutable module state in reduction.py"

    # The legacy entry is a pure forwarder with no executor of its own.
    spill_tree = trees["spill.py"]
    assert not any(isinstance(node, (ast.FunctionDef, ast.ClassDef))
                   for node in ast.walk(spill_tree))
    assert spill.aggregate is pipeline.aggregate
    assert pipeline.aggregate.__module__ == "pipeline"
    assert "Rules" in pipeline._execute.__code__.co_names
    assert "Driver" in pipeline._execute.__code__.co_names
    assert "order" in pipeline._execute.__code__.co_names
    # assembly.  It consumes order through Rules.finish, but owns no sort or
    # merge loop of its own.
    assert "Rules" in pipeline._execute.__code__.co_names
    assert "Driver" in pipeline._execute.__code__.co_names
    assert "order" in pipeline._execute.__code__.co_names
    assert "finish" in pipeline._execute.__code__.co_names
    for loop_name in ("heapq", "_merge", "_collapse", "_save", "sort"):
        assert loop_name not in pipeline._execute.__code__.co_names, loop_name
    assert "finish" not in set(sorter._merge.__code__.co_names)
    assert "finish" not in set(sorter._save.__code__.co_names)
    assert "finish" not in set(sorter._collapse.__code__.co_names)

    # order's own code refers only to the rule and driver ports.
    names = set(sorter.order.__code__.co_names)
    assert "finish" not in names
    assert names <= {"lift", "key", "combine", "_save", "_merge", "_collapse",
                     "append", "sort", "len", "range", "remove"}, names


def verify():
    rules_check()
    order_check()
    driver_check()
    boundary_check()
    print("STRUCTURE_OK")


if __name__ == "__main__":
    verify()
