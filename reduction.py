"""Pure aggregation rules: validation, keys, merging and final reduction.

Rules knows the business shape of records and segment cells.  It holds no
files, workspaces, handles, I/O callbacks or mutable global state, and it never
schedules or cleans up segments.  Every method only reads its arguments and
returns new values or a lazy result.
"""


class Rules:
    """Stateless business rules for the external group aggregate."""

    def lift(self, record):
        """Validate one input record and return a fresh segment cell."""
        if (not isinstance(record, (tuple, list)) or len(record) != 3
                or not isinstance(record[0], str)
                or not (record[1] is None or isinstance(record[1], str))
                or type(record[2]) is not int):
            raise ValueError("record")
        return [record[0], record[1], 1, record[2]]

    def key(self, cell):
        """Return the sortable key of a segment cell (None device first)."""
        return cell[0], cell[1] is not None, cell[1] or ""

    def combine(self, left, right):
        """Merge two same-key cells without mutating either argument."""
        return [left[0], left[1], left[2] + right[2], left[3] + right[3]]

    def finish(self, cells):
        """Lazily fold key-merged cells into public output tuples."""
        group, count, amount, devices = None, 0, 0, 0
        for cell in cells:
            if group is not None and group != cell[0]:
                yield (group, count, amount, devices)
                count, amount, devices = 0, 0, 0
            group = cell[0]
            count += cell[2]
            amount += cell[3]
            devices += cell[1] is not None
        if group is not None:
            yield (group, count, amount, devices)
