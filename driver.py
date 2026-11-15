"""File ownership boundary for segment I/O.

Driver alone owns real handles, the registry of live handles and the registry
of pending segments.  It performs and records single segment operations and
resource cleanup; it never compares keys, combines cells or walks cell streams.
Tokens are opaque hashable stand-ins for handles.  The exceptions collected by
cleanup are returned to the caller, which keeps the primary exception intact.
"""


class _Token:
    """Opaque, hashable capability handed to the scheduler."""

    __slots__ = ("_handle",)

    def __init__(self, handle):
        self._handle = handle

    def __repr__(self):
        return "<segment-token>"


class Driver:
    def __init__(self, workspace):
        self._workspace = workspace
        self._handles = []      # live handles, open order
        self._owned = []        # segment names still owned, allocation order

    # -- allocation -------------------------------------------------------
    def allocate(self):
        name = self._workspace.allocate()
        self._owned.append(name)
        return name

    # -- handles ----------------------------------------------------------
    def _track(self, handle):
        self._handles.append(handle)
        return _Token(handle)

    def open_read(self, name):
        return self._track(self._workspace.open(name, "r"))

    def open_write(self, name):
        return self._track(self._workspace.open(name, "w"))

    def _resolve(self, token):
        handle = token._handle
        if not any(handle is live for live in self._handles):
            raise RuntimeError("token")
        return handle

    def read(self, token):
        return self._resolve(token).read()

    def write(self, token, cell):
        self._resolve(token).write(cell)

    def close(self, token):
        handle = self._resolve(token)
        try:
            # Release ownership even if the close operation fails.
            self._handles.remove(handle)
        finally:
            handle.close()

    # -- segments ---------------------------------------------------------
    def remove(self, name):
        try:
            # Release ownership even if the remove operation fails.
            self._owned.remove(name)
        finally:
            self._workspace.remove(name)

    # -- cleanup ----------------------------------------------------------
    def cleanup(self):
        errors = []
        for handle in reversed(list(self._handles)):
            try:
                self.close(_Token(handle))
            except BaseException as error:
                errors.append(error)
        for name in reversed(list(self._owned)):
            try:
                self.remove(name)
            except BaseException as error:
                errors.append(error)
        return errors
