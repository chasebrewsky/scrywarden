"""Contains helpers for heterogeneous queue items."""

import typing as t


class Entry(t.NamedTuple):
    """Data structure that helps manage heterogeneous queues.

    This designates the source of where an entry came from, the entry kind from
    that source, and the data associated with the entry.
    """
    source: str
    kind: str
    data: t.Any

    def __repr__(self) -> str:
        return f"<Entry source={self.source!r} kind={self.kind!r}>"


class EntryBase:
    """Helper class that helps standardize entry creation."""

    SOURCE: str = ''
    """Constant that specifies the source of the entry."""

    class Kinds:
        """Nested constants of entry types related to this source."""

    @classmethod
    def create(cls, kind: str, data: t.Any) -> Entry:
        """Creates an entry of the particular source.

        Parameters
        ----------
        kind: str
            Entry type that came from the source.
        data: t.Any
            Data associated with the entry type.

        Returns
        -------
        Create entry.
        """
        return Entry(cls.SOURCE, kind, data)
