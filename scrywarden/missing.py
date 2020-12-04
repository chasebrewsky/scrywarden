import typing as t


class Missing:
    """Object type indicating that a value is missing, not None."""
    def __bool__(self) -> bool:
        return False


T = t.TypeVar('T')
MISSING = Missing()
"""Constant that indicates that a value is missing, not None."""

Unset = t.Union[T, Missing]
"""Type that indicates that a field may have the MISSING constant."""
