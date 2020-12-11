import typing as t
from collections import KeysView, ItemsView, ValuesView
from datetime import datetime, timezone
from uuid import uuid4, UUID

from scrywarden.typing import JSONValue


def get(data: JSONValue, field: t.Sequence[str]) -> JSONValue:
    """Returns the given JSON field value.

    Field value is a sequence of strings representing a potentially nested
    JSON value. If a field is only one level deep, then a sequence with one
    string is used.

    Parameters
    ----------
    data: JSONValue
        JSON data to get the value from.
    field: Sequence[str]
        Nested field string levels to get data from.

    Returns
    -------
    JSONValue
        Discovered JSON value.
    """
    if not field:
        return data
    key, remaining = field[0], field[1:]
    if isinstance(data, dict):
        try:
            target = data[key]
        except KeyError:
            raise KeyError(field) from None
    elif isinstance(data, list):
        try:
            target = data[int(key)]
        except (ValueError, IndexError):
            raise KeyError(field) from None
    else:
        raise KeyError(field) from None
    try:
        return get(target, remaining)
    except KeyError:
        raise KeyError(field) from None


def keys(data: JSONValue) -> t.Iterator[t.Tuple[str, ...]]:
    """Iterates over all the possible fields JSON data has.

    Dictionary keys will be iterated over and list indexes will be returned
    as strings. Keys are normalized as tuples.

    Parameters
    ----------
    data: JSONValue
        Decoded JSON value.

    Returns
    -------
    Iterator[Tuple[str, ...]]
        Iterator that returns tuples representing the field strings that can
        be retrieved from the message.
    """
    if isinstance(data, dict):
        for field, value in data.items():
            yield field,
            for nested_field in keys(value):
                yield field, *nested_field
    elif isinstance(data, list):
        for index, value in enumerate(data):
            field = str(index)
            yield field,
            for nested_field in keys(value):
                yield field, *nested_field


def copy(data: JSONValue) -> JSONValue:
    """Deep copies a JSON value.

    This is quicker than performing a copy or deepcopy since it only checks
    for lists and dictionaries.

    Parameters
    ----------
    data: JSONValue
        JSON value to copy.

    Returns
    -------
    JSONValue
        Copied JSON value.
    """
    if isinstance(data, dict):
        return {key: copy(value) for key, value in data.items()}
    if isinstance(data, list):
        return [copy(value) for value in data]
    return data


class Message(t.NamedTuple):
    """JSON data received from a data source.

    Uses a named tuple for the sake of usability + immutability.
    """

    id: UUID
    timestamp: datetime
    data: JSONValue

    @classmethod
    def create(
        cls,
        data: JSONValue,
        id: t.Optional[UUID] = None,
        timestamp: t.Optional[datetime] = None,
    ) -> 'Message':
        """Creates an instance of a message with default values.

        Parameters
        ----------
        data: JSONValue
            JSON value of the message.
        id: UUID
            Custom UUID of the message. This will default to a random UUID V4.
        timestamp: datetime
            Datetime of the message. This will default to the time this
            message instance is created.

        Returns
        -------
        Message
            Created message object.
        """
        return cls(
            id or uuid4(), timestamp or datetime.now(timezone.utc), data,
        )

    def __getitem__(self, item: t.Sequence[str]) -> JSONValue:
        if isinstance(item, str):
            item = (item,)
        return get(self.data, item)

    def __len__(self) -> int:
        return len([*iter(self)])

    def __iter__(self) -> t.Iterator[t.Tuple[str, ...]]:
        return keys(self.data)

    def __contains__(self, item: t.Sequence[str]) -> bool:
        try:
            _ = self[item]
            return True
        except KeyError:
            return False

    def keys(self) -> KeysView:
        return KeysView(self)

    def items(self) -> ItemsView:
        return ItemsView(self)

    def values(self) -> ValuesView:
        return ValuesView(self)

    def get(
        self,
        item: t.Sequence[str],
        default: t.Optional = None,
    ) -> t.Optional[JSONValue]:
        try:
            return self[item]
        except KeyError:
            return default

