import typing as t

from scrywarden.profile.reporters import Reporter, Mandatory
from scrywarden.transport.message import Message
from scrywarden.typing import JSONValue

if t.TYPE_CHECKING:
    from scrywarden.profile.base import Profile


class Field:
    """Defines how a feature model is generated for a profile

    This class mainly determines how the feature value is retrieved from a
    message. It defers how the anomaly score is generated to the attached
    reporter instance.

    Subclassing this class requires overriding the `get_value` method.

    Parameters
    ----------
    reporter: Optional[Reporter]
        Reporter instance to use for generating anomalies. Defaults to the
        Mandatory reporter.
    name: str
        Name of the field. This is set to the name of the class attribute on
        the profile it's attached to
    """
    def __init__(
        self,
        reporter: t.Optional[Reporter] = None,
        name: t.Optional[str] = None,
    ):
        self.name: t.Optional[str] = name
        self.reporter: Reporter = reporter or Mandatory()
        self.profile: 't.Optional[t.Type[Profile]]' = None

    def get_value(self, message: Message) -> JSONValue:
        """Retrieves the field value from a message.

        Parameters
        ----------
        message: Message
            Message to retrieve the value from.

        Returns
        -------
        JSONValue
            JSON value.
        """
        raise NotImplementedError()


class Single(Field):
    """Returns a single JSON value from the message.

    By default this pulls the same key value as the name of the field. This
    can be overridden by setting the `key` attribute.

    Parameters
    ----------
    key: Sequence[str]
        Key of the JSON value to retrieve. This can be a string or a tuple of
        strings that represent a nested JSON value.
    """
    def __init__(self, key: t.Optional[t.Sequence[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.key: t.Sequence[str] = key

    def get_value(self, message: Message) -> JSONValue:
        return message.get(self.key or self.name)


class Multi(Field):
    """Returns a JSON array made of multiple JSON values.

    Retrieves the each JSON value at each given key and builds a JSON array
    from them as the field value.

    Parameters
    ----------
    keys: Sequence[str]
        Sequence of JSON keys to retrieve.
    """
    def __init__(self, keys: t.Sequence[str], **kwargs):
        super().__init__(**kwargs)
        self.keys: t.Sequence[str] = keys

    def get_value(self, message: Message) -> JSONValue:
        values = []
        for key in self.keys:
            values.append(message.get(key))
        return values
