import typing as t

from scrywarden.entry import Entry, EntryBase
from scrywarden.transport.message import Message

if t.TYPE_CHECKING:
    from scrywarden.transport.base import Transport


class TransportEntry(EntryBase):
    """Queue entries generated from transports."""

    SOURCE = 'TRANSPORT'

    class Types:
        MESSAGE = 'MESSAGE'
        SHUTDOWN = 'SHUTDOWN'

    @classmethod
    def message(cls, message: 'Message') -> Entry:
        """Creates a queue message indicating a new message was received.

        Parameters
        ----------
        message: Message
            Message to send.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(cls.Types.MESSAGE, message)

    @classmethod
    def shutdown(cls, transport: 'Transport') -> Entry:
        """Creates a queue message indicating that a transport shutdown.

        Parameters
        ----------
        transport: Transport
            Transport instance that shutdown.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(cls.Types.SHUTDOWN, transport)
