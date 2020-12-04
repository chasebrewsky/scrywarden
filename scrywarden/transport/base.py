import logging
import threading
import typing as t
from queue import Queue, Full

from scrywarden.config import Config, parsers
from scrywarden.config.parsers import Parser
from scrywarden.timing import ExponentialBackoff
from scrywarden.transport.entry import TransportEntry

if t.TYPE_CHECKING:
    from scrywarden.entry import Entry
    from scrywarden.transport.message import Message

logger = logging.getLogger(__name__)


class Transport(threading.Thread):
    """Base class for creating a transport thread.

    Subclassing this base class directly requires the developer to create their
    own `run` method that pushes messages to the given transport queue.

    Parameters
    ----------
    name: str
        Name of the transport. Used for thread identification and general
        identification.
    """

    PARSER: t.Optional[Parser] = None

    def __init__(self, name: t.Optional[str] = None):
        super().__init__(name=name)
        self.name = ''
        self._queue: 't.Optional[Queue[Entry]]' = None
        self._shutdown: t.Optional[threading.Event] = None

    def setup(self, queue: Queue, shutdown: threading.Event) -> None:
        """Sets the values needed for the transport to run.

        Parameters
        ----------
        queue: Queue
            Thread queue to sent entries to.
        shutdown: Event
            Thread event that's set when the parent thread is shutting down.
            This is shared by all transport threads, so please do not set it
            in the threading process.
        """
        self._queue = queue
        self._shutdown = shutdown

    def configure(self, config: Config) -> Config:
        """Configures the transport based on passed in configuration params.

        Parameters
        ----------
        config: Config
            Config containing setting values for the transport.
        """
        return config.parse(self.PARSER) if self.PARSER else config

    def send(self, entry: 'Entry') -> None:
        """Sends an entry onto the queue.

        Performs exponential backoff of putting into the queue until it's
        either successful or the thread shuts down.

        Parameters
        ----------
        entry: Entry`
            Entry to place onto the queue.
        """
        backoff = ExponentialBackoff(1, initialize=True)
        while not self._shutdown.wait(backoff._timeout):
            try:
                return self._queue.put_nowait(entry)
            except Full:
                logger.debug(
                    "Transport queue full trying again in %.2f seconds",
                    backoff.next()
                )

    def send_message(self, message: 'Message') -> None:
        """Sends a transport message entry to the pipeline.

        Parameters
        ----------
        message: Message
            Message to send to the queue.
        """
        return self.send(TransportEntry.message(message))

    def send_shutdown(self) -> None:
        """Informs the parent thread that the transport has shutdown.

        This is important to do at the end of the transport process because
        it lets the parent thread know to shutdown if all transports have
        finished.
        """
        self._queue.put(TransportEntry.shutdown(self))


class EphemeralTransport(Transport):
    """Transport that processes once and exits."""

    def process(self) -> t.Iterable['Message']:
        """Process that returns an iterable of messages then stops.

        Returns
        -------
        Iterable[Message]
        """
        return []

    def run(self) -> None:
        """Main thread loop."""
        try:
            for message in self.process():
                self.send_message(message)
                if self._shutdown.is_set():
                    break
        except Exception as error:
            logger.exception(error)
        logger.info("Transport '%s' has been shutdown", self.name)
        self.send_shutdown()


class RepeatableTransport(Transport):
    """Transports the repeats the same process repeatedly.

    Ensures that the GIL is released once every process cycle, but should
    be used with care because the process cycles can take over the
    processing time.
    """

    def process(self) -> t.Iterable['Message']:
        """Method called on each iteration of the thread loop.

        Returns
        -------
        Iterable[Message]
            Iterable of messages.
        """
        return []

    def run(self) -> None:
        """Main thread loop."""
        while not self._shutdown.wait(0.001):
            try:
                for message in self.process():
                    self.send_message(message)
                    if self._shutdown.is_set():
                        break
            except Exception as error:
                logger.exception(error)
        logger.info("Transport '%s' has been shutdown", self.name)
        self.send_shutdown()


class IntervalTransport(Transport):
    """Transport that runs a set process at a set time interval.

    Parameters
    ----------
    interval: float
        Number of seconds to run the interval at.

    Attributes
    ----------
    interval: float
        Number of seconds to run the interval at.
    """
    PARSER = parsers.Options({
        'interval': parsers.Float(),
    })

    def __init__(self, interval: float = 5.0, **kwargs):
        super().__init__(**kwargs)
        self.interval: float = interval

    def configure(self, config: Config) -> Config:
        config = super().configure(config)
        self.interval = config.get_value('interval', self.interval)
        return config

    def process(self) -> t.Iterable['Message']:
        """Method called on each iteration of the thread loop.

        Returns
        -------
        Iterable[Message]
            Iterable of messages. Can be a simple data structure or a
            generator that yields messages.
        """
        return []

    def run(self) -> None:
        """Main thread loop."""
        timeout = 0.0
        while not self._shutdown.wait(timeout):
            try:
                for message in self.process():
                    self.send_message(message)
                    if self._shutdown.is_set():
                        break
            except Exception as error:
                logger.exception(error)
            timeout = self.interval
        logger.info("Transport '%s' has been shutdown", self.name)
        self.send_shutdown()
