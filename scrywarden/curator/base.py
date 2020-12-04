import logging
import threading
import typing as t
import queue as q

from pandas import DataFrame
from sqlalchemy.orm import sessionmaker

from scrywarden.curator.entry import CuratorEntry
from scrywarden.database import Investigation
from scrywarden.entry import Entry
from scrywarden.investigator.base import Investigator
from scrywarden.investigator.entry import InvestigatorEntry
from scrywarden.shipper import Shipper
from scrywarden.timing import ExponentialBackoff

logger = logging.getLogger(__name__)


class Curator:
    """Coordinates the investigation phase of anomaly detection.

    Coordinates anomalies from investigators to shippers.

    Manages the investigator and shipper threads and passes malicious
    anomaly findings from the investigators to the shippers.

    Parameters
    ----------
    investigators: Iterable[Investigator]
        Iterable of investigators to listen for malicious anomalies from.
    shippers: Iterable[Shipper]
        Iterable of shippers to send malicious anomaly findings to.
    queue_size: int
        Maximum size of entries allowed in the queue between the investigators
        and the curator.
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.
    """
    def __init__(
        self,
        investigators: t.Iterable[Investigator] = (),
        shippers: t.Iterable[Shipper] = (),
        queue_size: int = 10,
        session_factory: t.Optional[sessionmaker] = None,
    ):
        self.investigators: t.List[Investigator] = list(investigators)
        self.shippers: t.List[Shipper] = list(shippers)
        self.queue_size: int = queue_size
        self.session_factory: t.Optional[sessionmaker] = session_factory
        self._queue: 't.Optional[q.Queue[Entry]]' = None
        self._investigator_shutdown: threading.Event = threading.Event()
        self._shipper_shutdown: threading.Event = threading.Event()

    def start(self) -> None:
        """Starts the curator process.

        Stops the process if investigators or shippers are empty.
        """
        if not self.investigators:
            return logger.error("Curator contains no investigators to start")
        if not self.shippers:
            return logger.error("Curator contains no shippers to start")
        self._queue = q.Queue(self.queue_size)
        for shipper in self.shippers:
            shipper.session_factory = self.session_factory
            shipper.shutdown = self._shipper_shutdown
            shipper.start()
        for investigator in self.investigators:
            investigator.queue = self._queue
            investigator.session_factory = self.session_factory
            investigator.shutdown = self._investigator_shutdown
            investigator.start()
        try:
            while not self._investigator_shutdown.is_set():
                self._pull_entry()
        except KeyboardInterrupt:
            logger.warning("Received keyboard interrupt")
        except SystemExit:
            logger.warning("Received system exit")
        logger.info("Shutting down curator")
        self._investigator_shutdown.set()
        for investigator in self.investigators:
            investigator.join()
        while not self._queue.empty():
            self._pull_entry()
        self._shipper_shutdown.set()
        for shipper in self.shippers:
            shipper.queue.put(CuratorEntry.blip('Shutdown'))
            shipper.join()

    def _ship(self, investigation: Investigation, events: DataFrame):
        for shipper in self.shippers:
            backoff = ExponentialBackoff(initialize=True)
            while not self._investigator_shutdown.wait(backoff.timeout):
                try:
                    shipper.queue.put_nowait(CuratorEntry.malicious_activity(
                        investigation, events.copy(),
                    ))
                    break
                except q.Full:
                    logger.debug(
                        "Transport queue full trying again in %.2f seconds",
                        backoff.next(),
                    )

    def _pull_entry(self) -> None:
        entry = self._queue.get()
        logger.debug("Received entry type %s", entry[:2])
        try:
            self._handle_entry(entry)
        except Exception as error:
            logger.exception(error)

    def _handle_entry(self, entry: Entry):
        if entry.source == InvestigatorEntry.SOURCE:
            if entry.kind == InvestigatorEntry.Kinds.MALICIOUS_ACTIVITY:
                investigation, anomalies = entry.data
                return self._ship(investigation, anomalies)
            if entry.kind == InvestigatorEntry.Kinds.SHUTDOWN:
                logger.debug(
                    "Received investigator shutdown entry from investigator "
                    "'%s'", entry.data.name,
                )
                return self.investigators.remove(entry.data)
            raise ValueError(
                f"Received unknown investigator entry kind {entry.kind!r}",
            )
        raise ValueError(f"Received unknown entry source {entry.source!r}")
