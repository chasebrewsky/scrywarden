import logging
import threading
import typing as t
import queue as q

import pandas as pa
from sqlalchemy.orm import sessionmaker, Session

from scrywarden import database as db
from scrywarden.curator.entry import CuratorEntry
from scrywarden.entry import Entry
from scrywarden.config import Config
from scrywarden.config.parsers import Parser

logger = logging.getLogger(__name__)


class Shipper(threading.Thread):
    """Base class that deals with handling malicious anomalies.

    This is a thread that waits for a curator to send it malicious anomaly
    findings. Subclasses should override the `ship` method.

    Parameters
    ----------
    name: str
        Name of the shipper thread.
    """
    PARSER: t.Optional[Parser] = None

    def __init__(self, name: str = ''):
        super().__init__(name=name)
        self.queue_size: int = 10
        self.queue: 't.Optional[q.Queue[Entry]]' = None
        self.shutdown: t.Optional[threading.Event] = None
        self.session_factory: t.Optional[sessionmaker] = None

    def configure(self, config: Config) -> Config:
        """Configures the shipper from configuration settings.

        Parses the configuration object if a parser is set on the shipper
        class to validate the config.

        Parameters
        ----------
        config: Config
            Settings gathered from the configuration.
        """
        return config.parse(self.PARSER) if self.PARSER else config

    def ship(
        self,
        investigation: db.Investigation,
        anomalies: pa.DataFrame,
    ) -> None:
        """Handles events flagged as anomalous by an investigator.

        The anomaly dataframe will have the shape:

        * event_id (int)
        * message_id (str)
        * actor_id (int)
        * created_at (timestamp)
        * anomaly_id (int)
        * field_id (int)
        * score (float)

        Parameters
        ----------
        investigation: scrywarden.database.Investigation
            Investigation that detected the malicious anomalies.
        anomalies: DataFrame
            Malicious anomalies.
        """

    def run(self):
        """Main threading method."""
        self.queue = q.Queue(self.queue_size)
        while not self.shutdown.is_set():
            self._pull_entry()
        logger.info("Shutting down shipper '%s'", self.name)
        # Clear queue before shutdown.
        while not self.queue.empty():
            self._pull_entry()

    def _session(self, **kwargs) -> t.ContextManager[Session]:
        return db.managed_session(self.session_factory, **kwargs)

    def _pull_entry(self):
        entry = self.queue.get()
        try:
            self._handle_entry(entry)
        except Exception as error:
            logger.exception(error)

    def _handle_entry(self, entry: Entry) -> None:
        if entry.source == CuratorEntry.SOURCE:
            if entry.kind == CuratorEntry.Kinds.MALICIOUS_ACTIVITY:
                investigation, anomalies = entry.data
                return self.ship(investigation, anomalies)
            if entry.kind == CuratorEntry.Kinds.BLIP:
                return logger.debug("Received curator blip: %s", entry.data)
            raise ValueError(f"Received unknown curator entry {entry.kind!r}")
        raise ValueError(f"Received unknown entry source {entry.source!r}")
