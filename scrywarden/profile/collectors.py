import logging
import threading
import typing as t
from datetime import timedelta, datetime, timezone

import pandas as pa
from sqlalchemy.orm import sessionmaker, Session

from scrywarden import database as db
from scrywarden.config import parsers, Config
from scrywarden.exceptions import ConfigError
from scrywarden.profile import Profile
from scrywarden.timing import ExponentialBackoff, benchmark

logger = logging.getLogger(__name__)


class Collector:
    """Base class used to collect anomalies to analyze.

    Subclasses should implement the `collect` method.

    Parameters
    ----------
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.
    shutdown: threading.Event
        Threading event that indicates a shutdown occurred.

    Attributes
    ----------
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.
    shutdown: threading.Event
        Threading event that indicates a shutdown occurred.
    """
    PARSER: t.Optional[parsers.Parser] = None

    def __init__(
        self,
        session_factory: t.Optional[sessionmaker] = None,
        shutdown: t.Optional[threading.Event] = None,
    ):
        self.session_factory: t.Optional[sessionmaker] = session_factory
        self.shutdown: t.Optional[threading.Event] = shutdown

    def _session(self, **kwargs) -> t.ContextManager[Session]:
        return db.managed_session(self.session_factory, **kwargs)

    def collect(
        self,
        profile: Profile,
        investigation: db.Investigation,
        previous: t.Optional[db.Investigation] = None,
    ) -> pa.DataFrame:
        """Collects anomalies from a given profile.

        The returned dataframe should have the schema:

            * event_id (int)
            * message_id (str)
            * actor_id (int)
            * created_at (timestamp)
            * anomaly_id (int)
            * field_id (int)
            * score (float)

        Parameters
        ----------
        profile: Profile
            Profile to retrieve related anomalies from.
        investigation: scrywarden.database.Investigation
            Current investigation object.
        previous: Optional[scrywarden.database.Investigation]
            Previous investigation object.

        Returns
        -------
        Dataframe
            Dataframe containing the related anomalies to analyze.
        """
        raise NotImplementedError()

    def configure(self, config: Config) -> Config:
        """Configures the collector from a config object.

        If the PARSER attribute is set on the class, it uses it automatically
        to parse and validate the config.

        Parameters
        ----------
        config: Config
            Configuration object to parse values from.

        Returns
        -------
        Config
            Parsed config object.
        """
        return config.parse(self.PARSER) if self.PARSER else config

    def _get_first_event(
        self,
        profile: db.Profile,
        **kwargs,
    ) -> t.Optional[db.Event]:
        """Helper utility that retrieves the first event of the profile."""
        logger.debug("Retrieving first event of profile '%s'", profile.name)
        backoff = ExponentialBackoff(initialize=True, **kwargs)
        while not self.shutdown.wait(backoff.timeout):
            with self._session(expire_on_commit=False) as session:
                first_event = session.query(db.Event).join(
                    (db.Actor, db.Event.actor),
                    (db.Profile, db.Actor.profile),
                ).filter(
                    db.Profile.id == profile.id,
                ).order_by(db.Event.created_at.asc()).first()
                if first_event:
                    return first_event
                logger.info(
                    "First event not found trying again in %.2f seconds",
                    backoff.next(),
                )


class TimeRangeCollector(Collector):
    """Collector that retrieves anomalies of a specified time range.

    This collector retrieves anomalies related to a given profile between a
    given time range in seconds.

    In general it works like this:

    1. If no previous investigation occurred, create a new investigation and
    start from the earliest recorded anomaly for the given profile. Otherwise,
    start from where the previous investigation left off.
    2. Retrieve anomalies starting from the previous steps start time plus
    the given number of seconds.
    3. If no anomalies are found, search for the next event past the given
    time range. If an event is not found, wait a set interval and try again
    from step 2. If an event is found, then retrieve anomalies starting from
    the timestamp of the event to the starting  time plus the give number
    of seconds.

    At any time if the end of the search time frame extends past the current
    time, then it waits until the current time is caught up before performing
    a search. This current time can be configured to also be in the past. So
    if the current time is delayed by 15 seconds, then if a search time extends
    past now - 15 seconds, it waits until that delayed time. This is important
    if you some ingest that comes in delayed.

    Parameters
    ----------
    seconds: float
        Time window to search for anomalies in. Defaults to 60 seconds.
    interval: float
        Number of seconds to wait when anymore anomalies cannot be found.
        Defaults to 10 seconds.
    delay: float
        Number of seconds to offset the current time to in the past. Deafults
        to 0 seconds.
    """
    PARSER = parsers.Options({
        'seconds': parsers.Float(),
        'interval': parsers.Float(),
        'delay': parsers.Float(),
    })

    def __init__(
        self,
        seconds: float = 60.,
        interval: float = 10.,
        delay: float = 0.,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.seconds: float = seconds
        self.interval: float = interval
        self.delay: float = delay

    def collect(
        self,
        profile: Profile,
        investigation: db.Investigation,
        previous: t.Optional[db.Investigation] = None,
    ) -> pa.DataFrame:
        if not previous:
            return self._create_initial_investigation(profile.model)
        last_event: db.Event = self._get_last_investigation_event(previous)
        return self._loop_until_anomalies(profile.model, last_event.created_at)

    def configure(self, config: Config) -> Config:
        self.seconds = config.get_value('seconds', self.seconds)
        self.interval = config.get_value('interval', self.interval)
        return config

    def _get_last_investigation_event(
        self,
        investigation: db.Investigation,
    ) -> db.Event:
        logger.debug("Getting last investigation event")
        with self._session(expire_on_commit=False) as session:
            with benchmark(
                "Last investigation event fetched in %.2f seconds",
                logger=logger, level=logging.DEBUG,
            ):
                subquery = session.query(db.Event).join(
                    (db.Investigation, db.Event.investigations),
                ).filter(
                    db.Investigation.id == investigation.id,
                ).offset(0).subquery()
                return session.query(subquery).order_by(
                    subquery.c.created_at.desc(),
                ).first()

    def _loop_until_anomalies(
        self,
        profile: db.Profile,
        start: datetime,
    ) -> pa.DataFrame:
        timeout = 0.0
        logger.debug("Looping until events are found")
        while not self.shutdown.wait(timeout):
            with self._session() as session:
                anomalies = self._fetch_anomalies(session, profile, start)
                if not anomalies.empty:
                    return anomalies
                next_event: t.Optional[db.Event] = session.query(
                    db.Event,
                ).join(
                    (db.Actor, db.Event.actor),
                    (db.Profile, db.Actor.profile),
                ).filter(
                    db.Profile.id == profile.id,
                    db.Event.created_at > start,
                ).order_by(db.Event.created_at.asc()).first()
                if next_event:
                    return self._fetch_anomalies(
                        session, profile, next_event.created_at,
                    )
            timeout = self.interval
            logger.debug(
                "Matching events not found retrying in %.2f seconds", timeout,
            )
        return pa.DataFrame()

    def _wait(self, target: datetime) -> bool:
        timeout: float = 0.0
        while not self.shutdown.wait(timeout):
            now = datetime.now(timezone.utc)
            logger.debug("Target start time %s current time %s", target, now)
            if target + timedelta(seconds=self.delay) <= now:
                break
            timeout = (target - now).seconds
            logger.debug(
                "Search interval is beyond the current time + delay waiting "
                "%.2f seconds", timeout,
            )
        return self.shutdown.is_set()

    def _fetch_anomalies(
        self,
        session: Session,
        profile: db.Profile,
        start: datetime,
    ) -> pa.DataFrame:
        end = start + timedelta(seconds=self.seconds)
        if self._wait(end):
            return pa.DataFrame()
        logger.info("Fetching events between %s and %s", start, end)
        query = session.query(
            db.Event.id.label('event_id'),
            db.Event.message_id.label('message_id'),
            db.Event.actor_id.label('actor_id'),
            db.Event.created_at.label('created_at'),
            db.Anomaly.id.label('anomaly_id'),
            db.Anomaly.field_id.label('field_id'),
            db.Anomaly.score.label('score'),
        ).join(
            (db.Anomaly, db.Event.anomalies),
            (db.Actor, db.Event.actor),
            (db.Profile, db.Actor.profile),
        ).filter(
            db.Event.created_at > start,
            db.Event.created_at <= end,
            db.Profile.id == profile.id,
        )
        return pa.read_sql_query(
            query.statement, session.connection(), parse_dates=['created_at'],
        )

    def _create_initial_investigation(
        self,
        profile: db.Profile,
    ) -> pa.DataFrame:
        logger.info("Creating initial investigation")
        first_event = self._get_first_event(profile)
        if not first_event:
            return pa.DataFrame()
        return self._loop_until_anomalies(
            profile, first_event.created_at - timedelta(seconds=1),
        )


PARSER = parsers.Options({
    'class': parsers.Import(required=True, parent=Collector),
    'config': parsers.Options({}),
})


def parse_collector(config: Config) -> Collector:
    """Parses a collector from a config.

    Parameters
    ----------
    config: Config
        Configuration object to parse from.

    Returns
    -------
    Collector
        Configured collector object.
    """
    config = config.parse(PARSER)
    cls: t.Type[Collector] = config['class'].value
    collector: Collector = cls()
    try:
        collector.configure(config.get('config', {}))
    except Exception as error:
        raise ConfigError("Collector could not be configured") from error
    return collector
