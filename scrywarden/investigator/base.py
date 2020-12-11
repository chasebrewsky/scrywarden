import logging
import threading
import typing as t
import uuid
from datetime import datetime, timezone
from queue import Queue, Full

import pandas as pa
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, Session, joinedload

import scrywarden.database as db
from scrywarden.config import Config
from scrywarden.entry import Entry
from scrywarden.investigator.entry import InvestigatorEntry
from scrywarden.profile.analyzers import Analyzer
from scrywarden.profile.collectors import Collector
from scrywarden.profile.config import parse_profiles
from scrywarden.timing import ExponentialBackoff, benchmark
from scrywarden.profile.base import Profile

logger = logging.getLogger(__name__)


class Investigator(threading.Thread):
    """Handles the task of analyzing anomalies from a profile.

    Investigators will send a queue entry containing malicious anomalies
    within a dataframe of the shape:

        * event_id (int)
        * message_id (UUID)
        * actor_id (int)
        * created_at (timestamp)
        * anomaly_id (int)
        * field_id (int)
        * score (float)

    Parameters
    ----------
    profile: Optional[Profile]
        Profile to investigate anomalies from.
    collector: Optional[Collector]
        Collector instance used to collect anomalies from the profile.
    analyzer: Optional[Analyzer]
        Analyzer instance used to detect malicious anomalies.
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.
    queue: Optional[Queue]
        Threading queue to use to send investigation results.
    shutdown: Optional[threading.Event]
        Threading event that indicates that shutdown is occurring.
    group: str
        Investigation group name to use. Defaults to the default group.
    """

    def __init__(
        self,
        profile: t.Optional[Profile] = None,
        collector: t.Optional[Collector] = None,
        analyzer: t.Optional[Analyzer] = None,
        session_factory: t.Optional[sessionmaker] = None,
        queue: 't.Optional[Queue[Entry]]' = None,
        shutdown: t.Optional[threading.Event] = None,
        group: str = '',
    ):
        super().__init__()
        self.id: uuid.UUID = uuid.uuid4()
        self.profile: 't.Optional[Profile]' = profile
        self.collector: t.Optional[Collector] = collector
        self.analyzer: t.Optional[Analyzer] = analyzer
        self.session_factory: t.Optional[sessionmaker] = session_factory
        self.queue: 't.Optional[Queue[Entry]]' = queue
        self.shutdown: t.Optional[threading.Event] = shutdown
        self.group: str = group
        self._group: t.Optional[db.InvestigationGroup] = None
        self._model: t.Optional[db.Investigator] = None

    def run(self) -> None:
        """Runs the main investigation loop."""
        self.name = f"Investigator-{self.profile.name}"
        self.collector.shutdown = self.shutdown
        self.collector.session_factory = self.session_factory
        with self._session(expire_on_commit=False) as session:
            self.profile.sync(session)
            self._sync(session)
            self._sync_group(session)
        while not self.shutdown.is_set():
            result = self._investigate()
            if result is None:
                continue
            investigation, anomalies = result
            backoff = ExponentialBackoff(initialize=True)
            while not self.shutdown.wait(backoff.timeout):
                try:
                    self.queue.put_nowait(InvestigatorEntry.malicious_activity(
                        investigation,
                        anomalies,
                    ))
                    break
                except Full:
                    logger.debug(
                        "Investigator queue full retrying in %.2f seconds",
                        backoff.next()
                    )
        # Investigator model must be removed to allow unassigned
        # investigations to be removed.
        with self._session() as session:
            logger.debug("Deleting investigator from the database")
            session.delete(self._model)
            self._model = None
        logger.info(
            "Investigator for profile '%s' has been shutdown",
            self.profile.name,
        )

    @benchmark("Investigation completed in %.2f seconds", logger=logger)
    def _investigate(self) -> t.Optional[
        t.Tuple[db.Investigation, pa.DataFrame]
    ]:
        investigation, previous = self._create_investigation()
        logger.debug("Created investigation %s", investigation.id)
        with benchmark() as elapsed:
            anomalies = self.collector.collect(
                self.profile, investigation, previous=previous,
            )
            logger.info(
                "%d anomalies collected in %.2f seconds", len(anomalies),
                elapsed(),
            )
        if anomalies.empty:
            # Delete investigation because no investigation took place.
            with self._session() as session:
                session.delete(investigation)
            return None
        logger.debug("\n%s", anomalies)
        event_ids = anomalies['event_id'].drop_duplicates().astype(
            'object',
        )
        with self._session(expire_on_commit=False) as session:
            session.add(investigation)
            investigation.is_assigned = True
            assigned_events = []
            for event_id in event_ids.values:
                assigned_events.append({
                    'investigation_id': investigation.id,
                    'event_id': event_id,
                })
            statement = pg.insert(db.InvestigationEvent).values(
                assigned_events,
            )
            with benchmark() as elapsed:
                session.execute(statement)
                logger.info(
                    "%d events assigned to investigation %d in %.2f "
                    "seconds", len(assigned_events), investigation.id,
                    elapsed()
                )
        logger.debug("Analyzing collected events")
        with benchmark() as elapsed:
            malicious_anomalies = self.analyzer.analyze(anomalies)
            logger.info(
                "%d malicious anomalies found in %.2f seconds",
                len(malicious_anomalies), elapsed(),
            )
        logger.debug("\n%s", malicious_anomalies)
        with self._session(expire_on_commit=False) as session:
            session.add(investigation)
            investigation.completed_at = datetime.now(timezone.utc)
            session.flush()
        return investigation, malicious_anomalies

    @benchmark("Fetched features in %.2f seconds", logger=logger)
    def _get_features(
        self,
        session: Session,
        df: pa.DataFrame,
    ) -> pa.DataFrame:
        """Retrieves a list of related features from the DB.

        Performs a query that searches for any features with any field_id in
        the given field_id series and any actor_id in the given actor_id
        series. This ends up returning more features then there are in the
        message value DataFrame, but the query returns much quicker in
        practice.

        Parameters
        ----------
        session: Session
            SQLAlchemy session.

        Returns
        -------
        DataFrame
            Feature DataFrame containing the following fields:
                * feature_id (int): ID of the feature.
                * field_id (int): ID of the associated field.
                * actor_id (int): ID of the associated actor.
                * value (str): JSON value associated with the feature.
                * count (int): Current number of times this feature has been
                    present in messages.
        """
        unique_fa = df[['field_id', 'actor_id']].drop_duplicates()
        unique_field_ids = unique_fa['field_id'].drop_duplicates()
        unique_actor_ids = unique_fa['actor_id'].drop_duplicates()
        query = session.query(
            db.Feature.id.label('feature_id'),
            db.Feature.field_id.label('field_id'),
            db.Feature.actor_id.label('actor_id'),
            db.Feature.value.label('value'),
            db.Feature.count.label('count'),
        ).filter(
            db.Feature.field_id.in_(unique_field_ids.values.astype('object')),
            db.Feature.actor_id.in_(unique_actor_ids.values.astype('object')),
        )
        with benchmark() as elapsed:
            features = pa.read_sql_query(query.statement, session.connection())
            logger.info(
                "%d features fetched in %.2f seconds", len(features),
                elapsed(),
            )
        if not len(features):
            features['count'] = features['count'].astype('int')
            return features
        return features.set_index(['field_id', 'actor_id']).loc[
            unique_fa.set_index(['field_id', 'actor_id']).index
        ].reset_index()

    def _sync(self, session: Session):
        self._model = db.Investigator(
            id=self.id, profile_id=self.profile.model.id,
        )
        session.add(self._model)
        session.flush()

    def _session(self, **kwargs) -> t.ContextManager[Session]:
        return db.managed_session(self.session_factory, **kwargs)

    def _get_previous_investigation(self) -> t.Optional[db.Investigation]:
        """Retrieves an existing past investigation.

        Waits until events have been assigned to the investigation before
        returning the object. This allows for the previous investigation
        to have safely claimed a group of alerts to analyze before moving
        onward

        Returns
        -------
        Previous investigation or None.
        """
        with self._session(expire_on_commit=False) as session:
            query = session.query(db.Investigation).filter(
                db.Investigation.group == self._group,
            ).order_by(db.Investigation.created_at.desc()).options(
                joinedload(db.Investigation.group).subqueryload(
                    db.InvestigationGroup.profile,
                ),
            )
            investigation = query.first()
            if investigation is None or investigation.is_assigned:
                return investigation
        # Wait until the investigation has assigned itself events.
        backoff = ExponentialBackoff(after=1, initialize=True)
        while not self.shutdown.wait(backoff.timeout):
            with self._session(expire_on_commit=False) as session:
                investigation = session.query(db.Investigation).filter(
                    db.Investigation.group == self._group,
                ).order_by(db.Investigation.created_at.desc()).first()
                if investigation is None or investigation.is_assigned:
                    return investigation
                if investigation.created_by is None:
                    logger.warning(
                        "Deleting investigation %d since it never finished",
                        investigation.id,
                    )
                    session.delete(investigation)
                    try:
                        session.flush()
                    except Exception as error:
                        logger.exception(error)
                    backoff.reset(initialize=True)
                    continue
                logger.debug(
                    "Previous investigation still assigning trying again in "
                    "%.2f seconds", backoff.next(),
                )

    def _create_investigation(self) -> t.Tuple[
        db.Investigation, t.Optional[db.Investigation]
    ]:
        while not self.shutdown.is_set():
            previous = self._get_previous_investigation()
            with self._session(expire_on_commit=False) as session:
                investigation = db.Investigation(
                    created_by=self._model.id,
                    index=previous.index + 1 if previous else 1,
                    group=self._group,
                )
                session.add(investigation)
                # This transaction will fail if another investigator claimed
                # the next investigation before this one.
                try:
                    session.commit()
                    _ = investigation.group.profile
                    return investigation, previous
                except IntegrityError as error:
                    logger.exception(error)

    def _sync_group(self, session: Session) -> db.InvestigationGroup:
        statement = pg.insert(db.InvestigationGroup).values(
            profile_id=self.profile.model.id,
            name=self.group,
        ).on_conflict_do_nothing(
            index_elements=[
                db.InvestigationGroup.profile_id,
                db.InvestigationGroup.name,
            ],
        )
        session.execute(statement)
        session.commit()
        query = session.query(db.InvestigationGroup).filter(
            db.InvestigationGroup.profile_id == self.profile.model.id,
            db.InvestigationGroup.name == self.group,
        )
        self._group = query.one()
        return self._group


def parse_investigators(config: Config) -> t.List[Investigator]:
    """Parses a list of investigator from the YAML config.

    Parameters
    ----------
    config: Config
        Configuration object to parse from.

    Returns
    -------
    List[Investigator]
        List of parsed investigators.
    """
    profile_objects = parse_profiles(config, extras=['collector', 'analyzer'])
    investigators = []
    for value in profile_objects.values():
        investigators.append(Investigator(
            profile=value['profile'],
            collector=value['collector'],
            analyzer=value['analyzer'],
        ))
    return investigators
