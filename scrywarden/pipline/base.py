import logging
import threading
import time
import typing as t
from queue import Queue
from uuid import UUID, uuid4

import pandas as pa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.orm import Session, sessionmaker

import scrywarden.database as db
from scrywarden.pipline.entry import PipelineEntry
from scrywarden.entry import Entry
from scrywarden.config import parsers, Config
from scrywarden.timing import benchmark
from scrywarden.transport.entry import TransportEntry
from scrywarden.transport.message import Message
from scrywarden.profile.base import Profile, sync_profiles
from scrywarden.transport.base import Transport

logger = logging.getLogger(__name__)


class Pipeline:
    """Manages the identification phase of anomaly detection.

    The identification phase is when:

        * Actors are identified from messages.
        * Message field values are retrieved.
        * Identified message values are processed for anomalies.
        * Behavioral profiles are updated with the identified message values.

    This also manages the transport threads that messages are retrieved from.

    Parameters
    ----------
    transports: Iterable[Transport]
        Transports to receive messages from.
    profiles: Iterable[Profile]
        Profiles to identify and process messages through.
    session_factory: sessionmaker
        SQLAlchemy session factory.
    queue_size: int
        Number of messages to limit in the threading queue. Defaults to 500.
    timeout: float
        Number of seconds the pipeline will wait to process existing messages
        after a message is first received in the queue. This helps to keep
        messages moving at a steady pace even before the queue limit is
        reached.
    """
    PARSER = parsers.Options({
        'queue_size': parsers.Integer(),
        'timeout': parsers.Float(),
    })

    def __init__(
        self,
        transports: t.Iterable[Transport],
        profiles: t.Iterable[Profile],
        session_factory: sessionmaker,
        queue_size: int = 500,
        timeout: float = 10.0,
    ):
        self.transports: t.List[Transport] = list(transports)
        self.profiles: t.Tuple[Profile, ...] = tuple(profiles)
        self._profiles_by_id: t.Dict[int, Profile] = {}
        self._session_factory: sessionmaker = session_factory
        self._queue_size: int = queue_size
        self._queue: 't.Optional[Queue[Entry]]' = None
        self._timeout_length: float = timeout
        self._timeout: threading.Event = threading.Event()
        self._shutdown: threading.Event = threading.Event()
        self._process_id: UUID = uuid4()
        self._timer: t.Optional[threading.Timer] = None
        self._messages: t.List[Message] = []

    def configure(self, config: Config) -> Config:
        """Configures the pipeline according to the YAML config.

        Parameters
        ----------
        config: Config
            Configuration object to pull values from.

        Returns
        -------
        Config
            Parsed configuration object.
        """
        config = config.parse(self.PARSER)
        self._queue_size = config.get_value('limit', self._queue_size)
        self._timeout_length = config.get_value(
            'timeout', self._timeout_length,
        )
        return config

    def start(self):
        """Starts the pipeline process."""
        logger.info("Pipeline starting")
        # Check for duplicate profile names.
        profile_names = set()
        for profile in self.profiles:
            if profile.name in profile_names:
                raise ValueError(
                    "Received multiple profiles with the same profile "
                    f"name '{profile.name}'"
                )
            profile_names.add(profile.name)
        del profile_names
        with self._session(expire_on_commit=False) as session:
            sync_profiles(session, self.profiles)
            self._profiles_by_id = {
                profile.model.id: profile for profile in self.profiles
            }
        self._queue = Queue(self._queue_size)
        # Setup and run transports.
        logger.debug("Starting %d transports", len(self.transports))
        for transport in self.transports:
            transport.setup(self._queue, self._shutdown)
            transport.start()
        logger.debug("Running main loop")
        try:
            while not self._shutdown.is_set():
                entry = self._queue.get()
                logger.debug("Received entry type %s", entry[:2])
                try:
                    self._handle_entry(entry)
                except Exception as error:
                    logger.exception(error)
                if self._timeout.is_set():
                    self._timeout.clear()
                    self._process()
                if len(self._messages) >= self._queue_size:
                    self._process()
        except KeyboardInterrupt:
            logger.warning("Received keyboard interrupt")
        except SystemExit:
            logger.warning("System exiting")
        self._cancel_timeout()
        logger.info("Shutting down transports")
        self._shutdown.set()
        for transport in self.transports:
            transport.join()
        logger.info(
            "Clearing the remaining %d messages from the queue",
            len(self._messages),
        )
        self._process()

    def _session(self, **kwargs) -> t.ContextManager[Session]:
        return db.managed_session(self._session_factory, **kwargs)

    @benchmark("Pipeline process took %.2f seconds", logger=logger)
    def _process(self) -> None:
        self._process_id = uuid4()
        self._cancel_timeout()
        messages = self._messages
        self._messages = []
        logger.info("Processing %d messages", len(messages))
        dfs: t.List[pa.DataFrame] = []
        indexed_messages: t.Dict[int, Message] = {
            message.id.int: message for message in messages
        }
        with benchmark() as elapsed:
            for profile in self.profiles:
                dfs.append(profile.identify(messages))
            logger.info(
                "%d messages identified between %d profiles in %.2f seconds",
                len(messages), len(self.profiles), elapsed(),
            )
        values = pa.concat(dfs, ignore_index=True)
        values.sort_values('timestamp', ignore_index=True)
        logger.debug('Values before actor sync\n%s', values)
        with self._session() as session:
            actors = self._get_actors(session, values)
            values = values.merge(
                actors, how='left', left_on=['profile_id', 'actor_name'],
                right_index=True,
            )
            values = values.drop(columns=['actor_name'])
            logger.debug('Values with actor sync\n%s', values)
            features = self._get_features(
                session, values['field_id'], values['actor_id'],
            )
            logger.debug("Features\n%s", features)
        scored_values = []
        with benchmark() as elapsed:
            for profile_id, group in values.groupby('profile_id'):
                profile = self._profiles_by_id[profile_id]
                logger.debug(
                    "Processing messages for profile %s", profile.name,
                )
                result, features = profile.process(group, features)
                logger.debug(
                    "Profile %s processing complete", profile.name,
                )
                scored_values.append(result)
            logger.info(
                "%d values processed between %d profiles in %.2f seconds",
                len(values), len(self.profiles), elapsed(),
            )
        if not scored_values:
            return
        scored_values = pa.concat(scored_values, ignore_index=True)
        anomalies = scored_values[scored_values['score'] > 0.0]
        logger.debug("Anomalies\n%s", anomalies)
        with self._session() as session:
            features = self._update_features(session, values).drop(
                columns=['count'],
            ).set_index(['field_id', 'actor_id', 'value'])
            # Set the feature ID on the anomalies before generating events
            # so that the event anomalies have a reference to the feature
            # that triggered them.
            anomalies = anomalies.merge(
                features, 'left', left_on=['field_id', 'actor_id', 'value'],
                right_index=True,
            )
            self._generate_events(session, indexed_messages, anomalies)

    def _generate_events(
        self,
        session: Session,
        messages: t.Dict[int, Message],
        anomalies: pa.DataFrame,
    ) -> None:
        message_ids = anomalies['message_id'].drop_duplicates()
        message_values = []
        for message_id in message_ids.values:
            message = messages[message_id]
            message_values.append({
                'message_id': str(message.id),
                'data': message.data,
            })
        if message_values:
            statement = pg.insert(db.Message.__table__).values(
                message_values,
            ).on_conflict_do_nothing(index_elements=[db.Message.id])
            with benchmark() as elapsed:
                session.execute(statement)
                logger.info(
                    "%d messages upserted in %.2f seconds",
                    len(message_values), elapsed(),
                )
        events = []
        event_anomalies: t.List[t.List[t.Dict]] = []
        for profile_id, profile_group in anomalies.groupby('profile_id'):
            for (message_id, actor_id, timestamp), ma_group in (
                profile_group.groupby(['message_id', 'actor_id', 'timestamp'])
            ):
                anomaly_instances = []
                for _, row in ma_group.iterrows():
                    anomaly_instances.append({
                        'feature_id': row['feature_id'],
                        'field_id': row['field_id'],
                        'score': row['score'],
                    })
                if anomaly_instances:
                    events.append({
                        'message_id': str(UUID(int=message_id)),
                        'actor_id': int(actor_id),
                        'created_at': timestamp.to_pydatetime(),
                    })
                    event_anomalies.append(anomaly_instances)
        if events:
            with benchmark() as elapsed:
                result = session.execute(
                    pg.insert(db.Event.__table__).returning(
                        db.Event.id,
                    ).values(events),
                )
                logger.info(
                    "%d events created in %.2f seconds", len(events),
                    elapsed(),
                )
            flattened_anomalies = []
            for index, row in enumerate(result):
                for instance in event_anomalies[index]:
                    instance['event_id'] = row[0]
                    flattened_anomalies.append(instance)
            del event_anomalies
            with benchmark() as elapsed:
                session.execute(pg.insert(db.Anomaly.__table__).values(
                    flattened_anomalies,
                ))
                logger.info(
                    "%d event anomalies created in %.2f seconds",
                    len(flattened_anomalies), elapsed(),
                )

    def _update_features(
        self,
        session: Session,
        values: pa.DataFrame,
    ) -> pa.DataFrame:
        if values.empty:
            logger.debug("No value features to update")
            return pa.DataFrame()
        value_feature_count = values.groupby(
            ['field_id', 'actor_id', 'value'],
        ).agg(value_count=('message_id', 'nunique'))
        updates = []
        for (field_id, actor_id, value), row in value_feature_count.iterrows():
            updates.append({
                'field_id': int(field_id),
                'actor_id': int(actor_id),
                'value': value,
                'count': int(row['value_count']),
            })
        statement = pg.insert(db.Feature.__table__).values(updates)
        statement = statement.on_conflict_do_update(
            index_elements=[
                db.Feature.field_id, db.Feature.actor_id, db.Feature.value,
            ],
            set_={'count': db.Feature.count + statement.excluded.count},
        )
        with benchmark() as elapsed:
            session.execute(statement)
            logger.info(
                "%d features updated in %.2f seconds", len(updates), elapsed(),
            )
        return self._get_features(
            session, values['field_id'], values['actor_id'],
        )

    def _get_features(
        self,
        session: Session,
        field_ids: pa.Series,
        actor_ids: pa.Series,
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
        field_ids: Series
            Series of field_ids present in the current message value
            DataFrame.
        actor_ids: Series
            Series of actor_ids present in the current message value
            Dataframe.

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
        query = session.query(
            db.Feature.id.label('feature_id'),
            db.Feature.field_id.label('field_id'),
            db.Feature.actor_id.label('actor_id'),
            db.Feature.value.label('value'),
            db.Feature.count.label('count'),
        ).filter(
            db.Feature.field_id.in_(
                field_ids.drop_duplicates().values.astype('object'),
            ),
            db.Feature.actor_id.in_(
                actor_ids.drop_duplicates().values.astype('object'),
            ),
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

    def _get_actors(
        self,
        session: Session,
        values: pa.DataFrame,
    ) -> pa.DataFrame:
        unique_pa = values[['profile_id', 'actor_name']].drop_duplicates()
        statement = pg.insert(db.Actor.__table__).values([
            {'profile_id': row['profile_id'], 'name': row['actor_name']}
            for _, row in unique_pa.iterrows()
        ]).on_conflict_do_nothing(
            index_elements=[db.Actor.profile_id, db.Actor.name],
        )
        with benchmark() as elapsed:
            session.execute(statement)
            session.commit()
            logger.info(
                "%d actors upserted in %.2f seconds", len(unique_pa),
                elapsed(),
            )
        query = session.query(
            db.Actor.profile_id.label('profile_id'),
            db.Actor.id.label('actor_id'),
            db.Actor.name.label('actor_name'),
        ).filter(
            db.Actor.profile_id.in_(
                unique_pa['profile_id'].drop_duplicates().values.astype(
                    'object',
                ),
            ),
            db.Actor.name.in_(
                unique_pa['actor_name'].drop_duplicates().values,
            ),
        )
        with benchmark() as elapsed:
            actors = pa.read_sql_query(query.statement, session.connection())
            logger.info(
                "%d actors fetched in %.2f seconds", len(actors), elapsed(),
            )
        actors = actors.set_index(['profile_id', 'actor_name'])
        return actors

    def _initiate_shutdown(self) -> None:
        logger.info("Initiated pipeline shutdown")
        self._shutdown.set()
        self._queue.put(PipelineEntry.blip('initiating shutdown'))

    def _start_timeout(self) -> None:
        """Starts the timeout thread.

        The timeout thread ensures that data is being processed at set
        intervals by the pipeline. This is useful
        """
        if self._timer and self._timer.is_alive():
            return
        logger.debug("Starting timeout thread")
        self._timer = threading.Timer(
            self._timeout_length, self._trigger_timeout(self._process_id),
        )
        self._timer.start()

    def _trigger_timeout(self, process_id: UUID):
        def callback():
            logger.debug("Timeout triggered")
            if self._process_id != process_id:
                logger.debug(
                    "Canceling timeout: timeout process IDs do not match",
                )
                return
            self._timeout.set()
            self._queue.put(PipelineEntry.blip("Timeout triggered"))
        return callback

    def _cancel_timeout(self):
        if not self._timer or not self._timer.is_alive():
            return
        self._timer.cancel()
        self._timer = None
        logger.debug("Timeout canceled")

    def _handle_entry(self, entry: Entry) -> None:
        source, kind, data = entry
        if source == PipelineEntry.SOURCE:
            if kind == PipelineEntry.Kinds.BLIP:
                logger.debug("Received pipeline blip: %s", data)
                return
            raise ValueError(f"Received unknown pipeline entry kind {kind!r}")
        if source == TransportEntry.SOURCE:
            logger.debug("Received transport entry")
            if kind == TransportEntry.Types.MESSAGE:
                logger.debug(
                    "Received transport message %s %s", data.id, data.data)
                self._messages.append(data)
                return self._start_timeout()
            if kind == TransportEntry.Types.SHUTDOWN:
                return self._handle_transport_shutdown(data)
            raise ValueError(f"Received unknown transport entry kind {kind!r}")
        raise ValueError(f"Received unknown entry source {source!r}")

    def _handle_transport_shutdown(self, transport: Transport):
        logger.info("Transport '%s' has shutdown", transport.name)
        self.transports.remove(transport)
        if not self.transports:
            logger.debug("All transports have been shutdown")
            logger.info("Shutting down pipeline")
            self._shutdown.set()
            self._queue.put(PipelineEntry.blip('Initiating shutdown'))
