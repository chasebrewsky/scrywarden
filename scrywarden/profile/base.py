import logging
import typing as t
from types import MappingProxyType

import orjson
import pandas as pa
from pandas import DataFrame
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.orm.exc import NoResultFound

from scrywarden import database as db
from scrywarden.exceptions import ProfileError
from scrywarden.profile.fields import Field
from scrywarden.config import Config
from scrywarden.config.parsers import Parser
from scrywarden.transport.message import Message

logger = logging.getLogger(__name__)


class ProfileMeta(type):
    """Metaclass of the profile class.

    This class controls how each profile class is created. This process
    includes handling the inheritance of parent models and attaching the field
    instances to the profile class for processing.
    """
    def __new__(
        mcs,
        name: str,
        bases: t.Tuple[t.Type, ...],
        namespace: t.Dict[str, t.Any],
        **kwargs,
    ):
        if not [b for b in bases if isinstance(b, ProfileMeta)]:
            return super().__new__(mcs, name, bases, namespace)
        cls: t.Type[Profile] = super().__new__(mcs, name, bases, namespace)
        fields: t.Dict[str, Field] = {}
        # Inherit fields from parent classes.
        for base in bases:
            if not issubclass(base, Profile):
                continue
            for field in getattr(base, '__fields__', ()):
                fields[field.name] = field
        field_names: t.Set[str] = set()
        for name, attr in namespace.items():
            if not isinstance(attr, Field):
                continue
            if not attr.name:
                attr.name = name
            if attr.name in field_names:
                raise ProfileError(
                    f"{Profile.__name__} contains multiple fields with "
                    f"name '{attr.name}'"
                )
            attr.profile = cls
            field_names.add(attr.name)
            fields[attr.name] = attr
        cls.__fields__ = tuple(fields.values())
        return cls


class ProfileField(t.NamedTuple):
    """Data structure that groups related profile field data."""

    instance: Field  # Instance of the field.
    model: t.Optional[db.Field] = None  # Synced database model of he field.

    def sync(self, model: db.Field) -> 'ProfileField':
        """Syncs a file instance to a database model.

        Parameters
        ----------
        model: db.Field
            Database model to sync with.

        Returns
        -------
        ProfileField
            New profile field with the synced model.
        """
        return ProfileField(self.instance, model=model)


class FieldMapping(t.Mapping[str, ProfileField]):
    """Helper class that contains profile field information.

    Fields are mapped to their field names.

    Parameters
    ----------
    fields: Mapping[str, ProfileField]
        Mapping of profile fields to store.

    Attributes
    ----------
    by_id: Mapping[int, ProfileField]
        Mapping of the profile fields to their model ID.
    """
    def __init__(
        self,
        fields: t.Mapping[str, ProfileField],
    ):
        self._by_name: t.Mapping[str, ProfileField] = fields
        by_id = {}
        for name, field in fields.items():
            if field.model:
                by_id[field.model.id] = field
        self.by_id: t.Mapping[int, ProfileField] = MappingProxyType(by_id)

    def __getitem__(self, item: str) -> ProfileField:
        return self._by_name[item]

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._by_name)

    def __len__(self) -> int:
        return len(self._by_name)


class Profile(metaclass=ProfileMeta):
    """Base class used to create behavioral profiles.

    Subclassing this class allows for developers and end users to create a
    framework that can identify and detect anomalous activity on a stream
    of messages. Feature models are set by fields as class attributes on the
    class::

        from scrywarden.profile import Profile, fields

        class Example(Base):
            greeting = fields.Single()

    This profile example contains a field on the profile called `greeting`.
    This field checks messages for the field `greeting` in the message's JSON
    data and returns it as the feature model value for that feature. So if
    a JSON message with the following value is given::

        {"person": "Bob", "greeting": "Hello"}

    The resulting feature value is `"Hello"`.

    This value has to be associated with a given actor to mean anything though.
    In order to do that, the profile subclass has to set two methods,
    `matches` and `get_actor`::

        from scrywarden.profile import Profile, fields
        from scrywarden.transport.message import Message

        class Example(Base):
            greeting = fields.Single()

            def matches(message: Message) -> bool:
                return 'greeting' in message

            def get_actor(message: Message) -> str:
                return message['person']

    The `matches` method determines if the message matches the profile. If it
    doesn't, then it is removed from the anomaly detection process for this
    profile. The `get_actor` method determines the given actor for this
    behavioral profile. If the previous example data was passed to this
    profile, then it would return `"Bob"` which is the the `person` key value
    from the JSON object.
    """
    __fields__: t.Tuple[Field, ...]

    PARSER: t.Optional[Parser] = None

    def __init__(self, name: str = ''):
        self.name: str = name
        self.model: t.Optional[db.Profile] = None
        self.fields: FieldMapping = FieldMapping({
            field.name: ProfileField(field) for field in self.__fields__
        })

    def matches(self, message: Message) -> bool:
        """Method that determines if a message matches the profile.

        This is used during the identification phase of the anomaly detection
        process. It helps to filter out messages that don't need to be
        processed or may not contain the required fields to be processed.

        Parameters
        ----------
        message: Message
            Message to check.

        Returns
        -------
        bool
            If the given message matches the profile.
        """
        raise NotImplementedError()

    def get_actor(self, message: Message) -> str:
        """Method that determines the actor of a given message.

        This is used during the identification phase of the anomaly detection
        process. The returned actor must always be a string, and the default
        identification process will warn users when an empty string is
        returned, but will allow it to happen in order to process it.

        Parameters
        ----------
        message: Message
            Message received from a transport.

        Returns
        -------
        str
            Name of the actor.
        """
        raise NotImplementedError()

    def configure(self, config: Config) -> Config:
        """Optional overridable method that configures class from config.

        If a config parser is included on the PARSER class attribute, it uses
        that to parse the incoming config.

        Parameters
        ----------
        config: Config
            Config object created from the YAML config.

        Returns
        -------
        Config
            Config object passed into the function.
        """
        return config.parse(self.PARSER) if self.PARSER else config

    def sync(self, session: Session) -> None:
        """Syncs this profile to the database.

        Parameters
        ----------
        session: Session
            Current SQLAlchemy database session.
        """
        try:
            self.model = session.query(db.Profile).filter(
                db.Profile.name == self.name,
            ).options(joinedload(db.Profile.fields)).one()
        except NoResultFound:
            self.model = db.Profile(name=self.name)
            session.add(self.model)
            session.flush()
        fields: t.Dict[str, ProfileField] = {}
        for field in self.model.fields:
            if field.name not in self.fields:
                continue
            fields[field.name] = self.fields[field.name].sync(field)
        missing_field_names = self.fields.keys() - fields.keys()
        if missing_field_names:
            for field_name in missing_field_names:
                field = db.Field(name=field_name, profile=self.model)
                session.add(field)
                fields[field.name] = self.fields[field.name].sync(field)
            session.flush()
        self.fields = FieldMapping(fields)

    def identify(self, messages: t.Iterable[Message]) -> pa.DataFrame:
        """Creates a dataframe of the retrieved message field values.

        The created dataframe contains the following columns:

        * profile_id (int)
        * message_id (int)
        * timestamp (datetime)
        * actor_name (str)
        * field_id (int)
        * value (str)

        Parameters
        ----------
        messages: Iterable[Messages]
            Iterable of messages to process.

        Returns
        -------
        DataFrame
            Dataframe containing the identified messages for the profile.
        """
        df = pa.DataFrame(
            self._generate_rows(messages),
            columns=[
                'message_id', 'timestamp', 'actor_name',
                'field_id', 'value',
            ],
        )
        df['profile_id'] = self.model.id
        logger.debug('Profile %s identified messages\n%s', self.name, df)
        return df

    def process(
        self,
        values: pa.DataFrame,
        features: pa.DataFrame,
    ) -> t.Tuple[pa.DataFrame, pa.DataFrame]:
        """Creates a data frame containing the anomaly score for

        The message value dataframes should have the shape:

        * profile_id (int): ID of the profile.
        * message_id (int): UUID of the associated message.
        * timestamp (datetime): Timestamp the message took place.
        * actor_id (int): ID of the associated actor.
        * field_id (int): ID of the associated field.
        * value (str): Extracted message value.

        The features dataframe should have the shape:

        * feature_id (int): ID of the feature.
        * field_id (int): ID of the associated field.
        * actor_id (int): ID of the associated actor.
        * value (str): JSON value associated with the feature.
        * count (int): Current number of times this feature has been
            present in messages.

        Parameters
        ----------
        values: DataFrame
            DataFrame containing the matching profile message value info.
            Contains columns profile_id (int), message_id (int),
            field_id (int), actor_id (int), and value (str).
        features: DataFrame
            DataFrame containing the feature aggregation values. Contains
            columns feature_id (int), field_id (int), actor_id (int),
            value (str), and count (int).

        Returns
        -------
        DataFrame
            DataFrame containing the same columns as the values dataframe
            with an additional score (float) column indicating the fields
            anomaly score.
        """
        results = []
        for field_id, group in values.groupby('field_id'):
            result = self.fields.by_id[field_id].instance.reporter(
                group, features,
            )
            results.append(result)
            group = group.reset_index()
            features = update_feature_count(group, features)
        return pa.concat(results, ignore_index=True), features

    def _get_actor_name(self, message: Message) -> str:
        actor_name = self.get_actor(message)
        if not isinstance(actor_name, str):
            raise ValueError(
                f"Message {message.id} actor must be a string value",
            )
        return actor_name

    def _get_field_value(self, field: Field, message: Message) -> str:
        value = field.get_value(message)
        if value is None:
            return ''
        try:
            serialized_value = str(
                orjson.dumps(value, option=orjson.OPT_SORT_KEYS), 'utf-8'
            )
        except Exception as error:
            raise ValueError(
                f"Message {message.id} value for field {field.name!r} is not "
                "JSON serializable",
            ) from error
        return serialized_value

    def _generate_rows(self, messages: t.Iterable[Message]) -> t.Iterator[
        t.Tuple[int, str, int, str],
    ]:
        for message in messages:
            if not self.matches(message):
                continue
            try:
                actor_name = self._get_actor_name(message)
            except Exception as error:
                logger.exception(error)
                continue
            for field in self.fields.values():
                try:
                    value = self._get_field_value(field.instance, message)
                except Exception as error:
                    logger.exception(error)
                    continue
                yield (
                    message.id.int, pa.to_datetime(message.timestamp),
                    actor_name, field.model.id, value,
                )


def update_feature_count(
    values: pa.DataFrame,
    features: pa.DataFrame,
) -> pa.DataFrame:
    """Updates the current feature count based on the message values.

    Parameters
    ----------
    values: DataFrame
        Collected message values.
    features: DataFrame
        Current features indexed by their feature ID.
    Returns
    -------
    DataFrame
        Updated features with the updated feature counts.
    """
    grouped = values.groupby(
        ['field_id', 'actor_id', 'value'],
    ).agg(count=('message_id', 'nunique'))
    indexed = features.set_index(['field_id', 'actor_id', 'value'])
    indexed.loc[indexed.index.isin(grouped.index), 'count'] += grouped['count']
    indexed['count'] = indexed['count'].astype('int')
    missing = grouped.loc[~grouped.index.isin(indexed.index)]
    missing = missing.reset_index()
    indexed = indexed.reset_index()
    missing['feature_id'] = 0
    features = pa.concat([indexed, missing], ignore_index=True)
    return features


def sync_profiles(session: Session, profiles: t.Iterable[Profile]) -> None:
    """Helper function that syncs all profiles to the database.

    Parameters
    ----------
    session: Session
        Current SQLAlchemy session.
    profiles: Iterable[Profile]
        Iterable of profiles to sync.
    """
    for profile in profiles:
        profile.sync(session)
