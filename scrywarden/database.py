"""Defines all the database models and utilities."""

import typing as t
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Session

from scrywarden.config import parsers, Config

Base = declarative_base()


class Profile(Base):
    """Representation of a profile in the database."""

    __tablename__ = 'profile'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True)
    name = sa.Column(sa.String, unique=True)

    actors = relationship('Actor', back_populates='profile')
    fields = relationship('Field', back_populates='profile')
    investigation_groups = relationship(
        'InvestigationGroup', back_populates='profile',
    )


class Field(Base):
    """Representation of a profile field in the database."""

    __tablename__ = 'field'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True)
    profile_id = sa.Column(
        sa.Integer, sa.ForeignKey(Profile.id, ondelete='CASCADE'),
        nullable=False)
    name = sa.Column(sa.String, nullable=False)

    profile = relationship('Profile', back_populates='fields')
    features = relationship('Feature', back_populates='field')

    __table_args__ = (sa.UniqueConstraint(profile_id, name),)


class Actor(Base):
    """Representation of an actor in a profile dataset."""

    __tablename__ = 'actor'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True)
    profile_id = sa.Column(
        sa.Integer, sa.ForeignKey(Profile.id, ondelete='CASCADE'),
        nullable=False)
    name = sa.Column(sa.String, nullable=False)

    events = relationship('Event', back_populates='actor')
    profile = relationship('Profile', back_populates='actors')
    features = relationship('Feature', back_populates='actor')

    __table_args__ = (sa.UniqueConstraint(profile_id, name),)


class Feature(Base):
    """Feature model related to a profile evaluation."""

    __tablename__ = 'feature'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True)
    field_id = sa.Column(
        sa.Integer, sa.ForeignKey(Field.id, ondelete='CASCADE'),
        nullable=False)
    actor_id = sa.Column(
        sa.Integer, sa.ForeignKey(Actor.id, ondelete='CASCADE'),
        nullable=False)
    value = sa.Column(sa.String, nullable=False)
    count = sa.Column(sa.Integer, nullable=False)

    field = relationship('Field', back_populates='features')
    actor = relationship('Actor', back_populates='features')

    __table_args__ = (sa.UniqueConstraint(field_id, actor_id, value),)


class Message(Base):
    """Message received from a transport."""

    __tablename__ = 'message'

    id = sa.Column(
        f'{__tablename__}_id', pg.UUID(as_uuid=True), primary_key=True,
    )
    data = sa.Column(pg.JSONB(none_as_null=True))


class Event(Base):
    """Message that generated anomalies."""

    __tablename__ = 'event'

    id = sa.Column(
        f'{__tablename__}_id', sa.BigInteger, primary_key=True,
        autoincrement=True
    )
    message_id = sa.Column(
        pg.UUID(as_uuid=True), sa.ForeignKey(Message.id, ondelete='CASCADE'),
        nullable=False,
    )
    actor_id = sa.Column(
        sa.Integer, sa.ForeignKey(Actor.id, ondelete='CASCADE'),
        nullable=False,
    )
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, index=True,
        server_default=sa.func.now(),
    )

    actor = relationship('Actor', back_populates='events')
    anomalies = relationship('Anomaly', back_populates='event')
    investigations = relationship(
        'Investigation', secondary=lambda: InvestigationEvent,
        back_populates='events',
    )

    @property
    def score(self):
        return sum(anomaly.score for anomaly in self.anomalies)


class Anomaly(Base):
    """Field feature that generated an anomaly score."""

    __tablename__ = 'anomaly'

    id = sa.Column(
        f'{__tablename__}_id', sa.BigInteger, primary_key=True,
        autoincrement=True,
    )
    event_id = sa.Column(
        sa.BigInteger, sa.ForeignKey(Event.id, ondelete='CASCADE'),
        nullable=False,
    )
    field_id = sa.Column(
        sa.Integer, sa.ForeignKey(Field.id, ondelete='CASCADE'),
        nullable=False,
    )
    feature_id = sa.Column(
        sa.Integer, sa.ForeignKey(Feature.id, ondelete='CASCADE'),
        nullable=False,
    )
    score = sa.Column(sa.Float, nullable=False)

    event = relationship('Event', back_populates='anomalies')

    __table_args__ = (sa.UniqueConstraint(event_id, field_id),)


class InvestigationGroup(Base):
    """Group of investigations related to a profile."""

    __tablename__ = 'investigation_group'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True,
    )
    profile_id = sa.Column(
        sa.Integer, sa.ForeignKey(Profile.id, ondelete='CASCADE'),
        nullable=False,
    )
    name = sa.Column(sa.String, nullable=False, default='')

    profile = relationship('Profile', back_populates='investigation_groups')
    investigations = relationship('Investigation', back_populates='group')

    __table_args__ = (sa.UniqueConstraint(profile_id, name),)


class Investigator(Base):
    """Currently running investigator threads."""

    __tablename__ = 'investigator'

    id = sa.Column(
        f'{__tablename__}_id', pg.UUID(as_uuid=True), primary_key=True,
    )
    profile_id = sa.Column(
        sa.Integer, sa.ForeignKey(Profile.id, ondelete='SET NULL'),
    )
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, index=True,
        server_default=sa.func.now(),
    )


class Investigation(Base):
    """Investigation that took place for an investigation group."""

    __tablename__ = 'investigation'

    id = sa.Column(
        f'{__tablename__}_id', sa.Integer, primary_key=True,
        autoincrement=True,
    )
    group_id = sa.Column(
        f'{InvestigationGroup.__tablename__}_id', sa.Integer,
        sa.ForeignKey(InvestigationGroup.id, ondelete='CASCADE'),
        nullable=False,
    )
    index = sa.Column(sa.Integer, nullable=True)
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, index=True,
        server_default=sa.func.now(),
    )
    created_by = sa.Column(
        pg.UUID(as_uuid=True),
        sa.ForeignKey(Investigator.id, ondelete='SET NULL'),
    )
    completed_at = sa.Column(sa.DateTime(timezone=True), index=True)
    is_assigned = sa.Column(sa.Boolean, nullable=False, default=False)
    options = sa.Column(pg.JSONB)

    events = relationship(
        'Event', secondary=lambda: InvestigationEvent,
        back_populates='investigations',
    )
    group = relationship('InvestigationGroup', back_populates='investigations')

    __table_args__ = (sa.UniqueConstraint(group_id, index),)


InvestigationEvent = sa.Table(
    'investigation_event', Base.metadata,
    sa.Column(
        'investigation_id', sa.Integer,
        sa.ForeignKey(Investigation.id, ondelete='CASCADE'),
        nullable=False,
    ),
    sa.Column(
        'event_id', sa.BigInteger,
        sa.ForeignKey(Event.id, ondelete='CASCADE'),
        nullable=False,
    ),
    sa.UniqueConstraint('investigation_id', 'event_id'),
)
"""Many to many relation for investigations and events."""


PARSER = parsers.Options({
    'host': parsers.String(default='localhost'),
    'port': parsers.Integer(default=5432),
    'name': parsers.String(default='scrywarden'),
    'user': parsers.String(default='scrywarden'),
    'password': parsers.String(default='scrywarden'),
})


def parse_engine(config: Config) -> Engine:
    """Parses an SQLAlchemy engine from a config object.

    Parameters
    ----------
    config: Config
        Configuration object to parse engine from.

    Returns
    -------
    Engine
        SQLAlchemy engine.
    """
    config = config.parse(PARSER)
    return sa.create_engine(
        "postgresql+psycopg2://"
        f"{config['user'].value}:{config['password'].value}"
        f"@{config['host'].value}:{config['port'].value}/"
        f"{config['name'].value}",
    )


@contextmanager
def managed_session(
    factory: sessionmaker, **kwargs,
) -> t.ContextManager[Session]:
    """Wraps a session in a context manager to manage session lifetime.

    Makes sure to commit if the code in the context runs successfully,
    otherwise it rolls back. Also ensures that the session is closed.

    Parameters
    ----------
    factory: sessionmaker
        SQLAlchemy session factory to use.
    kwargs: Dict
        Keyword arguments to pass to the session factory.

    Returns
    -------
    Session
        Managed session object.
    """
    session = factory(**kwargs)
    try:
        yield session
        session.commit()
    except Exception as error:
        session.rollback()
        raise error
    finally:
        session.close()


def create_session_factory(engine: Engine) -> sessionmaker:
    """Creates a session factory from an engine.

    Parameters
    ----------
    engine: Engine
        SQLAlchemy engine.

    Returns
    -------
    sessionmaker
        Session factory.
    """
    return sessionmaker(bind=engine)


def migrate(engine: Engine) -> None:
    """Ensures that the connected database has all the database objects.

    Very simple migration. Does not handle changing the models currently.

    Parameters
    ----------
    engine: Engine
        SQLAlchemy engine.
    """
    Base.metadata.create_all(engine)
