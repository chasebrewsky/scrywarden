"""Contains transports that generate test messages at set intervals.

These are useful when performing testing on profiles.
"""

import json
import logging
import typing as t

from scrywarden.config import Setting, Config, parsers
from scrywarden.config.exceptions import ValidationError
from scrywarden.transport.message import Message
from scrywarden.transport.base import IntervalTransport
from scrywarden.typing import JSONValue, JSONList
from scrywarden.missing import MISSING, Unset

logger = logging.getLogger(__name__)


def _is_json(value: Setting) -> None:
    """Validates that a data structure is JSON serializable."""
    try:
        json.dumps(value)
    except Exception as error:
        raise ValidationError("Invalid JSON value") from error


class HeartbeatTransport(IntervalTransport):
    """Test transport that sends a JSON message at the given interval.

    Parameters
    ----------
    count: int
        Number of times to send the message per interval. Defaults to 1.
    data: JSONValue
        JSON data to send. Defaults to {"greeting": "hello"}.

    Attributes
    ----------
    count: int
        Number of times to send the message per interval. Defaults to 1.
    data: JSONValue
        JSON data to send. Defaults to {"greeting": "hello"}.
    """
    PARSER = IntervalTransport.PARSER.extend({
        'count': parsers.Integer(),
        'data': parsers.Parser(validators=[_is_json]),
    })

    def __init__(
        self,
        count: int = 1,
        data: Unset[JSONValue] = MISSING,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.count: int = count
        self.data: JSONValue = (
            data if data is not MISSING
            else {'person': 'George', 'greeting': 'hello'}
        )

    def configure(self, config: Config) -> Config:
        config = super().configure(config)
        self.count = config.get_value('count', self.count)
        if 'data' in config:
            self.data = json.dumps(config.get_value('data'))
        return config

    def process(self) -> t.Iterable[Message]:
        """Returns an iterable messages for the heartbeat.

        Returns
        -------
        Iterable[Message]
            Iterable of the amount of messages.
        """
        for _ in range(self.count):
            message = Message.create(self.data)
            logger.info("Sending heartbeat message %s", message.data)
            yield message


class MixedHeartbeatTransport(IntervalTransport):
    """Test transport that sends multiple JSON messages per interval.

    Parameters
    ----------
    count: int
        Number of times to send the message per interval. Defaults to 1.
    data: JSONValue
        JSON data to send. Defaults to {"greeting": "hello"}.
    """
    PARSER = IntervalTransport.PARSER.extend({
        'count': parsers.Integer(),
        'data': parsers.List(parsers.Parser(validators=[_is_json])),
    })

    def __init__(
        self,
        count: int = 1,
        data: Unset[JSONList] = MISSING,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.count: int = count
        self.data: JSONList = (
            data if data is not MISSING
            else [
                {'person': 'George', 'greeting': 'hello'},
                {'person': 'Ben', 'greeting': 'howdy'},
                {'person': 'Susan', 'greeting': 'salutations'},
            ]
        )

    def configure(self, config: Config) -> Config:
        config = super().configure(config)
        self.count = config.get_value('count', self.count)
        if 'data' in config:
            self.data = []
            for item in config.get_value('data'):
                self.data.append(json.dumps(item))
        return config

    def process(self) -> t.Iterable[Message]:
        """Returns an iterable messages for the heartbeat.

        Returns
        -------
        Iterable[Message]
            Iterable of the amount of messages.
        """
        for _ in range(self.count):
            for item in self.data:
                message = Message.create(item)
                logger.info("Sending heartbeat message %s", message.data)
                yield message
