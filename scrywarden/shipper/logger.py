import logging

from pandas import DataFrame

from scrywarden.shipper import Shipper
from scrywarden.config import parsers, Config
from scrywarden.config.exceptions import ValidationError

logger = logging.getLogger(__name__)


def is_valid_level(value: str) -> None:
    """Determines if the logging level value in the config is a valid level."""
    level = logging.getLevelName(value)
    if not isinstance(level, int):
        raise ValidationError(
            f"{value!r} is not a valid logging level",
        )


class LoggerShipper(Shipper):
    """Monitor that logs alerts to the standard logger."""

    PARSER = parsers.Options({
        'level': parsers.String(validators=[is_valid_level]),
    })

    def __init__(self, level: int = logging.INFO, **kwargs):
        super().__init__(**kwargs)
        self.level: int = level

    def configure(self, config: Config) -> None:
        config = super().configure(config)
        if 'level' in config:
            self.level = logging.getLevelName(config['level'].value)

    def ship(self, investigation, anomalies: DataFrame) -> None:
        for row in anomalies.iterrows():
            logger.log(self.level, "\n%s", row)


class LoggerCountShipper(Shipper):
    PARSER = parsers.Options({
        'level': parsers.String(validators=[is_valid_level]),
    })

    def __init__(self, level: int = logging.INFO, **kwargs):
        super().__init__(**kwargs)
        self.level: int = level

    def configure(self, config: Config) -> None:
        config = super().configure(config)
        if 'level' in config:
            self.level = logging.getLevelName(config['level'].value)

    def ship(self, investigation, anomalies: DataFrame) -> None:
        logger.log(self.level, "Received %d events", len(anomalies))
