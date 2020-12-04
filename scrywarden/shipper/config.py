import typing as t

from scrywarden.exceptions import ConfigError
from scrywarden.shipper import Shipper
from scrywarden.config import Config, parsers

PARSER = parsers.Dict(parsers.Options({
    'class': parsers.Import(required=True, parent=Shipper),
    'limit': parsers.Integer(default=10),
    'config': parsers.Options({}),
}))


def parse_shippers(
    config: Config,
) -> t.Dict[str, Shipper]:
    """Parses a dictionary of shippers from a config object.

    Parameters
    ----------
    config: Config
        Configuration object to parse the shippers from.

    Returns
    -------
    Dict[str, Shipper]
        Dictionary of shippers indexed by their name.
    """
    config = config.parse(PARSER)
    profiles: t.Dict[str, Shipper] = {}
    for name in config.value.keys():
        monitor_config = config[name]
        cls: t.Type[Shipper] = monitor_config['class'].value
        shipper: Shipper = cls(name=name)
        shipper.queue_size = monitor_config.get_value(
            'limit', shipper.queue_size,
        )
        try:
            shipper.configure(monitor_config.get('config', {}))
        except ConfigError as error:
            raise error
        except Exception as error:
            raise ConfigError(
                f"Shipper '{name}' could not be configured correctly.",
            ) from error
        profiles[name] = shipper
    return profiles
