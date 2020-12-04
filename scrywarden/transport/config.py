import typing as t

from scrywarden.exceptions import ConfigError
from scrywarden.config import parsers, Config
from scrywarden.transport.base import Transport

PARSER = parsers.Dict(parsers.Options({
    'class': parsers.Import(required=True, parent=Transport),
}))


def parse_transports(config: Config) -> t.Dict[str, Transport]:
    """Parses a dictionary of transports from a config object.

    Parameters
    ----------
    config: Config
        Configuration object to parse transports from.

    Returns
    -------
    Dict[str, Transport]
        Dictionary of the parsed transports indexed by their name.
    """
    config = config.parse(PARSER)
    transports: t.Dict[str, Transport] = {}
    for name in config.value.keys():
        transporter_config = config[name]
        cls: t.Type[Transport] = transporter_config['class'].value
        transport: Transport = cls(name=name)
        try:
            transport.configure(transporter_config)
        except ConfigError as error:
            raise error
        except Exception as error:
            raise ConfigError(
                f"Transporter '{name}' could not be configured correctly.",
            ) from error
        transports[name] = transport
    return transports
