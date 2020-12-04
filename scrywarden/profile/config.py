import typing as t

from scrywarden.exceptions import ConfigError
from scrywarden.profile import Profile
from scrywarden.config import parsers, Config
from scrywarden.profile.analyzers import parse_analyzer
from scrywarden.profile.collectors import parse_collector

PARSER = parsers.Dict(parsers.Options({
    'class': parsers.Import(required=True, parent=Profile),
    'config': parsers.Options({}),
    'collector': parsers.Options({}),
    'analyzer': parsers.Options({}),
}))


def parse_profiles(
    config: Config,
    extras: t.Sequence[str] = (),
) -> t.Dict[str, t.Dict[str, t.Any]]:
    """Parses profiles from a configuration.

    Returns a dictionary with the related objects. The complete dictionary
    has the following keys:

    * profile: The parsed profile.
    * collector: The parsed collector used for the profile.
    * analyzer: The parsed analyzer used for the profile.

    Parameters
    ----------
    config: Config
        Configuration object to parse from.
    extras: Sequence[str]
        Extra objects to load related to the profile. Right now this includes
        'collector' and 'analyzer'.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary containing the related parsed objects.
    """
    config = config.parse(PARSER)
    profiles: t.Dict[str, t.Dict[str, t.Any]] = {}
    for name in config.value.keys():
        profile_config = config[name]
        cls: t.Type[Profile] = profile_config['class'].value
        profile = cls(name=name)
        try:
            profile.configure(profile_config)
        except ConfigError as error:
            raise error
        except Exception as error:
            raise ConfigError(
                f"Profile '{name}' could not be configured",
            ) from error
        value = {'profile': profile}
        if 'collector' in extras and 'collector' in profile_config:
            try:
                value['collector'] = parse_collector(
                    profile_config['collector'],
                )
            except Exception as error:
                raise ConfigError(
                    f"Profile '{name}' collector could not be parsed",
                ) from error
        if 'analyzer' in extras and 'analyzer' in profile_config:
            try:
                value['analyzer'] = parse_analyzer(
                    profile_config['analyzer'],
                )
            except Exception as error:
                raise ConfigError(
                    f"Profile '{name}' analyzer could not be parsed",
                ) from error
        profiles[name] = value
    return profiles
