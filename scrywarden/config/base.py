import typing as t

import yaml

from scrywarden.config import exceptions
from scrywarden.config import settings
from scrywarden.missing import MISSING

if t.TYPE_CHECKING:
    from scrywarden.config.parsers import Parser, Transformer


class Config(t.Mapping[t.Sequence[str], 'Config']):
    def __init__(
        self,
        value: settings.Setting,
        key: t.Tuple[str, ...] = (),
    ):
        self.key = key
        self._value: settings.Setting = value
        self._cache: t.Dict[
            t.Tuple[str, ...], settings.Setting,
        ] = {(): self._value}

    def __getitem__(self, key: t.Sequence[str]) -> 'Config':
        normalized_key = settings.normalize_key(key)
        full_key = (*self.key, *normalized_key)
        if normalized_key in self._cache:
            return Config(
                self._cache[normalized_key],
                key=full_key,
            )
        try:
            value = settings.get(self._value, normalized_key)
        except KeyError:
            raise KeyError(full_key) from None
        self._cache[normalized_key] = value
        return Config(value, key=full_key)

    def __len__(self) -> int:
        return sum(1 for _ in iter(self))

    def __iter__(self) -> t.Iterator[t.Tuple[str, ...]]:
        return settings.keys(self._value)

    @property
    def value(self) -> settings.Setting:
        return settings.copy(self._value)

    def get(
        self,
        key: t.Sequence[str],
        default: t.Optional = MISSING,
    ) -> 'Config':
        try:
            return self[key]
        except KeyError:
            normalized_key = settings.normalize_key(key)
            full_key = (*self.key, *normalized_key)
            return Config(default, key=full_key)

    def value_as(self, transformer: 'Transformer') -> t.Any:
        try:
            return transformer(self.value)
        except Exception as error:
            raise exceptions.TransformationError(
                "Value could not be transformed", key=self.key,
            ) from error

    def get_value(self, key: t.Sequence[str], default: t.Optional = None):
        config = self.get(key)
        if config is None or config.value is MISSING:
            return default
        return config.value

    def get_value_as(
        self,
        key: t.Sequence[str],
        transformer: 'Transformer',
        default: t.Optional = None,
    ) -> t.Any:
        config = self.get(key)
        if config is None or config.value is MISSING:
            return default
        return config.value_as(transformer)

    def parse(self, parser: 'Parser') -> 'Config':
        return self.__class__(
            parser.parse(self.value, key=self.key), key=self.key,
        )


def parse_config(path: str) -> Config:
    """Parses a config object from a file at the given path.

    Parameters
    ----------
    path
        YAML file path to parse the config from.

    Returns
    -------
    Parsed config object.
    """
    with open(path, 'r') as file:
        return Config(yaml.safe_load(file))
