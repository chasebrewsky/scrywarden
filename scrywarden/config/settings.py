import typing as t

from scrywarden.missing import Unset, MISSING

T = t.TypeVar('T')
Setting = t.Union[T, 'SettingList', 'SettingDict']
SettingList = t.List[Setting]
SettingDict = t.Dict[str, Setting]
SettingKey = t.Tuple[str, ...]


def normalize_key(key: t.Sequence[str]) -> SettingKey:
    """Normalizes a key value into a tuple of strings.

    The given key can be a string or a sequence of strings. An empty string
    or iterable will result in an empty tuple, representing the root of the
    setting tree.

    Args:
        key: String or iterable of strings.

    Returns:
        Tuple key value.
    """
    if isinstance(key, tuple):
        return key
    if isinstance(key, str):
        return key,
    if isinstance(key, t.Sequence):
        return tuple(key)
    raise TypeError("Key value must be a string or an iterable of strings")


def get(
    setting: Setting,
    key: t.Sequence[str],
    default: Unset = MISSING,
) -> Setting:
    """Retrieve a key value from a given setting value.

    Args:
        setting: Setting to retrieve value from.
        key: Key of the value to retrieve.
        default: Default value to return if the value is not found.

    Returns:
        Found value or given default value.

    Raises:
        KeyError: If the key value cannot be found and no default value
            is given.
    """
    if not key:
        return setting
    normalized_key = normalize_key(key)
    try:
        return _get(setting, normalized_key)
    except KeyError:
        if default is MISSING:
            raise KeyError(key) from None
        return default


def _get(setting: Setting, key: SettingKey) -> Setting:
    """Internal implementation of retrieving a setting value.

    Only accepts normalized setting keys.

    Args:
        setting: Setting to search for key value in.
        key: Normalized setting key to retrieve.

    Returns:
        Found value.

    Raises:
        KeyError: If the value cannot be found.
    """
    if not key:
        return setting
    target, remaining = key[0], key[1:]
    if isinstance(setting, dict):
        found = setting[target]
        return _get(found, remaining)
    if isinstance(setting, list):
        if not target.isdigit():
            raise KeyError(target)
        try:
            found = setting[int(target)]
        except IndexError:
            raise KeyError(target)
        return _get(found, remaining)
    raise KeyError(key)


def update(setting: Setting, target: Setting) -> Setting:
    if isinstance(setting, dict):
        if not isinstance(target, dict):
            return target
        updated = {**setting}
        for key, value in target.items():
            updated[key] = update(setting.get(key, MISSING), value)
        return updated
    if isinstance(setting, list):
        if not isinstance(target, list):
            return target
        return [*setting, *target]
    return target


def keys(setting: Setting) -> t.Iterator[SettingKey]:
    if isinstance(setting, dict):
        for base, value in setting.items():
            yield base,
            for key in keys(value):
                yield base, *key
        return
    if isinstance(setting, list):
        for index, value in enumerate(setting):
            base = str(index)
            yield base,
            for key in keys(value):
                yield base, *key
        return


def copy(setting: Setting) -> Setting:
    if isinstance(setting, dict):
        return {key: copy(value) for key, value in setting.items()}
    if isinstance(setting, list):
        return [copy(value) for value in setting]
    return setting
