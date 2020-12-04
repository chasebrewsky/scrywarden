import os
import typing as t

from scrywarden.config.exceptions import (
    ValidationError, TransformationError, ParsingError,
)
from scrywarden.missing import Unset, MISSING
from scrywarden.config.settings import Setting
from scrywarden.module import import_string

T = t.TypeVar('T')
Fallback = t.Callable[[], Unset[T]]
Transformer = t.Callable[[t.Any], T]
Validator = t.Callable[[T], None]


def env(variable: str) -> Fallback:
    """Environment variable fallback function.

    Args:
        variable: Environment variable to retrieve value from.

    Returns:
        Getter function that retrieves either the string value of the
        environment variable for MISSING.
    """
    def callback():
        return os.environ.get(variable, MISSING)
    return callback


class Parser(t.Generic[T]):
    """Parses, transforms, and validates configuration values.

    Args:
        required: If the value is required from either the given value or one
            of the fallback values.
        fallbacks: Iterable of callables that will be used to retrieve a value
            if one is not given.
        default: Default value to use if a value is not given or retrieved
            from one of the fallback callables.
        transformers: Iterable of callables that will be used to transform
            the value before validation.
        validators: Iterable of callables that will be used to validate the
            parsed value.
    """
    def __init__(
        self,
        required: bool = False,
        fallbacks: t.Iterable[Fallback] = (),
        default: Unset[T] = MISSING,
        transformers: t.Iterable[Transformer] = (),
        validators: t.Iterable[Validator] = (),
    ):
        self.required: bool = required
        self.fallbacks: T.List[Fallback] = [*fallbacks]
        self.default: Unset[t.Union[T, t.Callable[[], T]]] = default
        self.transformers: t.List[Transformer] = [*transformers]
        self.validators: t.List[Validator] = [*validators]

    def parse(
        self,
        setting: Unset[Setting],
        key: t.Tuple[str, ...] = (),
    ) -> Unset[T]:
        """Parses the given setting value.

        Args:
            setting: Setting to parse.
            key: Current nested value in the setting. This is used to give
                context to ParsingError messages.

        Returns:
            Parsed value or MISSING if the value is missing.
        """
        if setting is MISSING:
            for fallback in self.fallbacks:
                setting = fallback()
                if setting is not MISSING:
                    break
            if setting is MISSING:
                if self.required:
                    raise ValidationError(
                        "Expected setting value but received none.", key=key,
                    )
                if self.default is not MISSING:
                    return self.default
                return setting
        setting = self.transform(setting, key=key)
        self.validate(setting, key=key)
        return setting

    def transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        setting = self._transform(setting, key=key)
        for transformer in self.transformers:
            try:
                setting = transformer(setting)
            except ParsingError as error:
                error.key = key
                raise error
            except Exception as error:
                raise TransformationError(
                    "Encountered unexpected error while transforming setting "
                    "value.", key=key,
                ) from error
        return setting

    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        return setting

    def validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        self._validate(setting, key=key)
        for validator in self.validators:
            try:
                setting = validator(setting)
            except ValidationError as error:
                error.key = key
                raise error
            except Exception as error:
                raise ValidationError(
                    "Encountered unexpected error while validating setting "
                    "value.", key=key,
                ) from error

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        """"""

    def __copy__(self):
        return self.__class__(
            required=self.required, fallbacks=self.fallbacks.copy(),
            default=self.default, transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )

    def copy(self):
        return self.__copy__()


class String(Parser[str]):
    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        if not isinstance(setting, str):
            setting = str(setting)
        return setting

    def _validate(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, str):
            raise ValidationError(
                f"Expected setting to be a string but received {setting!r}",
                key=key,
            )


class Integer(Parser[int]):
    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        if not isinstance(setting, int):
            try:
                setting = int(setting)
            except Exception as error:
                raise TransformationError(
                    "Expected setting to be an integer castable value but "
                    f"received incompatible value {setting!r}", key=key,
                ) from error
        return setting

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, int):
            raise ValidationError(
                f"Expected setting to be an integer but received {setting!r}",
                key=key,
            )


class Float(Parser[float]):
    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = (), ) -> T:
        if not isinstance(setting, float):
            try:
                setting = float(setting)
            except Exception as error:
                raise TransformationError(
                    "Expected setting to be a float castable value but "
                    f"received incompatible value {setting!r}", key=key,
                ) from error
        return setting

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, float):
            raise ValidationError(
                f"Expected setting to be a float but received {setting!r}",
                key=key,
            )


class Boolean(Parser[bool]):
    TRUE_STRINGS: t.Iterable[str] = ('true', 't', 'yes', 'y', '1')
    FALSE_STRINGS: t.Iterable[str] = ('false', 'f', 'no', 'n', '0')

    def __init__(
        self,
        true_strings: t.Iterable[str] = (),
        false_strings: t.Iterable[str] = (),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.true_strings: t.Set[str] = {
            value.lower() for value in (true_strings or self.TRUE_STRINGS)
        }
        self.false_strings: t.Set[str] = {
            value.lower() for value in (false_strings or self.FALSE_STRINGS)
        }

    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        if isinstance(setting, str):
            lowercase = setting.lower()
            if lowercase in self.true_strings:
                return True
            if lowercase in self.false_strings:
                return False
            return setting
        return bool(setting)

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, bool):
            raise ValidationError(
                f"Expected setting to be a bool but received {setting!r}",
                key=key,
            )

    def __copy__(self) -> 'Boolean':
        return self.__class__(
            true_strings=self.true_strings.copy(),
            false_strings=self.false_strings.copy(), required=self.required,
            fallbacks=self.fallbacks.copy(), default=self.default,
            transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )


class Options(Parser[t.Dict[str, t.Any]]):
    """Represents a dictionary with heterogeneous values.

    Args:
        parsers: Dictionary of parsers representing the expected keys and
            their expected parsing types.
    """
    def __init__(self, parsers: t.Mapping[str, Parser], **kwargs):
        super().__init__(**kwargs)
        self.parsers: t.Dict[str, Parser] = {**parsers}

    def transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        result = super().transform(setting, key=key)
        if not isinstance(setting, dict):
            return result
        parsed: t.Dict[str, t.Any] = {}
        for item, parser in self.parsers.items():
            value = parser.parse(setting.get(item, MISSING), key=(*key, item))
            if value is not MISSING:
                parsed[item] = value
        for key in setting.keys() - self.parsers.keys():
            parsed[key] = result[key]
        return parsed

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, dict):
            raise ValidationError(
                f"Expected setting to be a dict but received {setting!r}",
                key=key,
            )

    def extend(self, parsers: t.Mapping[str, Parser]) -> 'Options':
        copy = self.copy()
        copy.parsers.update(parsers)
        return copy

    def __copy__(self):
        return self.__class__(
            {key: parser.copy() for key, parser in self.parsers.items()},
            required=self.required, fallbacks=self.fallbacks.copy(),
            default=self.default, transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )


class Dict(Parser[t.Dict[str, t.Any]]):
    """Represents a dictionary with homogenous values.

    Args:
        parser: Parser to use for parsing each value.
    """
    def __init__(self, parser: t.Optional[Parser] = None, **kwargs):
        super().__init__(**kwargs)
        self.parser: t.Optional[Parser] = parser

    def transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        result = super().transform(setting, key=key)
        if not isinstance(setting, dict) or not self.parser:
            return result
        parsed: t.Dict[str, t.Any] = {}
        for item, value in result.items():
            parsed[item] = self.parser.parse(value, key=(*key, item))
        return parsed

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, dict):
            raise ValidationError(
                f"Expected setting to be a dict but received {setting!r}",
                key=key,
            )

    def __copy__(self) -> 'Dict':
        return self.__class__(
            parser=self.parser.copy() if self.parser else None,
            required=self.required, fallbacks=self.fallbacks.copy(),
            default=self.default, transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )


class List(Parser[t.List[t.Any]]):
    def __init__(
        self,
        parser: t.Optional[Parser] = None,
        separator: str = ',',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.parser: t.Optional[Parser] = parser
        self.separator: str = separator

    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        if isinstance(setting, list) or not isinstance(setting, str):
            return setting
        return setting.split(self.separator)

    def transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        result = super().transform(setting, key=key)
        if not isinstance(result, list) or not self.parser:
            return result
        parsed: t.List[t.Any] = []
        for index, value in enumerate(result):
            parsed.append(self.parser.parse(value, key=(*key, str(index))))
        return parsed

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if not isinstance(setting, list):
            raise ValidationError(
                f"Expected setting to be a list but received {setting!r}",
                key=key,
            )

    def __copy__(self):
        return self.__class__(
            parser=self.parser.copy() if self.parser else None,
            separator=self.separator, required=self.required,
            fallbacks=self.fallbacks.copy(), default=self.default,
            transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )


class Import(Parser[T]):
    def __init__(self, parent: t.Optional[t.Type[T]] = None, **kwargs):
        super().__init__(**kwargs)
        self.parent: t.Optional[t.Type] = parent

    def _transform(self, setting: Setting, key: t.Tuple[str, ...] = ()) -> T:
        if not isinstance(setting, str):
            raise TransformationError(
                f"Cannot import setting of type '{type(setting).__name__}' "
                "is must be a string", key=key,
            )
        try:
            return import_string(setting)
        except ImportError as error:
            raise TransformationError(
                f"Import string '{setting}' could not be imported", key=key,
            ) from error

    def _validate(self, setting: T, key: t.Tuple[str, ...] = ()) -> None:
        if self.parent is not None:
            if not issubclass(setting, self.parent):
                raise TransformationError(
                    "Imported value does not inherit from parent class "
                    f"'{self.parent.__name__}'", key=key,
                )

    def __copy__(self):
        return self.__class__(
            parent=self.parent, required=self.required,
            fallbacks=self.fallbacks.copy(), default=self.default,
            transformers=self.transformers.copy(),
            validators=self.validators.copy(),
        )
