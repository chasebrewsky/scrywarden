import typing as t


class ParsingError(Exception):
    """"""
    def __init__(self, *args, key: t.Tuple[str, ...] = ()):
        super().__init__(*args)
        self.key: t.Tuple[str, ...] = key

    def __str__(self):
        if self.key:
            return f"{self.key}: {super().__str__()}"
        return super().__str__()


class TransformationError(ParsingError):
    """"""


class ValidationError(ParsingError):
    """"""
