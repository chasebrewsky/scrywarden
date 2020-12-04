import csv
import logging
import typing as t

from scrywarden.config import Config, parsers
from scrywarden.transport.base import EphemeralTransport
from scrywarden.transport.message import Message

logger = logging.getLogger(__name__)


class CSVTransport(EphemeralTransport):
    """Transport that reads messages from a CSV file.

    By default it yields each row as a message with the message data as a
    dictionary containing each row value. The dictionary has each header as
    the key and each value as the string value from the CSV.

    Override the `transform` method to transform this dictionary value before
    setting it as the message value.

    Setting a `process_check` integer value will log a report message every
    number of rows to keep the user updated on it's progress.

    Parameters
    ----------
    file: str
        Path the CSV is located at.
    headers: Iterable[str]
        Header names to use for the CSV if none are given in the file.
    process_check: int
        Log a processing check message after this values set value.
    """

    PARSER = parsers.Options({
        'file': parsers.String(),
        'headers': parsers.List(parsers.String()),
        'process_check': parsers.Integer(),
    })

    def __init__(
        self,
        file: str = '',
        headers: t.Iterable[str] = (),
        process_check: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file: str = file
        self.headers = headers or None
        self.process_check: int = process_check
        self._create_messages: t.Callable[
            [csv.DictReader], t.Iterable[Message],
        ] = self._create_messages_without_log

    def process(self) -> t.Iterable[Message]:
        """Reads all rows as messages.

        Returns
        -------
        Iterable[Messages]
            Iterable of each row as messages.
        """
        if self.process_check:
            self._create_messages = self._create_messages_with_log
        with open(self.file) as file:
            reader = csv.DictReader(file, fieldnames=self.headers or None)
            yield from self._create_messages(reader)

    def configure(self, config: Config) -> Config:
        self.file = config.get_value('file', self.file)
        self.headers = config.get_value('headers', self.headers)
        self.process_check = config.get_value(
            'process_check', self.process_check,
        )
        return config

    def transform(self, row: t.Dict) -> t.Dict:
        """Overridable method that transforms the row value.

        This modifies the row data before setting it as the message data. This
        is useful for things like parsing numbers from the string values.

        Parameters
        ----------
        row: Dict
            Row represented as a dictionary value.

        Returns
        -------
        Dict
            Modified row dictionary.
        """
        return row

    def _create_messages_without_log(
        self,
        reader: csv.DictReader,
    ) -> t.Iterable[Message]:
        for row in reader:
            yield self._create_message(row)

    def _create_message(self, row: t.Dict) -> Message:
        return Message.create(self.transform(row))

    def _create_messages_with_log(
        self,
        reader: csv.DictReader,
    ) -> t.Iterable[Message]:
        for index, row in enumerate(reader, 1):
            if index % self.process_check == 0:
                logger.info("%d rows read from '%s'", index, self.file)
            yield self._create_message(row)
