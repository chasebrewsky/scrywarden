import logging
import os

import pandas as pa

from scrywarden.shipper import Shipper
from scrywarden.config import parsers, Config

logger = logging.getLogger(__name__)


class CSVShipper(Shipper):
    """Saves the malicious anomalies to a CSV.

    Appends results instead of writing over the file.

    Parameters
    ----------
    filename: str
        Path to the CSV file.
    """
    PARSER = parsers.Options({
        'filename': parsers.String(),
    })

    """Monitor that writes alerts to a CSV file."""
    def __init__(self, filename: str = 'alerts.csv', **kwargs):
        super().__init__(**kwargs)
        self.filename: str = filename

    def configure(self, config: Config) -> Config:
        config = super().configure(config)
        self.filename = config.get_value('filename', self.filename)
        return config

    def ship(self, investigation, anomalies: pa.DataFrame) -> None:
        logger.info(
            "Writing %d anomalies to '%s'", len(anomalies), self.filename,
        )
        write_header = True
        try:
            write_header = os.path.getsize(self.filename) == 0
        except FileNotFoundError:
            pass
        anomalies.to_csv(
            self.filename, mode='a+', header=write_header, index=False,
        )
