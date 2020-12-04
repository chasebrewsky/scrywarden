"""Contains classes that find malicious anomalies in a set of anomalies."""

import typing as t

import pandas as pa
from sqlalchemy.orm import sessionmaker, Session

from scrywarden import database as db
from scrywarden.config import parsers, Config


class Analyzer:
    """Base class used to detect malicious anomalies.

    Subclasses should implement the `analyze` method.

    Parameters
    ----------
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.

    Attributes
    ----------
    session_factory: Optional[sessionmaker]
        SQLAlchemy session factory.
    """
    PARSER: t.Optional[parsers.Parser] = None

    def __init__(self, session_factory: t.Optional[sessionmaker] = None):
        self.session_factory: t.Optional[sessionmaker] = session_factory

    def _session(self, **kwargs) -> t.ContextManager[Session]:
        return db.managed_session(self.session_factory, **kwargs)

    def analyze(self, anomalies: pa.DataFrame) -> pa.DataFrame:
        """Analyzes a given anomaly list for malicious anomalies.

        The given anomaly dataframe contains the columns:

            * event_id (int)
            * message_id (str)
            * actor_id (int)
            * created_at (timestamp)
            * anomaly_id (int)
            * field_id (int)
            * score (float)

        The returned dataframe should contain the same columns.

        Parameters
        ----------
        anomalies: DataFrame
            DataFrame containing the anomalies to analyze.

        Returns
        -------
        DataFrame
            Filtered anomalies that are malicious.
        """
        raise NotImplementedError()

    def configure(self, config: Config) -> Config:
        """Configures the analyzer from a config object.

        If the PARSER attribute is set on the class, it uses it automatically
        to parse and validate the config.

        Parameters
        ----------
        config: Config
            Configuration object to parse values from.

        Returns
        -------
        Config
            Parsed config object.
        """
        return config.parse(self.PARSER) if self.PARSER else config


class ExponentialDecayAnalyzer(Analyzer):
    """Subtracts the mean of all actors groups from a decaying constant value.

    This analyzer uses the exponential decay formula `y = a(1-b)^x` to
    subtract a decaying constant value from the average anomaly score of
    each actor group in the given anomaly list.

    The intuition behind this analyzer is that a small number of anomalies
    with high anomaly scores are not as suspicious as a large group of
    anomalies with high anomaly scores, so the small number of anomalies
    should be weighted less than the larger group.

    This is done by first grouping the anomalies by their actor. The mean of
    all scores in these anomaly groups and the count of all anomalies in each
    group are retrieved. The weighted mean is calculated by the following
    formula::

        weighted_mean = mean - (weight * (1 - decay) ** (count - 1))

    If this weighted mean passes a certain threshold, then its flagged as
    anomalous.

    For example, lets say the threshold to pass was .7 and the following values
    are given for a group::

        mean = .8
        weight = .2
        decay = .05

    When there is only one anomaly, the full weighted constant is removed
    from the original mean::

        .6 == .8 - (1 - .05) ** (1 - 1)

    This would not pass the threshold of .7 to be considered malicious, but
    if the count was 15::

        .702 == .8 - (1 - .05) ** (15 - 1)

    This would pass the threshold and those anomalies would be considered
    malicious.

    The decaying factor is used as a way to control the rate at which the
    constant value declines. With a small enough decaying factor, the
    rate of decay becomes almost linear.

    Parameters
    ----------
    weight: float
        Constant weight to subtract from the mean.
    decay: float
        Rate of decay the weight declines at based on the count.
    threshold: float
        Threshold the weighted mean must pass to be considered malicious.
    """

    PARSER = parsers.Options({
        'weight': parsers.Float(),
        'decay': parsers.Float(),
        'threshold': parsers.Float(),
    })

    def __init__(
        self,
        weight: float = 0.2,
        decay: float = .1,
        threshold: float = 0.5,
    ):
        super().__init__()
        self.weight: float = weight
        self.decay: float = decay
        self.threshold: float = threshold

    def configure(self, config: Config) -> Config:
        self.weight = config.get_value('weight', self.weight)
        self.decay = config.get_value('decay', self.decay)
        self.threshold = config.get_value('threshold', self.threshold)
        return config

    def analyze(self, anomalies: pa.DataFrame) -> pa.DataFrame:
        agg = anomalies.groupby('actor_id').agg(
            mean=('score', 'mean'),
            count=('event_id', 'count'),
        )
        agg['weighted_mean'] = agg['mean'] - (
            self.weight * (1 - self.decay) ** (agg['count'] - 1)
        )
        df = anomalies.merge(agg, 'left', left_on='actor_id', right_index=True)
        df = df[df['weighted_mean'] >= self.threshold]
        return df.drop(columns=['weighted_mean', 'mean', 'count'])


PARSER = parsers.Options({
    'class': parsers.Import(required=True, parent=Analyzer),
    'config': parsers.Options({}),
})


def parse_analyzer(config: Config) -> Analyzer:
    """Parses an analyzer from a config.

    Parameters
    ----------
    config: Config
        Configuration object to parse from.

    Returns
    -------
    Analyzer
        Configured analyzer object.
    """
    config = config.parse(PARSER)
    cls: t.Type[Analyzer] = config['class'].value
    analyzer: Analyzer = cls()
    analyzer.configure(config.get('config', {}))
    return analyzer
