import typing as t

from pandas import DataFrame

from scrywarden.database import Investigation
from scrywarden.entry import EntryBase, Entry

if t.TYPE_CHECKING:
    from scrywarden.investigator.base import Investigator


class InvestigatorEntry(EntryBase):
    """Creates queue entries that originate from investigators."""

    SOURCE = 'INVESTIGATOR'

    class Kinds:
        MALICIOUS_ACTIVITY = 'MALICIOUS_ACTIVITY'
        SHUTDOWN = 'SHUTDOWN'

    @classmethod
    def malicious_activity(
        cls,
        investigation: Investigation,
        anomalies: DataFrame,
    ) -> Entry:
        """Creates a queue entry that indicates malicious anomalies were found.

        Parameters
        ----------
        investigation: Investigation
            Investigation that found the malicious anomalies.
        anomalies: DataFrame
            Dataframe containing the malicious anomalies.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(
            cls.Kinds.MALICIOUS_ACTIVITY, (investigation, anomalies),
        )

    @classmethod
    def shutdown(cls, investigator: 'Investigator') -> Entry:
        """Creates a queue entry indicating that the investigator shutdown.

        Parameters
        ----------
        investigator: Investigator
            Investigator instance that shutdown.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(cls.Kinds.SHUTDOWN, investigator)
