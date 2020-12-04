from pandas import DataFrame

from scrywarden.database import Investigation
from scrywarden.entry import EntryBase, Entry


class CuratorEntry(EntryBase):
    """Creates queue entries that originate from curators."""

    SOURCE = 'CURATOR'

    class Kinds:
        MALICIOUS_ACTIVITY = 'EVENT_REPORT'
        BLIP = 'BLIP'

    @classmethod
    def malicious_activity(
        cls,
        investigation: Investigation,
        anomalies: DataFrame,
    ) -> Entry:
        """Create a queue entry indicating that malicious anomalies were found.

        Parameters
        ----------
        investigation: Investigation
            Investigation that found the malicious anomalies.
        anomalies: DataFrame
            Pandas dataframe containing the malicious anomalies.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(
            cls.Kinds.MALICIOUS_ACTIVITY, (investigation, anomalies),
        )

    @classmethod
    def blip(cls, reason: str) -> Entry:
        """Creates a queue entry that forces an iteration of the queue.

        Usually used when there's a shutdown event that requires the
        connected queues to force an iteration so they can shutdown properly.

        Parameters
        ----------
        reason: str
            Reason for the blip.

        Returns
        -------
        Entry
            Queue entry.
        """
        return cls.create(cls.Kinds.BLIP, reason)
