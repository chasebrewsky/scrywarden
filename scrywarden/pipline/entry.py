from scrywarden.entry import Entry, EntryBase


class PipelineEntry(EntryBase):
    SOURCE = 'PIPELINE'

    class Kinds:
        BLIP = 'BLIP'

    @classmethod
    def blip(cls, reason: str = '') -> Entry:
        """Creates a pipeline blip entry.

        This entry is used to force an iteration of the pipeline queue in
        case there are no more incoming entries to force an iteration.

        Parameters
        ----------
        reason: str
            Reason for the clip.

        Returns
        -------
        Blip entry type.
        """
        return cls.create(cls.Kinds.BLIP, reason)
