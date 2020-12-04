import logging
import typing as t

import pandas as pa

logger = logging.getLogger(__name__)


Reporter = t.Callable[[pa.DataFrame, pa.DataFrame], pa.DataFrame]
"""Generates a dataframe with an anomaly score for each row.

They're given the extracted message values and the related feature values
to retrieve an anomaly score for each.

The message value dataframes should have the shape:

* profile_id (int): ID of the profile.
* message_id (int): UUID of the associated message.
* timestamp (datetime): Timestamp the message took place.
* actor_id (int): ID of the associated actor.
* field_id (int): ID of the associated field.
* value (str): Extracted message value.

The features dataframe should have the shape:

* feature_id (int): ID of the feature.
* field_id (int): ID of the associated field.
* actor_id (int): ID of the associated actor.
* value (str): JSON value associated with the feature.
* count (int): Current number of times this feature has been
    present in messages.

Parameters
----------
values: DataFrame
    DataFrame containing the message values for the profile field this
    reporter is attached to.
features: DataFrame
    DataFrame containing the current feature values.

Returns
-------
DataFrame
    DataFrame containing an additional anomaly score column.
"""


def increment_total(values: pa.DataFrame) -> pa.DataFrame:
    """
    Sort by actor, timestamp to increment the total counts for each row
    within each actor group based on the time the message was received.

    Parameters
    ----------
    values: DataFrame
        Message value dataframe.

    Returns
    -------
    DataFrame
        Updated message value dataframe.
    """
    df = values.sort_values(['actor_id', 'timestamp'], ignore_index=True)
    df_index_a = df.reset_index()
    df_index_a_agg = df_index_a.groupby('actor_id').agg(
        fa_first=('index', 'first'),
    )
    df = df.merge(
        df_index_a_agg, 'left', left_on='actor_id', right_index=True,
    )
    df['total'] += df.index - df['fa_first']
    return df.drop(columns=['fa_first'])


def increment_count(values: pa.DataFrame) -> pa.DataFrame:
    """
    Sort by actor, timestamp, value to increment the total counts for each
    row within each actor, value group based on the time the message
    was received.

    Parameters
    ----------
    values: DataFrame
        Message value dataframe.

    Returns
    -------
    DataFrame
        Updated message value dataframe.
    """
    df = values.sort_values(
        ['actor_id', 'value', 'timestamp'], ignore_index=True,
    )
    df_index_av = df.reset_index()
    df_index_av_agg = df_index_av.groupby(['actor_id', 'value']).agg(
        fav_first=('index', 'first'),
    )
    df = df.merge(
        df_index_av_agg, 'left', left_on=['actor_id', 'value'],
        right_index=True,
    )
    df['count'] += df.index - df['fav_first']
    return df.drop(columns=['fav_first'])


class Mandatory(Reporter):
    """Reporter that requires a mandatory field.

    The following algorithm is used to determine the anomaly score for each
    message field:

    1. The feature fv for the analyzed model is first extracted from the
    message. If M contains a tuple with fv as a first element, then the
    tuple <fv, c> is extracted from M. If there is no tuple in M with fv
    as a first value, the message is considered anomalous. The procedure
    terminates here and an anomaly score of 1 is returned.

    2. As a second step, the approach checks if fv is anomalous at all for
    the behavioral profile being analyzed. c is compared to MN, which is
    defined as MN = ∑‖M‖i=1 ci / N, where ci is, for each tuple in M, the
    second element of the tuple. If c is greater or equal than MN, the
    message is considered to comply with the learned behavioral profile
    for that model, and an anomaly score of 0 is returned. The rationale
    behind this is that, in the past,the user has shown a significant
    number of messages with that particular fv.

    3. If c is less than MN, the message is considered some-what anomalous
    with respect to that model. Our approach calculates the relative
    frequency f of fv as f = cfv / N. The system returns an anomaly score
    of 1 - f.

    Parameters
    ----------
    weight: float
        Weight that this value should be given. By default it receives a
        weight of 1.
    """
    def __init__(self, weight: float = 1.0):
        self.weight: float = weight

    def __call__(
        self,
        values: pa.DataFrame,
        features: pa.DataFrame,
    ) -> pa.DataFrame:
        features = features.drop(columns=['feature_id'])
        unique_fa = values[['field_id', 'actor_id']].drop_duplicates()
        unique_fa_idx = unique_fa.set_index(['field_id', 'actor_id']).index
        features_by_fa = features.set_index(['field_id', 'actor_id'])
        matching_features = features[features_by_fa.index.isin(unique_fa_idx)]
        feature_fa_agg = matching_features.groupby(
            ['field_id', 'actor_id'],
        ).agg(
            groups=('value', 'count'),
            total=('count', 'sum'),
            mean=('count', 'mean'),
        )
        feature_fav_agg = features.set_index(['field_id', 'actor_id', 'value'])
        df = values.merge(
            feature_fa_agg, 'left', left_on=['field_id', 'actor_id'],
            right_index=True,
        )
        df = df.merge(
            feature_fav_agg, 'left', left_on=['field_id', 'actor_id', 'value'],
            right_index=True,
        )
        df = df.fillna(0)
        # Set the previous mean to calculate the updated mean later.
        df['previous_mean'] = 0.0
        enough_groups = df['groups'] > 1
        df.loc[enough_groups & (df['count'] != 0), 'previous_mean'] = (
            (df['mean'] * df['groups'] - df['count']) / (df['groups'] - 1)
        )
        df.loc[enough_groups & (df['count'] == 0), 'previous_mean'] = (
            df['mean']
        )
        df = increment_count(df)
        df = increment_total(df)
        df = df.apply(self._update_groups(), axis=1)
        df.loc[df['groups'] > 0, 'mean'] = (
            df['previous_mean'] + (
                (df['count'] - df['previous_mean']) / df['groups']
            )
        )
        df['score'] = 0.0
        df.loc[(df['value'] == '') | (df['count'] == 0), 'score'] = 1.0
        df.loc[(df['count'] < df['mean']) & (df['score'] != 1.0), 'score'] = (
            1 - (df['count'] / df['total'])
        )
        df['score'] *= self.weight
        return df.drop(
            columns=['groups', 'total', 'mean', 'count', 'previous_mean'],
        )

    def _update_groups(self) -> t.Callable[[pa.Series], pa.Series]:
        section, counter = -1, 0

        def callback(x: pa.Series) -> pa.Series:
            nonlocal section, counter
            if x['actor_id'] != section:
                section = x['actor_id']
                counter = 0
            if counter:
                x['groups'] += counter
            if x['count'] == 0.0:
                counter += 1
            return x

        return callback


class Optional(Reporter):
    """Reporter that calculates a score for an optional field.

    The following algorithm is used to determine the anomaly score for each
    message field:

    1. The feature fv for the analyzed model is first extracted from the
    message. If M contains a tuple with fv as a first element, the message
    is considered to match the behavioral profile, and an anomaly score of
    0 is returned.

    2. If there is no tuple in M with fv as a first element, the message
    is considered anomalous. The anomaly score in this case is defined as
    the probability p for the account to have a null value for this model.
    Intuitively, if an actor rarely reports a feature within a dataset,
    a message containing an fv that has never been seen before for this
    feature is highly anomalous. The probability p is calculated as
    p = cnull / N. If M does not have a tuple with null as a first
    element, cnull is considered to be 0. p is then returned as the
    anomaly score.

    Parameters
    ----------
    weight: float
        Weight that this value should be given. By default it receives a
        weight of 1.
    """
    def __init__(self, weight: float = 1.0):
        self.weight: float = weight

    def __call__(
        self,
        values: pa.DataFrame,
        features: pa.DataFrame,
    ) -> pa.DataFrame:
        features = features.drop(columns=['feature_id'])
        unique_fa = values[['field_id', 'actor_id']].drop_duplicates()
        unique_fa_idx = unique_fa.set_index(['field_id', 'actor_id']).index
        features_by_fa = features.set_index(['field_id', 'actor_id'])
        matching_features = features[features_by_fa.index.isin(unique_fa_idx)]
        features_fa_agg = matching_features.groupby(
            ['field_id', 'actor_id'],
        ).agg(total=('count', 'sum'))
        null_values = unique_fa.reset_index(drop=True)
        null_values['value'] = ''
        features_by_fav = features.set_index(['field_id', 'actor_id', 'value'])
        null_values = null_values.merge(
            features_by_fav, 'left', left_on=['field_id', 'actor_id', 'value'],
            right_index=True,
        )
        null_values = null_values.drop(columns=['value'])
        null_values = null_values.fillna(0)
        null_values = null_values.rename(columns={'count': 'null_count'})
        null_values = null_values.set_index(['field_id', 'actor_id'])
        df = values.merge(
            features_fa_agg, 'left', left_on=['field_id', 'actor_id'],
            right_index=True,
        )
        df = df.merge(
            null_values, 'left', left_on=['field_id', 'actor_id'],
            right_index=True,
        )
        df = df.merge(
            features_by_fav, 'left',
            left_on=['field_id', 'actor_id', 'value'], right_index=True,
        )
        df = df.fillna(0)
        df = increment_total(df)
        df = df.apply(self._update_count(), axis=1)
        df = increment_count(df)
        df['score'] = 0.0
        ne_null = df['value'] != ''
        e_zero = df['count'] == 0
        df.loc[ne_null & e_zero & (df['total'] == 0), 'score'] = 1.0
        df.loc[ne_null & e_zero & (df['score'] == 0.0), 'score'] = (
            df['null_count'] / df['total']
        )
        df['score'] *= self.weight
        return df.drop(columns=['total', 'null_count', 'count'])

    def _update_count(self) -> t.Callable[[pa.Series], pa.Series]:
        section, counter = -1, 0

        def callback(x: pa.Series) -> pa.Series:
            nonlocal section, counter
            if x['actor_id'] != section:
                section = x['actor_id']
                counter = 0
            if counter:
                x['null_count'] += counter
            if x['value'] == '':
                counter += 1
            return x

        return callback
