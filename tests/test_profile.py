import pandas as pa

from scrywarden.profile.base import update_feature_count


class TestUpdateFeatureCount:
    def test_missing_values(self):
        features = pa.DataFrame([
            (1, 1, 1, '"Hello"', 4),
            (2, 1, 2, '"Greetings"', 2),
        ], columns=['feature_id', 'field_id', 'actor_id', 'value', 'count'])
        values = pa.DataFrame([
            (1, 1, 2, '"Greetings"'),
            (2, 2, 1, '"Whats up?"'),
        ], columns=['message_id', 'field_id', 'actor_id', 'value'])
        expected = pa.DataFrame([
            (1, 1, 1, '"Hello"', 4),
            (2, 1, 2, '"Greetings"', 3),
            (0, 2, 1, '"Whats up?"', 1),
        ], columns=['feature_id', 'field_id', 'actor_id', 'value', 'count'])
        result = update_feature_count(values, features)
        columns = ['feature_id', 'field_id', 'actor_id', 'value', 'count']
        assert result[columns].equals(expected[columns])
