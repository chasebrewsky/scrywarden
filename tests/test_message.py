import pytest

from scrywarden.transport.message import keys, get


class TestGetFields:
    def test_primitive_value(self):
        """Primitive JSON values should not iterate."""
        assert [*keys(5)] == []

    def test_basic_dict(self):
        """Basic dictionary should iterate over keys."""
        assert {*keys({'one': 1, 'two': 2})} == {('one',), ('two',)}

    def test_basic_list(self):
        """Basic list should iterate over indexes."""
        assert {*keys([0, 1, 2])} == {('0',), ('1',), ('2',)}

    def test_complex(self):
        """Complex JSON should return all possible values."""
        data = {
            'nested': {
                'value': 'here',
                'integer': 5,
            },
            'array': [6, {'surprise': 'value'}]
        }
        assert {*keys(data)} == {
            ('nested',),
            ('nested', 'value'),
            ('nested', 'integer'),
            ('array',),
            ('array', '0'),
            ('array', '1'),
            ('array', '1', 'surprise'),
        }


class TestGetValue:
    def test_empty_field(self):
        """Empty field should return the original value."""
        assert get(5, ()) == 5

    def test_invalid_primitive_key(self):
        """Primitive JSON values should throw KeyError when given field."""
        with pytest.raises(KeyError):
            get(5, ('hello',))

    def test_basic_dict(self):
        """Basic dictionary should return the key value."""
        assert get({'nested': 'value'}, ('nested',)) == 'value'

    def test_basic_list(self):
        """Basic list should return the index value."""
        assert get([0, 1], ('1',)) == 1

    def test_complex(self):
        """Complex JSON structure should return value keys."""
        data = {
            'nested': {
                'value': 'here',
                'integer': 5,
            }, 'array': [6, {'surprise': 'value'}],
        }
        assert get(data, ('nested',)) == {
            'value': 'here',
            'integer': 5,
        }
        assert get(data, ('nested', 'value')) == 'here'
