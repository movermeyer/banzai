import pytest

import banzai
from banzai.io import regex_filter, RegexFilterer


class TestIOFunctions:

    def test_regex_filter_exclude(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('oo', 'z$', '^z')

        pipeline = banzai.pipeline(
            strings,
            regex_filter(exclude=rgxs))

        expected = ('bar', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_regex_filter_exclude_type(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(RegexFilterer):
            exclude = ('oo', 'z$', '^z')

        pipeline = banzai.pipeline(strings, Filterer)

        expected = ('bar', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_regex_filter_include(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('^b', '..z', '\.json$')

        pipeline = banzai.pipeline(
            strings,
            regex_filter(include=rgxs))

        expected = ('bar', 'biz', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_regex_filter_include_type(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(RegexFilterer):
            include = ('^b', '..z', '\.json$')

        pipeline = banzai.pipeline(strings, Filterer)

        expected = ('bar', 'biz', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_assert_include_exclude_raises(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('^b', '..z', '\.json$')

        pipeline = banzai.pipeline(
            strings,
            regex_filter(include=rgxs, exclude=['cow']))

        with pytest.raises(ValueError):
            result = tuple(pipeline)

    def test_assert_include_exclude_type_raises(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(RegexFilterer):
            include = ('^b', '..z', '\.json$')
            exclude = ('cow',)

        pipeline = banzai.pipeline(strings, Filterer)

        with pytest.raises(ValueError):
            result = tuple(pipeline)
