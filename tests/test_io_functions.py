import os
import operator
import tempfile

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


class TestFileFinder:

    def test_file_find(self):
        '''Set up a dir like
        dir/
          - foo
          - bar
          - baz
          - spam
          cows/
            - bessie
            - moomoo
            -bull

        Then verify that os.walk and regex_filter find them correctly.
        '''
        join = os.path.join
        tempdir = tempfile.mkdtemp()
        for filename in ('foo', 'bar', 'baz', 'spam'):
            with open(join(tempdir, filename), 'w') as f:
                pass
        os.mkdir(join(tempdir, 'cows'))
        for filename in ('bessie', 'moomoo', 'bull'):
            with open(join(tempdir, 'cows', filename), 'w') as f:
                pass

        class Walker:

            def __init__(self, *args):
                self.args = args

            def __iter__(self):
                for thing in os.walk(*self.args):
                    print(thing)
                    yield thing

        def flatten(upstream):
            for stringlist in upstream:
                yield from iter(stringlist)

        rgxs = ('oo', 'm')
        pipeline = banzai.pipeline(
            os.walk(tempdir),
            operator.itemgetter(2),
            flatten,
            regex_filter(include=rgxs))

        output = set(tuple(pipeline))
        assert output == {'spam', 'foo', 'moomoo'}
