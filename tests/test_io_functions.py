import os
import shutil
import operator
import tempfile

import pytest

import banzai
import banzai.io


class TestIOFunctions:

    def test_regex_filter_exclude(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('oo', 'z$', '^z')

        pipeline = banzai.pipeline(
            strings,
            banzai.io.regex_filter(exclude=rgxs))

        expected = ('bar', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_regex_filter_exclude_type(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(banzai.io.RegexFilterer):
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
            banzai.io.regex_filter(include=rgxs))

        expected = ('bar', 'biz', 'fuzzy', 'cow.json')
        result = tuple(pipeline)
        assert result == expected

    def test_regex_filter_include_type(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(banzai.io.RegexFilterer):
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
            banzai.io.regex_filter(include=rgxs, exclude=['cow']))

        with pytest.raises(ValueError):
            result = tuple(pipeline)

    def test_assert_include_exclude_type_raises(self):
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])

        class Filterer(banzai.io.RegexFilterer):
            include = ('^b', '..z', '\.json$')
            exclude = ('cow',)

        pipeline = banzai.pipeline(strings, Filterer)

        with pytest.raises(ValueError):
            result = tuple(pipeline)


class TestFileFinders:

    @classmethod
    def setup_class(cls):
        '''Set up a dir like
        dir/
          - foo
          - bar
          - baz
          - spam
          cows/
            - bessie
            - moomoo
            - bull
        '''
        join = os.path.join
        cls.tempdir = tempfile.mkdtemp()
        for filename in ('foo', 'bar', 'baz', 'spam'):
            with open(join(cls.tempdir, filename), 'w') as f:
                pass
        os.mkdir(join(cls.tempdir, 'cows'))
        for filename in ('bessie', 'moomoo', 'bull'):
            with open(join(cls.tempdir, 'cows', filename), 'w') as f:
                pass

    @classmethod
    def teardown_class(cls):
        """ teardown any state that was previously setup with a call to
        setup_class.
        """
        shutil.rmtree(cls.tempdir)

    def test_adhoc_file_find(self):
        '''Verify that os.walk and regex_filter find them correctly.
        '''
        def flatten(upstream):
            for stringlist in upstream:
                yield from iter(stringlist)

        rgxs = ('oo', 'm')
        pipeline = banzai.pipeline(
            os.walk(self.tempdir),
            operator.itemgetter(2),
            flatten,
            banzai.io.regex_filter(include=rgxs))

        output = set(tuple(pipeline))
        assert output == {'spam', 'foo', 'moomoo'}

    def test_file_find_match_relpath(self):
        rgxs = ('oo', 'm')
        file_finder = banzai.io.find_files(
            start_dir=self.tempdir,
            include=rgxs,
            match_abspath=False)

        pipeline = banzai.pipeline(file_finder)

        output = set(tuple(pipeline))
        expected_names = ('spam', 'foo', 'cows/moomoo')
        join = os.path.join
        expected = {join(self.tempdir, name) for name in expected_names}
        assert output == expected

    def test_file_find_match_abspath(self):
        rgxs = ('moomoo', 'spam')
        file_finder = banzai.io.find_files(self.tempdir, exclude=rgxs)

        pipeline = banzai.pipeline(file_finder)

        output = set(tuple(pipeline))
        expected_names = ('bar', 'baz', 'cows/bessie', 'cows/bull', 'foo')
        join = os.path.join
        expected = {join(self.tempdir, name) for name in expected_names}
        assert output == expected
