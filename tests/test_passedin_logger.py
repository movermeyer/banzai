import logging
import pytest

import banzai


class TestLogger:

    def test_passed_in_logger(self):
        '''Test that passed in logger is actually used.
        '''
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('oo', 'z$', '^z')

        logger = logging.getLogger('testlogger')
        pipeline = banzai.pipeline(
            strings,
            banzai.io.regex_filter(exclude=rgxs),
            logger=logger)

        assert pipeline.logger is logger

    def test_not_passed_in_logger(self):
        '''Control test, that default logger differs from
        random logger.
        '''
        strings = iter(['foo', 'bar', 'biz', 'fuzzy', 'cow.json'])
        rgxs = ('oo', 'z$', '^z')

        logger = logging.getLogger('testlogger')
        pipeline = banzai.pipeline(
            strings,
            banzai.io.regex_filter(exclude=rgxs))

        assert pipeline.logger is not logger

