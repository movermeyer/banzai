import re
from hercules import CachedAttr

import banzai


class RegexFilterer:
    '''Consumes a sequence of strings and drops/includes based on
    regexes.
    '''
    def __init__(self, *, upstream=None, exclude=None,
                 include=None, rgx_method='search'):
        self.upstream = upstream
        self.exclude = exclude or getattr(self, 'exclude', None)
        self.include = include or getattr(self, 'include', None)
        self._rgx_method = rgx_method

    def __iter__(self):
        filter_func = self.get_filter_func()
        for string in self.upstream:
            if filter_func(string):
                yield string

    def get_rgxs(self, rgxs):
        if rgxs is None:
            return
        elif isinstance(rgxs, (tuple, list, set)):
            for rgx in rgxs:
                yield self.compile_rgx(rgx)
        else:
            yield self.compile_rgx(rgx)

    def compile_rgx(self, rgx):
        if isinstance(rgx, str):
            rgx = re.compile(rgx)
        elif isinstance(rgx, (tuple, list)):
            rgx = re.compile(*rgx)
        elif isinstance(rgx, dict):
            rgx = re.compile(**rgx)
        elif hasattr(rgx, 'match'):
            pass
        return getattr(rgx, self._rgx_method)

    @CachedAttr
    def _exclude(self):
        return tuple(self.get_rgxs(self.exclude))

    @CachedAttr
    def _include(self):
        return tuple(self.get_rgxs(self.include))

    def get_filter_func(self):
        if self._exclude and not self._include:
            def filter_func(string):
                for rgx in self._exclude:
                    if rgx(string):
                        return False
                return True
        elif self._include and not self._exclude:
            def filter_func(string):
                for rgx in self._include:
                    if rgx(string):
                        return True
                return False
        elif self._include and self._exclude:
            msg = ("Can't apply both include and exclude patterns at the "
                   "same time. Pick one, please.")
            raise ValueError(msg)
        return filter_func


def regex_filter(*, exclude=None, include=None, rgx_method='search'):
    return RegexFilterer(
        exclude=exclude, include=include, rgx_method=rgx_method)

