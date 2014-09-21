import os
import re
import queue
import collections
import urllib.parse

import requests
import lxml.html

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
            yield self.compile_rgx(rgxs)

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


def find_files(start_dir, *, include=None, exclude=None, rgx_method='search', match_abspath=True):
    '''Walks the filesystem starting at start_dir and yields absolute
    filenames. If include or exclude is specified, applies them via a
    regex_filter.
    '''
    filter_func = None
    if include or exclude:
        filterer = regex_filter(
            exclude=exclude, include=include, rgx_method=rgx_method)
        filter_func = filterer.get_filter_func()
    join = os.path.join
    for dr, subdirs, filenames in os.walk(start_dir):
        for filename in filenames:
            path = join(dr, filename)
            if match_abspath:
                matchable = path
            else:
                matchable = filename
            if filter_func:
                if filter_func(matchable):
                    yield path
            else:
                yield path


class Url:
    '''A url object that can be compared with other url orbjects
    without regard to the vagaries of casing, encoding, escaping,
    and ordering of parameters in query strings.'''
    def __init__(self, url):
        self.url = url

    @CachedAttr
    def parts(self):
        parts = urllib.parse.urlparse(self.url)
        _query = dict(urllib.parse.parse_qsl(parts.query))
        # Nuke the old path.
        _query.pop('oldPath', None)
        _query = frozenset(_query.items())
        parts = parts._replace(
            query=_query,
            path=urllib.parse.unquote_plus(parts.path))
        return parts

    def __eq__(self, other):
        return self.parts == other.parts

    def __hash__(self):
        return hash(self.parts)

    def __repr__(self):
        return 'Url(%r)' % str(self)

    def __contains__(self, value):
        return value in str(self)

    def __str__(self):
        parts = self.parts
        query = urllib.parse.urlencode(sorted(parts.query))
        parts = parts._replace(
            params='',
            query=query,
            scheme='http',
            netloc='www.gpo.gov')
        return urllib.parse.urlunparse(parts)


class UrlFinder:
    '''Dumb web spider for spidering a site with GET'able urls.
    '''
    def __init__(self, start_url):
        self.start_url = Url(start_url)
        self.urls = queue.Queue()
        self.processed = set()
        self.added = set()

    def __iter__(self):
        self.urls.put(self.start_url)
        while True:
            try:
                url = self.urls.get()
            except IndexError:
                return
            if url in self.processed:
                continue
            for url in self.process_url(url):
                if self.should_yield(url):
                    yield url

    def should_yield(self, url):
        return True

    def get_lxml_doc(self, url):
        resp = self.get_resp(url)
        doc = lxml.html.fromstring(resp.content)
        doc.make_links_absolute(url)
        return doc

    def get_resp(self, url):
        '''Dumb, overridable method for fetching urls.
        '''
        resp = requests.get(url)
        return resp

    def already_processed(self, url):
        return Url(url) in self.processed

    def already_added(self, url):
        return Url(url) in self.added

    def gen_urls(self, url):
        doc = self.get_lxml_doc(url)
        yield from doc.xpath('//a@href')

    def should_follow(self, url):
        return True

    def process_url(self, url):
        self.added.add(url)
        for child_url in self.gen_urls(str(url)):
            child_url = Url(child_url)
            if self.should_follow(child_url):
                self.urls.put(child_url)
                self.added.add(child_url)
            yield child_url
        self.processed.add(url)

# def find_urls(start_url, requests_client):
#     import lxml.html

