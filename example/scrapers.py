class ScrapeRunner(object):
    '''In the case of dendrite, this component would
    simply import and yield from the scraper indicated
    in the command line args.
    '''
    def get_scraper(self):
        import pdb; pdb.set_trace()

    def __iter__(self):
        n = 0
        for thing in self.get_scraper():
            self.info('s: ' % thing)
            yield n
            n += 1


class Validator(object):

    def __iter__(self):
        for thing in self.upstream:
            self.info('v: %d' % thing)
            yield thing
        return
        for thing in self.upstream:
            thing.validate()
            yield thing


class JsonWriter(object):

    def __call__(self):
        for thing in self.upstream:
            self.info('w: %d' % thing)


class ReportWriter(object):

    def __call__(self):
        self.upstream()
        # Do more stuff.

    def finalize(self):
        import pdb; pdb.set_trace()

