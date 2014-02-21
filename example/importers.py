class ScrapeArtifactReader(object):
    pass


class DiGraphBuilder(object):
    pass


class DiGraphWriter(object):
    pass


class ReportWriter(object):

    def __call__(self):
        self.upstream()

    def finalize(self):
        import pdb; pdb.set_trace()

