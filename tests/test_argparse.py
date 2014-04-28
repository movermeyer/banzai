'''There tests verify that component instances are getting the
correct shortcuts set on them.
'''
import banzai


class Component1:
    def __iter__(self):
        yield


class Component2:
    def __iter__(self):
        yield from self.upstream


class Component3NoArgs:
    def __iter__(self):
        yield from self.upstream
        assert self.args.action == 'import'
        assert self.args.build_id == 'superbuild'
        assert not hasattr(self.args, 'cow')


class Component3WithArgs(Component3NoArgs):
    arguments = [
        (['--cow'], dict(
            type=str,
            default='moo',
            help='Cow sound.')),
        ]

    def __iter__(self):
        yield from self.upstream
        assert self.args.action == 'import'
        assert self.args.build_id == 'superbuild'
        assert self.args.cow == 'mooo'


class PipelineWithArgs(banzai.Pipeline):
    components = (Component1, Component2, Component3NoArgs)
    arguments = [
        (['action'], dict(
            type=str,
            default='scrape',
            help='Scrape stuff.')),
        (['build_id'], dict(
            type=str,
            help='The build id to import.')),
        ]


class PipelineWithComponentArgs(PipelineWithArgs):
    components = (Component1, Component2, Component3WithArgs)


class TestArgparse:

    def test_pipeline_args(self):
        argv = 'import superbuild'.split()
        tuple(PipelineWithArgs(argv=argv))

    def test_pipeline_component_args(self):
        argv = 'import superbuild --cow=mooo'.split()
        tuple(PipelineWithComponentArgs(argv=argv))
