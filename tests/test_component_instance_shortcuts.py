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
        assert self.upstream.component_type is Component1
        assert self.state is self.upstream.state
        assert self.warn == self.state.warn == self.upstream.state.warn
        assert self.warn == self.upstream.component_type.warn
        assert self.logger is self.upstream.state.logger
        assert self.logger is self.upstream.component_type.logger
        assert self.logger is self.state.logger
        assert self.config_obj is self.upstream.component_type.config_obj


class Pipeline(banzai.Pipeline):

    components = (Component1, Component2)


class TestComponentState:

    def test_state(self):
        tuple(Pipeline())