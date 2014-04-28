'''These tests verify that correct values are passed in as function args
after the pipeline inspects function/method components and passes in the
args they specify.
'''
import pytest

from banzai import pipeline, ComponentInvocationError


class Base:
    def test_args(self):
        stream = self.get_pipeline()
        tuple(stream)


def dummy_component_1(state, upstream, config_obj, logger, info):
    '''Only purposes of this one is to exist as an upstream component.
    '''
    yield


def dummy_component_2(state, upstream, config_obj, logger, info):
    '''This component performs all the tests when the pipeline iterates over it.
    '''
    assert upstream.component_type is dummy_component_1
    assert upstream.state is state
    assert config_obj is state.config_obj
    assert logger is state.logger
    assert info == state.info
    assert info == logger.info
    yield


class TestFunctionInvocation(Base):
    '''Test that args get passed to function components correctly.
    '''
    def get_pipeline(self):
        return pipeline(dummy_component_1, dummy_component_2)


class TestMethodInvocation(Base):
    '''Test that args get passed to bound method components correctly.
    '''
    def get_pipeline(self):
        return pipeline(self.dummy_component_1, self.dummy_component_2)

    def dummy_component_1(self, state, upstream, config_obj, logger, info):
        yield

    def dummy_component_2(self, state, upstream, config_obj, logger, info):
        assert upstream.component_type == self.dummy_component_1
        assert upstream.state is state
        assert config_obj is state.config_obj
        assert logger is state.logger
        assert info == state.info
        assert info == logger.info
        yield


class TestClassmethodInvocation(TestMethodInvocation):
    '''Test that args get passed to classmethod components correctly.
    '''
    def get_pipeline(self):
        return pipeline(self.dummy_component_1, self.dummy_component_2)

    @classmethod
    def dummy_component_1(cls, state, upstream, config_obj, logger, info):
        yield

    @classmethod
    def dummy_component_2(cls, state, upstream, config_obj, logger, info):
        assert upstream.component_type == cls.dummy_component_1
        assert upstream.state is state
        assert config_obj is state.config_obj
        assert logger is state.logger
        assert info == state.info
        assert info == logger.info
        yield


class TestStaticmethodInvocation(TestMethodInvocation):
    '''Test that args get passed to staticmethod components correctly.
    '''
    @staticmethod
    def dummy_component_1(state, upstream, config_obj, logger, info):
        yield

    @staticmethod
    def dummy_component_2(state, upstream, config_obj, logger, info):
        upstream_type = TestStaticmethodInvocation.dummy_component_1
        assert upstream.component_type == upstream_type
        assert upstream.state is state
        assert config_obj is state.config_obj
        assert logger is state.logger
        assert info == state.info
        assert info == logger.info
        yield


class TestBogusInvocation(TestMethodInvocation):
    '''Verify that correct error gets raised if function defines
    unavailable args.
    '''
    @staticmethod
    def dummy_component_1(state, *cow, pig=3):
        yield

    @staticmethod
    def dummy_component_2(state, cow):
        yield

    def test_args(self):
        stream = self.get_pipeline()
        with pytest.raises(ComponentInvocationError):
            tuple(stream)