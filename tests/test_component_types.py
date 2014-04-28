from banzai import pipeline


def counter():
    yield from range(5)

def fib(upstream):
    this = 0
    for number in upstream:
        this = this + number
        yield this


class Base:

    def test_output(self):
        stream = self.get_pipeline()
        output = tuple(stream)
        assert output == (0, 1, 3, 6, 10)

    def test_component_shortcuts(self):
        stream = self.get_pipeline()
        counter_component, fib_component = tuple(stream.gen_components())

        # Verify they can both access the same pipeline state.
        assert counter_component.state is fib_component.state

        # Verify that counter identifies as fib's upstream.
        assert fib_component.upstream is counter_component


class TestFibFunction(Base):
    '''Test fib on a sequence of module-level functions.
    '''
    def get_pipeline(self):
        return pipeline(counter, fib)


class TestFibMethod(Base):
    '''Test fib on a sequence of bound methods.
    '''
    def counter(self):
        yield from range(5)

    def fib(self, upstream):
        this = 0
        for number in upstream:
            this = this + number
            yield this

    def get_pipeline(self):
        return pipeline(self.counter, self.fib, args={})


class TestFibStaticmethod(Base):
    '''Test fib on a sequence of staticmethods.
    '''
    @staticmethod
    def counter():
        yield from range(5)

    @staticmethod
    def fib(upstream):
        this = 0
        for number in upstream:
            this = this + number
            yield this

    def get_pipeline(self):
        return pipeline(self.counter, self.fib, args={})


class TestFibClassmethod(Base):
    '''Test fib on a sequence of classmethods.
    '''
    @classmethod
    def counter(cls):
        yield from range(5)

    @classmethod
    def fib(cls, upstream):
        this = 0
        for number in upstream:
            this = this + number
            yield this

    def get_pipeline(self):
        return pipeline(self.counter, self.fib)


class TestImportString(Base):
    '''Test fib on a sequence of classmethods.
    '''
    def get_pipeline(self):
        return pipeline(
            'counter', 'fib',
            import_prefix='tests.test_component_types')
