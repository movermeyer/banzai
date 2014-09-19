'''
Idea is to facilitate pipeline of read/transform/write operations.

- Can have arbitrary readers, with defaults and expected uses.
- Input is run through one or more transformers: objects that provide
  and simple interface.
- Output of transformers is then handed to one or more writers.

The package just provides command line utilities and some basic plumbing.
Doesn't try to make important or constricting decisions. Think flask.

Todo:
 - Need functions to be acceptable as components.
 - Need pipelines to be iterable so they can be nested as components in
   other pipelines
 - Need to facilitate pipeline/component testability by making it easy
   to instantiate both and pass mock argv, or simply passing options
   as constructor data.
 - Also want to make it easy to invoke them programmatically rather
   than only through the CLI
'''
import sys
import types
import inspect
import logging
import argparse
import functools
import collections
from operator import attrgetter

from nmmd import TypeDispatcher
from visitors import StreamVisitor, TypeVisitor
from hercules import CachedAttr, set_default, SetDefault


__version__ = '0'


class UtilsMixin:
    '''Exposes lazy import functions.
    '''
    def resolve_name(self, name, module_name=None, raise_exc=False):
        '''Given a name string and module prefix, try to import the name.
        '''
        if not isinstance(name, str):
            return name
        if module_name is None:
            module_name, _, name = name.rpartition('.')
        try:
            module = __import__(module_name, globals(), locals(), [name], 0)
        except ImportError:
            if raise_exc:
                raise
        else:
            return getattr(module, name)

    def resolve_names(self, names, module_name=None, raise_exc=False):
        '''Try to import a sequence of names.
        '''
        for name in names:
            yield self.resolve_name(name, module_name, raise_exc=raise_exc)


# ------------------------------------------------------------------------
#  Confix mixin.
#-------------------------------------------------------------------------
class ShortcutSetError(Exception):
    '''Raised if code attempts to set shortcuts on an immutable object.
    '''


class ShortcutSetter:
    '''Determines whether/how to set pipeline state shortcuts
    on the passed-in object.
    '''
    ShortcutSetError = ShortcutSetError

    def __get__(self, inst, cls=None):
        '''Sets inst on the descriptor.
        '''
        self.inst = inst
        return self

    # ------------------------------------------------------------------------
    #  Short-cut setting internals.
    #-------------------------------------------------------------------------
    dispatcher = TypeDispatcher()

    # Attr, selfpath, (*aliases)
    default_shortcut_spec = [
        'state',
        'logger',
        'config_obj',
        ('log', 'logger.log'),
        ('info', 'logger.info'),
        ('debug', 'logger.debug'),
        ('error', 'logger.error'),
        ('warning', 'logger.warning', ('warn',)),
        ('critical', 'logger.critical'),
        ]

    @CachedAttr
    def compiled_spec(self):
        '''Compile the shortcut spec into a sequence of (attr, func) 2-tuples.
        Cached on the descriptor instance, which is set on each component
        class.
        '''
        user_spec = list(getattr(self, 'shortcut_spec', []))
        spec = self.default_shortcut_spec + user_spec

        compiled_spec = []
        for item in spec:
            if isinstance(item, str):
                attr = getter = item
                aliases = None
            elif isinstance(item, (list, tuple)):
                if len(item) == 2:
                    attr, getter = item
                    aliases = None
                elif len(item) == 3:
                    attr, getter, aliases = item

            compiled_spec.append((attr, attrgetter(getter)))
            for alias in aliases or []:
                compiled_spec.append((alias, attrgetter(getter)))
        return compiled_spec

    def gen_shortcut_items(self):
        '''Generate shortcut items.
        '''
        inst = self.inst
        for attr, getter in self.compiled_spec:
            yield attr, getter(inst)

    def _set_shortcuts(self, dest):
        '''Set each of the shortcut items on `dest`.
        '''

        for attr, val in self.gen_shortcut_items():
            # Avoid infinite recursion and circular references.
            if dest is val:
                continue
            try:
                setattr(dest, attr, val)
            except AttributeError:
                pass
                # msg = "Couldn't set shortcuts on immutable %r."
                # raise self.ShortcutSetError(msg % dest)
        return dest

    # -----------------------------------------------------------------------
    # Public interface.
    # -----------------------------------------------------------------------
    def shortcut_dict(self):
        '''A dictionary of shortcut items, used for passing kwargs to
        function/method components that can't access `self`.
        '''
        return dict(self.gen_shortcut_items())

    def __call__(self, dest):
        '''Set shortcuts on the obj if its type has a defined handler.
        '''
        return self.dispatcher.dispatch(dest)

    # -----------------------------------------------------------------------
    # The type-based handlers.
    # -----------------------------------------------------------------------
    def handle_type(self, type_):
        '''If the shortcuttable object is a class definition,
        set the shortcuts. Also set the .args descriptor.
        '''
        self._set_shortcuts(type_)
        type_.args = ArgsAccessor()
        return type_

    def handle_method(self, method):
        '''Noop, because we can't set attributes on methods.
        '''
        return method

    def generic_handler(self, inst):
        '''Handle class instances. This will also be the dispatched method if
        someone tries to set shortcuts on an immutable object like an int.
        To do: catch those and complain.
        '''
        self._set_shortcuts(inst)
        return inst


class ConfigMixin:
    '''Defines mixable aspects of pipeline config.
    '''
    # Descriptor that knows how to set shortcuts (or not) on types/objects.
    set_shortcuts = ShortcutSetter()

    @property
    def appname(self):
        '''The stringy name of the pipeline's config_obj.
        '''
        return self.config_obj.__class__.__name__

    @property
    def runner(self):
        '''This config_obj's runner instance.
        '''
        runner_cls = getattr(self.config_obj, 'runner_cls', PipelineRunner)
        return runner_cls()

    def _get_logger(self):
        logger = logging.getLogger(self.appname)
        loglevel = getattr(self.args, 'loglevel', None)
        if isinstance(loglevel, str):
            loglevel = getattr(logging, loglevel)
            logger.setLevel(loglevel)
        elif isinstance(loglevel, int):
            logger.setLevel(loglevel)
        return logger

    @CachedAttr
    def logger(self):
        '''Allow objects to specify their own logger.
        '''
        return set_default(self, '_logger', self._get_logger)


# ----------------------------------------------------------------------------
#  Args mixin.
#-----------------------------------------------------------------------------
class ArgsAccessor:
    '''Handles the failover lookups of parsed args on types.
    '''
    def __get__(self, inst, cls):
        self.inst = inst
        return self.get_args()

    def get_parser(self):
        '''If self.state
        '''
        return argparse.ArgumentParser()

    def get_arg_config(self):
        '''Get this object's argparse spec, including the pipeline config_obj
        argparse spec if any (if self is a component).
        '''
        inst = self.inst
        arg_config = []
        config_obj = getattr(inst, 'config_obj', None)
        if config_obj is not None and hasattr(config_obj, 'arguments'):
            arg_config += config_obj.arguments
        if hasattr(inst, 'arguments'):
            arg_config += inst.arguments
        return arg_config

    @property
    def argv(self):
        '''Return passed-in argv or sys.argv.
        '''
        config_obj = getattr(self.inst, 'config_obj', None) or self.inst
        return getattr(config_obj, '_argv', None) or sys.argv

    def get_args(self):
        '''Args are taken from 1) self._args 2) self.argv,
        3) sys.argv, 4) self.state. Facilitates testing and running
        pipelines programmatically. Mixed object need only define _args,
        or self.config_obj and self.argv, or self.state. Will attempt to
        parse argv using both pipeline-level and component-level argparse
        definitions.
        '''
        inst = self.inst

        # Return passed-in args. First try the component.
        args = getattr(inst, '_args', None)
        if args is not None:
            return args

        # Fall back to the config obj.
        config_obj = getattr(inst, 'config_obj', None)
        if config_obj is not None:
            args = getattr(config_obj, '_args', None)
            if args is not None:
                return args

        return self.parsed_args

    @CachedAttr
    def parsed_args(self):
        inst = self.inst

        # Then try to parse from config obj or top-level `arguments`.
        arg_config = self.get_arg_config()
        if arg_config is None:
            return inst.state.args

        parser = self.get_parser()
        for args, kwargs in arg_config:
            parser.add_argument(*args, **kwargs)
        inst._args, _ = parser.parse_known_args(self.argv)
        return inst._args


class ArgsMixin:
    '''Provides access to parsed argv or passed-in args.
    '''
    args = ArgsAccessor()


# ----------------------------------------------------------------------------
#  Component handling.
#-----------------------------------------------------------------------------
class ComponentLoader(StreamVisitor, UtilsMixin):
    '''Component list iterator that loads dotted import paths
    and rubber-stamps first class component references.
    '''
    def __init__(self, import_prefix=None):
        self.import_prefix = import_prefix

    def generic_visit(self, component):
        '''Rubber stamp functions/methods/types.
        '''
        return component

    def visit_str(self, component_name):
        '''Load stringy references.
        '''
        return self.resolve_name(
            component_name, module_name=self.import_prefix, raise_exc=True)


class ComponentInvocationError(Exception):
    '''Raised if component function signature requests args other than
    the supported args (state, upstream, logger, etc).
    '''


class ComponentInvoker:
    '''Class is responsible for invoking each type of component. Types get
    instantiated; functions/methods get requested arguments passed to them
    '''
    dispatcher = TypeDispatcher()

    def __init__(self, state, upstream, first):
        self.state = state
        self.upstream = upstream
        self.first = first

    def __call__(self, *args, **kwargs):
        return self.dispatcher.dispatch(*args, **kwargs)

    def _check_argnames(self, argdict, argnames, func):
        unsupported = set(argnames) - set(argdict)
        if unsupported:
            msg = (
                "Unsupported argument(s) %r were passed to "
                "component function %r. Component functions can "
                "only recieve the following arguments: %r.")
            vals = (unsupported, func, argdict.keys())
            raise ComponentInvocationError(msg % vals)

    def _get_argdata(self, func):
        argspec = inspect.getargspec(func)
        shortcut_dict = self.state.set_shortcuts.shortcut_dict()
        argdict = dict(shortcut_dict, upstream=self.upstream)
        return argdict, argspec

    def handle_tuple(self, args):
        return self.dispatcher.dispatch(make_step(*args))

    def handle_Iterable(self, thing):
        '''Need this generic handler so that ordinary iterables can be
        passed as pipeline components.
        '''
        try:
            setattr(thing, 'upstream', self.upstream)
        except AttributeError:
            pass
        return thing

    def handle_type(self, component_type):
        '''Types get instantiated, and upstream gets set.

        This should be upgraded to pass requested things to __init__.
        '''
        comp = component_type()
        comp.upstream = self.upstream
        return comp

    def handle_function(self, func):
        '''Functions that call for any aspects of the pipeline state in
        their argspec get those values passed in.

        If the function doesn't take "upstream" as an argument, this
        assumes it's a step, not a component.
        '''
        argdict, argspec = self._get_argdata(func)
        argnames = argspec.args or []
        if 'upstream' not in argnames and not self.first:
            return self.dispatcher.dispatch(make_step(func))
        self._check_argnames(argdict, argnames, func)
        argnames += argspec.keywords or []
        args = tuple(map(argdict.get, argnames))
        return func(*args)

    def handle_method(self, method):
        '''Methods that call for any aspects of the pipeline state in
        their argspec get those values passed in. Excludes `self` arg.

        If the method doesn't take "upstream" as an argument, this
        assumes it's a step, not a component.
        '''
        argdict, argspec = self._get_argdata(method)
        argnames = argspec.args[1:] or []
        if 'upstream' not in argnames and not self.first:
            return self.dispatcher.dispatch(make_step(method))
        self._check_argnames(argdict, argnames, method)
        argnames += argspec.keywords or []
        args = tuple(map(argdict.get, argnames))
        return method(*args)

    def handle_Callable(self, thing):
        '''Required for callable instances and C functions like
        operator.itemgetter.
        '''
        if hasattr(thing, '__call__'):
            has_argsig = thing.__call__
        else:
            has_argsig = thing
        try:
            argdict, argspec = self._get_argdata(has_argsig)
        except TypeError:
            argnames = []
        else:
            argnames = argspec.args or []
        if 'upstream' not in argnames and not self.first:
            return self.dispatcher.dispatch(make_step(thing))
        self._check_argnames(argdict, argnames, thing)
        argnames += argspec.keywords or []
        args = tuple(map(argdict.get, argnames))
        return thing(*args)

class ComponentAccessor:
    '''This class enables component types to access the upstream component
    .upstream and .state while ensuring the `upstream` and any other
    requests args are passed to function components.
    '''
    def __init__(self, component_type, state, upstream=None, first=False):
        self.state = state
        self.component_type = component_type
        self.upstream = upstream
        self.first = first
        self.invoker = ComponentInvoker(state, upstream, first)

    def __repr__(self):
        tmpl = '{0.__class__.__qualname__}({1})>'
        return tmpl.format(self, self.component_type)

    def __iter__(self):
        yield from self.invoked_component

    def __call__(self):
        return self.invoked_component()

    def __get__(self, inst, cls):
        return self.invoked_component

    # ------------------------------------------------------------------------
    # Also want to pass context manager events through to the component.
    # ------------------------------------------------------------------------
    def __enter__(self, *args, **kwargs):
        return self.invoked_component.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        return self.invoked_component.__exit__(*args, **kwargs)

    @CachedAttr
    def invoked_component(self):
        '''If the component is a class, this is a no-op, but if it's a
        function, any request args get passed to it. Doing this with a
        descriptor delays instantiation of the component until it's
        accessed, which is probably the least surprising way to go.
        '''
        # We need to set shortcuts on the component type, not the instance.
        comp_type = self.component_type
        self.state.set_shortcuts(comp_type)

        # And this instantiates the type.
        return self.invoker(comp_type)


class PipelineRunner(ArgsMixin, ConfigMixin, UtilsMixin):
    '''The main function of banzai, this class executes sequences of
    components by inspecting each one, soliciting its output (if any),
    and passing it to the next component.

    One instance exists per component, but all component instances
    share a single state object.
    '''
    def __init__(self, *components, config_obj=None, argv=None, args=None,
                 import_prefix=None, state=None, state_cls=None, **kwargs):

        # Set the components.
        if components is None and config_obj is None:
            msg = "Pipeline creation requires: 1) a list of components, or 2) a config_obj."
            raise ValueError(msg)
        components = components or kwargs.get('components', [])
        components = components or getattr(config_obj, 'components', None)
        self.components = components or []

        # Setup the config obj.
        config_obj = config_obj or PipelineConfig()
        self.config_obj = config_obj

        # Arg parsing needs these available from the config_obj.
        config_obj._args = args
        config_obj._argv = argv
        if getattr(config_obj, 'state_cls', None) is None:
            config_obj.state_cls = state_cls

        # State is available via a property.
        self._state = state

        # Store this for component loading later on.
        self.import_prefix = import_prefix

        # Config obj might need state.
        config_obj.state = self.state

    def get_state_subclass(self):
        state_cls = getattr(self.config_obj, 'state_cls', None)
        state_cls = state_cls
        bases = []
        if state_cls is not None:
            bases.append(state_cls)
            new_name = state_cls.__name__
        else:
            new_name = 'DefaultPipelineState'
        if state_cls is not PipelineState:
            bases.append(PipelineState)
        return type(new_name, tuple(bases), {})

    @CachedAttr
    def state(self):
        '''Set the pipeline state.
        '''
        if self._state is not None:
            return self._state
        return self.get_state_subclass()()

    def gen_components(self):
        '''Set upstream, state, and other required attributes on each
        component.
        '''
        loader = ComponentLoader(self.import_prefix)
        upstream = None
        first = True
        components = list(self.components)

        # Handle generators specially. Otherwise the type dispatcher
        # will accidentally exhaust them.
        first = components[0]
        if isinstance(first, types.GeneratorType):
            comp_type = components.pop(0)
            component = ComponentAccessor(comp_type, self.state, upstream, first=first)
            first = False
            upstream = component
            yield component

        for comp_type in loader.itervisit(components):
            component = ComponentAccessor(comp_type, self.state, upstream, first=first)
            upstream = component
            yield component

    def __iter__(self):
        '''Iterator over or call the last component to trigger the chain.
        '''
        self.set_shortcuts(self.state)
        for comp in self.gen_components():
            pass
        comp = comp.invoked_component
        if hasattr(comp, '__enter__'):
            with comp:
                yield from self.run_last_comp(comp)
        else:
            yield from self.run_last_comp(comp)

    def run_last_comp(self, comp):
        if isinstance(comp, collections.Iterable):
            yield from comp
        elif hasattr(comp, '__iter__'):
            yield from comp
        elif callable(comp):
            return comp()
        else:
            msg = 'Pipeline component %r must be either callable or iterable.'
            raise TypeError(msg % comp)


class PipelineState(ArgsMixin, ConfigMixin, UtilsMixin):
    '''An empty object where components can store state (like
    cumulative report data) and be sure it's shared among all
    components in the pipeline.
    '''
    @property
    def state(self):
        '''Quick hack to avoid infinite recursion.
        '''
        return self

    def pipeline(self, *components, **kwargs):
        '''Call the equivalent of the top-level `pipeline` function, but
        pass the parent pipeline's state and config_obj. Handy for delegating
        to child pipelines.
        '''
        inherit_attrs = ('state', 'config_obj')
        for attr in inherit_attrs:
            if attr in kwargs:
                continue
            kwargs[attr] = getattr(self, attr)
        return pipeline(*components, **kwargs)


def pipeline(*components, **kwargs):
    '''Convenience function for invoking simple pipelines.
    '''
    return PipelineRunner(*components, **kwargs)


class PipelineConfig:
    '''Exposes default pipeline config functionality (nothing yet).
    '''


class Pipeline(PipelineConfig):
    '''A subclassable base, in case you want to run pipeline like:

    class Mine(Pipeline):
        components = [
            'component1',
            'component2',
            ]

    banzai.run(Mine)

      or

    yield from Mine()
    '''
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        runner = getattr(self, 'runner_cls', PipelineRunner)
        yield from runner(
            config_obj=self._config_obj,
            components=self._components, **self.kwargs)

    @property
    def _config_obj(self):
        return getattr(self, 'config_obj', self)

    @property
    def _components(self):
        return getattr(self, 'components', self.args)


def run(Pipeline):
    for thing in Pipeline():
        pass


def make_step(function, *partial_args, **partial_kwargs):
    '''A special partial and than can consumes upstream and repeated
    calls partial'd function on the stream items.
    '''
    @functools.wraps(function)
    def wrapped(upstream):
        for token in upstream:
            yield function(token, *partial_kwargs, **partial_kwargs)
    return wrapped

