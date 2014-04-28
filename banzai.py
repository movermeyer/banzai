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
                msg = "Couldn't set shortcuts on immutable %r."
                raise self.ShortcutSetError(msg % dest)

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
        self.dispatcher.dispatch(dest)

    # -----------------------------------------------------------------------
    # The type-based handlers.
    # -----------------------------------------------------------------------

    def handle_type(self, type_):
        '''If the shortcuttable object is a class definition,
        set the shortcuts. Also set the .args descriptor.
        '''
        self._set_shortcuts(type_)
        type_.args = ArgsAccessor()

    def handle_MethodType(self, method):
        '''Noop, because we can't set attributes on methods.
        '''
        pass

    def generic_handler(self, inst):
        '''Handle class instances. This will also be the dispatched method if
        someone tries to set shortcuts on an immutable object like an int.
        Some
        '''
        self._set_shortcuts(inst)


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

    @CachedAttr
    def logger(self):
        '''Allow objects to specify their own logger.
        '''
        return set_default(self, '_logger', logging.getLogger(self.appname))


# ----------------------------------------------------------------------------
#  Args mixin.
#-----------------------------------------------------------------------------
class ArgsAccessor:
    '''Handles the failover lookups of parsed args on types.
    '''
    def __get__(self, inst, cls):
        self.inst = inst
        return self.args

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
        return getattr(self, '_argv', sys.argv)

    @CachedAttr
    def args(self):
        '''Args are taken from 1) self._args 2) self.argv,
        3) sys.argv, 4) self.state. Facilitates testing and running
        pipelines programmatically. Mixed object need only define _args,
        or self.config_obj and self.argv, or self.state. Will attempt to
        parse argv using both pipeline-level and component-level argparse
        definitions.
        '''
        inst = self.inst

        # Return passed-in args first.
        args = getattr(inst, '_args', None)
        if args is not None:
            return args

        # Then try to parse from config obj or top-level `arguments`.
        arg_config = self.get_arg_config()
        if arg_config is None:
            return inst.state.args

        parser = self.get_parser()
        for args, kwargs in arg_config:
            parser.add_argument(*args, **kwargs)
        inst._args = parser.parse_args(self.argv)
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


class MethodTypeProxy:
    '''Proxy get operations through to the wrapped MethodType. Handle
    set operations on the proxy. Necessary so MethodType components supper
    upstream.state
    '''
    def __init__(self, methodtype_instance):
        object.__setattr__(self, 'func', methodtype_instance.__func__)

    def __getattr__(self, *args, **kwargs):
        func = object.__getattribute__(self, 'func')
        return func.__getattribute__(*args, **kwargs)

    def __setattr__(self, *args, **kwargs):
        func = object.__getattribute__(self, 'func')
        func.__setattr__(*args, **kwargs)


class ComponentInvoker:
    '''Class is responsible for invoking each type of component. Types get
    instantiated, functions get requested arguments passed to them, and
    methods get wrapped with an attribute get/set proxy object and otherwise
    treated like functions.
    '''
    dispatcher = TypeDispatcher()

    def __init__(self, state, upstream):
        self.state = state
        self.upstream = upstream

    def __call__(self, *args, **kwargs):
        return self.dispatcher.dispatch(*args, **kwargs)

    def handle_type(self, component_type):
        '''Types get instantiated, and upstream gets set.
        '''
        comp = component_type()
        comp.upstream = self.upstream
        return comp

        # '''Bound methods get wrapped in a proxy object and treated
        # like functions.
        # '''
        # # component_type = MethodTypeProxy(component_type)
        # return self.handle_function(component_type)

    def handle_function(self, func):
        '''Functions that call for any aspects of the pipeline state in
        their argspec get those values passed in.
        '''
        argspec = inspect.getargspec(func)
        shortcut_dict = self.state.set_shortcuts.shortcut_dict()
        argdict = dict(shortcut_dict, upstream=self.upstream)
        args = tuple(map(argdict.get, argspec.args))
        # Do arg checking here. Also: kwargs?
        return func(*args)

    def handle_method(self, method):
        '''Functions that call for any aspects of the pipeline state in
        their argspec get those values passed in.
        '''
        argspec = inspect.getargspec(method)
        shortcut_dict = self.state.set_shortcuts.shortcut_dict()
        argdict = dict(shortcut_dict, upstream=self.upstream)
        args = tuple(map(argdict.get, argspec.args[1:]))
        return method(*args)

    def generic_handler(self, obj):
        import pdb; pdb.set_trace()


class ComponentAccessor:
    '''This class enables class components to access the upstream component
    .upstream and .state while ensuring the `upstream` and any other
    requests args are passed to function components.
    '''
    def __init__(self, component_type, state, upstream=None):
        self.state = state
        self.component_type = component_type
        # component_type.upstream = upstream
        self.upstream = upstream
        self.invoker = ComponentInvoker(state, upstream)

    def __repr__(self):
        tmpl = '{0.__class__.__qualname__}({1.__qualname__}, state={0.state!r}, upstream={0.upstream!r})>'
        return tmpl.format(self, self.component_type)

    def __iter__(self):
        yield from self.invoked_component

    def __call__(self):
        return self.invoked_component()

    def __get__(self, inst, cls):
        return self.invoked_component

    @CachedAttr
    def invoked_component(self):
        '''If the component is a class, this is a no-op, but if it's a
        function, any request args get passed to it. Doing this with a
        descriptor delays instantiation of the component until it's
        accessed, which is probably the least surprising way to go.
        '''
        if self.component_type.__name__ == 'counter':
            import pdb; pdb.set_trace()
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
    share a single state object. Multi-threaders beware.
    '''
    def __init__(self, *components, config_obj=None, argv=None, args=None,
                 import_prefix=None, state=None, **kwargs):
        if components is None and config_obj is None:
            msg = "Pipeline creation requires: 1) a list of components, or 2) a config_obj."
            raise ValueError(msg)
        self.components = components or kwargs.get('components', [])
        self.import_prefix = import_prefix
        self.config_obj = config_obj or PipelineMixin()
        self._state = state
        self._args = args

    @CachedAttr
    def state(self):
        '''Set the pipeline state.
        '''
        if self._state is not None:
            return self._state
        state = getattr(self.config_obj, 'pipeline_state_cls', PipelineState)
        return state()

    def gen_components(self):
        '''Set upstream, state, and other required attributes on each
        component.
        '''
        loader = ComponentLoader(self.import_prefix)
        upstream = None
        for comp_type in loader.itervisit(self.components):
            component = ComponentAccessor(comp_type, self.state, upstream)
            upstream = component
            yield component

    def __iter__(self):
        '''Iterator over or call the last component to trigger the chain.
        '''
        self.set_shortcuts(self.state)
        for comp in self.gen_components():
            pass
        if isinstance(comp, collections.Iterable):
            yield from comp
        elif callable(comp):
            return comp()
        else:
            msg = 'Pipeline component %r must be either callable or iterable.'
            raise TypeError(msg % comp)



class PipelineState(ArgsMixin, ConfigMixin):
    '''An empty object where components can store state (like
    cumulative report data) and be sure it's shared among all
    components in the pipeline.
    '''
    @property
    def state(self):
        '''Quick hack to avoid infinite recursion.
        '''
        return self


def pipeline(*components, **kwargs):
    '''Convenience function for invoking simple pipelines.
    '''
    return PipelineRunner(*components, **kwargs)


class PipelineMixin:
    '''Exposes default pipeline config functionality (nothing yet).
    '''


class Pipeline(PipelineMixin):
    '''A subclassable base, in case you want to run pipeline like:

    class Mine(Pipeline):
        components = [
            'component1',
            'component2',
            ]

    for thing in Mine():
        pass
    '''
    def __iter__(self):
        runner = getattr(self, 'runner_cls', PipelineRunner)
        yield from runner(config_obj=self)

    def pipeline(self, *components, **kwargs):
        '''Call the equivalent of the top-level `pipeline` function, but
        pass this pipeline's state and config_obj. Handy for delegating
        to child pipelines.
        '''
        inherit_attrs = ('state', 'config_obj')
        for attr in inherit_attrs:
            if attr in kwargs:
                continue
            kwargs[attr] = getattr(self, attr)
        return pipeline(*components, **kwargs)
