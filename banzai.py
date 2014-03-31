'''
Idea is to facilitate pipeline of read/transform/write operations.

- Can have arbitrary readers, with defaults and expected uses.
- Input is run through one or more transformers: objects that provide
  and simple interface.
- Output of transformers is then handed to one or more writers.

The package just provides command line utilities and some basic plumbing.
Doesn't try to make important or constricting decisions. Think flask.
'''
import inspect
import logging
import argparse
import collections


__version__ = '0'


class Pipeline(object):
    '''The main function of banzai, this class executes sequences of
    components by inspecting each one, soliciting its output (if any),
    and passing it to the next component.

    One pipeline instance exists per component, but all pipeline instances
    for each set of command components share a single state object.
    Multi-threaders beware.
    '''
    def __init__(self):
        self.components = []

    def add_components(self, components):
        self.components += list(components)

    def prepare_components(self):
        '''Make useful attributes on the components at run time.
        '''
        for comp_type in self.components:
            comp_type.state = self.state
            set_shortcuts(comp_type, self.controller)
            comp_type.utils = self.utils

    def set_comp_sequence_dict(self):
        '''Set a dict on self where each comp type is mapped to the
        compy type assigned to the next step in the pipeline.
        '''
        mapping = {}
        comps = self.components
        for i in range(len(self.components)):
            comp_type = comps[i]
            try:
                next_comptype = comps[i + 1]
            except IndexError:
                next_comptype = None
            mapping[comp_type] = next_comptype
        self.comp_seq_dict = mapping

    def set_type_instance_dict(self):
        '''Map component types to instances.
        '''
        mapping = {}
        for comp_type in self.components:
            mapping[comp_type] = comp_type()
        self.type_instance_dict = mapping

    def run(self):
        '''Run the components in order.

        XXX: Work this out so that components can be generators like
        def mygen(upstream):
            upstream.info('cow')
            for animal in upstream:
                yield 'moo'
        '''
        self.prepare_components()
        self.set_comp_sequence_dict()
        self.set_type_instance_dict()

        # Set upstream and downstream attrs on each pair of comps.
        if not self.components:
            return
        for comp_type in self.components:
            comp = self.type_instance_dict[comp_type]
            next_comptype = self.comp_seq_dict[comp_type]
            if next_comptype is not None:
                next_comp = self.type_instance_dict[next_comptype]
                next_comp.upstream = comp
                comp.downstream = next_comp

        # To run the whole sequence, we call the last one.
        # It will pull results from downstream as needed, and so on.
        if hasattr(comp, '__iter__'):
            returned_something = False
            for token in comp:
                pass
                returned_something = True
            if not returned_something:
                msg = 'Iteratable component %r did not yield any output.'
                self.warn(msg % comp)
        elif callable(comp):
            comp()
        else:
            raise TypeError('Last compoenent must be iterable or callable.')


        # Allow the last object the chance to finish things up.
        finalize = getattr(comp, 'finalize', None)
        if finalize is not None:
            finalize()


class PipelineState(object):
    '''An empty object where components can store state (like
    cumulative report data) and be sure it's shared among all
    components in the pipeline.
    '''


class AppBuilder(object):
    '''Given an App object, configure a custom cement cli app,
    and run it.
    '''
    def __init__(self, config_cls):
        self.config_obj = config_cls()
        self.utils = Utils()
        self.parser = argparse.ArgumentParser()

        # Get the pipeline state.
        pipeline_state_cls = getattr(
            self.config_obj, 'pipeline_state_cls', PipelineState)
        self.state = pipeline_state_cls()

        # Get the pipeline instance.
        self.pipeline_cls = getattr(self.config_obj, 'pipeline_cls', Pipeline)
        self.appname = config_cls.__name__

        self.init_cli_app()
        self.set_shortcuts(self.config_obj)

    def run(self):
        try:
            for action in self.args.actions.split(','):
                # Creat a pipeline.
                pipeline = self.pipeline_cls()
                self.set_shortcuts(pipeline)
                self.set_shortcuts(pipeline.state)

                # Try to let it resolve the command first.
                if hasattr(self.config_obj, 'get_components'):
                    cmd_config = self.config_obj.get_components(action)
                    components = self.load_components(cmd_config)

                # Otherwise use the top level components on the app.
                else:
                    command = getattr(self.config_obj, action)
                    cmd_config = command()
                    components = self.load_components(cmd_config)

                # Add the components and run it.
                pipeline.add_components(components)
                pipeline.run()

        except Exception:
            import traceback
            traceback.print_exc()
            self.post_mortem()

    def load_components(self, cmd_config):
        import_prefix = cmd_config.get('import_prefix')
        components = cmd_config['components']
        components = tuple(self.utils.resolve_names(
            components, module_name=import_prefix, raise_exc=True))
        return components

    def init_cli_app(self):
        '''Sets up the command line interface through cement.
        '''
        for args, kwargs in self.config_obj.arguments:
            self.parser.add_argument(*args, **kwargs)

    def set_shortcuts(self, obj):
        set_shortcuts(obj, self)

    @property
    def args(self):
        args = getattr(self, '_args', None)
        if args is None:
            args = self._args = self.parser.parse_args()
        return args

    def post_mortem(self):
        for debugger in ('pdb', 'pudb', 'ipdb'):
            enabled = getattr(self.args, debugger, False)
            if enabled:
                try:
                    post_mortem = self.utils.resolve_name('post_mortem', module_name=debugger)
                except ImportError as exc:
                    msg = (
                        "Please install %s in order to use it "
                        "with the post mortem debugger.")
                    import traceback
                    traceback.print_exc()
                    self.state.warn(msg % debugger)
                post_mortem()

class Utils:
    '''Exposes lazy import functions.
    '''
    def resolve_name(self, name, module_name=None, raise_exc=False):
        '''Given a name string and module prefix, try to import the name.
        '''
        if module_name is None:
            module_name, _, name = name.rpartition('.')
        try:
            module = __import__(module_name, globals(), locals(), [name], -1)
        except ImportError:
            if raise_exc:
                raise
            return
        return getattr(module, name)

    def resolve_names(self, names, module_name=None, raise_exc=False):
        '''Try to import a sequence of names.
        '''
        for name in names:
            yield self.resolve_name(name, module_name, raise_exc=raise_exc)

utils = Utils()


def set_shortcuts(obj, controller):
    '''Make references to the cli app and its goodies
    avaialable on obj.
    '''
    obj.utils = utils
    obj.controller = controller
    obj.state = controller.state

    # App shortcuts.
    obj.args = controller.args

    # Logging shortcuts.
    log = logging.getLogger(controller.appname)
    obj.log = log
    obj.info = log.info
    obj.debug = log.debug
    obj.error = log.error
    obj.warning = obj.warn = log.warn
    obj.critical = log.critical


def run(config_obj):
    '''Given a config class, run the app.
    '''
    AppBuilder(config_obj).run()

