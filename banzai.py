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
import collections

from cement.core import backend, foundation, controller, handler


class Pipeline(object):
    '''The main function of banzai, this class executes sequences of
    components by inspecting each one, soliciting its output (if any),
    and passing it to the next component.

    One pipeline instance exists per component, but all pipeline instances
    for each set of command components share a single state object.
    Multi-threaders beware.
    '''
    def __init__(self, components, state, controller, utils):
        self.components = components
        self.state = state
        self.controller = controller
        set_app_shortcuts(self, controller)
        set_app_shortcuts(state, controller)
        self.utils = utils

    def prepare_components(self):
        '''Make useful attributes on the components at run time.
        '''
        for comp_type in self.components:
            comp_type.state = self.state
            set_app_shortcuts(comp_type, self.controller)
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
        for comp_type in self.components:
            comp = self.type_instance_dict[comp_type]
            next_comptype = self.comp_seq_dict[comp_type]
            if next_comptype is not None:
                next_comp = self.type_instance_dict[next_comptype]
                next_comp.upstream = comp
                comp.downstream = next_comp

        # To run the whole sequence, we call the last one.
        # It will pull results from downstream as needed, and so on.
        if callable(comp):
            comp()
        else:
            raise TypeError('Last compoenent must be callable.')

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
        self.config_cls = config_cls
        self.utils = Utils()

        # Initialize the cli app.
        self.cli_app = self.get_cli_app()

    def run(self):
        try:
            import pudb; pudb.set_trace()
            self.cli_app.setup()
            self.cli_app.run()
        finally:
            self.cli_app.close()

    def get_cli_app(self):
        '''Sets up the command line interface through cement.
        '''
        utils = self.utils
        config_obj = self.config_cls()

        # Get the pipeline state.
        pipeline_state_cls = getattr(
            config_obj, 'pipeline_state_cls', PipelineState)
        pipeline_state = pipeline_state_cls()

        # Get the pipeline instance.
        pipeline_cls = getattr(config_obj, 'pipeline_cls', Pipeline)

        # ---------------------------------------------------------------------
        # Define the cli app controller.
        # ---------------------------------------------------------------------
        class Controller(controller.CementBaseController):
            Meta = config_obj.ControllerMeta

        # For each command defined in the banzai app, add a command line
        # flaggy thing.
        commands = []
        for name, member in inspect.getmembers(config_obj):
            if not getattr(member, '_is_pipeline', False):
                continue
            cmd_name = name
            cmd_config = member()
            import_prefix = cmd_config.get('import_prefix')
            components = cmd_config['components']
            components = tuple(utils.resolve_names(
                components, module_name=import_prefix))

            def method(self):
                pipeline = pipeline_cls(
                    components, pipeline_state,
                    controller=self, utils=utils)
                pipeline.run()

            # Patch the method name to appease cement.
            method.func_name = cmd_name

            # commands.append()

            # Pass the metadata to command config.
            cmd_meta = cmd_config['command_meta']
            method = controller.expose(**cmd_meta)(method)
            setattr(Controller, cmd_name, method)

        # ---------------------------------------------------------------------
        # Define the cli app itself.
        # ---------------------------------------------------------------------
        import pdb;pdb.set_trace()
        x = Controller()
        class CliApp(foundation.CementApp):
            class Meta:
                label = 'helloworld'
                base_controller = Controller

        cli_app = CliApp()
        return cli_app


class Utils:
    '''Exposes lazy import functions.
    '''
    def resolve_name(self, name, module_name=None):
        '''Given a name string and module prefix, try to import the name.
        '''
        if module_name is None:
            module_name, _, name = name.rpartition('.')
        module = __import__(module_name, globals(), locals(), [name], -1)
        return getattr(module, name)

    def resolve_names(self, names, module_name=None):
        '''Try to import a sequence of names.
        '''
        for name in names:
            yield self.resolve_name(name, module_name)

utils = Utils()


def set_app_shortcuts(obj, controller):
    '''Make references to the cli app and its goodies
    avaialable on obj.
    '''
    obj.utils = utils
    obj.controller = controller
    app = controller.app
    obj.app = app

    # App shortcuts.
    obj.args = app.args
    obj.argv = app.argv
    obj.pargs = app.pargs

    # Logging shortcuts.
    log = app.log
    obj.log = log
    obj.info = log.info
    obj.debug = log.debug
    obj.error = log.error
    obj.warn = log.warn


def pipeline(f):
    '''Dumb decorator for marking pipeline functions.
    '''
    f._is_pipeline = True
    return f


def run(config_obj):
    '''Given a config class, run the app.
    '''
    AppBuilder(config_obj).run()

