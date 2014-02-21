import banzai


class MyApp:

    class ControllerMeta:
        label = 'base'
        description = 'My cow app!!'

        config_defaults = dict(
            fastmode=False,
            )

        arguments = [
            (['-f', '--fast'], dict(
                action='store_true',
                help='Deactivate scraper rate limit.')),
            (['--pdb'], dict(
                action='store_true',
                help='Enable postmortem debugger.'))]

    commands = {
        'scrape': dict(
            import_prefix='example.scrapers',
            command_meta=dict(
                help='Scrape things.',
                aliases=['Burt']),
            components=[
                'ScrapeRunner',
                'Validator',
                'JsonWriter',
                'ReportWriter']),

        'import': dict(
            import_prefix='example.importers',
            command_meta=dict(
                help='Import things.',
                aliases=['Ernie']),
            components=[
                'ScrapeArtifactReader',
                'DiGraphBuilder',
                'DiGraphWriter',
                'ReportWriter'])
        }


if __name__ == '__main__':
    banzai.run(MyApp)
