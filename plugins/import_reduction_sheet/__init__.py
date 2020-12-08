from sparrow.plugins import SparrowPlugin
from .cli import import_data


class DataReductionImportPlugin(SparrowPlugin):
    name = "data-reduction-import"

    def on_setup_cli(self, cli):
        cli.add_command(import_data)
