from click import command, option
from sparrow.import_helpers import get_data_directory
from sparrow import construct_app
from .importer import TRaILImporter

@command()
@option('--redo', '-r', is_flag=True, default=False)
@option('--stop-on-error', is_flag=True, default=False)
@option('--verbose', '-v', is_flag=True, default=False)
@option('--show-data', '-S', is_flag=True, default=False)
def cli(redo=False, stop_on_error=False, verbose=False, show_data=False):
    data_dir = get_data_directory()
    app, db = construct_app(minimal=True)

    fn = data_dir/"Data_Reduction_Sheet.xlsx"
    # The unit of work for a session is a row in the data-reduction sheet...
    TRaILImporter(db, fn)
