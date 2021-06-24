from click import command, option
from sparrow.import_helpers import get_data_directory
from sparrow.cli.util import with_database
from .importer import TRaILImporter
from IPython import embed
from pathlib import Path

from logging import getLogger, WARNING

logger = getLogger(__name__)


@command(name="import-data")
@option("--redo", "-r", is_flag=True, default=False)
@option("--stop-on-error", is_flag=True, default=False)
@option("--verbose", "-v", is_flag=True, default=False)
@option("--show-data", "-S", is_flag=True, default=False)
@with_database
def import_data(db, redo=False, stop_on_error=False, verbose=False, show_data=False):
    """Import data from TRaIL's data reduction format"""
    data_dir = Path("/data")
    #data_dir = get_data_directory()
    # The unit of work for a session is a row in the data-reduction sheet...
    TRaILImporter(db, data_dir, redo=redo)
