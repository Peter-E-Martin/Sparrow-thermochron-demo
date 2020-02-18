import numpy as N

from pprint import PrettyPrinter
from os import environ, listdir, path
from datetime import datetime
from click import command, option, echo, secho, style
from pathlib import Path
from sparrow.database import get_or_create
from sparrow.import_helpers import BaseImporter, SparrowImportError
from sparrow.util import relative_path
from sparrow.import_helpers.util import ensure_sequence
from yaml import load
from pandas import read_excel, isna
from re import compile

unit_regex = compile(r"^(.+)\s\(([a-zA-Z/\%]+)\)$")
def split_unit(name):
    res = unit_regex.match(name)
    g = res.groups()
    (param, unit) = g
    return param, unit

pp = PrettyPrinter(indent=2)

class TRaILImporter(BaseImporter):
    def __init__(self, db, metadata_file, **kwargs):
        super().__init__(db)
        self.verbose = kwargs.pop("verbose", False)

        spec = relative_path(__file__, 'column-spec.yaml')
        with open(spec) as f:
            self.column_spec = load(f)

        self.iterfiles([metadata_file])

    def import_datafile(self, fn, rec, **kwargs):
        """
        Import an original data file
        """
        df = read_excel(fn, sheet_name="Complete Summary Table")
        assert len(self.column_spec) == len(df.columns)

        for ix, row in df.iterrows():
            # If more than 80% of columns are empty, we assume we have an empty row
            if sum(row.isnull()) > len(df.columns)*.8:
                continue
            self.import_row(row)

    def _build_specs(self, row):
        """Test each value for basic conformance with the column spec and return
        the spec and column info"""
        for i, (spec, (key, value)) in enumerate(zip(self.column_spec, row.items())):
            ## Convert shorthand spec to dict
            if isinstance(spec, str):
                header = spec
                spec = {'header':  spec}
            header = spec.pop('header', None)

            # Expand ± shorthand
            if header == '±':
                header = None
                spec['error_for'] = spec.pop('error_for', i-1)
                spec['error_metric'] = spec.pop('error_metric', '1s analytical uncertainty')

            # See if we should skip importing the column
            if spec.get('skip', False):
                continue

            if header is not None:
                try:
                    assert header == key
                except AssertionError:
                    secho(f"Header {header} does not match {key}")
            else:
                header = key

            spec['index'] = i

            # Try to split units from column headers
            unit = None
            try:
                param, unit = split_unit(header)
            except (AttributeError, KeyError):
                param = header
            if 'parameter' not in spec:
                spec['parameter'] = param
            if 'unit' not in spec:
                spec['unit'] = unit

            yield spec, value

    def _apply_errors(self, specs):
        """Get values for columns that contain only errors and apply them to
        the appropriate data columns."""
        datum_ix = {}
        rest = []
        for spec, value in specs:
            error_for = spec.get('error_for', None)
            if error_for is None:
                rest.append((spec, value))
                continue
            # We could have multiple destinations for a single error (though unlikely)
            for err_dest in ensure_sequence(error_for):
                datum_ix[err_dest] = (spec, value)

        for spec, value in rest:
            error = None
            error_value = datum_ix.get(spec['index'], None)
            if error_value is not None:
                (err_spec, error) = error_value
                # Set error unit given units of both error and value column
                spec['error_unit'] = err_spec.get("unit", spec.get('unit', None))

            yield spec, value, error

    def itervalues(self, row):
        specs = self._build_specs(row)
        specs_with_errors = self._apply_errors(specs)
        for spec, value, error in specs_with_errors:
            vals = spec.pop("values", None)
            if vals and value in vals:
                value = vals[value]

            del spec['index']
            spec['value'] = value
            if error is not None:
                spec['error'] = error
            yield spec

    def import_row(self, row):
        for spec in self.itervalues(row):
            pp.pprint(spec)
            #import IPython; IPython.embed(); raise
