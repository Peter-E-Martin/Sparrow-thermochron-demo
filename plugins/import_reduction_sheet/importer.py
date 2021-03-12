from rich import print
from click import secho
from sparrow.import_helpers import BaseImporter
from sparrow.util import relative_path
from sparrow.import_helpers.util import ensure_sequence
from yaml import load
from pandas import read_excel
from re import compile
from IPython import embed
import numpy as N

def split_unit(name):
    """Split units (in parentheses) from the rest of the data."""
    unit_regex = compile(r"^(.+)\s\(([a-zA-Z/\%]+)\)$")
    res = unit_regex.match(name)
    g = res.groups()
    (param, unit) = g
    return param, unit


def get_first(ls, n):
    """Get first n items of a list"""
    items = ls[0:n]
    ls = ls[n:]
    return items


def split_attributes(vals):
    """Split data from attributes"""
    data = []
    attributes = []
    for v in vals:
        try:
            num = float(v["value"])
            data.append(v)
        except ValueError:
            attributes.append(v)
    return data, attributes


datum_type_fields = [
    "parameter",
    "unit",
    "error_unit",
    "error_metric",
    "is_computed",
    "is_interpreted",
    "description",
]
attribute_fields = ["parameter", "value"]


def create_datum(val):
    v = val.pop("value")
    err = val.pop("error", None)

    type = {k: v for k, v in val.items() if k in datum_type_fields}

    return {"value": v, "error": err, "type": type}


def create_attribute(val):
    return {k: v for k, v in val.items() if k in attribute_fields}


def create_analysis(type, vals, **kwargs):
    data, attributes = split_attributes(vals)
    return dict(
        analysis_type=type,
        datum=[create_datum(d) for d in data if d is not None],
        attribute=[create_attribute(d) for d in attributes],
        **kwargs,
    )


class TRaILImporter(BaseImporter):
    def __init__(self, db, metadata_file, **kwargs):
        super().__init__(db)
        self.verbose = kwargs.pop("verbose", False)

        spec = relative_path(__file__, "column-spec.yaml")
        with open(spec) as f:
            self.column_spec = load(f)

        self.iterfiles([metadata_file], **kwargs)

    def import_datafile(self, fn, rec, **kwargs):
        """
        Import an original data file
        """
        df = read_excel(fn, sheet_name="Complete Summary Table")
        assert len(self.column_spec) == len(df.columns)

        yield from self.import_projects(df)

    def split_grain_information(self, df):
        # We banish underscores from sample names entirely to split.
        # Only dashes. This simplifies our life tremendously.
        df[["sample_name", "-", "grain"]] = df["Full Sample Name"].str.replace("_","-").str.rpartition("-")
        # Go back to previous separators
        df["sample_name"] = df.apply(lambda row: row["Full Sample Name"][0:len(row["sample_name"])], axis=1)
        df.drop(columns=["-"], inplace=True) # don't need it

        # Find the number of grains per sample
        n_grains = df.pivot_table(index=['sample_name'], aggfunc='size')
        singles = df.sample_name.isin(n_grains[n_grains == 1].index)
        df.loc[singles, "sample_name"] = df.loc[singles,"Full Sample Name"]
        df.loc[singles, "grain"] = N.nan

        return df

    def import_projects(self, df):

        for name, gp in df.groupby("Owner"):
            gp = self.split_grain_information(gp)
            nsamples = len(gp)
            project_name = f"{name} – {nsamples} samples"

            project = {"name": project_name}

            for ix, row in gp.iterrows():
                # If more than 80% of columns are empty, we assume we have an
                # empty row and don't import
                if sum(row.isnull()) > len(df.columns) * 0.8:
                    continue
                yield self.import_row(project, row)

    def _build_specs(self, row):
        """Test each value for basic conformance with the column specs in
        `column-spec.yaml` and return the spec and column info.
        """
        for i, (spec, (key, value)) in enumerate(zip(self.column_spec, row.items())):
            # Convert shorthand spec to dict
            if isinstance(spec, str):
                header = spec
                spec = {"header": spec}
            header = spec.pop("header", None)

            # Expand ± shorthand
            if header == "±":
                header = None
                spec["error_for"] = spec.pop("error_for", i - 1)
                spec["error_metric"] = spec.pop(
                    "error_metric", "1s analytical uncertainty"
                )

            # See if we should skip importing the column
            if spec.get("skip", False):
                continue

            if header is not None:
                try:
                    assert header == key
                except AssertionError:
                    secho(f"Header {header} does not match {key}")
            else:
                header = key

            spec["index"] = i

            # Try to split units from column headers
            unit = None
            try:
                param, unit = split_unit(header)
            except (AttributeError, KeyError):
                param = header
            if "parameter" not in spec:
                spec["parameter"] = param
            if "unit" not in spec:
                spec["unit"] = unit

            yield spec, value

    def _apply_errors(self, specs):
        """Get values for columns that contain only errors and apply them to
        the appropriate data columns."""
        datum_ix = {}
        rest = []
        for spec, value in specs:
            error_for = spec.get("error_for", None)
            if error_for is None:
                rest.append((spec, value))
                continue
            # We could have multiple destinations for a single
            # error (though unlikely)
            for err_dest in ensure_sequence(error_for):
                datum_ix[err_dest] = (spec, value)

        for spec, value in rest:
            error = None
            error_value = datum_ix.get(spec["index"], None)
            if error_value is not None:
                (err_spec, error) = error_value
                # Set error unit given units of both error and value column
                spec["error_unit"] = err_spec.get("unit", spec.get("unit", None))

            yield spec, value, error

    def itervalues(self, row):
        specs = self._build_specs(row)
        specs_with_errors = self._apply_errors(specs)
        for spec, value, error in specs_with_errors:
            vals = spec.get("values", None)
            try:
                spec["value"] = vals[value]
            except (IndexError, TypeError):
                spec["value"] = value

            del spec["index"]
            if error is not None:
                spec["error"] = error
            yield spec

    def import_row(self, project, row):
        # Get a semi-cleaned set of values for each row
        cleaned_data = list(self.itervalues(row))

        [researcher, sample] = cleaned_data[0:2]

        shape_data = cleaned_data[2:11]
        element_data = cleaned_data[11:22]

        # for spec in sample_data:
        #     pp.pprint(spec)
        grain_note = cleaned_data[27:]
        shape_data += grain_note

        material = shape_data.pop(-4)

        shape = create_analysis("Grain shape", shape_data)
        elements = create_analysis("Element data", element_data)
        raw_date = create_analysis("Raw date", cleaned_data[22:24])
        corrected_date = create_analysis("Corrected date", cleaned_data[24:27])

        sample = {
            "project": [project],
            "name": row["Full Sample Name"],
            "session": [{
                "date": "2019-11-22T00:00:00",
                "technique": {"id": "(U+Th)/He thermochronology"},
                "target": {"id": str(material["value"])},
                "analysis": [shape, elements, raw_date, corrected_date],
            }]
        }

        if row["grain"] is not None:
            sample["member_of"] = {"name": row["sample_name"]}

        print(sample)
        res = self.db.load_data("sample", sample)

        print("")
        return res
