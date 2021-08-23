from rich import print
from click import secho
from sparrow.import_helpers import BaseImporter
from sparrow.util import relative_path
from sparrow.import_helpers.util import ensure_sequence
from yaml import load
from pandas import read_excel
from re import compile
from IPython import embed
import numpy as np
import glob

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

# Identify which dicts in the list "vals" passed to create_analysis
# are data and which are attributes. Based on whether 'Value'-keyed
# item in each dict is a float
def split_attributes(vals):
    """Split data from attributes"""
    data = []
    attributes = []
    for v in vals:
        try:
            float(v["value"])
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

# Make dict with Datum1 schema. Requires value, uncertainty, and "type" which gives
# parameter measured as str, unit as str, and other type fields listed above if included
def create_datum(val):
    v = val.pop("value")
    err = val.pop("error", None)

    type = {k: v for k, v in val.items() if k in datum_type_fields}

    return {"value": v, "error": err, "type": type}

# Make dict with Attribute schema. Parameter measured as str and value as str
def create_attribute(val):
    return {k: v for k, v in val.items() if k in attribute_fields}

# type is a str giving a descriptive name for the kind of analysis
# vals is list of dictionaries related to the analysis, selected from
# the cleaned_data variable created by itervalues method
def create_analysis(type, vals, **kwargs):
    data, attributes = split_attributes(vals)
    return {
        'analysis_type': type,
        'datum': [create_datum(d) for d in data if d is not None],
        'attribute': [create_attribute(d) for d in attributes]
    }

class TRaILImporter(BaseImporter):
    def __init__(self, db, data_dir, **kwargs):
        super().__init__(db)
        file_list = glob.glob(str(data_dir)+'/*.xlsx')
        self.image_folder = data_dir / "Photographs and Measurement Data"

        self.verbose = kwargs.pop("verbose", False)
        
        # Generate list of expected columns. Some include dict where the
        # expected column has key "header", along with other relevant info
        spec = relative_path(__file__, "column-spec.yaml")
        with open(spec) as f:
            self.column_spec = load(f)
        
        # Calls Sparrow base code for each file in passed list and sends to import_datafile
        self.iterfiles(file_list, **kwargs)

    def import_datafile(self, fn, rec, **kwargs):
        """
        Import an original data file
        """
        # TODO identify which version of the data reduction sheet is in each passed file
        # TODO make column spec file for each version of the data reduction sheet
        # May be easier to open the column spec file here
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
        df.loc[singles, "grain"] = np.nan

        return df

    def import_projects(self, df):

        for name, gp in df.groupby("Owner"):
            # split_grain_information adds columns for sample name and aliquot number
            # to the main dataframe
            gp = self.split_grain_information(gp)
            nsamples = len(gp)
            project_name = f"{name} – {nsamples} samples"

            project = {"name": project_name}

            for ix, row in gp.iterrows():
                # If more than 80% of columns are empty, we assume we have an
                # empty row and don't import
                if sum(row.isnull()) > len(df.columns) * 0.8:
                    continue
                # This line calls import_row, which triggers the building of the
                # sample schema that ultimately is imported to the database
                yield self.import_row(project, row)

    def _build_specs(self, row):
        """Test each value for basic conformance with the column specs in
        `column-spec.yaml` and return the spec and column info.
        """
        for i, (spec, (key, value)) in enumerate(zip(self.column_spec, row.items())):
            # Convert shorthand spec to dict
            # For items in column-spec.yaml that are only a decriptive str, create
            # dict where "header" is the passed spec. Otherwise, remove the "header"
            # item from the dict and create a variable called header with its value
            if isinstance(spec, str):
                header = spec
                spec = {"header": spec}
            header = spec.pop("header", None)

            # Expand ± shorthand by including "error_for" key with a value equal
            # to that passed in column-spec.yaml (if included) or the preceding
            #column's index. This is used in _apply_errors to link the uncertainty
            # and value for a given datum
            if header == "±":
                header = None
                spec["error_for"] = spec.pop("error_for", i - 1)
                spec["error_metric"] = spec.pop(
                    "error_metric", "1s analytical uncertainty"
                )

            # See if we should skip importing the column based on column-spec.yaml
            # continue statements in a yield function return nothing
            if spec.get("skip", False):
                continue

            # if dictionary is passed and doesn't include header, assign to column name
            if header is not None:
                try:
                    assert header == key
                except AssertionError:
                    secho(f"Header {header} does not match {key}")
                    # embed()
                    # raise
            else:
                header = key
            
            # Add column index to ID assoc uncertainty columns later
            spec["index"] = i

            # Try to split units from column headers using split_unit function
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
            
    # Tool to build each component schema primarily using the column-spec.yaml file.
    # Begins with all but values and errors and adds the "value" and "error" keys to
    # the dict with their numerical values.
    # This is done with yield statements, making the flow a little difficult to follow
    # because the code will execute in chunks that bounce back and forth.
    def itervalues(self, row):
        specs = self._build_specs(row)
        specs_with_errors = self._apply_errors(specs)
        for spec, value, error in specs_with_errors:
            # If excel error in value, change to 'N/A' string
            try:
                if np.isnan(value):
                    value = 'N/A'
            except TypeError:
                pass
            # If excel error in an uncertainty, change to None
            try:
                if np.isnan(error):
                    error = None
            except TypeError:
                pass
            # if a discrete set of values is allowed (e.g., mineral type)
            # set the value of the parameter to the correct value
            vals = spec.get("values", None)
            try:
                spec["value"] = vals[value]
            except (IndexError, TypeError):
                spec["value"] = value
            
            # Delete index value from dictionary
            del spec["index"]
            
            # If the value has an associted uncertainty, include it in the yielded dict
            if error is not None:
                spec["error"] = error
            yield spec

    def link_image_files(self, row, session):
        if row["grain"] is None:
            return
        sample = row["Full Sample Name"]
        grain_images = list(self.image_folder.glob(sample+'*.tif'))

        for f in grain_images:
            rec, added = self._create_data_file_record(f)
            model = {
                "data_file": rec.uuid,
                "session": session.id
            }
            self.db.load_data("data_file_link", model)

    # Main method to build the sample schema. The majority of the work
    # is done by the itervalues method and create_analysis function
    def import_row(self, project, row):
        parent_sample = {
            "project": [project],
            "name": row["sample_name"],
            "material": "rock", 
        }

        if row["grain"] is None:
            self.db.load_data("sample", parent_sample)
            return

        # Get a semi-cleaned set of values for each row
        cleaned_data = list(self.itervalues(row))
        
        [researcher, sample] = cleaned_data[0:2]
        
        shape_data = cleaned_data[2:11]
        noble_gas_data = cleaned_data[11:12]
        icp_ms_data = cleaned_data[12:15]
        if icp_ms_data[2]['value'] == 0 and shape_data[6]['value'] == 'zircon':
            icp_ms_data[2]['value'] = 'N.M.'
        calculated_data = cleaned_data[15:22]
        if calculated_data[5]['value'] == 0 and shape_data[6]['value'] == 'zircon':
            calculated_data[5]['value'] = 'N.M.'
            calculated_data[5]['error'] = None
        raw_date = cleaned_data[22:24]
        corr_date = cleaned_data[24:27]
        
        grain_note = cleaned_data[27:]
        shape_data += grain_note

        material = shape_data.pop(-4)

        # We should figure out how to not require meaningless dates
        meaningless_date = "1900-01-01 00:00:00+00"
        
        # Build sample schema using row of imported data
        sample = {
            "member_of": parent_sample,
            "name": row["Full Sample Name"],
            "material": str(material["value"]), 
            # Here we pass a list of dicts instead of a single dict
            # because each (U-Th)/He analysis consists of three
            # individual sessions
            # "project": [{'name':'fakeproject'}, {'name':'fakeproject2'}],
            "session": [
                {
                    "technique": {"id": "Grain quality inspection"},
                    "instrument": {"name": "Leica microscope"},
                    "date": meaningless_date, 
                    # analysis is passed a list of dicts, where each dict is displayed
                    # as a single box on the front end. create_analysis function is 
                    # critical to the bulk of information ultimately included in Sparrow
                    "analysis": [
                        create_analysis("Grain shape", shape_data)
                    ]
                },
                {
                    "technique": {"id": "Noble-gas mass spectrometry"},
                    "instrument": {"name": "ASI Alphachron → Pfeiffer Balzers QMS"},
                    "date": meaningless_date,
                    "analysis": [
                        create_analysis("Noble gas measurements", noble_gas_data)
                    ]
                },
                {
                    "technique": "Trace element measurement",
                    "instrument": {"name": "Agilent 7900 Quadrupole ICP-MS"},
                    "date": meaningless_date,
                    "analysis": [
                        create_analysis("Element data", icp_ms_data)
                    ]
                },
                {
                    # Ideally we'd provide a date but we don't really have one
                    "technique": {"id": "(U+Th)/He age estimation"},
                    "date": meaningless_date,
                    "analysis": [
                        create_analysis("Derived parameters", calculated_data),
                        create_analysis("Raw date", raw_date),
                        create_analysis("Corrected date", corr_date)
                    ],
                }
            ]
        }
                
        print(sample)
        res = self.db.load_data("sample", sample)
        
        # I think this line should be working with images eventually.
        # Not sure it's doing anything for now
        self.link_image_files(row, res.session_collection[0])

        print("")
        return res
