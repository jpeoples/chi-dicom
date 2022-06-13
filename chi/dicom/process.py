"""Implement the processing"""
from .load import SeriesLoadResult
from .scanner import scan
from .types import attr_set, get_unitizer

def required_tags(unitizer, attributes):
    tags = unitizer.required_tags().copy()

    for name, attr in attributes.items():
        tags |= attr.required_tags()

    return tags


def yield_units(scanner, unitizer):
    yield from unitizer.items(scanner)

def _mapping(scanner, unitizer_items, attributes):
    for name, unit in unitizer_items:
        unit_scanner = unit.unit_scanner(scanner)
        attrs = {}
        for attr_name, attr in attributes.items():
            val = attr.get_value(unit_scanner)
            attrs[attr_name] = val
        yield name, attrs

def mapping(scanner, unitizer_items, attributes, as_dict=False):
    items = _mapping(scanner, unitizer_items, attributes)
    if as_dict:
        return dict(items)
    else:
        return items

class DICOMProcessor:
    def __init__(self, unitizer, attributes):
        self.unitizer = unitizer
        self.attributes = attributes

    def process_directory(self, dir, as_dict=False):
        required = required_tags(self.unitizer, self.attributes)
        scanner = scan(dir, required)
        units = yield_units(scanner, self.unitizer)

        return mapping(scanner, units, self.attributes, as_dict=as_dict)

def read_dicom_attributes(dir_or_files, attributes, unitizer=None, as_dict=False):
    attributes = attr_set(attributes)
    unitizer = get_unitizer(unitizer)

    proc = DICOMProcessor(unitizer, attributes)
    return proc.process_directory(dir_or_files, as_dict=as_dict)

def dicom_series(dir_or_files):
    attributes = {"loader": SeriesLoadResult.as_attr()}

    for sid, dct in read_dicom_attributes(dir_or_files, attributes):
        yield sid, dct['loader']


