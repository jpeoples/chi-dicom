"""Implement the processing"""
from .scanner import GDCMScannerResult

def required_tags(unitizer, attributes):
    tags = unitizer.required_tags().copy()

    for name, attr in attributes.items():
        tags |= attr.required_tags()

    return tags

def make_scanner(required_tags, files_or_dir):
    if isinstance(files_or_dir, str):
        return GDCMScannerResult.scan_dir(files_or_dir, required_tags).index()
    else:
        return GDCMScannerResult.scan_files(files_or_dir, required_tags).index()

def yield_units(scanner, unitizer):
    yield from unitizer.items(scanner)

def mapping(scanner, unitizer_items, attributes):
    results = {}
    for name, unit in unitizer_items:
        unit_scanner = unit.unit_scanner(scanner)
        attrs = {}
        for attr_name, attr in attributes.items():
            val = attr.get_value(unit_scanner)
            attrs[attr_name] = val
        results[name] = attrs
    return results

class DICOMProcessor:
    def __init__(self, unitizer, attributes):
        self.unitizer = unitizer
        self.attributes = attributes

    def process_directory(self, dir):
        required = required_tags(self.unitizer, self.attributes)
        scanner = make_scanner(required, dir)
        units = yield_units(scanner, self.unitizer)

        return mapping(scanner, units, self.attributes)

