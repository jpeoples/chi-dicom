"""Implement the processing"""
import itertools
from .load import SeriesLoadResult
from .scanner import scan
from .types import attr_set, get_unitizer
from . import util


class _aggregate_tags:
    @staticmethod
    def __call__(*args):
        return _aggregate_tags.from_iterable(*args)

    @staticmethod
    def from_iterable(it):
        return frozenset.union(*(a.required_tags() for a in it))

aggregate_tags=_aggregate_tags()

def required_tags(unitizer, attributes):
    return aggregate_tags.from_iterable(itertools.chain((unitizer,), attributes.values()))


def read_attributes(unitizer_items, attributes, as_dict=False):
    return util.value_map(lambda x: attributes.get_values(x, as_dict=True), unitizer_items, as_dict=as_dict)

class DICOMProcessor:
    def __init__(self, unitizer, attributes):
        self.unitizer = unitizer
        self.attributes = attributes
        self.required = required_tags(self.unitizer, self.attributes)

    def process_directory(self, dir, as_dict=False):
        scanner = scan(dir, self.required)
        units = self.unitizer.items(scanner)

        return read_attributes(units, self.attributes, as_dict=as_dict)

def get_processor(attributes, unitizer=None):
    attributes = attr_set(attributes)
    unitizer = get_unitizer(unitizer)
    proc = DICOMProcessor(unitizer, attributes)
    return proc

def read_dicom_attributes(dir_or_files, attributes, unitizer=None, as_dict=False):
    proc = get_processor(attributes, unitizer)
    return proc.process_directory(dir_or_files, as_dict=as_dict)

def dicom_series(dir_or_files):
    attributes = {"loader": SeriesLoadResult.as_attr()}

    for sid, dct in read_dicom_attributes(dir_or_files, attributes):
        yield sid, dct['loader']


