
import collections
import fnmatch
import os, os.path

import SimpleITK as sitk
import pandas

class Tag(collections.namedtuple('Tag', ['group', 'element'])):
    @classmethod
    def from_tag_string(cls, s, split_char='|'):
        group_string, element_string = s.split(split_char)
        group = int(group_string, 16)
        element = int(element_string, 16)

        return cls(group, element)

    @classmethod
    def from_pydicom_attr(cls, s):
        import pydicom.datadict
        import pydicom.tag
        pdcm_tag = pydicom.datadict.tag_for_keyword(s)
        if pdcm_tag is None:
            raise ValueError(f"String {s} is not an official pydicom attribute")
        
        return cls.from_pydicom_tag(pydicom.tag.Tag(pdcm_tag))

    @classmethod
    def from_pydicom_tag(cls, pdcm):
        return cls(pdcm.group, pdcm.elem)

    @classmethod
    def from_gdcm_tag(cls, gtag):
        return cls(gtag.GetGroup(), gtag.GetElement())

    def pydicom(self):
        import pydicom.tag
        return pydicom.tag.TupleTag((self))

    def gdcm(self):
        import gdcm
        if self.is_private:
            return gdcm.PrivateTag(self.group, self.element)
        else:
            return gdcm.Tag(self.group, self.element)

    def keyword(self):
        import pydicom.datadict
        result = pydicom.datadict.keyword_for_tag(self.pydicom())
        if result == "":
            raise ValueError("Tag {self} doesn't have a keyword in pydicom")
        return result
    
    def tag_string(self):
        return f"{self.group:04x}|{self.element:04x}"

    @property
    def is_private(self):
        return (self.element >= 0x10) and ((self.group % 2) == 1)

def _prep_gdcm_scanner(tags):
    import gdcm
    scanner = gdcm.Scanner()
    for tag in tags:
        if tag.is_private:
            scanner.AddPrivateTag(tag.gdcm())
        else:
            scanner.AddTag(tag.gdcm())

    return scanner

def scan_files(files, tags, tag_to_string=lambda t: t.tag_string()):
    scanner = _prep_gdcm_scanner(tags)
    succ = scanner.Scan(files)
    if not succ:
        raise RuntimeError("Scanner Failure!")

    results = {}
    for file in files:
        for tag in tags:
            value = scanner.GetValue(file, tag.gdcm())
            if value is None:
                value = ""
            value = value.strip()
            results.setdefault(file, {})[tag_to_string(tag)] = value

    return pandas.DataFrame.from_dict(results, orient='index')

def check_has_tags(scan_result, tags):
    if len(tags) > 1:
        tag_strings = frozenset(t.tag_string() for t in tags)
        assert tag_strings.issubset(scan_result.columns)
    else:
        assert list(tags)[0].tag_string() in scan_result.columns

#-------------------------------------------------------------------------------
#
# The following implements loading of series/subseries into SimpleITK images,
# using the scanning facilities above.
#
#-------------------------------------------------------------------------------

MULTI_VOLUME_TAGS = frozenset((
    Tag.from_tag_string("0020|000e"), # Series Instance UID
    Tag.from_tag_string("0020|0012"), # Acquisition Number
    Tag.from_tag_string("0008|0008"), # Image Type
    Tag.from_tag_string("0020|0037"), # Image Orientation (Patient)
    Tag.from_tag_string("0018|9089") # Diffusion Gradient Orientation
))

SERIES_TAG = Tag.from_tag_string("0020|000e")

def get_subseries(scan_result, multi_volume_tags=MULTI_VOLUME_TAGS):
    """Check for subseries based on the set of multi_volume_tags"""
    assert len(scan_result.loc[:,SERIES_TAG.tag_string()].unique())==1

    check_has_tags(scan_result, multi_volume_tags)

    subseries = {}
    for tag in multi_volume_tags:
        groups = scan_result.groupby(tag.tag_string())
        if len(groups) > 1:
            subseries[tag] = groups

    return subseries

def get_series(scan_result):
    """Check for the series in the scan"""
    check_has_tags(scan_result, set([SERIES_TAG]))

    series_ids = scan_result.loc[:, SERIES_TAG.tag_string()].unique()
    return series_ids

# TODO -- Can we get the IPP values from a scan result, rather than requiring a separate (internal) scan here?
def sort_dicom_files(files):
    """Sort DICOM files based on position and orientation"""
    import gdcm
    sorter = gdcm.IPPSorter()
    succ = sorter.Sort(files)
    sorted_files = sorter.GetFilenames()

    if succ:
        return sorted_files
    else:
        raise RuntimeError("Could not sort DICOM files base on Image Position (Patient)")

def load_dicom_files(series_file_names, return_metadata=False, do_not_sort=False):
    """Load a set of dicom files into a volume, loading metadata if desired."""
    if not isinstance(series_file_names, list):
        series_file_names = list(series_file_names)

    if not do_not_sort:
        series_file_names = sort_dicom_files(series_file_names)

    series_reader = sitk.ImageSeriesReader()
    series_reader.SetFileNames(series_file_names)

    if return_metadata:
        # Configure the reader to load all of the DICOM tags (public+private):
        # By default tags are not loaded (saves time).
        # By default if tags are loaded, the private tags are not loaded.
        # We explicitly configure the reader to load tags, including the
        # private ones.
        series_reader.MetaDataDictionaryArrayUpdateOn()
        series_reader.LoadPrivateTagsOn()
        image3D = series_reader.Execute()
        return image3D, series_reader
    else:
        image3D = series_reader.Execute()
        return image3D


def list_files(d, glob_string=None):
    """List of all files under root d matching glob_string"""
    def _list():
        for root, dirs, files in os.walk(d):
            if glob_string is None:
                filtered_files = files
            else:
                filtered_files = fnmatch.filter(files, glob_string)
            yield from (os.path.join(root, f) for f in filtered_files)

    return (list(_list()))

def scan_dir(dir, tags):
    files = list_files(dir, "*.dcm")
    return scan_files(files, tags)

class SeriesLoadResult:
    """Interpret a set of files corresponding to a SeriesInstanceUID as volume(s).
    
    The files are a complete set for a given SeriesInstanceUID. The subseries parameter is
    the result of get_subseries for these files. The result is XB!
    """
    def __init__(self, files, subseries, scan_result, series_id):
        self.files = files
        self.subseries = subseries
        self.scan_result = scan_result
        self.series_id = series_id

    @classmethod
    def from_scan_result(cls, scan_result, series_id=None):
        check_has_tags(scan_result, MULTI_VOLUME_TAGS)
        if series_id is None:
            series = get_series(scan_result)
            assert len(series) == 1
            series_id = next(iter(series))
        else:
            scan_result = scan_result.loc[scan_result[SERIES_TAG.tag_string()]==series_id]

        return cls(list(scan_result.index), get_subseries(scan_result), scan_result, series_id)

    @classmethod
    def from_files(cls, files):
        scan_result = scan_files(files, MULTI_VOLUME_TAGS)
        return cls.from_scan_result(scan_result)

    @classmethod
    def from_dir(cls, dir):
        scan_result = scan_dir(dir, MULTI_VOLUME_TAGS)
        return cls.from_scan_result(scan_result)

    def has_subseries(self):
        return bool(self.subseries)
    
    def load_series(self):
        assert not self.has_subseries()
        return load_dicom_files(self.files, return_metadata=False)
    
    def subseries_tags(self):
        return set(t for t in self.subseries)

    def load_subseries(self, tag):
        assert self.has_subseries()
        for val, files in self.subseries[tag].groups.items():
            yield val, load_dicom_files(files)
    
    def load_specific_subseries(self, tag, val):
        files = self.subseries[tag].groups[val]
        return load_dicom_files(files)

    
