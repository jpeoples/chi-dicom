import collections
import fnmatch
import os

from . import scattr
import SimpleITK as sitk

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
        return pydicom.datadict.keyword_for_tag(self.pydicom())
    
    def tag_string(self):
        return f"{self.group:04x}|{self.element:04x}"

    @property
    def is_private(self):
        return (self.element >= 0x10) and ((self.group % 2) == 1)

def as_tag(val, val2=None):
    """A flexible function for creating tags."""
    import gdcm
    import pydicom.tag

    if hasattr(val, 'as_tag'):
        return val.as_tag()
    elif isinstance(val, Tag):
        return val
    elif isinstance(val, gdcm.Tag):
        return Tag.from_gdcm_tag(val)
    elif isinstance(val, pydicom.tag.BaseTag):
        return Tag.from_pydicom_tag(val)
    elif isinstance(val, str):
        s = val
        try:
            return Tag.from_tag_string(s)
        except ValueError as etagstr:
            try:
                return Tag.from_pydicom_attr(s)
            except ValueError as epdcmattr:
                raise ValueError(f"String '{s}' is neither a tag string or a valid pydicom attribute keyword") from etagstr and epdcmattr
    elif isinstance(val, collections.abc.Sequence):
        group, elem = val
        if isinstance(group, int) and isinstance(elem, int):
            return Tag(group, elem)
        else:
            raise ValueError(f"A 2-tuple passed to tag must consist of integers -- got ({group}, {elem})")
    elif isinstance(val, int):
        assert val2 is not None
        group, elem = val, val2
        if isinstance(group, int) and isinstance(elem, int):
            return Tag(group, elem)
        else:
            raise ValueError(f"Values must consist of integers -- got ({group}, {elem})")

def tag_iter(it):
    return map(as_tag, it)

def tag_list(it):
    return list(tag_iter(it))

def tag_set(it, mutable=False):
    set_ctr = set if mutable else frozenset
    return set_ctr(tag_iter(it))

class DICOMTagStringConverter(scattr.LookupStringConverter):
    def to_string(self, tag):
        return tag.tag_string()

    def from_string(self, s):
        return Tag.from_tag_string(s)

class DICOMFileScanner(scattr.ResourceScanner):
    TagType = Tag

    def __init__(self, tags=None, tag_string_converter=None):
        if tags is None:
            tags = set()
        self.tags = tag_set(tags, mutable=True)
        self.tag_string_converter = tag_string_converter if tag_string_converter is not None else DICOMTagStringConverter()

    def add_tag(self, tag):
        assert isinstance(tag, self.TagType)
        if tag not in self.tags:
            self.tags.add(tag)

    def _prep_gdcm_scanner(self):
        import gdcm
        scanner = gdcm.Scanner()
        for tag in self.tags:
            if tag.is_private:
                scanner.AddPrivateTag(tag.gdcm())
            else:
                scanner.AddTag(tag.gdcm())
        
        return scanner

    def _execute_scan(self, scanner, file_list):
        succ = scanner.Scan(file_list)
        if not succ:
            raise RuntimeError("Scanner failure!")

    def _mapping(self, scanner, file_list):
        for file in file_list:
            for tag in self.tags:
                yield file, tag, scanner.GetValue(file, tag.gdcm())

    def scan(self, file_list):
        scanner = self._prep_gdcm_scanner()
        self._execute_scan(scanner, file_list)

        return scattr.ResourceLookupResult.from_mapping(self._mapping(scanner, file_list), self.tag_string_converter)

MULTI_VOLUME_TAGS = tag_set((
    "0020|000e", # Series Instance UID
    "0020|0012", # Acquisition Number
    "0008|0008", # Image Type
    "0020|0037", # Image Orientation (Patient)
    "0018|9089" # Diffusion Gradient Orientation
))

SERIES_TAG = as_tag("0020|000e")

def get_multi_volume_tags():
    return MULTI_VOLUME_TAGS.copy()

def get_subseries(scan_result, multi_volume_tags=None):
    """Check for separate volumes in series a la slicer"""
    if multi_volume_tags is None:
        multi_volume_tags = get_multi_volume_tags()

    assert multi_volume_tags.issubset(scan_result.tags)
    subseries = {}
    for tag in multi_volume_tags:
        # Get all unique values of tag.
        vals = scan_result.tag_values(tag)
        if len(vals) > 1:
            subseries[tag] = {val: scan_result.files_with_tag_value(tag, val) for val in vals}

    return subseries

def get_series(scan_result):
    """Given scan result, get list of series ids"""
    assert SERIES_TAG in scan_result.tags
    series_IDs = scan_result.tag_values(SERIES_TAG)
    if not series_IDs:
        raise RuntimeError("ERROR: provided files do not contain any DICOM series.")
    return series_IDs

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


def scan_files(files, tags):
    return DICOMFileScanner(tags).scan(files)

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
    def __init__(self, files, subseries, return_metadata):
        self.files = files
        self.subseries = subseries
        self.return_metadata = return_metadata

    @classmethod
    def from_scan(cls, scan, series_id=None, return_metadata=False):
        if series_id is None:
            series_ids = get_series(scan)
            if len(series_ids) > 1:
                raise RuntimeError("More than 1 series in set!")
            
            series_id = next(iter(series_ids))
            files = scan.files
        else:
            files = scan.files_with_tag_value(SERIES_TAG, series_id)
            # If this actually reduces the file set, we need to filter the scanner
            if len(files) < len(scan.files):
                scan = scan.filter_files(files)
            else:
                assert len(files) == len(scan.files)

        subseries = get_subseries(scan)
        return cls(files, subseries, return_metadata)



    @classmethod
    def from_files(cls, files, return_metadata=False):
        scan = scan_files(files, tags=get_multi_volume_tags())
        return cls.from_scan(scan, return_metadata=return_metadata)


    @classmethod
    def from_dir(cls, data_directory, return_metadata=False):
        scan = scan_dir(data_directory, tags=get_multi_volume_tags())
        return cls.from_scan(scan, return_metadata=return_metadata)


    def has_subseries(self):
        return bool(self.subseries)

    def subseries_tags(self):
        return set(self.subseries.keys())

    def load_subseries(self, tag):
        assert(self.has_subseries())
        for val, files in self.subseries[tag].items():
            yield val, load_dicom_files(files, return_metadata=self.return_metadata)

    def load_specific_subseries(self, tag, value):
        return load_dicom_files(self.subseries[tag][value], return_metadata=self.return_metadata)

    def load_series(self):
        assert(not self.has_subseries())

        return load_dicom_files(self.files, return_metadata=self.return_metadata)

    @classmethod
    def as_attr(cls):
        @scattr.attribute(tags=MULTI_VOLUME_TAGS, default_name="SeriesLoadResult")
        def attr(unit_scanner):
            return cls.from_scan(unit_scanner)
        return attr



