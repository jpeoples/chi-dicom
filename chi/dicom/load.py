import gdcm
import SimpleITK as sitk

from .attribute import attribute
from .types import tag, tag_set
from .scanner import GDCMScannerResult, ScannerResult, list_files

MULTI_VOLUME_TAGS = tag_set((
    "0020|000e", # Series Instance UID
    "0020|0012", # Acquisition Number
    "0008|0008", # Image Type
    "0020|0037", # Image Orientation (Patient)
    "0018|9089" # Diffusion Gradient Orientation
))

SERIES_TAG = tag("0020|000e")

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
                scan = scan.filter(SERIES_TAG, series_id)
            else:
                assert len(files) == len(scan.files)

        subseries = get_subseries(scan)
        return cls(files, subseries, return_metadata)



    @classmethod
    def from_files(cls, files, return_metadata=False):
        scan = GDCMScannerResult.scan_files(files, tags=get_multi_volume_tags())
        return cls.from_scan(scan, return_metadata=return_metadata)


    @classmethod
    def from_dir(cls, data_directory, return_metadata=False):
        files = list_files(data_directory, "*.dcm")
        return cls.from_files(files, return_metadata=return_metadata)


    def has_subseries(self):
        return bool(self.subseries)

    def subseries_tags(self):
        return set(self.subseries.keys())

    def load_subseries(self, tag):
        assert(self.has_subseries())
        for val, files in self.subseries[tag].items():
            yield val, load_dicom_files(files, return_metadata=self.return_metadata)

    def load_series(self):
        assert(not self.has_subseries())

        return load_dicom_files(self.files, return_metadata=self.return_metadata)


@attribute(tags=MULTI_VOLUME_TAGS)
def sitk_image(unit_scanner):
    # Unitized on series
    return SeriesLoadResult.from_scan(unit_scanner)