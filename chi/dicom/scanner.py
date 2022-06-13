import os
import fnmatch

from  . import types


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

class ScannerResult:
    def all_results(self):
        """Iterator of all file, tag, value triples"""
        for file in self.files:
            for tag in self.tags:
                val = self.get(file, tag)
                yield file, tag, val

    def _mapping_for_file_iter(self, file):
        for tag in self.tags:
            val = self.get(file, tag)
            if val is not None:
                yield tag, val

    def mapping_for_file(self, file, as_dict=False):
        """pairs mapping tag -> value for a given file"""
        mapping = self._mapping_for_file_iter(file)
        if as_dict:
            return dict(mapping)
        else:
            return mapping

    def _mapping_for_tag_iter(self, tag):
        for file in self.files:
            val = self.get(file, tag)
            yield file, val

    def mapping_for_tag(self, tag, as_dict=False):
        """Pairs mapping file -> value for a given tag"""
        mapping = self._mapping_for_tag_iter(tag)
        if as_dict:
            return dict(mapping)
        else:
            return mapping

    def get(self, file, tag):
        """The value of tag for file"""
        raise NotImplementedError

    def tag_values(self, tag):
        """The set of values a tag takes across the set of files"""
        return set((self.get(f, tag) for f in self.files))

    def files_with_tag_value(self, tag, value):
        """All files with tag=value"""
        return set((f for f in self.files if self.get(f, tag) == value))

    def partition_by_tag(self, tag):
        for value in self.tag_values(tag):
            yield value, self.files_with_tag_value(tag, value)

    def index(self):
        return IndexedScannerResult.from_mapping(self.all_results())

    def filter_files(self, files):
        if not isinstance(files, set):
            files = set(files)

        if files == self.files:
            return self

        mapping = filter(lambda res: res[0] in files, self.all_results())
        return IndexedScannerResult.from_mapping(mapping)

    def filter_tag(self, tag, val):
        return self.filter_files(self.files_with_tag_value(tag, val))

def filter_dict_on_keys(dct, keys):
    return {k: v for k, v in dct.items() if k in keys}


class IndexedScannerResult(ScannerResult):
    def __init__(self, file_tag_val, tag_file_val, tag_val_file):
        self.file_tag_val = file_tag_val
        self.tag_file_val = tag_file_val
        self.tag_val_file = tag_val_file

        self._files = set(file_tag_val.keys())
        self._tags = set(tag_file_val.keys())

    def mapping_for_file(self, file, as_dict=False):
        dct = self.file_tag_val[file]
        if as_dict:
            return dct
        else:
            return dct.items()

    def mapping_for_tag(self, tag, as_dict=False):
        dct = self.tag_file_val[tag]
        if as_dict:
            return dct
        else:
            return dct.items()


    @classmethod
    def from_mapping(cls, mapping):
        file_tag_val = {}
        tag_file_val = {}
        tag_val_file = {}

        for file, tag, val in mapping:
            file_tag_val.setdefault(file, {})[tag] = val
            tag_file_val.setdefault(tag, {})[file] = val
            tag_val_file.setdefault(tag, {}).setdefault(val, set()).add(file)
        
        return cls(file_tag_val, tag_file_val, tag_val_file)


    def filter_files(self, files):
        if not isinstance(files, set):
            files = set(files)

        if files == self.files:
            return self

        file_tag_val = filter_dict_on_keys(self.file_tag_val, files)
        tag_file_val = {}
        tag_val_file = {}
        for tag, file_val in self.tag_file_val.items():
            new_fv = filter_dict_on_keys(file_val, files)
            if new_fv:
                tag_file_val[tag] = new_fv

            val_file_pairs = ((v, files.intersection(fs)) for v, fs in self.tag_val_file[tag].items())
            val_file = {v: fs for v, fs in val_file_pairs if fs}
            tag_val_file[tag] = val_file

        return IndexedScannerResult(file_tag_val, tag_file_val, tag_val_file)

    def get(self, file, tag):
        return self.file_tag_val.get(file, {}).get(tag, None)

    def tag_values(self, tag):
        return set(self.tag_val_file[tag].keys())

    def files_with_tag_value(self, tag, value):
        return self.tag_val_file[tag].get(value, [])

    def index(self):
        return self

    @property
    def files(self):
        return self._files

    @property
    def tags(self):
        return self._tags

class GDCMScannerResult(ScannerResult):
    """Essentially this contains a files -> tag -> value mapping that can be queried variously."""
    def __init__(self, files, tags, scanner):
        self._scanner = scanner
        self._files = files
        self._tags = tags

    @classmethod
    def scan_files(cls, files, tags):
        """Scan a set of dicom files for a set of tags"""
        import gdcm
        scanner = gdcm.Scanner()
        tags = types.tag_set(tags)
        for tag in tags:
            if tag.is_private:
                scanner.AddPrivateTag(tag.gdcm())
            else:
                scanner.AddTag(tag.gdcm())

        succ = scanner.Scan(files)
        if not succ:
            raise RuntimeError("Scanner failure")

        return cls(set(files), tags, scanner)

    @classmethod
    def scan_dir(cls, dir, tags):
        """Scan a directory of DICOM files for a set of tags."""
        files = list_files(dir, "*.dcm")
        return cls.scan_files(files, tags)

    @property
    def files(self):
        return self._files

    @property
    def tags(self):
        return self._tags
        
    def get(self, file, tag):
        """The value of tag for file"""
        return self._scanner.GetValue(file, tag.gdcm())

    def tag_values(self, tag):
        """The set of values a tag takes across the set of files"""
        return set(self._scanner.GetOrderedValues(tag.gdcm()))

    def files_with_tag_value(self, tag, value):
        """All files with tag=value"""
        return set(self._scanner.GetAllFilenamesFromTagToValue(tag.gdcm(), value))
