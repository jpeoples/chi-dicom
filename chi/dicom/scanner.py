import collections
from . import types
from .util import list_files



def scan_files(files, tags):
    tags = types.tag_set(tags)
    return GDCMScannerResult.scan_files(files, tags).index()

def scan_dir(dir, tags):
    tags = types.tag_set(tags)
    return GDCMScannerResult.scan_dir(dir, tags).index()

def scan(file_list_or_dir, tags):
    if isinstance(file_list_or_dir, str):
        return scan_dir(file_list_or_dir, tags)
    else:
        return scan_files(file_list_or_dir, tags)

class ScannerResult:
    def to_dataframe(self, tag_to_string=None):
        return self.index().to_dataframe(tag_to_string)

    def to_csv(self, fname, tag_to_string=None):
        df = self.to_dataframe(tag_to_string=tag_to_string)
        df.to_csv(fname)

    @staticmethod
    def from_mapping(mapping):
        return IndexedDFScannerResult.from_mapping(mapping)

    @classmethod
    def from_csv(cls, fname, string_to_tag=None):
        import pandas
        df = pandas.read_csv(fname, index_col=0, dtype=str)
        return cls.from_dataframe(df, string_to_tag=string_to_tag)

    @classmethod
    def from_dataframe(cls, df, string_to_tag=None):
        return IndexedDFScannerResult.from_dataframe(df, string_to_tag)


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
        return IndexedDFScannerResult.from_mapping(self.all_results())

    def filter_files(self, files):
        if not isinstance(files, set):
            files = set(files)

        if files == self.files:
            return self

        mapping = filter(lambda res: res[0] in files, self.all_results())
        return IndexedDFScannerResult.from_mapping(mapping)

    def filter_tag(self, tag, val):
        return self.filter_files(self.files_with_tag_value(tag, val))

def filter_dict_on_keys(dct, keys):
    return {k: v for k, v in dct.items() if k in keys}

def dict_merge(a, b):
    a = a.copy()
    a.update(b)
    return a

def merge_list_dicts(a, b):
    all_keys = set(a.keys()) | set(b.keys())

    return {k: set(a.get(k, set())) | set(b.get(k, set())) for k in all_keys}

def drop_empty(dct):
    return {k: v for k, v in dct.items() if len(v)>0}

import pandas

def make_df(dct, orient="index"):
    df = pandas.DataFrame.from_dict(dct, orient=orient, dtype=str)
    df.columns = pandas.Index([types.as_tag(v) for v in df.columns], tupleize_cols=False)

    return df
    

class IndexedDFScannerResult(ScannerResult):
    def __init__(self, df):
        self.df = df
        self._files = frozenset(self.df.index)
        self._tags = frozenset(self.df.columns)

        self._tag_val_files_cache = {}


    @property
    def files(self):
        return self._files

    @property
    def tags(self):
        return self._tags

    def index(self):
        return self

    def merge(self, scanner):
        assert len(scanner.files & self.files) == 0
        assert scanner.tags == self.tags
        return IndexedDFScannerResult(pandas.concat((self.df, scanner.df)))

    def to_dataframe(self, tag_to_string=None):
        if tag_to_string is None:
            tag_to_string = lambda t: t.tag_string()

        out_df = self.df.copy()
        print(out_df.columns)
        out_df.columns = [tag_to_string(t) for t in self.df.columns]
        return out_df

    @classmethod
    def from_dataframe(cls, df, string_to_tag=None):
        if string_to_tag is None:
            string_to_tag=lambda s: types.Tag.from_tag_string(s)

        df.columns = pandas.Index([string_to_tag(t) for t in df.columns], tupleize_cols=False)

        return cls(df)

    def all_results(self, as_dict=False):
        def _iter():
            for file, row in self.df.iterrows():
                for tag, val in row.items():
                    yield file, tag, val

        it = _iter()
        if as_dict:
            return dict(it)
        else:
            return it

    def mapping_for_file(self, file, as_dict=False):
        row = self.df.loc[file]
        if as_dict:
            return row.to_dict()
        else:
            return row.items()

    def mapping_for_tag(self, tag, as_dict=False):
        col = self.df.loc[:, tag]
        if as_dict:
            return col.to_dict()
        else:
            return col.items()



    @classmethod
    def from_mapping(cls, mapping):
        file_tag_val = collections.defaultdict(dict)
        for file, tag, val in mapping:
            file_tag_val[file][tag] = val.strip() if val else val

        #df = pandas.DataFrame.from_dict(file_tag_val, orient='index')
        df = make_df(file_tag_val, orient="index")
        return cls(df)

    def filter_files(self, files):
        if set(files) == self.files:
            return self

        return IndexedDFScannerResult(self.df.loc[files])

    def _get_val_files_for_tag(self, tag):
        if tag in self._tag_val_files_cache:
            return self._tag_val_files_cache[tag]
        else:
            val_files = self.df.groupby(tag).groups
            self._tag_val_files_cache[tag] = val_files
            return val_files

    def get(self, file, tag):
        return self.df.loc[file, tag]

    def tag_values(self, tag):
        return set(self._get_val_files_for_tag(tag).keys())

    def files_with_tag_value(self, tag, value):
        return list(self._get_val_files_for_tag(tag).get(value, []))







class IndexedScannerResult(ScannerResult):
    def __init__(self, file_tag_val, tag_file_val, tag_val_file, df=None):
        self.file_tag_val = file_tag_val
        self.tag_file_val = tag_file_val
        self.tag_val_file = tag_val_file

        self._files = frozenset(file_tag_val.keys())
        self._tags = frozenset(tag_file_val.keys())

        self._df = df

    def merge(self, scanner):
        assert len(scanner.files & self.files) == 0
        assert scanner.tags == self.tags

        scanner = scanner.index()

        file_tag_val = self.file_tag_val.copy()
        file_tag_val.update(scanner.file_tag_val)

        tag_file_val = {}
        tag_val_file = {}
        for tag in self.tags:
            tag_file_val[tag] = dict_merge(self.tag_file_val[tag], scanner.tag_file_val[tag])
            tag_val_file[tag] = merge_list_dicts(self.tag_val_file[tag], scanner.tag_val_file[tag])

        return IndexedScannerResult(file_tag_val, tag_file_val, tag_val_file)
                

        

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

    def to_dataframe(self, tag_to_string=None):
        if self._df is None:
            import pandas
            if tag_to_string is None:
                tag_to_string = lambda t: t.tag_string()
            tag_file_val = {tag_to_string(t): m for t, m in self.tag_file_val.items()}
            #df = pandas.DataFrame.from_dict(tag_file_val, orient="columns")
            df = make_df(tag_file_val, orient="columns")
            self._df = df
            return df
        else:
            return self._df


    @classmethod
    def from_dataframe(cls, df, string_to_tag=None):
        import pandas
        if string_to_tag is None:
            string_to_tag=lambda s: types.Tag.from_tag_string(s)

        old_columns = df.columns
        df.columns = pandas.Index([string_to_tag(t) for t in df.columns], tupleize_cols=False)

        tags_files_values = df.to_dict(orient="dict")
        #tags_files_values = {string_to_tag(t): m for t, m in tags_files_values.items()}

        files_tags_values = df.to_dict(orient="index")
        #files_tags_values = {file: {string_to_tag(tag): val for tag, val in tag_vals.items()} for file, tag_vals in files_tags_values.items()}

        tags_values_files = {tag: df.groupby(tag).groups for tag in df.columns}
        #tags_values_files = {string_to_tag(tag): df.groupby(tag).groups for tag in df.columns}

        df.columns = old_columns

        return cls(files_tags_values, tags_files_values, tags_values_files, df)

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
        return frozenset(self._scanner.GetOrderedValues(tag.gdcm()))

    def files_with_tag_value(self, tag, value):
        """All files with tag=value"""
        return frozenset(self._scanner.GetAllFilenamesFromTagToValue(tag.gdcm(), value))
