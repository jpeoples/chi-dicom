import collections
import itertools

import pandas


def make_df(dct, orient="index"):
    df = pandas.DataFrame.from_dict(dct, orient=orient, dtype=str)

    return df

class LookupStringConverter:
    def to_string(self, value):
        raise NotImplementedError

    def from_string(self, s):
        raise NotImplementedError



class ResourceLookupResult:
    def __init__(self, df, lookup_string_converter):
        self.df = df
        self.lookup_string_converter = lookup_string_converter
        self._files = frozenset(self.df.index)
        self._tags = frozenset([self.string_to_tag(v) for v in self.df.columns])

        self._tag_val_files_cache = {}

    def tag_to_string(self, t):
        return self.lookup_string_converter.to_string(t)

    def string_to_tag(self, s):
        return self.lookup_string_converter.from_string(s)

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
        return ResourceLookupResult(pandas.concat((self.df, scanner.df)))

    def to_csv(self, fname):
        df = self.to_dataframe()
        df.to_csv(fname)

    @classmethod
    def from_csv(cls, fname):
        import pandas
        df = pandas.read_csv(fname, index_col=0, dtype=str)
        return cls.from_dataframe(df)

    # TODO Remove copy?
    def to_dataframe(self):
        out_df = self.df.copy()
        return out_df

    @classmethod
    def from_dataframe(cls, df):
        return cls(df)

    
    def all_results(self, as_dict=False):
        def _iter():
            for file, row in self.df.iterrows():
                for tag, val in row.items():
                    yield file, self.string_to_tag(tag), val

        it = _iter()
        if as_dict:
            return dict(it)
        else:
            return it

    def mapping_for_file(self, file, as_dict=False):
        row = self.df.loc[file]
        if as_dict:
            return {self.string_to_tag(k): v for k, v in row.items()}
        else:
            return ((self.string_to_tag(k), v) for k, v in row.items())

    def mapping_for_tag(self, tag, as_dict=False):
        tag = self.tag_to_string(tag)
        col = self.df.loc[:, tag]
        if as_dict:
            return col.to_dict()
        else:
            return col.items()

    @classmethod
    def from_mapping(cls, mapping, lookup_string_converter):
        file_tag_val = collections.defaultdict(dict)
        for file, tag, val in mapping:
            file_tag_val[file][lookup_string_converter.to_string(tag)] = val.strip() if val else val

        #df = pandas.DataFrame.from_dict(file_tag_val, orient='index')
        df = make_df(file_tag_val, orient="index")
        return cls(df, lookup_string_converter)

    def filter_files(self, files):
        if set(files) == self.files:
            return self

        return ResourceLookupResult(self.df.loc[files], self.lookup_string_converter)

    def _get_val_files_for_tag(self, tag):
        tag = self.tag_to_string(tag)
        if tag in self._tag_val_files_cache:
            return self._tag_val_files_cache[tag]
        else:
            val_files = self.df.groupby(tag).groups
            self._tag_val_files_cache[tag] = val_files
            return val_files

    def get(self, file, tag):
        return self.df.loc[file, self.tag_to_string(tag)]

    def tag_values(self, tag):
        return set(self._get_val_files_for_tag(tag).keys())

    def files_with_tag_value(self, tag, value):
        return list(self._get_val_files_for_tag(tag).get(value, []))

    def filter_tag(self, tag, val):
        return self.filter_files(self.files_with_tag_value(tag, val))

    def partition_by_tag(self, tag):
        for value in self.tag_values(tag):
            yield value, self.files_with_tag_value(tag, value)


# This is the basic interface for scanning resources
class ResourceScanner:
    TagType=None # Should be defined by subclasses
    def scan(self, resource_set) -> ResourceLookupResult:
        raise NotImplementedError


class Attribute:
    def default_name(self):
        return None

    def required_tags(self):
        """Return the list of required tags"""
        raise NotImplementedError

    def get_value(self, unit_scan):
        """Return the value of the attribute for a given unit"""
        raise NotImplementedError

    def __call__(self, unit_scan):
        return self.get_value(unit_scan)

    def modify(self, post_proc=None, additional_tags=None, default_name=None):
        if post_proc is None and default_name is None and additional_tags is None:
            return self

        if additional_tags is None:
            additional_tags = set()
        tags = self.required_tags() | set(additional_tags)

        func = (lambda us: post_proc(self(us))) if post_proc else self
        default_name = default_name if default_name else self.default_name()

        return BasicAttribute(tags, func, default_name=default_name if default_name else self.default_name())

class BasicAttribute(Attribute):
    def __init__(self, tags, function, default_name=None):
        self.tags = tags
        self.function = function
        self._default_name = default_name

        if self._default_name is None:
            try:
                self._default_name = self.function.default_name()
            except AttributeError:
                pass

    def default_name(self):
        return self._default_name

    def required_tags(self):
        return self.tags

    def get_value(self, unit_scan):
        return self.function(unit_scan)


def _value_map(f, kv_pair_iter):
    for k, v in kv_pair_iter:
        yield k, f(v)

def value_map(f, kv_pair_iter, as_dict=False):
    it = _value_map(f, kv_pair_iter)
    if as_dict:
        return dict(it)
    else:
        return it

class AttributeSet(collections.abc.Mapping):
    def __init__(self):
        self.attributes = {}

    def add(self, a, post_proc=None, name=None):
        #a = as_attr(a, post_proc=post_proc)
        assert isinstance(a, Attribute)
        self.attributes[name] = a

    def get_values(self, unit_scanner, as_dict=False):
        return value_map(lambda attr: attr.get_value(unit_scanner), self.items(), as_dict=as_dict)

    def __getitem__(self, k):
        return self.attributes[k]

    def __iter__(self):
        return iter(self.attributes)

    def __len__(self):
        return len(self.attributes)

    def __contains__(self, key):
        return key in self.attributes

    def keys(self):
        return self.attributes.keys()

    def values(self):
        return self.attributes.values()

    def items(self):
        return self.attributes.items()

def attribute(tags, default_name=None):
    def wrapper(f):
        _default_name = default_name
        if default_name is None:
            _default_name = f.__name__
        return BasicAttribute(set(tags), f, default_name=_default_name)

    return wrapper

def lookup_tag(t, post_proc=None, default_name=None):
    @attribute([t], default_name=default_name)
    def lookup(unit_scan):
        tvs = unit_scan.tag_values(t)
        if len(tvs) != 1:
            raise ValueError(f"Tag {t} has {len(tvs)} unique values in the current unit. Should be 1.")
        v = tvs.pop()
        return v
    return lookup.modify(post_proc)

def as_attr(a, post_proc=None, additional_tags=None, default_name=None):
    if isinstance(a, Attribute):
        attr = a
    else:
        attr = a.as_attr()

    return attr.modify(post_proc=post_proc, additional_tags=additional_tags, default_name=default_name)

def attr_set(v):
    if isinstance(v, AttributeSet):
        return v
    elif isinstance(v, collections.abc.Mapping):
        kv_pairs = [(k, as_attr(_v)) for k, _v in v.items()]
    elif isinstance(v, collections.abc.Iterable):
        kv_pairs = [(a.default_name(), a) for a in map(as_attr, v)]

    aset = AttributeSet()
    for k, v in kv_pairs:
        aset.add(v, name=k)
    return aset

class Unitizer:
    def required_tags(self):
        raise NotImplementedError

    def items(self, scanner):
        raise NotImplementedError

class SingleTagUnitizer(Unitizer):
    def __init__(self, tag):
        self.tag = tag

    def required_tags(self):
        return frozenset([self.tag])

    def items(self, scanner):
        return value_map(scanner.filter_files, scanner.partition_by_tag(self.tag))

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
    return value_map(lambda x: attributes.get_values(x, as_dict=True), unitizer_items, as_dict=as_dict)


# Allow multiple scan types somehow........
class ResourceScannerBuilder:
    def __init__(self, rs):
        self.rs = rs

    def scanner_for(self, unitizer, attributes):
        tags = required_tags(unitizer, attributes)

        rs = self.rs()
        for tag in tags:
            assert isinstance(tag, self.rs.TagType)
            rs.add_tag(tag)
        
        return rs

class AttributeScanner:
    def __init__(self, unitizer, attributes, rsb):
        self.unitizer = unitizer
        self.attributes = attributes
        self.scanner = rsb.scanner_for(unitizer, attributes)

    def scan(self, resources):
        scan_result = self.scanner.scan(resources)
        ui = self.unitizer.items(scan_result)
        attrs = read_attributes(ui, self.attributes, True)
        return attrs


