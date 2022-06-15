import collections.abc
from . import util

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

    @property
    def is_private(self):
        return (self.element >= 0x10) and ((self.group % 2) == 1)



def as_tag(val, val2=None):
    """A flexible function for creating tags."""
    import gdcm
    import pydicom.tag

    if isinstance(val, Tag):
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
        tags = self.required_tags() | tag_set(additional_tags)

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




class AttributeSet(collections.abc.Mapping):
    def __init__(self):
        self.attributes = {}

    def add(self, a, post_proc=None, name=None):
        a = as_attr(a, post_proc=post_proc)
        self.attributes[name] = a

    def get_values(self, unit_scanner, as_dict=False):
        return util.value_map(lambda attr: attr.get_value(unit_scanner), self.items(), as_dict=as_dict)

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
        return BasicAttribute(tag_set(tags), f, default_name=_default_name)

    return wrapper

def lookup_tag(t, post_proc=None, default_name=None):
    t = as_tag(t)

    if default_name is None:
        try:
            default_name = t.keyword()
        except ImportError:
            pass
    
    @attribute([t], default_name=default_name)
    def lookup(unit_scan):
        tvs = unit_scan.tag_values(t)
        if len(tvs) != 1:
            raise ValueError(f"Dicom tag {t} ({t.keyword()}) has {len(tvs)} unique values in the current unit. Should be 1.")
        v = tvs.pop()
        return v
    
    return lookup.modify(post_proc)

@attribute(set(), default_name="NumberOfSlices")
def number_of_slices(unit_scanner):
    return len(unit_scanner.files)


def as_attr(a, post_proc=None, additional_tags=None, default_name=None):
    if isinstance(a, Attribute):
        attr = a
    elif isinstance(a, (Tag, str)):
        attr = lookup_tag(a)
    elif isinstance(a, tuple):
        attr = as_attr(*a)
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

SERIES_TAG = as_tag("0020|000e")
class SingleTagUnitizer(Unitizer):
    def __init__(self, tag):
        self.tag = tag

    @classmethod
    def by_series(cls):
        return cls(SERIES_TAG)

    def required_tags(self):
        return frozenset([self.tag])

    def items(self, scanner):
        return util.value_map(scanner.filter_files, scanner.partition_by_tag(self.tag))

def get_unitizer(v=None):
    if isinstance(v, Unitizer):
        return v
    elif isinstance(v, Tag):
        return SingleTagUnitizer(v)
    elif isinstance(v, str):
        return SingleTagUnitizer(as_tag(v))
    elif v is None:
        return SingleTagUnitizer(SERIES_TAG)



            
