import collections
from typing import Sequence

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

def tag(val, val2=None):
    """A flexible function for creating tags."""
    import gdcm
    import pydicom.tag

    if isinstance(val, Tag):
        return val
    elif isinstance(val, gdcm.Tag):
        return Tag.from_gdcm_tag(val)
    elif isinstance(val, pydicom.tag.BaseTag):
        return Tag.from_pydicom_tag
    elif isinstance(val, str):
        s = val
        try:
            return Tag.from_tag_string(s)
        except ValueError as etagstr:
            try:
                return Tag.from_pydicom_attr(s)
            except ValueError as epdcmattr:
                raise ValueError(f"String '{s}' is neither a tag string or a valid pydicom attribute keyword") from etagstr and epdcmattr
    elif isinstance(val, Sequence):
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
    return map(tag, it)

def tag_list(it):
    return list(tag_iter(it))

def tag_set(it, mutable=False):
    set_ctr = set if mutable else frozenset
    return set_ctr(tag_iter(it))



            

