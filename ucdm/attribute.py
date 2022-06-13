from .types import tag_set, tag
import functools

class Attribute:
    def required_tags(self):
        """Return the list of required tags"""
        raise NotImplementedError

    def get_value(self, unit_scan):
        """Return the value of the attribute for a given unit"""
        raise NotImplementedError

    def __call__(self, unit_scan):
        return self.get_value(unit_scan)

# Maybe allow or not Nullable
class BasicAttribute(Attribute):
    def __init__(self, tags, function, post_proc=None):
        self.tags = tags
        self.function = function
        if post_proc is None:
            post_proc = lambda s: s.strip() if isinstance(s, str) else s
        self._post_proc = post_proc

    def required_tags(self):
        return self.tags

    def get_value(self, unit_scan):
        return self.post_proc(self.function(unit_scan))

    def post_proc(self, v):
        return self._post_proc(v)


def make_attribute(tags, function, post_proc=None):
    return BasicAttribute(tag_set(tags), function, post_proc=post_proc)

def attribute(tags, post_proc=None):
    def wrapper(f):
        return make_attribute(tags, f, post_proc)

    return wrapper

def lookup_tag(t, post_proc=None):
    t = tag(t)
    
    @attribute([t], post_proc)
    def lookup(unit_scan):
        tvs = unit_scan.tag_values(t)
        if len(tvs) != 1:
            raise ValueError(f"Dicom tag {t} ({t.keyword()}) has {len(tvs)} unique values in the current unit. Should be 1.")
        v = tvs.pop()
        return v
    
    return lookup

@attribute(set())
def number_of_slices(unit_scanner):
    return len(unit_scanner.files)
        





