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

class BasicAttribute(Attribute):
    def __init__(self, tags, function):
        self.tags = tags
        self.function = function

    def required_tags(self):
        return self.tags

    def get_value(self, unit_scan):
        return self.function(unit_scan)

def make_attribute(tags, function):
    return BasicAttribute(tag_set(tags), function)

def attribute(tags):
    def wrapper(f):
        return make_attribute(tags, f)

    return wrapper

def lookup_tag(t):
    t = tag(t)
    
    @attribute([t])
    def lookup(unit_scan):
        tvs = unit_scan.tag_values(t)
        if len(tvs) != 1:
            raise ValueError(f"Dicom tag {t} ({t.keyword()}) has {len(tvs)} unique values in the current unit. Should be 1.")
        return tvs.pop()
    
    return lookup
        





