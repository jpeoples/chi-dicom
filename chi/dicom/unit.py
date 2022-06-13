from .types import tag

class Unit:
    def __init__(self, files):
        if isinstance(files, frozenset):
            self.files = files
        else:
            self.files = frozenset(files)

    def file_set(self):
        return self.files

    def unit_scanner(self, scanner):
        return scanner.filter_files(self.file_set())

class Unitizer:
    def required_tags(self):
        raise NotImplementedError

    def items(self, scanner):
        raise NotImplementedError

SERIES_TAG = tag("0020|000e")
class SingleTagUnitizer(Unitizer):
    def __init__(self, tag):
        self.tag = tag

    @classmethod
    def by_series(cls):
        return cls(SERIES_TAG)

    def required_tags(self):
        return frozenset([self.tag])

    def items(self, scanner):
        for value, files in scanner.partition_by_tag(self.tag):
            yield value, Unit(files)


    
        