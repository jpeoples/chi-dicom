class Unit:
    def __init__(self, files):
        if isinstance(files, set):
            self.files = files
        else:
            self.files = set(files)

    def file_set(self):
        return self.files

    def unit_scanner(self, scanner):
        return scanner.filter_files(self.file_set())

class Unitizer:
    def required_tags(self):
        raise NotImplementedError

    def items(self, scanner):
        raise NotImplementedError

class BasicUnitizer(Unitizer):
    def __init__(self, tag):
        self.tag = tag

    def required_tags(self):
        return set([self.tag])

    def items(self, scanner):
        for value, files in scanner.partition_by_tag(self.tag):
            yield value, Unit(files)


    
        