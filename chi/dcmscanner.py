# The approach of this module is very simple --
# The key idea is to give a list of files to scan, with a set of tags to retrieve, and output that to a csv file.
#
# The complexity commes in the specification of the list. Currently supported:
#    - list all dicom files recursively in a directory
#    - list all dicom files contained in all zip files contained, recursively, in a directory
#    - Operate from a list of dcm file paths
#    - Operate from a list of (name, zipfile) pairs
#
# All methods can also filter out existing entries in a given csv file (output is accepted)
#
# The INDEX problem
#     - Previously we always would use file path as index. This creates some problems --
#         - Absolute or relative paths? Relative to what?
#         - In case of name, zip specified files, how to we handle names? Concat path  of zip dirname with name in archive? (ie treat as if you would unzip here?)
#               - IMO this is the best option, but we should then also store the name and zip file somewhere as their own special columns
#
#  

from chi import dicom

import argparse
import fnmatch
import json
import pathlib
import os, os.path
import zipfile

import pandas
import pydicom
from tqdm import tqdm

def make_parser(f=None):
    parser = argparse.ArgumentParser()
    if f:
        f(parser)
    subparsers = parser.add_subparsers()
    return parser, subparsers


class _EntryPoint:
    def __init__(self, f):
        self.f = f
        self._parser = None
        self.name = f.__name__

        f.parser = self.parser


    def prepare_parser(self, parser, subparsers):
        parser = subparsers.add_parser(self.name)
        if self._parser:
            self._parser(parser)

        parser.set_defaults(cmd=self.f)

    def parser(self, f):
        self._parser = f
        return f

class EntryPoints:
    def __init__(self):
        self.entrypoints = []
        self.parser_functions = []

    def common_parser(self, parser):
        for pf in self.parser_functions:
            pf(parser)

    def point(self, f):
        ep =  _EntryPoint(f)
        self.entrypoints.append(ep)
        return f

    def add_common_parser(self, f):
        self.parser_functions.append(f)
        return f
    

    def parse_args(self):
        parser, subparsers = make_parser(self.common_parser)
        for ep in self.entrypoints:
            ep.prepare_parser(parser, subparsers)

        args = parser.parse_args()
        return args

    def main(self):
        args = self.parse_args()
        args.cmd(args)

def main():
    entry.main()

entry = EntryPoints()

def file_line_generator(fname):
    with open(fname) as f:
        for l in f:
            yield l.strip()

SPECIAL_TAG_CASES = {
    ":multivol:": dicom.MULTI_VOLUME_TAGS
}
def special_tag_cases(name):
    return SPECIAL_TAG_CASES[name]

def handle_tag_list(tags):
    for tag in tags:
        if tag.endswith(".tags"):
            yield from file_line_generator(tag)
        else:
            yield tag

def read_tag_string(tags, special_cases=None, name_mapping=None):
    if special_cases is None:
        special_cases = {}

    if tags in special_cases:
        tag = special_cases[tags]
    else:
    
        try:
            tag = frozenset([dicom.Tag.from_pydicom_attr(tags)])
        except ValueError:
            tag = frozenset([dicom.Tag.from_tag_string(tags)])

    def get_tag_name(tag, input_name, index, count):
        try:
            return tag.keyword()
        except ValueError:
            if count == 1:
                return input_name
            else:
                return f"{input_name}_{index}"


    if name_mapping is not None:
        count = len(tag)
        lst = sorted(tag)
        for ix, tg in enumerate(lst):
            name = get_tag_name(tg, tags, ix, count)
            name_mapping[tg] = name

    return tag 

def read_tagset(cf, special_cases=None, name_mapping=None):
    assert isinstance(cf, list)
    return frozenset().union(*[read_tag_string(x, special_cases=special_cases, name_mapping=name_mapping) for x in cf])

def read_json_tag_set(cf):
    if isinstance(cf, str):
        return read_tag_string(cf)
    else:
        return read_tagset(list(cf))



def use_tag_config(tag_conf):
    results = {}
    with open(tag_conf) as f:
        conf = json.load(f)
        for name, cf in conf.items():
            tag_set = read_json_tag_set(cf)
            results[name] = tag_set

    return results

def get_tag_set_for_args(args):
    special_tag_cases = SPECIAL_TAG_CASES.copy()
    if args.tag_conf is not None:
        tag_conf = use_tag_config(args.tag_conf)
        special_tag_cases.update(tag_conf)

    tag_string_list = list(handle_tag_list(args.tags))
    name_mapping = {}
    tag_set = read_tagset(tag_string_list, special_cases=special_tag_cases, name_mapping=name_mapping)
    return tag_set, name_mapping

    

def get_index_file_pairs_for_args(args, exclusions=frozenset()):
    index = pandas.read_csv(args.index, index_col=0)
    to_use = index.index.difference(exclusions)
    index=index.loc[to_use]

    assert 'ZipFile' in index.columns
    for zfname, tab in tqdm(index.groupby('ZipFile')):
        zfpath = os.path.join(args.root, zfname)
        with zipfile.ZipFile(zfpath, "r") as zf:
            for ix, name in tab['ArcName'].items():
                with zf.open(name) as fp:
                    yield ix, fp




def load_existing_output_file(args):
    if args.output is None or not os.path.exists(args.output):
        return pandas.DataFrame()
    else:
        return pandas.read_csv(args.output, index_col=0)

def make_empty_df(index_cols, col_names):
    if len(index_cols) > 1:
        ix = pandas.MultiIndex.from_arrays([[]]*len(index_cols), names=index_cols)
    else:
        ix = pandas.Index([], name=index_cols[0])

    return pandas.DataFrame([], index=ix, columns=col_names)

def make_df_from_result_dict(read_results, args, tag_labels):
    ixnames = ["FileName"]
    if read_results:
        new_results = pandas.DataFrame.from_dict(read_results, orient='index')
        new_results.index.name = ixnames[0]
    else:
        new_results = make_empty_df(ixnames, tag_labels)

    return new_results

def merge_results(old, new, args):
    if args.do_not_merge:
        return new
    
    if args.overwrite:
        primary = new
        secondary = old
    elif args.skip_existing:
        primary = old
        secondary = new

    secondary_keep = secondary.index.difference(primary.index)
    secondary = secondary.loc[secondary_keep]

    return pandas.concat([primary, secondary], axis='rows')


@entry.point
def scan(args):
    tag_set, name_mapping = get_tag_set_for_args(args)
    print(name_mapping)

    existing = load_existing_output_file(args)
    if args.skip_existing:
        exclusions = frozenset(existing.index)
    else:
        exclusions = frozenset([])

    read_results = {}

    def fix_val(x):
        return "" if x is None else str(x.value)
    tag_to_string = lambda t: name_mapping.get(t, t.tag_string())

    for ix, fp in get_index_file_pairs_for_args(args, exclusions=exclusions):
        with pydicom.dcmread(fp, stop_before_pixels=True, specific_tags=tag_set) as dcm:
            read_results[ix] = {tag_to_string(tag): fix_val(dcm.get(tag)) for tag in tag_set} 


    tag_labels = [tag_to_string(t) for t in tag_set]
    new_results = make_df_from_result_dict(read_results, args, tag_labels)

    output = merge_results(existing, new_results, args)

    output.to_csv(args.output)

@scan.parser
def scan_parser(parser):
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--tags", nargs="+", required=True, action='extend')
    parser.add_argument("--index", required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--do_not_merge", action="store_true")
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument("--tag_conf", required=False)

def fix_path(path):
    return pathlib.Path(path).as_posix()

@entry.point
def zip_archive_index(args):
    zips = dicom.list_files(args.root, "*.zip")
    
    if os.path.exists(args.output) and not args.reread_all:
        existing = pandas.read_csv(args.output, index_col=0)
        existing_zips = set(existing['ZipFile'])
        tables = [existing]
    else:
        existing_zips = set()
        tables = []
    
    
    relzips = [os.path.relpath(z, args.root) for z in zips]

    zrelz = [(z, relz) for z, relz in zip(zips, relzips) if relz not in existing_zips]

    for z, relz in tqdm(zrelz):
        relz = os.path.relpath(z, args.root)

        zdir = os.path.dirname(relz)
        try:
            zf = zipfile.ZipFile(z, "r")
        except zipfile.BadZipFile:
            print("Bad zip:", z)
            continue
        
        with zf:
            names = zf.namelist()
            full_index = [os.path.join(zdir, name) for name in names]
            index = pandas.Index(full_index, name="FileName")
            df = pandas.DataFrame({"ZipFile": [relz]*len(index), "ArcName": names}, index=index)
            tables.append(df)
    
    outdf = pandas.concat(tables, axis='rows')
    outdf.to_csv(args.output)

@zip_archive_index.parser
def zip_archive_index_parser(parser):
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--reread_all", action='store_true')






    






if __name__=="__main__": main()