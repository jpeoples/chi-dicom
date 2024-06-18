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

import time

from joblib import Parallel, delayed


class Tic:
    def __init__(self):
        self.tic()

    def get_time(self):
        return time.perf_counter_ns()

    def process_diff(self, diff):
        return diff / 1e9

    def tic(self):
        self._last = self.get_time()
    
    def toc(self):
        diff = self.get_time() - self._last
        return self.process_diff(diff)


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
        tic = Tic()
        args.cmd(args)
        tdiff = tic.toc()
        print(f"Ran in {tdiff:0.05f} seconds")

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

    

def get_index_file_pairs_for_args(args, index=None):
    if index is None:
        index = load_index(args)

    assert 'ZipFile' in index.columns
    for zfname, tab in tqdm(index.groupby('ZipFile')):
        zfpath = os.path.join(args.root, zfname)
        with zipfile.ZipFile(zfpath, "r") as zf:
            for ix, name in tab['ArcName'].items():
                with zf.open(name) as fp:
                    yield ix, fp

def scan_process_zip(zfname, tab, args, name_mapping, tag_set):
    def fix_val(x):
        return "" if x is None else str(x.value)
    tag_to_string = lambda t: name_mapping.get(t, t.tag_string())

    zfpath = os.path.join(args.root, zfname)
    read_results = {}
    with zipfile.ZipFile(zfpath, "r") as zf:
        for ix, name in tab['ArcName'].items():
            with zf.open(name) as fp:
                with pydicom.dcmread(fp, stop_before_pixels=True, specific_tags=tag_set) as dcm:
                    read_results[ix] = {tag_to_string(tag): fix_val(dcm.get(tag)) for tag in tag_set} 

    return read_results

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

def load_index(args):
    index = pandas.read_csv(args.index, index_col=0)
    return index

def reduce_table_for_batch(index, args):
    if args.batch_size > 0 and args.batch_offset > -1:
        zips = sorted(index['ZipFile'].unique())
        zips = set(zips[args.batch_offset:args.batch_offset+args.batch_size])
        
        index = index.loc[index['ZipFile'].isin(zips)]

    return index

@entry.point
def scan(args):
    tag_set, name_mapping = get_tag_set_for_args(args)
    print(name_mapping)

    read_results = {}


    index = load_index(args)
    index = reduce_table_for_batch(index, args)

    results = Parallel(n_jobs=args.jobs, verbose=10)(delayed(scan_process_zip)(zfname, tab, args, name_mapping, tag_set) for zfname, tab in index.groupby("ZipFile"))
    for res in results:
        read_results.update(res)



    tag_to_string = lambda t: name_mapping.get(t, t.tag_string())
    tag_labels = [tag_to_string(t) for t in tag_set]
    scan_results = make_df_from_result_dict(read_results, args, tag_labels)

    output = index.join(scan_results, how='inner', validate='one_to_one')
    output.to_csv(args.output)

@scan.parser
def scan_parser(parser):
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--tags", nargs="+", required=True, action='extend')
    parser.add_argument("--index", required=True)
    parser.add_argument("--tag_conf", required=False)
    parser.add_argument("--batch_offset", required=False, default=-1, type=int)
    parser.add_argument("--batch_size", required=False, default=0, type=int)

def fix_path(path):
    return pathlib.Path(path).as_posix()

def zip_archive_index_process(z, relz, args):
    relz = fix_path(os.path.relpath(z, args.root))

    zdir = os.path.dirname(relz)
    try:
        zf = zipfile.ZipFile(z, "r")
    except zipfile.BadZipFile:
        print("Bad zip:", z)
        return None
    
    with zf:
        names = zf.namelist()
        full_index = [fix_path(os.path.join(zdir, name)) for name in names]
        index = pandas.Index(full_index, name="FileName")
        df = pandas.DataFrame({"ZipFile": [relz]*len(index), "ArcName": names}, index=index)
        return df
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
    
    
    relzips = [fix_path(os.path.relpath(z, args.root)) for z in zips]

    zrelz = [(z, relz) for z, relz in zip(zips, relzips) if relz not in existing_zips]
    print(len(relzips), len(zips), len(zrelz))


    new_tables = Parallel(n_jobs=args.jobs, verbose=10)(delayed(zip_archive_index_process)(z, relz, args) for z, relz in zrelz)
    tables.extend([t for t in new_tables if t is not None])

    #for z, relz in tqdm(zrelz):
    #    relz = fix_path(os.path.relpath(z, args.root))

    #    zdir = os.path.dirname(relz)
    #    try:
    #        zf = zipfile.ZipFile(z, "r")
    #    except zipfile.BadZipFile:
    #        print("Bad zip:", z)
    #        continue
    #    
    #    with zf:
    #        names = zf.namelist()
    #        full_index = [fix_path(os.path.join(zdir, name)) for name in names]
    #        index = pandas.Index(full_index, name="FileName")
    #        df = pandas.DataFrame({"ZipFile": [relz]*len(index), "ArcName": names}, index=index)
    #        tables.append(df)
    
    outdf = pandas.concat(tables, axis='rows')
    outdf.to_csv(args.output)

@zip_archive_index.parser
def zip_archive_index_parser(parser):
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--reread_all", action='store_true')

@entry.point
def index_info(args):
    index = load_index(args)
    zipfiles = sorted(index['ZipFile'].unique())

    nzips = len(zipfiles)
    print("There are ", nzips, "zip files")

    if args.batch_size > 0:
        mod = nzips % args.batch_size
        if mod == 0:
            max_base = nzips - args.batch_size
        else:
            max_base = nzips - mod

        batches = max_base / args.batch_size
        print("With batch size", args.batch_size, "we will use", batches, "tasks")
        print(f"0-{max_base}:{args.batch_size}")

        


@index_info.parser
def index_info_parser(parser):
    parser.add_argument("--index", required=True)
    parser.add_argument("--batch_size", required=False, type=int, default=0)

@entry.add_common_parser
def common_parser(parser):
    parser.add_argument("--jobs", type=int, default=1)




    






if __name__=="__main__": main()