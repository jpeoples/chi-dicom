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
from chi.util import EntryPoints, DFBatchParRun

import fnmatch
import json
import pathlib
import os, os.path
import zipfile

import pandas
import pydicom

from joblib import Parallel, delayed

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

def yield_files(zfpath, tab, tag_set, args):
    if args.raw_dicom:
        for f in tab.index:
            with pydicom.dcmread(os.path.join(args.root, f), stop_before_pixels=True, specific_tags=tag_set) as dcm:
                yield f, dcm
    else:
        with zipfile.ZipFile(zfpath, "r") as zf:
            for ix, name in tab['ArcName'].items():
                with zf.open(name) as fp:
                    with pydicom.dcmread(fp, stop_before_pixels=True, specific_tags=tag_set) as dcm:
                        yield ix, dcm

def MISSING():
    pass

def _fix_val(x, missing_val="_chidcm_missing_", empty_val="_chidcm_empty_"):
    if x is MISSING:
        return missing_val
    elif x is None or x.is_empty:
        return empty_val
    else:
        return str(x.value)



def scan_process_zip(zfname, tab, args, name_mapping, tag_set):
    tag_to_string = lambda t: name_mapping.get(t, t.tag_string())

    if args.group_key == "ZipFile":
        assert zfname == tab['ZipFile'].unique()[0]
    else:
        input_zfname = zfname
        zfname = tab[args.group_key].unique()
        assert len(zfname) == 1
        zfname = zfname[0]

    zfpath = os.path.join(args.root, zfname)
    read_results = {}
    for ix, dcm in yield_files(zfpath, tab, tag_set, args): 
        read_results[ix] = {tag_to_string(tag): _fix_val(dcm.get(tag, MISSING)) for tag in tag_set} 
    #with zipfile.ZipFile(zfpath, "r") as zf:
    #    for ix, name in tab['ArcName'].items():
    #        with zf.open(name) as fp:
    #            with pydicom.dcmread(fp, stop_before_pixels=True, specific_tags=tag_set) as dcm:
    #                read_results[ix] = {tag_to_string(tag): fix_val(dcm.get(tag)) for tag in tag_set} 

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

def scan_process_zip_wrapper(bpr, zf_tab, args, name_mapping, tag_set):
    result = scan_process_zip(zf_tab[0], zf_tab[1], args, name_mapping, tag_set)
    result = zf_tab[1].join(pandas.DataFrame.from_dict(result, orient='index'), validate='one_to_one', how='inner')
    return bpr.table(result)

@entry.point
def scan(args):
    tag_set, name_mapping = get_tag_set_for_args(args)
    print(name_mapping)

    #read_results = {}
    index = load_index(args)
    bpr = DFBatchParRun.from_function(scan_process_zip_wrapper)
    info = bpr.iter_info(index, group_key=args.group_key)
    table = bpr.run_from_args(args, iter_args=(info,), execute_args=(args, name_mapping, tag_set))
    if args.output_file is not None:
        table.to_csv(args.output_file)

@scan.parser
def scan_parser(parser):
    DFBatchParRun.update_parser(parser)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output_file", required=True)
    parser.add_argument("--tags", nargs="+", required=True, action='extend')
    parser.add_argument("--index", required=True)
    parser.add_argument("--tag_conf", required=False)
    parser.add_argument("--group_key", required=False, default="ZipFile")
    parser.add_argument("--raw_dicom", action='store_true')

def fix_path(path):
    return pathlib.Path(path).as_posix()

def zip_archive_index_process(z, relz):
    zdir = os.path.dirname(relz)
    try:
        zf = zipfile.ZipFile(z, "r")
    except zipfile.BadZipFile:
        print("Bad zip:", z)
        return None
    
    with zf:
        names = zf.namelist()
        # TODO Make this configurable
        names = [n for n in names if n.endswith(".dcm")]
        full_index = [fix_path(os.path.join(zdir, name)) for name in names]
        index = pandas.Index(full_index, name="FileName")
        df = pandas.DataFrame({"ZipFile": [relz]*len(index), "ArcName": names}, index=index)
        return df

def zip_archive_index_process_wrapper(bpr, arg, args):
    ix, arg = arg
    z = arg['ZipFile']
    relz = fix_path(os.path.relpath(z, args.root))
    table = zip_archive_index_process(z, relz)
    return bpr.table(table)
    
@entry.point
def zip_archive_index(args):
    zips = dicom.list_files(args.root, "*.zip")
    bpr = DFBatchParRun.from_function(zip_archive_index_process_wrapper) 
    table = pandas.DataFrame({"ZipFile": zips})
    iter_info = bpr.iter_info(table)
    results = bpr.run_from_args(args, iter_args=(iter_info,), execute_args=(args,))

    if args.output_file is not None:
        results.to_csv(args.output_file, index=True)
    
    # TODO Support for exclusions in batch par run?
    #if os.path.exists(args.output) and not args.reread_all:
    #    existing = pandas.read_csv(args.output, index_col=0)
    #    existing_zips = set(existing['ZipFile'])
    #    tables = [existing]
    #else:
    #    existing_zips = set()
    #    tables = []
    
@zip_archive_index.parser
def zip_archive_index_parser(parser):
    DFBatchParRun.update_parser(parser)

    parser.add_argument("--root", required=True)
    parser.add_argument("--output_file", required=True)
    #parser.add_argument("--reread_all", action='store_true')

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

#@entry.add_common_parser
#def common_parser(parser):
#    parser.add_argument("--jobs", type=int, default=1)

import pathlib
def list_at_depth(root, depth=1):
    cur_depth=0
    for r, s, f in os.walk(root, topdown=True):
        rel_root = pathlib.Path(r).relative_to(root).as_posix()
        if rel_root == ".":
            cur_depth=1
        else:
            cur_depth = rel_root.count("/") + 2

        if cur_depth == depth:
            print(r, s, f)
            if len(f) != 0:
                print(f"There are {len(f)} files at depth {cur_depth} that are being ignored.\n    Root {r}\n    Files: {f}")
            yield from [fix_path(os.path.relpath(os.path.join(r, _s), root)) for _s in s]

import pydicom.errors
def is_dicom(f, parse=False):
    if parse:
        try:
            dcm = pydicom.dcmread(f, stop_before_pixels=True)
        except pydicom.errors.InvalidDicomError:
            return False
        else:
            return True
    else:
        return f.endswith(".dcm")

def dicom_recursive_search(bpr, arg, cmdargs):
    ix, row = arg
    sdir = row['Subdirectory']
    root = cmdargs.root

    files = dicom.list_files(os.path.join(root, sdir))
    
    rel_files = []
    for f in files:
        if is_dicom(f):
            relf = fix_path(os.path.relpath(f, root))
            rel_files.append(relf)

    tab = pandas.Series(sdir, index=rel_files).to_frame("Subdirectory")
    tab.index.name = "File"
    return bpr.table(tab)

def full_file_list(root):
    files = dicom.list_files(root)
    rel_path = [fix_path(os.path.relpath(f, root)) for f in files]
    return pandas.Series(files, index=rel_path).to_frame("FilePath")

def dicom_file_check(bpr, arg, cmdargs):
    chunk, tab = arg
    dcms = []
    for ix, row in tab.iterrows():
        rel_path = ix
        full_path = row['FilePath']
        

        if is_dicom(full_path, cmdargs.check_dicom_parse):
            dcms.append(dict(File=rel_path, Subdirectory=os.path.dirname(rel_path)))
    if dcms:
        return bpr.table(pandas.DataFrame.from_records(dcms, index="File"))  

import itertools
def chunker(size):
    for ix in itertools.count():
        yield from itertools.repeat(ix, size)

    

@entry.point
def dicom_search(args):
    if args.depth>-1:
        directories = pandas.DataFrame({"Subdirectory": list_at_depth(args.root, args.depth)})
        bpr = DFBatchParRun.from_function(dicom_recursive_search)
        info = bpr.iter_info(directories)
    else:
        directories = full_file_list(args.root)
        print(f"Found total of {directories.shape[0]} files")
        bpr = DFBatchParRun.from_function(dicom_file_check)
        directories['ChunkLabel'] = pandas.Series(dict(zip(directories.index, chunker(args.chunk_size))))
        info = bpr.iter_info(directories, 'ChunkLabel')
        
    
    results = bpr.run_from_args(args, iter_args=(info, ), execute_args=(args,))

    if args.output_file is not None:
        #if args.depth==-1:
        #    results.to_csv(args.output_file, index=False)
        #else:
        results.to_csv(args.output_file)
    
@dicom_search.parser
def dicom_search_parser(parser):
    DFBatchParRun.update_parser(parser)
    parser.add_argument("--root", required=True)
    parser.add_argument("--depth", required=False, type=int, default=1)
    parser.add_argument("--chunk_size", required=False, type=int, default=500)
    parser.add_argument("--output_file", required=False)
    parser.add_argument("--check_dicom_parse", action='store_true')

if __name__=="__main__": main()
