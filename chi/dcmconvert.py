from chi.util import DFBatchParRun, EntryPoints
import pandas
import pydicom

from chi import dicom, dcmscanner

entry = EntryPoints()
def main():
    entry.main()

def read_tag(s):
    try:
        tg = dicom.Tag.from_pydicom_attr(s)
    except ValueError:
        tg = dicom.Tag.from_tag_string(s)

    return tg


# TODO Add support for multiple sub series tag specification (ie take files from Ac num X AND orientation Y)
class ConvertBatchParRun(DFBatchParRun):
    def __init__(self, convert_func, input_root, output_root, output_tag):
        self.convert_func = convert_func
        self.input_root = input_root
        self.output_root = output_root
        self.output_tag = output_tag

    def iteration_count(self, iter_info, dcm):
        return super().iteration_count(iter_info)

    def iterate(self, start, stop, iter_info, dcm):
        for ix, row in super().iterate(start, stop, iter_info):
            series = row['SeriesInstanceUID']
            SERIES = dicom.Tag.from_pydicom_attr("SeriesInstanceUID").keyword()
            full = row['FullSeries']
            if not full:
                subseriestag = row['SubSeriesTag']
                subseries = row['SubSeriesTagValue']
                # Here we are assuming this is read with dcmscanner
                SUBSERIES = read_tag(subseriestag).keyword()
                #SUBSERIES = dicom.Tag.from_tag_string(subseriestag).tag_string()

            def tryint(x):
                try:
                    return int(x)
                except ValueError:
                    return None

            dcm_rows = (dcm[SERIES] == series) 
            if not full:
                dcm_rows &= (dcm[SUBSERIES].map(lambda s: str(tryint(s)))==str(int(subseries)))
            
            yield ix, row, dcm.loc[dcm_rows]

    def execute_one(self, arg):
        ix, row, dcm_loc = arg
        result = self.convert_func(self.input_root, self.output_root, self.output_tag, ix, row, dcm_loc)
        return self.single(result)


import tempfile
def get_tempdir():
    td = os.getenv("TMPDISK", None) # TODO Make this less CAC specific
    if td is None:
        td = tempfile.mkdtemp()
    return td
    

@entry.point
def convert(args):
    runner = ConvertBatchParRun(convert_impl, args.dicom_root, args.output_root, args.output_column)
    dcm = pandas.read_csv(args.dicom_index, index_col=0)
    convs = pandas.read_csv(args.conversions)
    iter_info = runner.iter_info(convs)
    results = runner.run_from_args(args, iter_args=(iter_info, dcm))
    if args.output_file is not None:
        results.to_csv(args.output_file, index=False)

def convert_impl(input_root, output_root, target_tag, ix, row, dcm):
    out_filename = row[target_tag]
    name, ext = os.path.splitext(out_filename)
    if ext == ".gz":
        name, ext0 = os.path.splitext(name)
        ext = ext0 + ext

    assert ext != ""
    tmp_root = get_tempdir()
    tmp_folder = os.path.join(tmp_root, name)
    os.makedirs(tmp_folder, exist_ok=True)
    out_files = extract_selected_dicoms(dcm, input_root, tmp_folder)

    output_file = os.path.join(output_root, out_filename)
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    # TODO It would be  better to use our scan results from dcmscanner, but there is a keyword vs tag name issue!
    loader = dicom.SeriesLoadResult.from_files(out_files)
    assert not loader.has_subseries()
    img = loader.load_series()
    sitk.WriteImage(img, output_file)

    orow = row.copy()
    return orow


@convert.parser
def convert_parser(parser):
    ConvertBatchParRun.update_parser(parser)
    # Anything else
    parser.add_argument("--dicom_root", required=True)
    parser.add_argument("--dicom_index", required=True)
    parser.add_argument("--conversions", required=True)
    # TODO Do we need an index column info?
    parser.add_argument("--output_root", required=True)
    parser.add_argument("--output_column", required=True) # Column specifying output name in conversions
    parser.add_argument("--output_file", required=False)

import shutil
import os
import zipfile
import SimpleITK as sitk

def extract_selected_dicoms(dcm, input_root, output_folder):
    files = list(dcm.index)
    if ('ZipFile' in dcm.columns) and ('ArcName' in dcm.columns):
        zip_mode = True
    else:
        zip_mode = False


    # Delete the output files if they exist
    # TODO Support for zip archives.
    if os.listdir(output_folder):
        for n in os.listdir(output_folder):
            os.unlink(os.path.join(output_folder, n))
    # TODO this really ought to be grouped by zipfile and the file opened once, no? 
    out_files = []
    if zip_mode:
        for zf, tab in dcm.groupby("ZipFile"):
            in_zip = os.path.join(input_root, zf)
            with zipfile.ZipFile(in_zip, "r") as zf:
                for f, dcmrow in tab.iterrows():
                    dcmname = os.path.basename(f)
                    output_file = os.path.join(output_folder, dcmname)
                    name = dcmrow['ArcName']
                    with zf.open(name) as f, open(output_file, "wb") as of:
                        shutil.copyfileobj(f, of)
                    out_files.append(output_file)
    else:
        for f, dcmrow in dcm.iterrows():
            inpath = os.path.join(input_root, f)
            dcmname = os.path.basename(f)
            output_file = os.path.join(output_folder, dcmname)
            shutil.copy(inpath, output_file)
            out_files.append(output_file)

    return out_files


def filter_impl(input_root, output_root, target_tag, ix, row, dcm):
    output_folder = os.path.join(output_root, row[target_tag])
    os.makedirs(output_folder, exist_ok=True)

    out_files = extract_selected_dicoms(dcm, input_root, output_folder)

    orow = row.copy()
    orow['FileCount'] = len(out_files)
    return orow

@entry.point
def filter(args):
    runner = ConvertBatchParRun(filter_impl, args.dicom_root, args.output_root, args.output_column)
    dcm = pandas.read_csv(args.dicom_index, index_col=0)
    convs = pandas.read_csv(args.conversions)
    iter_info = runner.iter_info(convs)
    results = runner.run_from_args(args, iter_args=(iter_info, dcm))
    if args.output_file is not None:
        results.to_csv(args.output_file, index=False)




filter.parser(convert_parser)

if __name__=="__main__": main()

