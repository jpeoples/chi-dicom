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

            dcm_rows = (dcm[SERIES] == series) 
            if not full:
                dcm_rows &= (dcm[SUBSERIES]==subseries)
            
            yield ix, row, dcm.loc[dcm_rows]

    def execute_one(self, arg):
        ix, row, dcm_loc = arg
        result = self.convert_func(self.input_root, self.output_root, self.output_tag, ix, row, dcm_loc)
        return self.single(result)




@entry.point
def convert(args):
    return None

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



def filter_impl(input_root, output_root, target_tag, ix, row, dcm):
    files = list(dcm.index)
    if ('ZipFile' in dcm.columns) and ('ArcName' in dcm.columns):
        zip_mode = True
    else:
        zip_mode = False

    output_folder = os.path.join(output_root, row[target_tag])
    os.makedirs(output_folder, exist_ok=True)
    # Delete the output files if they exist
    # TODO Support for zip archives.
    if os.listdir(output_folder):
        for n in os.listdir(output_folder):
            os.unlink(os.path.join(output_folder, n))
    for f, dcmrow in dcm.iterrows():
        if zip_mode:
            in_zip = os.path.join(input_root, dcmrow['ZipFile'])
            dcmname = os.path.basename(f)
            output_file = os.path.join(output_folder, dcmname)
            name = dcmrow['ArcName']
            with zipfile.ZipFile(in_zip, 'r') as zf:
                with zf.open(name) as f, open(output_file, 'wb') as of:
                    shutil.copyfileobj(f, of)

        else:
            inpath = os.path.join(input_root, f)
            dcmname = os.path.basename(f)
            output_file = os.path.join(output_folder, dcmname)
            shutil.copy(inpath, output_file)

    orow = row.copy()
    orow['FileCount'] = len(files)
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

