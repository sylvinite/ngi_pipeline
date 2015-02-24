from __future__ import print_function

import argparse
import bz2
import glob
import gzip
import os
import re
import sys

def main(input_dir_list=None, input_file_list=None):
    if not input_dir_list: input_dir_list = []
    if not input_file_list: input_file_list = []
    sthlm_format_pattern = re.compile(r'(?P<lane>\d)_(?P<date>\d{6})_(?P<flowcell>([^_]\w)+)_(?P<sample_id>P\d{3,4}_\d{3})_(?P<read_num>[12]).fastq(?P<ext>.(gzip|gz|bz2|bzip2)?)$')
    for input_dir in input_dir_list:
        input_file_list.extend(filter(os.path.isfile, glob.glob(os.path.join(input_dir, "*"))))
    for input_file in input_file_list:
        base_path, input_file_name = os.path.split(input_file)
        try:
            m = sthlm_format_pattern.match(input_file_name).groupdict()
        except AttributeError:
            # No match
            continue
        # Get the index from the header
        if m.get("ext") in (".gz", ".gzip"):
            f = gzip.open(input_file, 'r')
        elif m.get("ext") in (".bz2", ".bzip2"):
            f = bz2.BZ2File(input_file, 'r')
        else:
            f = open(input_file, 'r')
        m["index"] = f.readline().strip().split(":")[9]
        f.close()
        illumina_name = "{sample_id}_{index}_L00{lane}_R{read_num}_001.fastq{ext}".format(**m)
        symlink_path = os.path.join(base_path, illumina_name)
        print('Symlinking "{}" to "{}"'.format(input_file, symlink_path), file=sys.stderr)
        os.symlink(input_file, symlink_path)

if __name__=="__main__":
    parser = argparse.ArgumentParser("Convert our previous custom Sthlm naming format to the standard Illumina one.")
    parser.add_argument("-d", "--input_dir", dest="input_dir_list", action="append",
            help="The input directory to search for files to change. Use multiple times for multiple directories.")
    parser.add_argument("-f", "--input-file", dest="input_file_list", action="append",
            help="The input file to change. Use multiple times for multiple files.")
    args = vars(parser.parse_args())
    main(**args)
