import os
import glob
import re
import sys
import argparse
import random



def main(args):
    #create fake project name
    (project_name, project_id) = create_proj_name("T.Irma")
    #now iterate over all FC I need to create
    for i in xrange(args.FC):
        #create fake FC name
        FC_name = create_fake_FC()
        #create fake sample names



instruments = ["ST-E00201", "ST-E00202", "ST-E00203", "ST-E00204", "ST-E00205"] # five instruments


def create_proj_name(header=None):
    if header == None:
        header = "F.Vezzi"
    year = ''.join(str(random.randint(0,9)) for i in xrange(2))
    pnum = ''.join(str(random.randint(0,9)) for i in xrange(2))
    return "{}_{}_{}".format(header, year, pnum)


def create_fake_FC():
    # create something like 160217_ST-E00201_0063_AHJHNYCCXX
    new = False
    while (not new):
        # date
        date = ''.join(str(random.randint(0,9)) for i in xrange(6))
        # instrument name
        instrument = instruments[andom.randint(0,4)]
        # run id
        run_id = ''.join(str(random.randint(0,9)) for i in xrange(4))
        # FC position
        FC_pos = random.choice(["A", "B"])
        # FC name
        FC_name = ''.join(str(random.choice( ["A","B","C","E","F","G","H","I","L","M","N","O","P","Q","R","S","T","U","V","Z"])) for i in xrange(9))
        # now compose the name
        run_name = "{}_{}_{}{}".format(date, instrument, run_id, FC_pos, FC_name)
        # check if this FC exists already
        if not os.path.exists(rnu_name):
            # this FC does not exists, fine
            new = True
    return run_name



if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This script generates a test suite for NGI-pipeline and piper testing. Needs as input two fastq files (read 1 and read 2) and creates as many FC as specified """)
    parser.add_argument('--fastq1', help="path to fastq file containing read 1", type=str,  required=True)
    parser.add_argument('--fastq2', help="path to fastq file containing read 2", type=str,  required=True)
    parser.add_argument('--FC', help="number of FC to be created", type=int,  required=True)
    
    args = parser.parse_args()
    main(args)