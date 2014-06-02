"""The Piper automated launcher script.

For each project directory, this script needs to:
    1. Build a report.tsv file detailing all the files
    2. Build a runconfig.xml file using the automated 
    3. Poossssibly build a new sbatch file for the samples
    4. Launch the Piper job via the sbatch file
"""
from __future__ import print_function

import collections
import os
import re
import shutil
import time

from scilifelab.log import minimal_logger
from scilifelab.utils.config import load_yaml_config_expand_vars
from scilifelab_pipeline.common import parse_lane_from_filename, \
                                       find_fastq_read_pairs, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)

def main(projects_to_analyze, config_file_path):
    """The main method.

    :param list flowcell_dirs_to_analyze: A lst of flowcell directories containing fastq files to analyze.
    :param str config_file_path: The path to the configuration file.
    """
    config = load_yaml_config_expand_vars(config_file_path)
    create_report_tsv(projects_to_analyze)

    # Fetch requisite info for automatic config builder
    # Run Johan's converter script if needed
    # sbatch relevant workflow
    # Decide how to track jobs that are running -- write to database?


def create_report_tsv(projects_to_analyze):
    """Generate a tsv-formatted file as input for Piper and write to top level of project,
    unless a report.xml file exists already (as it will for Uppsala projects).

    This file has the format:

        #SampleName     Lane    ReadLibrary     FlowcellID
        P567_102        1       WGS     AH0JYUADXX
        P567_102        2       WGS     AH0JYUADXX

    :param list projects_to_analyze: The list of flowcell directories
    :returns: The path to the report.tsv file
    :rtype: str
    """
    report_header = ("#SampleName", "Lane", "ReadLibrary", "FlowcellID")

    for project in projects_to_analyze:
        report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
        report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
        if os.path.exists(report_xml_path): continue

        ## TODO Activate this check/move thing later
        #if os.path.exists(report_tsv_path):
        #    path, orig_filename = os.path.split(report_tsv_path)
        #    orig_basename, orig_ext = os.path.splitext(orig_filename)
        #    mv_filename = orig_basename + time.strftime("_%Y-%m-%d_%H:%M:%S") + orig_ext
        #    mv_path = os.path.join(path, mv_filename)
        #    LOG.info("Moving preexisting report.tsv file to {}".format(mv_path))
        #    shutil.move(report_tsv_path, mv_path)
        with open(report_tsv_path, 'w') as rtsv_fh:
            LOG.info("Writing {}".format(report_tsv_path))
            print("\t".join(report_header), file=rtsv_fh)
            for sample in project:
                for fcid in sample:
                    # How to determine lanes, readlibraries?
                    fcid_path = os.path.join(project.base_path,
                                             project.dirname,
                                             sample.dirname,
                                             fcid.dirname)
                    for fq_pairname in find_fastq_read_pairs(directory=fcid_path).keys():
                        lane = parse_lane_from_filename(fq_pairname)
                        read_library = "Get this from the database somehow"
                        print("\t".join([sample.name, lane, read_library, fcid.name]), file=rtsv_fh)
