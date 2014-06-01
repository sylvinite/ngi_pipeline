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

from scilifelab.utils.config import load_yaml_config_expand_vars
from scilifelab_pipeline.common import parse_project_sample_lane_from_filename, \
                                       find_fastq_read_pairs, get_flowcell_id_from_dirtree

def main(projects_to_analyze, config_file_path):
    """The main method.

    :param list flowcell_dirs_to_analyze: A lst of flowcell directories containing fastq files to analyze.
    :param str config_file_path: The path to the configuration file.
    """
    config = load_yaml_config_expand_vars(config_file_path)
    create_report_tsv(projects_to_analyze)


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
    report_header = "#SampleName", "Lane", "ReadLibrary", "FlowcellID"

    for project in projects_to_analyze:
        report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
        report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
        if os.path.exists(report_xml_path): continue

        with open("report_tsv_path", 'w') as rtsv_fh:
            print(report_header, file=rtsv_fh)
            for sample in project.samples:
                for fcid in sample.fcids:
                    # How to determine lanes, readlibraries?
                    fcid_path = os.path.join(project.base_path,
                                             project.dirname,
                                             sample.dirname,
                                             fcid.dirname)
                    for fq_pairname, fq_pair in find_fastq_read_pairs(directory=fcid_path):
                        lane = lane_from_filename(fq_pairname)
                        read_library = "Get this from the database somehow"
                        print("\t".join([sample.name, lane, read_ilbrary, fcid.name]), file=rtsv_fh)

    #for flowcell_dir in flowcell_dirs_to_analyze:
    #    flowcell = get_flowcell_id_from_dirtree(flowcell_dir)
    #    # Get file pairs
    #    import ipdb; ipdb.set_trace()
    #    file_dict = find_fastq_read_pairs(directory=flowcell_dir)
    #    for fq_file in file_dict.keys():
    #        try:
    #            import ipdb; ipdb.set_trace()
    #            project, sample, lane = parse_project_sample_lane_from_filename(fq_file)
    #            ## TODO Need to figure out how to find the ReadLibrary -- I guess this is probably
    #            ##      The "ReadLibrary" is the sequencing library that was sequenced (as you may know more than one library can be prepared from a sample)
    #            ##      this allows us to track the sample back to the libraries sequenced and include it in the read group information in the bam files.
    #            samples.append("\t".join(["_".join(project, sample), lane, "Check Back Later", flowcell]))
    #        except ValueError as e:
    #            LOG.warn(e)
    #project_root_directory = os.pardir(os.pardir(flowcell_dir))
    #file_location = os.path.join(project_root_directory, "report.tsv")
    #with open(file_location, 'a+') as f:
    #    ## TODO Need to add newlines
    #    f.writelines(samples)
