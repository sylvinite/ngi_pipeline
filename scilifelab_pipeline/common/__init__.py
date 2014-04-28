#!/usr/bin/env python
"""
This script organizes demultiplexed (CASAVA 1.8) sequencing data into the relevant
project/sample/flowcell directory structure.
"""

from __future__ import print_function

import argparse
import fnmatch
import glob
import importlib
import os
import subprocess
import unittest
import yaml

## TODO migrate this out of bcbio-specific code
from scilifelab.bcbio.qc import FlowcellRunMetricsParser

from scilifelab.utils.config import load_yaml_config_expand_vars
from scilifelab.log import minimal_logger
# Set up logging for this script
LOG = minimal_logger(__name__)


def main(config_file_path, demux_fcid_dirs=None, restrict_to_projects=None, restrict_to_samples=None):
    """
    The main launcher method.

    :param str config_file_path: The path to the configuration file.
    :param str demux_fcid_dirs: The CASAVA-produced demux directory(s). Optional.
    :param list restrict_to_projects: A list of projects; analysis will be restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be restricted to these. Optional.
    """
    if not restrict_to_projects:
        restrict_to_projects = []
    if not restrict_to_samples:
        restrict_to_samples = []
    demux_fcid_dirs_set = set()
    dirs_to_analyze = set()
    config = load_yaml_config_expand_vars(config_file_path)
    if demux_fcid_dirs:
        demux_fcid_dirs_set.update(demux_fcid_dirs)
    else:
        # Check for newly-delivered data in the INBOX
        inbox_directory = config.get('INBOX')
        demux_fcid_dirs_set.update(check_for_new_flowcells(inbox_directory))
    for demux_fcid_dir in demux_fcid_dirs_set:
        dirs_to_analyze.update(setup_analysis_directory_structure(demux_fcid_dir,
                                                                  config_file_path,
                                                                  restrict_to_projects,
                                                                  restrict_to_samples))
    if not dirs_to_analyze:
        LOG.info("No directories found to process.")

    # The configuration file decides which pipeline class we use
    analysis_pipeline_module_name = config.get("analysis", {}).get("analysis_pipeline")
    if not analysis_pipeline_module_name:
        ## TODO Should we have a default?
        LOG.warn("Warning: No analysis pipeline specified in configuration file. "\
                 "Falling back to bcbio-nextgen.")
        ## TODO implement scilifelab_pipeline.piper_sll
        analysis_pipeline_module_name = "scilifelab_pipeline.bcbio_sll"
    #AnalysisPipelineClass = get_class(analysis_pipeline)
    analysis_module = importlib.import_module(analysis_pipeline_module_name)
    launch_method = config.get("analysis", {}).get("analysis_launch_method") or "localhost"
    for sample_directory in dirs_to_analyze:
        ## TODO this is not what is returned -- it's a dict! See ll.302-305
        for sample_to_process in analysis_module.build_run_configs(samples_dir=sample_directory,
                                                            config_path=config_file_path):
            analysis_module.launch_pipeline(sample_to_process['run_config'],
                                            sample_to_process['work_dir'],
                                            launch_method)
        #analysis_instance = AnalysisPipelineClass(sample_directory)
        #analysis_instance.build_config_file()
        #analysis_instance.launch_pipeline(launch_method)


def get_class( kls ):
    """http://stackoverflow.com/questions/452969/does-python-have-an-equivalent-to-java-class-forname"""
    parts = kls.split('.')
    module = ".".join(parts[:-1])
    m = __import__( module )
    for comp in parts[1:]:
        m = getattr(m, comp)            
    return m


## SO! How to do this?
# Easy to check for a flowcell that is finished transferring over: check for second_read_finished or whatever.
# But how do we check if a flowcell has already begun processing?
# We don't want to rebuild the config files and requeue the job if it's already in process.
# We could touch a file that says, "processing begun" that contains the PID.
# When finished, we could touch a file that says processing_complete with the PID.
# But what if a project has been processed in one way (e.g. qc_pipeline) but needs more processing
# (e.g. alignment or best-practice analysis).
def check_for_new_flowcells(inbox_directory, num_days_ago=None):
    """Checks for newly-delivered data in the inbox_directory,
    ensuring somehow that the data transfer has finished.

    :param str inbox_directory: The path to the directory to which new data is transferred after demultiplexing.
    :param str num_days_ago: If a folder has not been modified more recently than this it is excluded. Default is no time limit.

    :returns: A list of newly-delivered, demultiplexed flowcell directories.
    :rtype: list
    """
    if not inbox_directory:
        return []
    else:
        new_flowcell_directories = set()
        # Not sure what this will be because I don't know what Uppsala uses
        project_dir_match_patterns = ("*",)
        # Omit hidden directories
        project_dir_filter_patterns = (".*",)
        for directory in os.listdir(inbox_directory):
            # This gets a little rough up ahead but I can't stand to nest this many if-fors
            if os.path.isdir(directory) and \
                any([ fnmatch.fnmatch(directory, ptn) for ptn in project_dir_match_patterns]) and \
                not any([ fnmatch.fnmatch(directory, ptn) for ptn in project_dir_filter_patterns]):
                    # Is there an age limit specified?
                    if num_days_ago:
                        file_age = datetime.datetime.now() - \
                                   datetime.datetime.fromtimestamp(
                                                        os.path.getmtime(directory))
                        if file_age.days > num_days_ago:
                            # Directory is too old for consideration
                            continue
                    new_flowcell_directories.add(directory)
    return list(new_flowcell_directories)


def setup_analysis_directory_structure(fc_dir, config_file_path, restrict_to_projects=None,
                                       restrict_to_samples=None):
    """
    Copy and sort files from their CASAVA-demultiplexed flowcell structure
    into their respective project, sample, FCIDs. This collects samples
    split across multiple flowcells.

    :param str fc_dir: The directory created by CASAVA for this flowcell.
    :param str config_file_path: The location of the configuration file.
    :param list restrict_to_projects: Specific projects within the flowcell to process exclusively
    :param list restrict_to_samples: Specific projects within the flowcell to process exclusively

    :returns: A list of sample directories that need to be run through the analysis pipeline
    :rtype: list
    """
    LOG.info("Setting up analysis for demultiplexed data in folder \"{}\"".format(fc_dir))
    # Load config, expanding shell variables in paths
    config = load_yaml_config_expand_vars(config_file_path)
    analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
    if not os.path.exists(fc_dir):
        LOG.error("Error: Flowcell directory {} does not exist".format(fc_dir))
        return []
    if not os.path.exists(analysis_top_dir):
        LOG.error("Error: Analysis top directory {} does not exist".format(analysis_top_dir))
        return []
    # Map the directory structure for this flowcell
    try:
        fc_dir_structure = parse_casava_directory(fc_dir)
    except RuntimeError as e:
        LOG.error("Error when processing flowcell dir \"{}\": e".format(fc_dir))
        return []

    # Parse the flowcell dir
    fc_dir_structure = parse_casava_directory(fc_dir)
    fc_date, fc_name = [fc_dir_structure['fc_date'],fc_dir_structure['fc_name']]
    fc_run_id = "{}_{}".format(fc_date,fc_name)

    # Copy the basecall stats directory.
    ## TODO I don't know what these extra two lines of comments refer to
    #       This will be causing an issue when multiple directories are present...
    # syncing should be done from archive, preserving the Unaligned* structures
    _copy_basecall_stats([os.path.join(fc_dir_structure['fc_dir'], d) for d in
                                        fc_dir_structure['basecall_stats_dir']],
                                        analysis_top_dir)

    # Iterate over the projects in the flowcell directory
    sample_directories = []
    if not fc_dir_structure.get('projects'):
        LOG.warn("No projects found in specified flowcell directory \"{}\"".format(fc_dir))
    # for-else
    for project in fc_dir_structure.get('projects', []):
        # If specific projects are specified, skip those that do not match
        project_name = project['project_name']
        if len(restrict_to_projects) > 0 and project_name not in restrict_to_projects:
            LOG.debug("Skipping project {}".format(project_name))
            continue
        LOG.info("Setting up project {}".format(project.get("project_dir")))
        # Create a project directory if it doesn't already exist
        project_dir = os.path.join(analysis_top_dir, project_name)
        if not os.path.exists(project_dir):
            ## TODO change to mkdir -p
            os.mkdir(project_dir, 0770)
        # Iterate over the samples in the project
        for sample in project.get('samples', []):
            # If specific samples are specified, skip those that do not match
            ## this appears to be some scilifelab-specific naming process?
            sample_name = sample['sample_name'].replace('__','.')
            if len(restrict_to_samples) > 0 and sample_name not in restrict_to_samples:
                LOG.debug("Skipping sample {}".format(sample_name))
                continue
            LOG.info("Setting up sample {}".format(sample.get("sample_dir")))
            # Create a directory for the sample if it doesn't already exist
            sample_dir = os.path.join(project_dir, sample_name)
            if not os.path.exists(sample_dir):
                ## TODO change to mkdir -p
                os.mkdir(sample_dir, 0770)
            # Create a directory for the flowcell if it does not exist
            dst_sample_fcid_dir = os.path.join(sample_dir, fc_run_id)
            if not os.path.exists(dst_sample_fcid_dir):
                ## TODO change to mkdir -p
                os.mkdir(dst_sample_fcid_dir, 0770)
            # rsync the source files to the sample directory
            src_sample_dir = os.path.join(fc_dir_structure['fc_dir'],
                                          project['data_dir'],
                                          project['project_dir'],
                                          sample['sample_dir'])
            #LOG.info("Copying sample files from \"{}\" to \"{}\"...".format(
            #                                src_sample_dir, dst_sample_fcid_dir))
            sample_files = do_rsync([os.path.join(src_sample_dir,f) for f in
                                    sample.get('files',[])],dst_sample_fcid_dir)
            sample_directories.append(dst_sample_fcid_dir)
    else:
        # touch the file that shows that we've processed this flowcell so that check_for_new_flowcells will know we've finished this one
        pass

    return sample_directories


def parse_casava_directory(fc_dir):
    """
    Traverse a CASAVA-1.8-generated directory structure and return a dictionary
    of the elements it contains.
    The flowcell directory tree has (roughly) the structure:

    |-- Data
    |   |-- Intensities
    |       |-- BaseCalls
    |-- InterOp
    |-- Unaligned
    |   |-- Basecall_Stats_C2PUYACXX
    |-- Unaligned_16bp
        |-- Basecall_Stats_C2PUYACXX
        |   |-- css
        |   |-- Matrix
        |   |-- Phasing
        |   |-- Plots
        |   |-- SignalMeans
        |   |-- Temp
        |-- Project_J__Bjorkegren_13_02
        |   |-- Sample_P680_356F_dual56
        |   |   |-- <fastq files are here>
        |   |   |-- <SampleSheet.csv is here>
        |   |-- Sample_P680_360F_dual60
        |   |   ...
        |-- Undetermined_indices
            |-- Sample_lane1
            |   ...
            |-- Sample_lane8

    :param str fc_dir: The directory created by CASAVA for this flowcell.

    :returns: A dictionary of the flowcell directory tree.
    :rtype: dict

    :raises RuntimeError: If the fc_dir does not exist or cannot be accessed,
                          or if Flowcell RunMetrics could not be parsed properly.
    """
    projects = []
    fc_dir = os.path.abspath(fc_dir)
    parser = FlowcellRunMetricsParser(fc_dir)
    run_info = parser.parseRunInfo()
    runparams = parser.parseRunParameters()

    ## TODO how important is it to have this information? Should it cause processing to fail or just toss a warning?
    fc_name = run_info.get('Flowcell', None)
    fc_date = run_info.get('Date', None)
    fc_pos = runparams.get('FCPosition','')
    #try:
    #    fc_name = run_info['Flowcell']
    #    fc_date = run_info['Date']
    #    fc_pos = runparams['FCPosition']
    #except KeyError:
    #    raise RuntimeError("Could not parse flowcell information \"{}\" "\
    #                   "from Flowcell RunMetrics in flowcell {}".format(e, fc_dir))

    # "Unaligned*" because SciLifeLab dirs are called "Unaligned_Xbp"
    # (where "X" is the index length) and there is also an "Unaligned" folder
    unaligned_dir_pattern = os.path.join(fc_dir,"Unaligned*")
    basecall_stats_dir_pattern = os.path.join(unaligned_dir_pattern,"Basecall_Stats_*")
    basecall_stats_dir = [os.path.relpath(d,fc_dir) for d in glob.glob(basecall_stats_dir_pattern)]
    # e.g. 131030_SN7001362_0103_BC2PUYACXX/Unaligned_16bp/Project_J__Bjorkegren_13_02/
    project_dir_pattern = os.path.join(unaligned_dir_pattern,"Project_*")
    for project_dir in glob.glob(project_dir_pattern):
        project_samples = []
        sample_dir_pattern = os.path.join(project_dir,"Sample_*")
        # e.g. <Project_dir>/Sample_P680_356F_dual56/
        for sample_dir in glob.glob(sample_dir_pattern):
            fastq_file_pattern = os.path.join(sample_dir,"*.fastq.gz")
            samplesheet_pattern = os.path.join(sample_dir,"*.csv")
            fastq_files = [os.path.basename(file) for file in glob.glob(fastq_file_pattern)]
            samplesheet = glob.glob(samplesheet_pattern)
            assert len(samplesheet) == 1, \
                    "Error: could not unambiguously locate samplesheet in %s" % sample_dir
            sample_name = os.path.basename(sample_dir).replace("Sample_","").replace('__','.')
            project_samples.append({'sample_dir': os.path.basename(sample_dir),
                                    'sample_name': sample_name,
                                    'files': fastq_files,
                                    'samplesheet': os.path.basename(samplesheet[0])})
        project_name = os.path.basename(project_dir).replace("Project_","").replace('__','.')
        projects.append({'data_dir': os.path.relpath(os.path.dirname(project_dir),fc_dir),
                         'project_dir': os.path.basename(project_dir),
                         'project_name': project_name,
                         'samples': project_samples})
    return {'fc_dir': fc_dir,
            'fc_name': '{}{}'.format(fc_pos,fc_name),
            'fc_date': fc_date,
            'basecall_stats_dir': basecall_stats_dir,
            'projects': projects}


## is this used?
def copy_undetermined_index_files(casava_data_dir, destination_dir):
    """
    Copy fastq files with "Undetermined" index reads to the destination directory.

    :param str casava_data_dir: The directory containing 
    :param str destination_dir:
    """
    # List of files to copy
    copy_list = []
    # List the directories containing the fastq files
    fastq_dir_pattern = os.path.join(casava_data_dir,"Undetermined_indices","Sample_lane*")
    # Pattern matching the fastq_files
    fastq_file_pattern = "*.fastq.gz"
    # Samplesheet name
    samplesheet_pattern = "SampleSheet.csv"
    samplesheets = []
    for dir in glob.glob(fastq_dir_pattern):
        copy_list += glob.glob(os.path.join(dir,fastq_file_pattern))
        samplesheet = os.path.join(dir,samplesheet_pattern)
        if os.path.exists(samplesheet):
            samplesheets.append(samplesheet)
    # Merge the samplesheets into one
    new_samplesheet = os.path.join(destination_dir,samplesheet_pattern)
    new_samplesheet = _merge_samplesheets(samplesheets,new_samplesheet)
    # Rsync the fastq files to the destination directory
    do_rsync(copy_list,destination_dir)

def _merge_samplesheets(samplesheets, merged_samplesheet):
    """
    Merge multiple Illumina SampleSheet.csv files into one.

    :param list samplesheets: A list of the paths to the SampleSheet.csv files to merge.
    :param str merge_samplesheet: The path <...>

    :returns: <...>
    :rtype: str
    """
    data = []
    header = []
    for samplesheet in samplesheets:
        with open(samplesheet) as fh:
            csvread = csv.DictReader(fh, dialect='excel')
            header = csvread.fieldnames
            for row in csvread:
                data.append(row)
    with open(merged_samplesheet, "w") as outh:
        csvwrite = csv.DictWriter(outh, header)
        csvwrite.writeheader()
        csvwrite.writerows(sorted(data, key=lambda d: (d['Lane'],d['Index'])))
    return merged_samplesheet


def _copy_basecall_stats(source_dirs, destination_dir):
    """Copy relevant files from the Basecall_Stats_FCID directory
       to the analysis directory
    """
    for source_dir in source_dirs:
        # First create the directory in the destination
        dirname = os.path.join(destination_dir,os.path.basename(source_dir))
        try:
            os.mkdir(dirname)
        except:
            pass
        # List the files/directories to copy
        files = glob.glob(os.path.join(source_dir,"*.htm"))
        files += glob.glob(os.path.join(source_dir,"*.metrics"))
        files += glob.glob(os.path.join(source_dir,"*.xml"))
        files += glob.glob(os.path.join(source_dir,"*.xsl"))
        for dir in ["Plots","css"]:
            d = os.path.join(source_dir,dir)
            if os.path.exists(d):
                files += [d]
        do_rsync(files,dirname)


##  this could also work remotely of course
def do_rsync(src_files, dst_dir):
    ## TODO check parameters here
    cl = ["rsync","-car"]
    cl.extend(src_files)
    cl.append(dst_dir)
    cl = map(str, cl)
    # For testing, just touch the files rather than copy them
    # for f in src_files:
    #    open(os.path.join(dst_dir,os.path.basename(f)),"w").close()
    subprocess.check_call(cl)
    return [ os.path.join(dst_dir,os.path.basename(f)) for f in src_files ]


#def load_config(config_file_path):
#    """Load YAML config file, replacing environmental variables.
#
#    :param str config_file_path: The path to the (yaml-formatted) configuration file to be parsed.
#
#    :returns: A dict of the configuration file with shell variables expanded.
#    :rtype: dict
#    """
#    with open(config_file_path) as in_handle:
#        config = yaml.load(in_handle)
#    config = _expand_paths(config)
#    return config
#
#def _expand_paths(config):
#    for field, setting in config.items():
#        if isinstance(config[field], dict):
#            config[field] = _expand_paths(config[field])
#        else:
#            config[field] = expand_path(setting)
#    return config
#
#def expand_path(path):
#    """ Combines os.path.expandvars with replacing ~ with $HOME.
#    """
#    try:
#        return os.path.expandvars(path.replace("~", "$HOME"))
#    except AttributeError:
#        return path
#

class OrganizeCopyTests(unittest.TestCase):
    """
    """

## a cron job will run this periodically, passing only the config file;
## the script will check for newly-delivered flowcells and process them
if __name__=="__main__":
    parser = argparse.ArgumentParser("Sort and transfer a demultiplxed illumina run.")
    parser.add_argument("--config", required=True,
            help="The path to the configuration file.")
    parser.add_argument("--project", action="append",
            help="Restrict processing to these projects. "\
                 "Use flag multiple times for multiple projects.")
    parser.add_argument("--sample", action="append",
            help="Restrict processing to these samples. "\
                 "Use flag multiple times for multiple projects.")
    parser.add_argument("demux_fcid_dir", nargs='*', action="store",
            help="The path to the Illumina demultiplexed fc directories to process. "\
                 "If not specified, new data will be checked for in the "\
                 "\"INBOX\" directory specifiedin the configuration file.")

    args_ns = parser.parse_args()
    main(config_file_path=args_ns.config,
         demux_fcid_dirs=args_ns.demux_fcid_dir,
         restrict_to_projects=args_ns.project,
         restrict_to_samples=args_ns.sample)

