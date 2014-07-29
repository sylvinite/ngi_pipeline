#!/usr/bin/env python
"""
This module organizes demultiplexed (CASAVA 1.8) sequencing data into the relevant
project/sample/flowcell directory structure.
"""

from __future__ import print_function

import glob
import importlib
import os
import re
import sys

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database import get_project_id_from_name
from ngi_pipeline.database.session import get_charon_session_for_project
from ngi_pipeline.database.process_tracking import get_all_tracked_processes, \
                                                   record_workflow_process_local, \
                                                   write_status_to_charon, \
                                                   check_if_flowcell_analysis_are_running, \
                                                   record_workflow_process_run_local
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.filesystem import do_rsync, safe_makedir
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.utils.parsers import FlowcellRunMetricsParser, \
                                       determine_library_prep_from_fcid

LOG = minimal_logger(__name__)

# This is called via Celery when a new flowcell is delivered from Sthlm or Uppsala, one flowcell per time
def process_demultiplexed_flowcell(demux_fcid_dirs, restrict_to_projects=None, restrict_to_samples=None, config_file_path=None):
    if len(demux_fcid_dirs) > 1:
        error_message = ("Only one flowcell can be specified at this point"
                             "The following flowcells have been specified: {} ".format(",".join(demux_fcid_dirs))) ## better to use set
        LOG.info(error_message)
        sys.exit("Quitting: " + error_message)

    process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects, restrict_to_samples, config_file_path)



def process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects=None, restrict_to_samples=None, config_file_path=None):
    """Sort demultiplexed Illumina flowcells into projects and launch their analysis.

    :param list demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param str config_file_path: The path to the configuration file; can also be
                                 specified via environmental variable "NGI_CONFIG"
    """
    if not config_file_path: config_file_path = locate_ngi_config()
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    config = load_yaml_config(config_file_path)

    # Sort/copy each raw demux FC into project/sample/fcid format -- "analysis-ready"
    projects_to_analyze = dict()
    for demux_fcid_dir in demux_fcid_dirs_set:
        # These will be a bunch of Project objects each containing Samples, FCIDs, lists of fastq files
        projects_to_analyze = setup_analysis_directory_structure(demux_fcid_dir,
                                                                 config,
                                                                 projects_to_analyze,
                                                                 restrict_to_projects,
                                                                 restrict_to_samples)
    if not projects_to_analyze:
        if restrict_to_projects:
            error_message = ("No projects found to process; the specified flowcells "
                             "({fcid_dirs}) do not contain the specified project(s) "
                             "({restrict_to_projects}) or there was an error "
                             "gathering required information.").format(
                                    fcid_dirs = ",".join(demux_fcid_dirs_set),
                                    restrict_to_projects = ",".join(restrict_to_projects))
        else:
            error_message = ("No projects found to process in flowcells {}"
                             "or there was an error gathering required "
                             "information.".format(",".join(demux_fcid_dirs_set)))
        LOG.info(error_message)
        sys.exit("Quitting: " + error_message)
    else:
        # Don't need the dict functionality anymore; revert to list
        projects_to_analyze = projects_to_analyze.values()
    
    ##project to analyse contained only in the current flowcell(s), I am ready to analyse the projects at flowcell level only
    launch_analysis_for_projects_flowcells(projects_to_analyze)


def launch_analysis_for_projects_flowcells(projects_to_analyze, restrict_to_samples=None, config_file_path=None):
    """Launch the analysis of projects.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param list restrict_to_samples: A list of sample names to which we will restrict our analysis
    :param list config_file_path: The path to the NGI Pipeline configuration file.
    """
    if not config_file_path:
        config_file_path = locate_ngi_config()
    config = load_yaml_config(config_file_path)
    for project in projects_to_analyze:
        # Get information from the database regarding which workflows to run
        try:
            workflow = get_workflow_for_project(project.name)
        except (ValueError, IOError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project, e))
            LOG.error(error_msg)
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}. Skipping.".format(analysis_module,
                                                           workflow,
                                                           project))
            LOG.error(error_msg)
            continue
        ##now process each flowcell, one per time

        for sample in project.samples.values():
            for libprep in sample:
                for fcid in libprep:
                    #check that the current FlowCell is not already being analysed
                    
                    analysis_running = check_if_flowcell_analysis_are_running(project,
                        sample, libprep, fcid, config)
                    #if I am not running nothing on this run then I can start to analyse it
                    # IMPORTANT I know that this is a run so I need to start run specific analysis
                    # another function will take care of project specific analysis
                    if not analysis_running: #if this flowcell run is not already being analysed
                        try:
                            workflow = "dna_alignonly"  #must be taken from somewhere, either config file or Charon
                            #when I call an Engine at flowcell level I expect that the engine starts by defining its own
                            #folder structure and subsequently start analysis at flowcell level.
                            p_handle = analysis_module.analyze_flowcell_run(project=project,
                                                       sample= sample,
                                                       libprep = libprep,
                                                       fcid = fcid,
                                                       workflow_name=workflow,
                                                       config_file_path=config_file_path)
                            record_workflow_process_run_local(p_handle, workflow, project,
                             sample, libprep, fcid, analysis_module, config)
        
                        except Exception as e:
                            error_msg = ('Cannot process project "{}": {}'.format(project, e))
                            LOG.error(error_msg)
                            continue

## NOTE This will be the function that is called by the Workflow Watcher script, or whatever we want to call it
##      By this I mean the script that checks intermittently to determine if we can move on with the next workflow,
##      whether this is something periodic (like a cron job) or something triggered by the completion of another part
##      of the code (event-based, i.e. via Celery)
##
##      At the moment it requires a list of projects to analyze, which suggests it is called by another
##      function that has just finished doing something with those project (i.e. the Celery approach);
##      if it is to be called periodically, I would suggest that the periodic calling function
##      (i.e. whatever script the cron job calls) uses another function that goes through the database
##      and finds all Projects for which the "Status" is not "Complete" or something to that effect,
##      and then hands that list to this function.
def launch_analysis_for_projects(projects_to_analyze, restrict_to_samples=None, config_file_path=None):
    """Launch the analysis of projects.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param list restrict_to_samples: A list of sample names to which we will restrict our analysis
    :param list config_file_path: The path to the NGI Pipeline configuration file.
    """

    if not config_file_path:
        config_file_path = locate_ngi_config()
    config = load_yaml_config(config_file_path)
    for project in projects_to_analyze:
        # Get information from the database regarding which workflows to run
        try:
            workflow = get_workflow_for_project(project.name)
        except (ValueError, IOError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project, e))
            LOG.error(error_msg)
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}. Skipping.".format(analysis_module,
                                                           workflow,
                                                           project))
            LOG.error(error_msg)
            continue
        try:
            #this happens at project level butI need to track actions at Samples level!!!!
            p_handle = analysis_module.analyze_project(project=project,
                                                       workflow_name=workflow,
                                                       config_file_path=config_file_path)

            #this must be tracked at run level
            # For now only tracking this on the project level
            record_workflow_process_local(p_handle, workflow, project, analysis_module, config)
        except Exception as e:
            error_msg = ('Cannot process project "{}": {}'.format(project, e))
            LOG.error(error_msg)
            continue


# This can be run intermittently to track the status of jobs and update the database accordingly,
# as well as to remove entries from the local database if the job has completed (but ONLY ONLY ONLY
# once the status has been successfully written to Charon!!)
def check_update_jobs_status(config_file_path=None, projects_to_check=None):
    """Check and update the status of jobs associated with workflows/projects;
    this goes through every record kept locally, and if the job has completed
    (either successfully or not) AND it is able to update Charon to reflect this
    status, it deletes the local record.

    :param str config_file_path: The path to the configuration file (optional if
                                 it is defined as env var or in default location)
    :param list projects_to_check: A list of project names to check (exclusive, optional)
    """
    if not config_file_path:
        config_file_path = locate_ngi_config()
    config = load_yaml_config(config_file_path)
    db = get_all_tracked_processes()
    for project_name, project_dict in db.iteritems():
        LOG.info("Checking workflow {} for project {}...".format(project_dict["workflow"],
                                                                 project_name))
        return_code = project_dict["p_handle"].poll()
        if return_code is not None:
            # Job finished somehow or another; try to update database.
            LOG.info('Workflow "{}" for project "{}" completed '
                     'with return code "{}". Attempting to update '
                     'Charon database.'.format(project_dict['workflow'],
                                               project_name, return_code))
            # Only if we succesfully write to Charon will we remove the record
            # from the local db; otherwise, leave it and try again next cycle.
            try:
                project_id = project_dict['project_id']
                write_status_to_charon(project_id, return_code)
                LOG.info("Successfully updated Charon database.")
                try:
                    # This only hits if we succesfully update Charon
                    remove_record_from_local_tracking()
                except RuntimeError:
                    # I find myself compulsively double-logging
                    LOG.error(e)
                    continue
            except RuntimeError as e:
                LOG.warn(e)
                continue
        else:
            LOG.info('Workflow "{}" for project "{}" (pid {}) '
                     'still running.'.format(project_dict['workflow'],
                                             project_name,
                                             project_dict['p_handle'].pid))


def get_workflow_for_project(project_name):
    """Get the workflow that should be run for this project from the database.

    :param str project_name: The name of the project

    :returns: The names of the workflow that should be run.
    :rtype: str
    :raises ValueError: If the project cannot be found in the database
    :raises IOError: If the database cannot be reached
    """
    ## NOTE Temporary until this is developed fully and the database populated
    return "NGI"

    # Keep the connection so we can pass it to the validation function
    #db_project_object = get_charon_session_for_project(project_name)
    #return db_project_object.get("workflow")


def setup_analysis_directory_structure(fc_dir, config, projects_to_analyze,
                                       restrict_to_projects=None, restrict_to_samples=None):
    """
    Copy and sort files from their CASAVA-demultiplexed flowcell structure
    into their respective project/sample/libPrep/FCIDs. This collects samples
    split across multiple flowcells.

    :param str fc_dir: The directory created by CASAVA for this flowcell.
    :param dict config: The parsed configuration file.
    :param set projects_to_analyze: A dict (of Project objects, or empty)
    :param list restrict_to_projects: Specific projects within the flowcell to process exclusively
    :param list restrict_to_samples: Specific samples within the flowcell to process exclusively

    :returns: A list of NGIProject objects that need to be run through the analysis pipeline
    :rtype: list

    :raises OSError: If the analysis destination directory does not exist or if there are permissions errors.
    :raises KeyError: If a required configuration key is not available.
    """
    LOG.info("Setting up analysis for demultiplexed data in source folder \"{}\"".format(fc_dir))
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
    if not os.path.exists(analysis_top_dir):
        error_msg = "Error: Analysis top directory {} does not exist".format(analysis_top_dir)
        LOG.error(error_msg)
        raise OSError(error_msg)
    if not os.path.exists(fc_dir):
        LOG.error("Error: Flowcell directory {} does not exist".format(fc_dir))
        return []
    # Map the directory structure for this flowcell
    try:
        fc_dir_structure = parse_casava_directory(fc_dir)
    except RuntimeError as e:
        LOG.error("Error when processing flowcell dir \"{}\": {}".format(fc_dir, e))
        return []
    # From RunInfo.xml
    fc_date = fc_dir_structure['fc_date']
    # From RunInfo.xml (name) & runParameters.xml (position)
    fcid = fc_dir_structure['fc_name']
    fc_short_run_id = "{}_{}".format(fc_date, fcid)

    if not fc_dir_structure.get('projects'):
        LOG.warn("No projects found in specified flowcell directory \"{}\"".format(fc_dir))
    # Iterate over the projects in the flowcell directory
    for project in fc_dir_structure.get('projects', []):
        project_name = project['project_name']
        # If specific projects are specified, skip those that do not match
        if restrict_to_projects and project_name not in restrict_to_projects:
            LOG.debug("Skipping project {}".format(project_name))
            continue
        #now check if this project can be parsed via Charon
        try:
            ## NOTE NOTE NOTE that this means we have to be able to access Charon
            ##                to process things. I dislike this but I have no
            ##                other way to get the Project ID
            project_id = get_project_id_from_name(project_name)
        except (RuntimeError, ValueError) as e:
            error_msg = ('Cannot proceed with project "{}" due to '
                         'Charon-related error: {}'.format(project_name, e))
            LOG.error(error_msg)
            continue
        LOG.info("Setting up project {}".format(project.get("project_name")))
        # Create a project directory if it doesn't already exist, including
        # intervening "DATA" directory
        project_dir = os.path.join(analysis_top_dir, "DATA", project_name)
        if not os.path.exists(project_dir): safe_makedir(project_dir, 0770)
        try:
            project_obj = projects_to_analyze[project_dir]
        except KeyError:
            project_obj = NGIProject(name=project_name, dirname=project_name,
                                     project_id=project_id,
                                     base_path=analysis_top_dir)
            projects_to_analyze[project_dir] = project_obj
        # Iterate over the samples in the project
        for sample in project.get('samples', []):
            # Our SampleSheet.csv names are like Y__Mom_14_01 for some reason
            sample_name = sample['sample_name'].replace('__','.')
            # If specific samples are specified, skip those that do not match
            if restrict_to_samples and sample_name not in restrict_to_samples:
                LOG.debug("Skipping sample {}".format(sample_name))
                continue
            LOG.info("Setting up sample {}".format(sample_name))
            # Create a directory for the sample if it doesn't already exist
            sample_dir = os.path.join(project_dir, sample_name)
            if not os.path.exists(sample_dir): safe_makedir(sample_dir, 0770)
            # This will only create a new sample object if it doesn't already exist in the project
            sample_obj = project_obj.add_sample(name=sample_name, dirname=sample_name)
            # Get the Library Prep ID for each file
            pattern = re.compile(".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$")
            fastq_files = filter(pattern.match, sample.get('files', []))
            seqrun_dir = None
            for fq_file in fastq_files:
                libprep_name = determine_library_prep_from_fcid(project_id, sample_name, fcid)
                libprep_object = sample_obj.add_libprep(name=libprep_name,
                                                        dirname=libprep_name)
                libprep_dir = os.path.join(sample_dir, libprep_name)
                if not os.path.exists(libprep_dir): safe_makedir(libprep_dir, 0770)
                seqrun_object = libprep_object.add_seqrun(name=fc_short_run_id,
                                                          dirname=fc_short_run_id)
                seqrun_dir = os.path.join(libprep_dir, fc_short_run_id)
                if not os.path.exists(seqrun_dir): safe_makedir(seqrun_dir, 0770)
                seqrun_object.add_fastq_files(fq_file)
            # rsync the source files to the sample directory
            #    src: flowcell/data/project/sample
            #    dst: project/sample/flowcell_run
            src_sample_dir = os.path.join(fc_dir_structure['fc_dir'],
                                          project['data_dir'],
                                          project['project_dir'],
                                          sample['sample_dir'])
            for libprep in sample_obj:
            #this function works at run_level, so I have to process a single run
            #it might happen that in a run we have multiple lib preps for the same sample
                #for seqrun in libprep:
                src_fastq_files = [ os.path.join(src_sample_dir, fastq_file)
                                    for fastq_file in seqrun_object.fastq_files ] ##MARIO: check this
                LOG.info("Copying fastq files from {} to {}...".format(sample_dir, seqrun_dir))
                do_rsync(src_fastq_files, seqrun_dir)
    return projects_to_analyze


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

    :returns: A dict of information about the flowcell, including project/sample info
    :rtype: dict

    :raises RuntimeError: If the fc_dir does not exist or cannot be accessed,
                          or if Flowcell RunMetrics could not be parsed properly.
    """
    projects = []
    fc_dir = os.path.abspath(fc_dir)
    LOG.info("Parsing flowcell directory \"{}\"...".format(fc_dir))
    
    
    parser = FlowcellRunMetricsParser(fc_dir)
    run_info = parser.parseRunInfo()
    runparams = parser.parseRunParameters()
    try:
        fc_name = run_info['Flowcell']
        fc_date = run_info['Date']
        fc_pos = runparams['FCPosition']
    except KeyError as e:
        raise RuntimeError("Could not parse flowcell information {} "
                           "from Flowcell RunMetrics in flowcell {}".format(e, fc_dir))
    # "Unaligned*" because SciLifeLab dirs are called "Unaligned_Xbp"
    # (where "X" is the index length) and there is also an "Unaligned" folder
    unaligned_dir_pattern = os.path.join(fc_dir,"Unaligned*")
    basecall_stats_dir_pattern = os.path.join(unaligned_dir_pattern,"Basecall_Stats_*")
    basecall_stats_dir = [os.path.relpath(d,fc_dir) for d in glob.glob(basecall_stats_dir_pattern)]
    # e.g. 131030_SN7001362_0103_BC2PUYACXX/Unaligned_16bp/Project_J__Bjorkegren_13_02/
    project_dir_pattern = os.path.join(unaligned_dir_pattern,"Project_*")
    for project_dir in glob.glob(project_dir_pattern):
        LOG.info("Parsing project directory \"{}\"...".format(project_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
        project_samples = []
        sample_dir_pattern = os.path.join(project_dir,"Sample_*")
        # e.g. <Project_dir>/Sample_P680_356F_dual56/
        for sample_dir in glob.glob(sample_dir_pattern):
            LOG.info("Parsing samples directory \"{}\"...".format(sample_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
            fastq_file_pattern = os.path.join(sample_dir,"*.fastq.gz")
            samplesheet_pattern = os.path.join(sample_dir,"*.csv")
            fastq_files = [os.path.basename(file) for file in glob.glob(fastq_file_pattern)]
            ## NOTE that we don't wind up using this SampleSheet for anything so far as I know
            ## TODO consider removing; however, some analysis engines that
            ##      we want to include in the future may need them.
            #samplesheet = glob.glob(samplesheet_pattern)
            #assert len(samplesheet) == 1, \
            #        "Error: could not unambiguously locate samplesheet in {}".format(sample_dir)
            sample_name = os.path.basename(sample_dir).replace("Sample_","").replace('__','.')
            project_samples.append({'sample_dir': os.path.basename(sample_dir),
                                    'sample_name': sample_name,
                                    'files': fastq_files,
            #                        'samplesheet': os.path.basename(samplesheet[0])})
                                   })
        project_name = os.path.basename(project_dir).replace("Project_","").replace('__','.')
        projects.append({'data_dir': os.path.relpath(os.path.dirname(project_dir),fc_dir),
                         'project_dir': os.path.basename(project_dir),
                         'project_name': project_name,
                         'samples': project_samples})
    return {'fc_dir': fc_dir,
            'fc_name': '{}{}'.format(fc_pos, fc_name),
            'fc_date': fc_date,
            'basecall_stats_dir': basecall_stats_dir,
            'projects': projects}


def _copy_basecall_stats(source_dirs, destination_dir):
    """Copy relevant files from the Basecall_Stats_FCID directory
       to the analysis directory
    """
    for source_dir in source_dirs:
        # First create the directory in the destination
        dirname = os.path.join(destination_dir,os.path.basename(source_dir))
        safe_makedir(dirname)
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


# This isn't used at the moment
def copy_undetermined_index_files(casava_data_dir, destination_dir):
    """
    Copy fastq files with "Undetermined" index reads to the destination directory.
    :param str casava_data_dir: The Unaligned directory (e.g. "<FCID>/Unaligned_16bp")
    :param str destination_dir: Eponymous
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

# Also not used at the moment
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
