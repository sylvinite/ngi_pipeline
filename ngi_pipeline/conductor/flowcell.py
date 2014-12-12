#!/usr/bin/env python


from __future__ import print_function

import glob
import os
import re
import sys

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.conductor.launchers import launch_analysis
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.communicate import get_project_id_from_name
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import do_rsync, do_symlink, safe_makedir
from ngi_pipeline.utils.parsers import determine_library_prep_from_fcid, \
                                       determine_libprep_from_uppsala_samplesheet, \
                                       parse_lane_from_filename

LOG = minimal_logger(__name__)

UPPSALA_PROJECT_RE = re.compile(r'\w{2}-\d{4}')


def process_demultiplexed_flowcell(demux_fcid_dir_path, restrict_to_projects=None,
                                   restrict_to_samples=None, restart_failed_jobs=False,
                                   config_file_path=None):
    """Call process_demultiplexed_flowcells, restricting to a single flowcell.
    Essentially a restrictive wrapper.

    :param str demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon.
    :param str config_file_path: The path to the NGI configuration file; optional.
    """
    if type(demux_fcid_dir_path) is not str:
        error_message = ("The path to a single demultiplexed flowcell should be "
                         "passed to this function as a string.")
        raise ValueError(error_message)
    process_demultiplexed_flowcells([demux_fcid_dir_path], restrict_to_projects,
                                    restrict_to_samples, restart_failed_jobs,
                                    config_file_path=config_file_path)


@with_ngi_config
def process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects=None,
                                    restrict_to_samples=None,
                                    restart_failed_jobs=False,
                                    config=None, config_file_path=None):
    """Sort demultiplexed Illumina flowcells into projects and launch their analysis.

    :param list demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon.
    :param dict config: The parsed NGI configuration file; optional.
    :param str config_file_path: The path to the NGI configuration file; optional.
    """
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    # Sort/copy each raw demux FC into project/sample/fcid format -- "analysis-ready"
    projects_to_analyze = dict()
    for demux_fcid_dir in demux_fcid_dirs_set:
        # These will be a bunch of Project objects each containing Samples, FCIDs, lists of fastq files
        projects_to_analyze = setup_analysis_directory_structure(demux_fcid_dir,
                                                                 projects_to_analyze,
                                                                 restrict_to_projects,
                                                                 restrict_to_samples,
                                                                 create_files=True,
                                                                 config=config)
    if not projects_to_analyze:
        if restrict_to_projects:
            error_message = ("No projects found to process; the specified flowcells "
                             "({fcid_dirs}) do not contain the specified project(s) "
                             "({restrict_to_projects}) or there was an error "
                             "gathering required information.").format(
                                    fcid_dirs = ",".join(demux_fcid_dirs_set),
                                    restrict_to_projects = ",".join(restrict_to_projects))
        else:
            error_message = ("No projects found to process in flowcells {} "
                             "or there was an error gathering required "
                             "information.".format(",".join(demux_fcid_dirs_set)))
        LOG.info(error_message)
        sys.exit("Quitting: " + error_message)
    else:
        projects_to_analyze = projects_to_analyze.values() # Don't need the dict functionality anymore; revert to list
    for project in projects_to_analyze:
        if UPPSALA_PROJECT_RE.match(project.project_id):
            LOG.info('Creating Charon records for Uppsala project "{}" if they are missing'.format(project))
            create_charon_entries_from_project(project, sequencing_facility="NGI-U")
        else:
            # I hate this
            for sample in project:
                for libprep in sample:
                    if libprep.name == "Unknown":
                        LOG.info('Populating Charon with records for project/sample/libprep '
                                 '{}/{}/{}'.format(project, sample, libprep))
                        # This is horrible what am I doing somebody stop me
                        tmp_proj = NGIProject(project.name,
                                              project.dirname,
                                              project.project_id,
                                              project.base_path)
                        tmp_proj.samples = {sample.name: sample}
                        tmp_proj.samples[sample.name].libpreps = {libprep.name: libprep}
                        create_charon_entries_from_project(tmp_proj)
    launch_analysis(projects_to_analyze, restart_failed_jobs)


### TODO rework so that the creation of the NGIObjects and the actual creation of files are different functions?
@with_ngi_config
def setup_analysis_directory_structure(fc_dir, projects_to_analyze,
                                       restrict_to_projects=None, restrict_to_samples=None,
                                       create_files=True,
                                       ign_only=True,
                                       config=None, config_file_path=None):
    """
    Copy and sort files from their CASAVA-demultiplexed flowcell structure
    into their respective project/sample/libPrep/FCIDs. This collects samples
    split across multiple flowcells.

    :param str fc_dir: The directory created by CASAVA for this flowcell.
    :param dict config: The parsed configuration file.
    :param set projects_to_analyze: A dict (of Project objects, or empty)
    :param bool create_files: Alter the filesystem (as opposed to just parsing flowcells) (default True)
    :param bool ign_only: Only process IGN projects (default True)
    :param list restrict_to_projects: Specific projects within the flowcell to process exclusively
    :param list restrict_to_samples: Specific samples within the flowcell to process exclusively

    :returns: A list of NGIProject objects that need to be run through the analysis pipeline
    :rtype: list

    :raises KeyError: If a required configuration key is not available.
    """
    LOG.info("Setting up analysis for demultiplexed data in source folder \"{}\"".format(fc_dir))
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    if ign_only: charon_session = CharonSession()
    analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
    if not os.path.exists(analysis_top_dir):
        error_msg = "Error: Analysis top directory {} does not exist".format(analysis_top_dir)
        LOG.error(error_msg)
        raise OSError(error_msg)
    fc_dir = fc_dir if os.path.isabs(fc_dir) else os.path.join(analysis_top_dir, fc_dir)
    if not os.path.exists(fc_dir):
        LOG.error("Error: Flowcell directory {} does not exist".format(fc_dir))
        return []
    # Map the directory structure for this flowcell
    try:
        fc_dir_structure = parse_casava_directory(fc_dir)
    except (OSError, ValueError) as e:
        LOG.error("Error when processing flowcell dir \"{}\": {}".format(fc_dir, e))
        return []
    fc_full_id = fc_dir_structure['fc_full_id']
    if not fc_dir_structure.get('projects'):
        LOG.warn("No projects found in specified flowcell directory \"{}\"".format(fc_dir))
    # Iterate over the projects in the flowcell directory
    for project in fc_dir_structure.get('projects', []):
        project_name = project['project_name']
        # If specific projects are specified, skip those that do not match
        if ign_only:
            if not UPPSALA_PROJECT_RE.match(project_name):
                # We can't determine if Uppsala projects are IGN as we have no
                # data for Uppsala projects in Charon; process all of them
                try:
                    project_bpa = charon_session.project_get(project_name).get("best_practice_analysis")
                except (CharonError, RuntimeError, ValueError) as e:
                    LOG.warn('Could not retrieve project id from Charon (record missing?). '
                     'Probably  project {} is not an IGN (no mixed flowcells) '
                     '(error: {})'.format(project_name, e))
                    continue
                if not project_bpa in config.get('analysis',{}).get('best_practice_analysis',{}):
                    # If this is not an IGN project, skip it
                    continue
        if restrict_to_projects and project_name not in restrict_to_projects:
            LOG.debug("Skipping project {}".format(project_name))
            continue
        try:
            # This requires Charon access -- maps e.g. "Y.Mom_14_01" to "P123"
            project_id = get_project_id_from_name(project_name)
        except (CharonError, RuntimeError, ValueError) as e:
            LOG.warn('Could not retrieve project id from Charon (record missing?). '
                     'Using project name ("{}") as project id '
                     '(error: {})'.format(project_name, e))
            project_id = project_name
        LOG.info("Setting up project {}".format(project.get("project_name")))
        # Create a project directory if it doesn't already exist, including
        # intervening "DATA" directory
        project_dir = os.path.join(analysis_top_dir, "DATA", project_id)
        project_sl_dir = os.path.join(analysis_top_dir, "DATA", project_name)
        if create_files:
            safe_makedir(project_dir, 0770)
            if not project_dir == project_sl_dir and \
               not os.path.exists(project_sl_dir):
                os.symlink(project_dir, project_sl_dir)
        try:
            project_obj = projects_to_analyze[project_dir]
        except KeyError:
            project_obj = NGIProject(name=project_name, dirname=project_id,
                                     project_id=project_id,
                                     base_path=analysis_top_dir)
            projects_to_analyze[project_dir] = project_obj
        # Iterate over the samples in the project
        for sample in project.get('samples', []):
            # Stockholm names are like Y__Mom_14_01 for some reason
            sample_name = sample['sample_name'].replace('__','.')
            # If specific samples are specified, skip those that do not match
            if restrict_to_samples and sample_name not in restrict_to_samples:
                LOG.debug("Skipping sample {}: not in specified samples {}".format(sample_name, ", ".join(restrict_to_samples)))
                continue
            LOG.info("Setting up sample {}".format(sample_name))
            # Create a directory for the sample if it doesn't already exist
            sample_dir = os.path.join(project_dir, sample_name)
            if create_files: safe_makedir(sample_dir, 0770)
            # This will only create a new sample object if it doesn't already exist in the project
            sample_obj = project_obj.add_sample(name=sample_name, dirname=sample_name)
            # Get the Library Prep ID for each file
            pattern = re.compile(".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$")
            fastq_files = filter(pattern.match, sample.get('files', []))
            # For each fastq file, create the libprep and seqrun objects
            # and add the fastq file to the seqprep object
            # Note again that these objects only get created if they don't yet exist;
            # if they do exist, the existing object is returned
            for fq_file in fastq_files:
                # Requires Charon access
                try:
                    libprep_name = determine_library_prep_from_fcid(project_id, sample_name, fc_full_id)
                except ValueError:
                    # This flowcell has not got library prep information in Charon and
                    # is probably an Uppsala project; if so, we can parse the libprep name
                    # from the SampleSheet.csv
                    try:
                        if fc_dir_structure['samplesheet_path']:
                            lane_num = parse_lane_from_filename(fq_file)
                            # This throws a ValueError if it can't find anything
                            libprep_name = determine_libprep_from_uppsala_samplesheet(
                                                fc_dir_structure['samplesheet_path'],
                                                project_id=project_id,
                                                sample_id=sample_name,
                                                seqrun_id=fc_full_id,
                                                lane_num=lane_num)
                        else:
                            raise ValueError()
                    except ValueError:
                        LOG.error('Project "{}" / sample "{}" / fastq "{}" '
                                  'has no libprep information in Charon and it '
                                  'could not be determined from the SampleSheet.csv. '
                                  'Setting library prep to "Unknown"'.format(project_name,
                                                                             sample_name,
                                                                             fq_file))
                        libprep_name = "Unknown"
                libprep_object = sample_obj.add_libprep(name=libprep_name,
                                                        dirname=libprep_name)
                libprep_dir = os.path.join(sample_dir, libprep_name)
                if create_files: safe_makedir(libprep_dir, 0770)
                seqrun_object = libprep_object.add_seqrun(name=fc_full_id,
                                                          dirname=fc_full_id)
                seqrun_dir = os.path.join(libprep_dir, fc_full_id)
                if create_files: safe_makedir(seqrun_dir, 0770)
                seqrun_object.add_fastq_files(fq_file)
            if fastq_files and create_files:
                # rsync the source files to the sample directory
                #    src: flowcell/data/project/sample
                #    dst: project/sample/libprep/flowcell_run
                src_sample_dir = os.path.join(fc_dir_structure['fc_dir'],
                                              project['data_dir'],
                                              project['project_dir'],
                                              sample['sample_dir'])
                for libprep_obj in sample_obj:
                    for seqrun_obj in libprep_obj:
                        src_fastq_files = [os.path.join(src_sample_dir, fastq_file) for
                                           fastq_file in seqrun_obj.fastq_files]
                        seqrun_dst_dir = os.path.join(project_obj.base_path, project_obj.dirname,
                                                      sample_obj.dirname, libprep_obj.dirname,
                                                      seqrun_obj.dirname)
                        LOG.info("Copying fastq files from {} to {}...".format(src_sample_dir, seqrun_dir))
                        #try:
                        ## FIXME this exception should be handled somehow when rsync fails
                        do_symlink(src_fastq_files, seqrun_dir)
                        #do_rsync(src_fastq_files, seqrun_dir)
                        #except subprocess.CalledProcessError as e:
                        #    ## TODO Here the rsync has failed
                        #    ##      should we delete this libprep from the sample object in this case?
                        #    ##      this could be an issue downstream if e.g. Piper expects these files
                        #    ##      and they are missing
                        #    LOG.warn('Error when performing rsync for "{}/{}/{}": '
                        #              '{}'.format(project, sample, libprep, e,))
    return projects_to_analyze


def parse_casava_directory(fc_dir):
    """
    Traverse a CASAVA-1.8-generated directory structure and return a dictionary
    of the elements it contains.
    The flowcell directory tree for HiSeq 2500 runs has (roughly) the structure:

    |-- Data
    |   |-- Intensities
    |       |-- BaseCalls
    |-- InterOp
    |-- SampleSheet.csv
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
        |   |-- Sample_P680_360F_dual60
        |   |   ...
        |-- Undetermined_indices
            |-- Sample_lane1
            |   ...
            |-- Sample_lane8

    The structure for X-Ten flowcells is close to but not exactly the same

    :param str fc_dir: The directory created by CASAVA for this flowcell.

    :returns: A dict of information about the flowcell, including project/sample info
    :rtype: dict

    :raises OSError: If the fc_dir does not exist or cannot be accessed
    """
    projects = []
    fc_dir = os.path.abspath(fc_dir)

    if not os.access(fc_dir, os.F_OK): os_msg = "does not exist"
    if not os.access(fc_dir, os.R_OK): os_msg = "could not be read (permission denied)"
    if locals().get('os_msg'): raise OSError("Error with flowcell dir {}: directory {}".format(fc_dir, os_msg))

    LOG.info('Parsing flowcell directory "{}"...'.format(fc_dir))
    try:
        samplesheet_path = os.path.abspath(glob.glob(os.path.join(fc_dir, "SampleSheet.csv"))[0])
        LOG.debug("SampleSheet.csv found at {}".format(samplesheet_path))
    except IndexError:
        LOG.warn("Could not find samplesheet in directory {}".format(fc_dir))
        samplesheet_path = None

    fc_full_id = os.path.basename(fc_dir)
    # "Unaligned*" because SciLifeLab dirs are called "Unaligned_Xbp"
    # (where "X" is the index length) and there is also an "Unaligned" folder
    unaligned_dir_pattern = os.path.join(fc_dir, "Unaligned*")
    # e.g. 131030_SN7001362_0103_BC2PUYACXX/Unaligned_16bp/Y__Mom_15_01/

    project_dir_pattern = os.path.join(unaligned_dir_pattern, "*")
    for project_dir in glob.glob(project_dir_pattern):
        LOG.info('Parsing project directory "{}"...'.format(
                            project_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
        project_name = os.path.basename(project_dir).replace('__', '.')
        project_samples = []
        sample_dir_pattern = os.path.join(project_dir, "*")

        # e.g. <Project_dir>/P680_356F_dual56/
        for sample_dir in glob.glob(sample_dir_pattern):
            LOG.info('Parsing samples directory "{}"...'.format(sample_dir.split(
                                                os.path.split(fc_dir)[0] + "/")[1]))
            sample_name = os.path.basename(sample_dir).replace('__','.')
            fastq_file_pattern = os.path.join(sample_dir, "*.fastq.gz")
            fastq_files = [os.path.basename(fq) for fq in glob.glob(fastq_file_pattern)]

            project_samples.append({'sample_dir': os.path.basename(sample_dir),
                                    'sample_name': sample_name,
                                    'files': fastq_files})
        if not project_samples:
            LOG.warn('No samples found for project "{}" in fc {}'.format(project_name, fc_dir))
        else:
            projects.append({'data_dir': os.path.relpath(os.path.dirname(project_dir), fc_dir),
                             'project_dir': os.path.basename(project_dir),
                             'project_name': project_name,
                             'samples': project_samples})

    if not projects:
        raise ValueError('No projects or no projects with sample found in '
                         'flowcell directory {}'.format(fc_dir))
    else:
        return {'fc_dir'    : fc_dir,
                'fc_full_id': fc_full_id,
                'projects': projects,
                'samplesheet_path': samplesheet_path}
