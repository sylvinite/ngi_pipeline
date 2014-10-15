"""The Piper automated launcher script."""
from __future__ import print_function

import collections
import os
import re
import shlex
import shutil
import subprocess
import time

from ngi_pipeline.piper_ngi import workflows
from ngi_pipeline.piper_ngi.utils import create_log_file_path, create_exit_code_file_path
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.piper_ngi.local_process_tracking import is_seqrun_analysis_running_local, \
                                                          is_sample_analysis_running_local, \
                                                          record_process_seqrun, \
                                                          record_process_sample
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, rotate_log, safe_makedir
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)


def get_subtasks_for_level(level):
    """For a given level (e.g. "seqrun" or "sample"), get all the associated
    subtasks that should be run (e.g. "qc", "dna_alignonly")

    :param str level: The level ("seqrun", "sample")
    :returns: The names (strings) of the workflows that should be run at that level
    :rtype: tuple
    """
    if level == "seqrun":
        return ["dna_alignonly"] #"qc",
    elif level == "sample":
        return ["merge_process_variantcall"]
    else:
        raise NotImplementedError('The level "{}" has no associated subtasks.')


@with_ngi_config
def analyze_seqrun(project, sample, libprep, seqrun, config=None, config_file_path=None):
    """Analyze data at the sequencing run (individual fastq) level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param NGILibraryPrep libprep: The library prep to analyzed
    :seqrun NGISeqrun seqrun: The sequencing run to analyzed
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """

    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    for workflow_subtask in get_subtasks_for_level(level="seqrun"):
        if not is_seqrun_analysis_running_local(workflow_subtask=workflow_subtask,
                                                project_id=project.project_id,
                                                sample_id=sample.name,
                                                libprep_id=libprep.name,
                                                seqrun_id=seqrun.name):
            try:
                ## Temporarily logging to a file until we get ELK set up
                log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                     project_base_path=project.base_path,
                                                     project_name=project.name,
                                                     sample_id=sample.name,
                                                     libprep_id=libprep.name,
                                                     seqrun_id=seqrun.name)
                rotate_log(log_file_path)
                # Store the exit code of detached processes
                exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                            project_base_path=project.base_path,
                                                            project_name=project.name,
                                                            sample_id=sample.name,
                                                            libprep_id=libprep.name,
                                                            seqrun_id=seqrun.name)
                build_setup_xml(project, config, sample, libprep.name, seqrun.name)
                command_line = build_piper_cl(project, workflow_subtask, exit_code_path, config)
                p_handle = launch_piper_job(command_line, project, log_file_path)
                try:
                    record_process_seqrun(project=project, sample=sample, libprep=libprep,
                                          seqrun=seqrun, workflow_subtask=workflow_subtask,
                                          analysis_module_name="piper_ngi",
                                          analysis_dir=project.analysis_dir,
                                          pid=p_handle.pid)
                except CharonError as e:
                    ## This is a problem. If the job isn't recorded, we won't
                    ## ever know that it has been run and its results will be ignored.
                    ## I think? Or no I guess if it's relaunched then the results will be there.
                    ## But we will have multiple processes running.
                    ## FIXME fix this
                    LOG.error("<Could not record ...>")
                    continue
            except (NotImplementedError, RuntimeError) as e:
                error_msg = ('Processing project "{}" / sample "{}" / libprep "{}" / '
                             'seqrun "{}" failed: {}'.format(project, sample, libprep, seqrun,
                                                           e.__repr__()))
                LOG.error(error_msg)

@with_ngi_config
def analyze_sample(project, sample, config=None, config_file_path=None):
    """Analyze data at the sample level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    charon_session = CharonSession()
    # Determine if we can begin sample-level processing yet.
    # Conditions are that the coverage is above 20X
    # If these conditions become more complex we can create a function for this
    sample_total_autosomal_coverage = charon_session.sample_get(project.project_id,
                                     sample.name).get('total_autosomal_coverage')
    if sample_total_autosomal_coverage > 28.4:
        LOG.info('Sample "{}" in project "{}" is ready for processing.'.format(sample, project))
        for workflow_subtask in get_subtasks_for_level(level="sample"):
            if not is_sample_analysis_running_local(workflow_subtask=workflow_subtask,
                                                    project_id=project.project_id,
                                                    sample_id=sample.name):
                try:
                    ## Temporarily logging to a file until we get ELK set up
                    log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                         project_base_path=project.base_path,
                                                         project_name=project.name,
                                                         sample_id=sample.name)
                    rotate_log(log_file_path)
                    # Store the exit code of detached processes
                    exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                                project_base_path=project.base_path,
                                                                project_name=project.name,
                                                                sample_id=sample.name)

                    build_setup_xml(project, config, sample)
                    command_line = build_piper_cl(project, workflow_subtask, exit_code_path, config)
                    p_handle = launch_piper_job(command_line, project, log_file_path)
                    try:
                        record_process_sample(project=project, sample=sample,
                                              workflow_subtask=workflow_subtask,
                                              analysis_module_name="piper_ngi",
                                              analysis_dir=project.analysis_dir,
                                              pid=p_handle.pid)
                    except RuntimeError as e:
                        LOG.error(e)
                        continue
                except (NotImplementedError, RuntimeError) as e:
                    error_msg = ('Processing project "{}" / sample "{}" failed: '
                                 '{}'.format(project, sample, e.__repr__()))
                    LOG.error(error_msg)
    else:
        LOG.info('Sample "{}" in project "{}" is not yet ready for '
                 'processing.'.format(sample, project))


def launch_piper_job(command_line, project, log_file_path=None):
    """Launch the Piper command line.

    :param str command_line: The command line to execute
    :param Project project: The Project object (needed to set the CWD)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    cwd = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    file_handle=None
    if log_file_path:
        try:
            file_handle = open(log_file_path, 'w')
        except Exception as e:
            LOG.error('Could not open log file "{}"; reverting to standard logger (error: {})'.format(log_file_path, e))
            log_file_path = None
    popen_object = execute_command_line(command_line, cwd=cwd, shell=True,
                                        stdout=(file_handle or subprocess.PIPE),
                                        stderr=(file_handle or subprocess.PIPE)
                                        )
    if not log_file_path:
        log_process_non_blocking(popen_object.stdout, LOG.info)
        log_process_non_blocking(popen_object.stderr, LOG.warn)
    return popen_object


def build_piper_cl(project, workflow_name, exit_code_path, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param NGIProject project: The project object to analyze.
    :param str workflow_name: The name of the workflow to execute (e.g. "dna_alignonly")
    :param str exit_code_path: The path to the file to which the exit code for this cl will be written
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    # Find Piper global configuration:
    #   Check environmental variable PIPER_GLOB_CONF_XML
    #   then the config file
    #   then the file globalConfig.xml in the piper root dir

    piper_rootdir = config.get("piper", {}).get("path_to_piper_rootdir")
    piper_global_config_path = (os.environ.get("PIPER_GLOB_CONF_XML") or
                                config.get("piper", {}).get("path_to_piper_globalconfig") or
                                (os.path.join(piper_rootdir, "globalConfig.xml") if
                                piper_rootdir else None))
    if not piper_global_config_path:
        error_msg = ("Could not find Piper global configuration file in config file, "
                     "as environmental variable (\"PIPER_GLOB_CONF_XML\"), "
                     "or in Piper root directory.")
        raise ValueError(error_msg)

    # Find Piper QScripts dir:
    #   Check environmental variable PIPER_QSCRIPTS_DIR
    #   then the config file
    piper_qscripts_dir = (os.environ.get("PIPER_QSCRIPTS_DIR") or
                          config['piper']['path_to_piper_qscripts'])
    if not piper_qscripts_dir:
        error_msg = ("Could not find Piper QScripts directory in config file or "
                    "as environmental variable (\"PIPER_QSCRIPTS_DIR\").")
        raise ValueError(error_msg)

    LOG.info('Building workflow command line(s) for '
             'project "{}" / workflow "{}"'.format(project, workflow_name))
    ## NOTE This key will probably exist on the project level, and may have multiple values.
    ##      Workflows may imply a number of substeps (e.g. basic = qc, alignment, etc.) ?
    try:
        setup_xml_path = project.setup_xml_path
    except AttributeError:
        error_msg = ('Project "{}" has no setup.xml file. Skipping project '
                     'command-line generation.'.format(project))
        raise ValueError(error_msg)

    cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                          qscripts_dir_path=piper_qscripts_dir,
                                          setup_xml_path=setup_xml_path,
                                          global_config_path=piper_global_config_path,
                                          output_dir=project.analysis_dir)
    # Blank out the file if it already exists
    open(exit_code_path, 'w').close()
    return add_exit_code_recording(cl, exit_code_path)


def add_exit_code_recording(cl, exit_code_path):
    """Takes a command line and returns it with increased pizzaz"""
    record_exit_code = "; echo $? > {}".format(exit_code_path)
    if type(cl) is list:
        # This should work, right? Right
        cl = " ".join(cl)
    return cl + record_exit_code


def build_setup_xml(project, config, sample=None, libprep_id=None, seqrun_id=None):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param NGIProject project: The project to be converted.
    :param dict config: The (parsed) configuration file for this machine/environment.
    :param NGISample sample: the sample object
    :param str library_id: id of the library
    :param str seqrun_id: flowcell identifier

    :returns: A list of Project objects with setup.xml paths as attributes.
    :rtype: list
    """

    if not seqrun_id:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}"'.format(project, sample.name))
    else:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}", libprep "{}", seqrun "{}"'.format(project, sample,
                                                                 libprep_id, seqrun_id))

    project_top_level_dir = os.path.join(project.base_path, "DATA", project.dirname)
    analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    safe_makedir(analysis_dir, 0770)
    safe_makedir(os.path.join(analysis_dir, "logs"))
    cl_args = {'project': project.name}
    # Load needed data from database
    try:
        # Information we need from the database:
        # - species / reference genome that should be used (hg19, mm9)
        # - analysis workflows to run (QC, DNA alignment, RNA alignment, variant calling, etc.)
        # - adapters to be trimmed (?)
        ## <open connection to project database>
        #reference_genome = proj_db.get('species')
        reference_genome = 'GRCh37'
        # sequencing_center = proj_db.get('Sequencing Center')
        cl_args["sequencing_center"] = "NGI"
    except:
        ## Handle database connection failures here once we actually try to connect to it
        pass

    # Load needed data from configuration file
    try:
        cl_args["reference_path"] = config['supported_genomes'][reference_genome]
        cl_args["uppmax_proj"] = config['environment']['project_id']
    except KeyError as e:
        error_msg = ("Could not load required information from "
                     "configuration file and cannot continue with project {}: "
                     "value \"{}\" missing".format(project, e.message))
        raise ValueError(error_msg)

    try:
        cl_args["sfc_binary"] = config['piper']['path_to_setupfilecreator']
    except KeyError:
        # Assume setupFileCreator is on path
        cl_args["sfc_binary"] = "setupFileCreator"


    if not seqrun_id:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-setup.xml".format(project, sample.name))
    else:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-{}_setup.xml".format(project, sample.name, seqrun_id))

    cl_args["output_xml_filepath"]  = output_xml_filepath
    cl_args["sequencing_tech"]      = "Illumina"
    cl_args["qos"] = "seqver"
    setupfilecreator_cl = ("{sfc_binary} "
                           "--output {output_xml_filepath} "
                           "--project_name {project} "
                           "--sequencing_platform {sequencing_tech} "
                           "--sequencing_center {sequencing_center} "
                           "--uppnex_project_id {uppmax_proj} "
                           "--reference {reference_path} "
                           "--qos {qos}".format(**cl_args))
    #NOTE: here I am assuming the different dir structure, it would be wiser to change the object type and have an uppsala project
    if not seqrun_id:
        #if seqrun_id is none it means I want to create a sample level setup xml
        for libprep in sample:
            for seqrun in libprep:
                sample_run_directory = os.path.join(project_top_level_dir, sample.dirname, libprep.name, seqrun.name )
                for fastq_file_name in os.listdir(sample_run_directory):
                    #MARIO: I am not a big fun of this, IGN object need to be created from file system in order to avoid this things
                    fastq_file = os.path.join(sample_run_directory, fastq_file_name)
                    setupfilecreator_cl += " --input_fastq {}".format(fastq_file)
    else:
        #I need to create an xml file for this sample_run
        sample_run_directory = os.path.join(project_top_level_dir, sample.dirname, libprep_id, seqrun_id )
        for fastq_file_name in sample.libpreps[libprep_id].seqruns[seqrun_id].fastq_files:
            fastq_file = os.path.join(sample_run_directory, fastq_file_name)
            setupfilecreator_cl += " --input_fastq {}".format(fastq_file)

    try:
        LOG.info("Executing command line: {}".format(setupfilecreator_cl))
        subprocess.check_call(shlex.split(setupfilecreator_cl))
        project.setup_xml_path = output_xml_filepath
        project.analysis_dir   = analysis_dir
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        error_msg = ("Unable to produce setup XML file for project {}; "
                     "skipping project analysis. "
                     "Error is: \"{}\". .".format(project, e))
        raise RuntimeError(error_msg)
