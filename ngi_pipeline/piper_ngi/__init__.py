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
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, safe_makedir
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)


@with_ngi_config
def analyze_flowcell_run(project, sample, libprep, seqrun, workflow_name, config=None, config_file_path=None):
    """The main method for analyze flowcells (Run Level).

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param NGILibraryPrep libprep: The library prep to analyzed
    :seqrun NGISeqrun seqrun: The sequencing run to analyzed
    :param str workflow_name: The workflow (e.g. alignment) to execute
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    #Here I am in the Piper World: this means that I can be Engine specific!!!
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    #things to to do
    # 1- convert the current project/sample/libprep/seqrun structure into the piper structure
    # 2- build setup_xml specific for this fc run
    # 3- run piper at fc run
    try:
        convert_sthlm_to_uppsala(project, seqrun) #this converts the entire project, it is not specific to the sample. Not a big deal, it will simply complain about the fact that it has alredy converted it
        ## FIXME I think I broke this
        build_setup_xml(project, config, sample , libprep.name, seqrun.name)
        command_line = build_piper_cl(project, "dna_alignonly", config)
        return launch_piper_job(command_line, project)
    ## FIXME define exceptions more narrowly
    except Exception as e:
        error_msg = ('Processing project "{}" / sample "{}" / libprep "{}" / '
                     'seqrun "{}" failed: {}'.format(project, sample, libprep, seqrun,
                                                   e.__repr__()))
        LOG.error(error_msg)
        raise


@with_ngi_config
def analyze_sample_run(project, sample, config=None, config_file_path=None):
    """The main method for sample-level analysis.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)

    :returns: The subprocess.Popen object for the process or None if job is finished
    :rtype: subprocess.Popen or None
    :raises RuntimeError: If the process cannot be started
    """
    LOG.info('Determining if we can start sample-level analysis for project "{}" / sample "{}"'.format(project, sample))
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    charon_session = CharonSession()
    try:
        sample_dict = charon_session.sample_get(project.project_id, sample.name)
    except CharonError as e:
        raise RuntimeError('Could not fetch information for project "{}" / '
                           'sample "{}" from Charon database; cannot '
                           'proceed.'.format(project, sample))
    # Check if I can run sample level analysis
    if not sample_dict.get('total_autosomal_coverage'):     # Doesn't exist or is 0
        LOG.info('Sample "{}" from project "{}" not yet sequenced or not yet analyzed'.format(sample, project))
    # If coverage is above 20X we can proceed.
    ## Use Charon validation for this possibly
    elif float(sample_dict.get("total_autosomal_coverage")) > 20.0:
        ## NOTE this may be unnecessary soon/already
        if not sample.dirname.startswith("Sample_"):
            # Switch to Piper naming convention for Sthlm samples
            sample.dirname = "Sample_{}".format(sample.dirname)
        try:
            ## FIXME I think I broke this
            build_setup_xml(project, config, sample)
            command_line = build_piper_cl(project, "merge_process_variantCall", config)
            LOG.info('Executing command line "{}"...'.format(command_line))
            return launch_piper_job(command_line, project)
        ## FIXME define exceptions more narrowly
        except  Exception as e:
            error_msg = 'Processing project "{}" / sample "{}" failed: {}'.format(project, sample, e.__repr__())
            #LOG.error(error_msg)
            raise
    else:
        LOG.info('Insufficient coverage for sample "{}" to start sample-level analysis: '
                 'waiting more data.'.format(sample))


## Your time will come
def convert_sthlm_to_uppsala(project, fcid):
    """Convert projects from Stockholm style (three-level) to Uppsala style
    (two-level) using the sthlm2UUSNP Java utility and produces a
    report.tsv for use as input to Piper.

    :param NGIProject project: The project to be converted.
    :param NGISeqRun fcid: The flowcell ID to be converted.
    """
    # Requires sthlm2UUSNP on PATH
    cl_template = "sthlm2UUSNP -i {input_dir} -o {output_dir} -f {flowcell}"
    LOG.info("Converting Sthlm project \"{}\" to UUSNP format".format(project))
    input_dir = os.path.join(project.base_path, "DATA", project.dirname)
    uppsala_dirname = "{}".format(project.dirname)
    output_dir = os.path.join(project.base_path, "DATA_UUSNP", uppsala_dirname)
    #check if for this flowcell I have already generate the data
    if not os.path.exists(os.path.join(output_dir,fcid.name)):
        com = cl_template.format(input_dir=input_dir, output_dir=output_dir,
                                 flowcell=fcid)
        try:
            subprocess.check_call(shlex.split(com))
        except subprocess.CalledProcessError as e:
            error_msg = ("Unable to convert Sthlm->UU format for "
                         "project {} / flowcell {}: {}".format(project, fcid, e))
            raise RuntimeError(error_msg)
    project.dirname = uppsala_dirname
    project.name = uppsala_dirname
    for sample in project.samples.values():
        # Naming expected by Piper; might consider whether to set sample.name as well
        if not sample.dirname.startswith("Sample_"):
            sample.dirname =  "Sample_{}".format(sample.dirname)
            #sample.name    =  "Sample_{}".format(sample.name)


def launch_piper_job(command_line, project):
    """Launch the Piper command line.

    :param str command_line: The command line to execute
    :param Project project: The Project object (needed to set the CWD)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    cwd = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    ## TODO Would like to log these to the log -- can we get a Logbook filehandle-like object?
    ## TODO add exception handling
    popen_object = execute_command_line(command_line, cwd=cwd)
    return popen_object


def build_piper_cl(project, workflow_name, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param NGIProject project: The project object to analyze.
    :param str workflow_name: The name of the workflow to execute
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
    return cl


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

    if seqrun_id == None:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}"'.format(project, sample.name))
    else:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}", seqrun "{}"'.format(project, sample.name, seqrun_id))

    project_top_level_dir = os.path.join(project.base_path, "DATA_UUSNP", project.dirname)
    analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    if not os.path.exists(analysis_dir):
        safe_makedir(analysis_dir, 0770)

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
        error_msg = ("Could not load required information from"
                     " configuration file and cannot continue with project {}:"
                     " value \"{}\" missing".format(project, e.message))
        raise ValueError(error_msg)

    try:
        cl_args["sfc_binary"] = config['piper']['path_to_setupfilecreator']
    except KeyError:
        # Assume setupFileCreator is on path
        cl_args["sfc_binary"] = "setupFileCreator"


    if seqrun_id == None:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-setup.xml".format(project, sample.name))
    else:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-{}_setup.xml".format(project, sample.name, seqrun_id))

    cl_args["output_xml_filepath"]  = output_xml_filepath
    cl_args["sequencing_tech"]      = "Illumina"

    setupfilecreator_cl = ("{sfc_binary} "
                           "--output {output_xml_filepath} "
                           "--project_name {project} "
                           "--sequencing_platform {sequencing_tech} "
                           "--sequencing_center {sequencing_center} "
                           "--uppnex_project_id {uppmax_proj} "
                           "--reference {reference_path} ".format(**cl_args))
    #NOTE: here I am assuming the different dir structure, it would be wiser to change the object type and have an uppsala project

    if seqrun_id is None:
        #if seqrun_id is none it means I want to create a sample level setup xml
        for libprep in sample:
            for seqrun in libprep:
                sample_directory = os.path.join(project_top_level_dir, seqrun.name, sample.dirname)
                setupfilecreator_cl += " --input_sample {}".format(sample_directory)
    else:
        sample_directory = os.path.join(project_top_level_dir, seqrun_id, sample.dirname)
        setupfilecreator_cl += " --input_sample {}".format(sample_directory)

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


#def create_report_tsv(project):
#    """Generate a tsv-formatted file as input for Piper and write to top level of project,
#    unless a report.xml file exists already (as it will for Uppsala projects).
#    Produces one report.tsv for each project, if the report.xml does not exist.
#
#    This file has the format:
#
#        #SampleName     Lane    ReadLibrary     FlowcellID
#        P567_102        1       A               AH0JYUADXX
#        P567_102        2       B               AH0JYUADXY
#
#    :param NGIProject project: The project to be converted.
#    """
#    report_header = ("#SampleName", "Lane", "ReadLibrary", "FlowcellID")
#
#    report_paths = []
#    report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
#    report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
#    ## TODO I think we might need to replace this file if the project changes
#    ##      -- might be cheapest to just generate a new one every time
#    if os.path.exists(report_xml_path):
#        report_paths.append(report_xml_path)
#        LOG.info("Found preexisting report.xml file for project {project}: " \
#                 "{report_xml}".format(project, report_xml_path))
#
#    #if os.path.exists(report_tsv_path):
#    #    path, orig_filename = os.path.split(report_tsv_path)
#    #    orig_basename, orig_ext = os.path.splitext(orig_filename)
#    #    mv_filename = orig_basename + time.strftime("_%Y-%m-%d_%H:%M:%S") + orig_ext
#    #    mv_path = os.path.join(path, mv_filename)
#    #    LOG.info("Moving preexisting report.tsv file to {}".format(mv_path))
#    #    shutil.move(report_tsv_path, mv_path)
#    with open(report_tsv_path, 'w') as rtsv_fh:
#        report_paths.append(report_tsv_path)
#        LOG.info("Writing {}".format(report_tsv_path))
#        print("\t".join(report_header), file=rtsv_fh)
#        for sample in project:
#            for fcid in sample:
#                fcid_path = os.path.join(project.base_path,
#                                         project.dirname,
#                                         sample.dirname,
#                                         fcid.dirname)
#                for fq_pairname in find_fastq_read_pairs_from_dir(directory=fcid_path).keys():
#                    try:
#                        lane = parse_lane_from_filename(fq_pairname)
#                    except ValueError as e:
#                        LOG.error("Could not get lane from filename for file {} -- skipping ({})".format(fq_pairname, e))
#                        raise ValueError(error_msg)
#                    ## TODO pull from Charon database
#                    read_library = "<NotImplemented>"
#                    print("\t".join([sample.name, lane, read_library, fcid.name]), file=rtsv_fh)
