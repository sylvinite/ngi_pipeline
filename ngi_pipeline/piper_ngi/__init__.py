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
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.filesystem import safe_makedir
from ngi_pipeline.utils import load_modules, execute_command_line
from ngi_pipeline.utils.config import load_xml_config, load_yaml_config
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)


def analyze_flowcell_run(project, sample, libprep, fcid, workflow_name, config_file_path):
    """The main method for analyse flowcells (Run Level).

    :param NGIProject project_to_analyze : The project -- to analyze!!
    :param Sample sample_to_analyze: the sample that need to be analyzed
    :param libprep libprep: which library prep needs to be analysed
    :fcid fcid: gueass.... which fcid need to be analysed
    :param str workflow_name: The workflow (e.g. alignment)
    :param str config_file_path: The path to the configuration file.

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    #Here I am in the Piper World: this means that I can be Engine specific!!!
    config = load_yaml_config(config_file_path)
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    #things to to do
    # 1- convert the current project/sample/libprep/fcid structure into the piper structure
    # 2- build setup_xml specific for this fc run
    # 3- run piper at fc run
    try:
        clean_project_from_tsv_waiting_for_johan_fix(project)
        convert_sthlm_to_uppsala(project) #this converts the entire project, it is not specific to the sample. Not a big deal, it will simply complain about the fact that it has alredy converted it
        build_setup_xml(project, config, sample , libprep.name, fcid.name)
        command_line = build_piper_cl(project, "dna_alignonly", config)
        popen_object = launch_piper_job(command_line, project)
        return popen_object
    
    except Exception as e:
        error_msg = "Processing project {} sample {} fcid {} failed: {}".format(project,
        sample, fcid, e.__repr__())
        LOG.error(error_msg)
        raise

#HOT-FIX this needs to be removed
def clean_project_from_tsv_waiting_for_johan_fix(project):
    project_folder = os.path.join(project.base_path, "DATA_UUSNP", project.dirname)
    if os.path.isdir(project_folder):
        for fcid in  os.listdir(project_folder):
            fcid_dir = os.path.join(project_folder,fcid)
            path_to_tsv = os.path.join(fcid_dir, "report.tsv")
            if os.path.isfile(path_to_tsv):
                os.remove(path_to_tsv)



def analyse_sample_run(sample, project, config_file_path):
    """The main method for analyse samples (sample Level).

    :param CharonSampleEntry sample : Sample to be analysed as Charon entry
    :param NGIProject project : project object reconstructed from Charon (might be incomplete)
    :param str config_file_path: The path to the configuration file
    
    :returns: The subprocess.Popen object for the process or 1 if no process is started
    :rtype: subprocess.Popen
    """

    sample_id = sample["sampleid"]
    config = load_yaml_config(config_file_path)
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    #check if I can run sample level analysis
    if "total_autosomal_coverage" not in sample or sample["total_autosomal_coverage"] == 0.0 :
        LOG.info("Sample {} not yet sequenced or not yet analysed".format(sample_id))
    elif float(sample["total_autosomal_coverage"]) > 20.0:
        #change the sample dir name to the piper sample dir name format
        sampleObj         = project.samples[sample["sampleid"]]
        sampleObj.dirname = "Sample_{}".format(project.samples[sample["sampleid"]].dirname)
        try:
            build_setup_xml(project, config, sampleObj , None, None)
            command_line = build_piper_cl(project, "merge_process_variantCall", config)
            LOG.info("now I only need to run the command: {}".format(command_line))
            popen_object = launch_piper_job(command_line, project)
            return popen_object
        except  Exception as e:
            error_msg = "Processing project {} sample {} failed: {}".format(project, sample, e.__repr__())
            LOG.error(error_msg)
            raise
    else:
        LOG.info("Coverage not reached for sample {}: wait more data".format(sample_id))
    
    return 1


#### I AM NOT USING THIS.....
#def analyze_project(project, workflow_name, config_file_path):
    """The main method for project (samples?).

    :param NGIProject project_to_analyze : The project -- to analyze!!
    :param str workflow_name: The workflow (e.g. alignment, variant calling)
    :param str config_file_path: The path to the configuration file.

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
"""
    config = load_yaml_config(config_file_path)
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    # Valid only for this session
    load_modules(modules_to_load)
   
    try:
        ## Temporary until the directory format switch
        # report.xml is created by sthlm2UUSNP (in convert_sthlm_to_uppsala)
        convert_sthlm_to_uppsala(project)
        build_setup_xml(project, config)
        command_line = build_piper_cl(project, "dna_alignonly", config)

        popen_object = launch_piper_job(command_line, project)
        return popen_object
    except Exception as e:
        error_msg = "Processing project {} failed: {}".format(project, e.__repr__())
        LOG.error(error_msg)
        raise
"""

def convert_sthlm_to_uppsala(project):
    """Convert projects from Stockholm style (three-level) to Uppsala style
    (two-level) using the sthlm2UUSNP Java utility.

    :param NGIProject project: The project to be converted.

    :returns: A list of projects with Uppsala-style directories as attributes.
    :rtype: list
    """
    # Requires sthlm2UUSNP on PATH
    cl_template = "sthlm2UUSNP -i {input_dir} -o {output_dir}"
    LOG.info("Converting Sthlm project {} to UUSNP format".format(project))
    input_dir = os.path.join(project.base_path, "DATA", project.dirname)
    uppsala_dirname = "{}".format(project.dirname)
    output_dir = os.path.join(project.base_path, "DATA_UUSNP", uppsala_dirname)
    com = cl_template.format(input_dir=input_dir, output_dir=output_dir)
    try:
        subprocess.check_call(shlex.split(com))
    except subprocess.CalledProcessError as e:
        error_msg = ("Unable to convert Sthlm->UU format for "
                     "project {}: {}".format(project, e))
        LOG.error(error_msg)
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
        LOG.error(error_msg)
        raise ValueError(error_msg)

    # Find Piper QScripts dir:
    #   Check environmental variable PIPER_QSCRIPTS_DIR
    #   then the config file
    piper_qscripts_dir = (os.environ.get("PIPER_QSCRIPTS_DIR") or
                          config['piper']['path_to_piper_qscripts'])
    if not piper_qscripts_dir:
        error_msg = ("Could not find Piper QScripts directory in config file or "
                    "as environmental variable (\"PIPER_QSCRIPTS_DIR\").")
        LOG.error(error_msg)
        raise ValueError(error_msg)

    LOG.info("Building workflow command line(s) for "
             "project {} and workflow {}".format(project, workflow_name))
    ## NOTE This key will probably exist on the project level, and may have multiple values.
    ##      Workflows may imply a number of substeps (e.g. basic = qc, alignment, etc.) ?
    try:
        setup_xml_path = project.setup_xml_path
    except AttributeError:
        error_msg = ("Project {} has no setup.xml file. Skipping project "
                     "command-line generation.".format(project))
        LOG.error(error_msg)
        raise ValueError(error_msg)

    
    cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                          qscripts_dir_path=piper_qscripts_dir,
                                          setup_xml_path=setup_xml_path,
                                          global_config_path=piper_global_config_path,
                                          output_dir=project.analysis_dir)
    return cl


def build_setup_xml(project, config, sample = None, libprep_id = None, fcid_id = None):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param NGIProject project: The project to be converted.
    :param dict config: The (parsed) configuration file for this machine/environment.
    :param SampleOgj sample: the sample object
    :param library_id: id of the library
    :fcid_id: flowcell identifier

    :returns: A list of Project objects with setup.xml paths as attributes.
    :rtype: list
    """
    
    if fcid_id == None:
        LOG.info("Building Piper setup.xml file for project {} sample {}".format(project, sample.name))
    else:
        LOG.info("Building Piper setup.xml file for project {} sample {}, fcid {}".format(project,
            sample.name, fcid_id))
    project_top_level_dir = os.path.join(project.base_path, "DATA_UUSNP", project.dirname)
    if not os.path.exists(os.path.join(project.base_path,"ANALYSIS", project.dirname)):
        safe_makedir(os.path.join(project.base_path,"ANALYSIS", project.dirname), 0770)

    analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)

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
        LOG.error(error_msg)
        raise ValueError(error_msg)

    try:
        cl_args["sfc_binary"] = config['piper']['path_to_setupfilecreator']
    except KeyError:
        # Assume setupFileCreator is on path
        cl_args["sfc_binary"] = "setupFileCreator"


    if fcid_id == None:
        output_xml_filepath = os.path.join( analysis_dir,
                                        "{}_{}_setup.xml".format(project, sample.name))
    else:
        output_xml_filepath = os.path.join( analysis_dir,
                                        "{}_{}_{}_setup.xml".format(project, sample.name, fcid_id))

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

    if fcid_id is None:
        #if fcid_id is none it means I want to create a sample level setup xml
        for libprep in sample:
            for fcid in libprep:
                sample_directory = os.path.join(project_top_level_dir, fcid.name, sample.dirname)
                setupfilecreator_cl += " --input_sample {}".format(sample_directory)
    else:
        sample_directory = os.path.join(project_top_level_dir, fcid_id, sample.dirname)
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
        LOG.error(error_msg)
        raise RuntimeError(error_msg)


def create_report_tsv(project):
    """Generate a tsv-formatted file as input for Piper and write to top level of project,
    unless a report.xml file exists already (as it will for Uppsala projects).
    Produces one report.tsv for each project, if the report.xml does not exist.

    This file has the format:

        #SampleName     Lane    ReadLibrary     FlowcellID
        P567_102        1       A               AH0JYUADXX
        P567_102        2       B               AH0JYUADXY

    :param NGIProject project: The project to be converted.
    """
    report_header = ("#SampleName", "Lane", "ReadLibrary", "FlowcellID")

    report_paths = []
    report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
    report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
    ## TODO I think we might need to replace this file if the project changes
    ##      -- might be cheapest to just generate a new one every time
    if os.path.exists(report_xml_path):
        report_paths.append(report_xml_path)
        LOG.info("Found preexisting report.xml file for project {project}: " \
                 "{report_xml}".format(project, report_xml_path))

    #if os.path.exists(report_tsv_path):
    #    path, orig_filename = os.path.split(report_tsv_path)
    #    orig_basename, orig_ext = os.path.splitext(orig_filename)
    #    mv_filename = orig_basename + time.strftime("_%Y-%m-%d_%H:%M:%S") + orig_ext
    #    mv_path = os.path.join(path, mv_filename)
    #    LOG.info("Moving preexisting report.tsv file to {}".format(mv_path))
    #    shutil.move(report_tsv_path, mv_path)
    with open(report_tsv_path, 'w') as rtsv_fh:
        report_paths.append(report_tsv_path)
        LOG.info("Writing {}".format(report_tsv_path))
        print("\t".join(report_header), file=rtsv_fh)
        for sample in project:
            for fcid in sample:
                fcid_path = os.path.join(project.base_path,
                                         project.dirname,
                                         sample.dirname,
                                         fcid.dirname)
                for fq_pairname in find_fastq_read_pairs_from_dir(directory=fcid_path).keys():
                    try:
                        lane = parse_lane_from_filename(fq_pairname)
                    except ValueError as e:
                        LOG.error("Could not get lane from filename for file {} -- skipping ({})".format(fq_pairname, e))
                        raise ValueError(error_msg)
                    ## TODO pull from Charon database
                    read_library = "<NotImplemented>"
                    print("\t".join([sample.name, lane, read_library, fcid.name]), file=rtsv_fh)




