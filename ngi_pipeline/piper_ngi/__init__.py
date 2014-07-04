"""The Piper automated launcher script.

For each project directory, this script needs to:
    1. Build a report.tsv file detailing all the files
    2. Build a runconfig.xml file using the automated thinger
    3. Launch the Piper job via the sbatch file
    4. Track the jobs somehow
"""
from __future__ import print_function

import collections
import os
import re
import shlex
import shutil
import subprocess
import time

from ngi_pipeline.common import parse_lane_from_filename, find_fastq_read_pairs, \
                                get_flowcell_id_from_dirtree
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.piper_ngi import workflows
from ngi_pipeline.utils import load_modules
from ngi_pipeline.utils.config import load_xml_config, load_yaml_config

LOG = minimal_logger(__name__)

def main(projects_to_analyze, config_file_path):
    """The main method.

    :param list flowcell_dirs_to_analyze: A lst of flowcell directories containing fastq files to analyze.
    :param str config_file_path: The path to the configuration file.
    """
    config = load_yaml_config(config_file_path)
    ## Problem with module system java version loading at the moment
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    ## Possibly the Java error could be non-fatal if it's available on PATH
    # Valid only for this session
    load_modules(modules_to_load)
    create_report_tsv(projects_to_analyze)
    convert_sthlm_to_uppsala(projects_to_analyze)
    build_setup_xml(projects_to_analyze, config)
    # Temporary until the file format switch
    build_piper_cl(projects_to_analyze, config)
    launch_piper_jobs(projects_to_analyze)
    # Need to write workflow status to database under relevant heading!


def execute_command_line(cl, stdout=None, stderr=None, cwd=None):
    """Execute a command line and return the PID.

    :param cl: Can be either a list or a string, if string, gets shlex.splitted
    :param file stdout: The filehandle destination for STDOUT (can be None)
    :param file stderr: The filehandle destination for STDERR (can be None)
    :param str cwd: The directory to be used as CWD for the process launched

    :returns: Process ID of launched process
    :rtype: str

    :raises RuntimeError: If the OS command-line execution failed.
    """
    if cwd and not os.path.isdir(cwd):
        LOG.warn("CWD specified, \"{}\", is not a valid directory for "
                 "command \"{}\". Setting to None.".format(cwd, cl))
        cwd = None
    if type(cl) is str:
        cl = shlex.split(cl)
    LOG.info("Executing command line: {}".format(" ".join(cl)))
    try:
        p_handle = subprocess.Popen(cl, stdout = stdout,
                                        stderr = stderr,
                                        cwd = cwd)
        error_msg = None
    except OSError:
        error_msg = ("Cannot execute command; missing executable on the path? "
                     "(Command \"{}\")".format(command_line))
    except ValueError:
        error_msg = ("Cannot execute command; command malformed. "
                     "(Command \"{}\")".format(command_line))
    except subprocess.CalledProcessError as e:
        error_msg = ("Error when executing command: \"{}\" "
                     "(Command \"{}\")".format(e, command_line))
    if error_msg:
        raise RuntimeError(error_msg)
    return p_handle.pid


def symlink_convert_file_names(projects_to_analyze):
    for project in projects_to_analyze:
        for sample in project:
            for fcid in sample:
                for fastq in fcid:
                    m = re.match(r'(?P<sample_name>P\d+_\d+)_(?P<index>\w+)_L\d{2}(?P<lane_num>\d)_(?P<read>R\d)_.*(?P<ext>fastq.*)', fastq)
                    args_dict = m.groupdict()
                    args_dict.update({"date_fcid": fcid.name})
                    scilifelab_named_file = "{lane_num}_{date_fcid}_{sample_name}_{read}.{ext}".format(**args_dict)
                    try:
                        fcid_path =  os.path.join(project.base_path,
                                                 project.dirname,
                                                 sample.dirname,
                                                 fcid.dirname,)
                        src_fastq = os.path.join(fcid_path, fastq)
                        dst_fastq = os.path.join(fcid_path, scilifelab_named_file)
                        os.symlink(src_fastq, dst_fastq)
                    except OSError:
                        pass

def convert_sthlm_to_uppsala(projects_to_analyze):
    """Convert projects from Stockholm style (three-level) to Uppsala style
    (two-level) using the sthlm2UUSNP Java utility.

    :param list projects_to_analyze: The projects to be converted.

    :returns: A list of projects with Uppsala-style directories as attributes.
    :rtype: list
    """
    #for project in projects_to_analyze:
    #    uu_project_name = "{}_UUSNP".format(project.dirname)
    #    uu_project_dir = os.path.join(project.base_path, uu_project_name)
    #    try:
    #        os.mkdir(uu_project_dir)
    #    except OSError:
    #        pass
    #    for sample in project:
    #        for fcid in sample:
    #            symlink_dst_path = os.path.join(uu_project_dir, fcid.dirname)
    #            symlink_src_path = os.path.join(project.base_path,
    #                                            project.dirname,
    #                                            sample.dirname,
    #                                            fcid.dirname)
    #            try:
    #                os.symlink(symlink_src_path, symlink_dst_path)
    #            except OSError:
    #                pass
    # Requires sthlm2UUSNP on PATH
    symlink_convert_file_names(projects_to_analyze)
    cl_template = "sthlm2UUSNP -i {input_dir} -o {output_dir}"
    for project in projects_to_analyze:
        LOG.info("Converting Sthlm project {} to UUSNP format".format(project))
        input_dir = os.path.join(project.base_path, project.dirname)
        uppsala_dirname = "{}_UUSNP".format(project.dirname)
        output_dir = os.path.join(project.base_path, uppsala_dirname)
        cl = cl_template.format(input_dir=input_dir, output_dir=output_dir)
        try:
            subprocess.check_call(shlex.split(cl))
        except subprocess.CalledProcessError as e:
            error_msg = ("Unable to convert Sthlm->UU format for "
                         "project {}: {}".format(project, e))
            LOG.error(error_msg)
            continue
        read_tsv_src = os.path.join(project.base_path, project.dirname, "report.tsv")
        read_tsv_dst = os.path.join(project.base_path, uppsala_dirname, "report.tsv")
        project.dirname = uppsala_dirname
        #read_xml_original = os.path.join(project_base_dir, "report.xml")
        project.dirname = uppsala_dirname
        project.name = uppsala_dirname
        try:
            shutil.copy(read_tsv_src, read_tsv_dst)
        except shutil.Error:
            # Fuckit
            pass


def launch_piper_jobs(projects_to_analyze):
    for project in projects_to_analyze:
        cwd = os.path.join(project.base_path, project.dirname)
        for command_line in project.command_lines:
            ## Would like to log these to the log
            pid = execute_command_line(command_line, cwd=cwd)


def build_piper_cl(projects_to_analyze, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param list projects_to_analyze: A list of Project objects to analyze.
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    try:
        # Default is the file globalConfig.xml in the piper root dir
        try:
            piper_globalconfig_path = config.get("piper", {}).get("path_to_piper_globalconfig")
        except KeyError:
            path_to_piper_rootdir = config['piper']['path_to_piper_rootdir']
            # Default is the file globalConfig.xml in the piper root dir
            piper_globalconfig_path = os.path.join(path_to_piper_rootdir, "globalConfig.xml")
        path_to_piper_globalconfig = config['piper']['path_to_piper_globalconfig']
        path_to_piper_qscripts = config['piper']['path_to_piper_qscripts']
    except KeyError as e:
        error_msg = "Could not load key \"{}\" from config file; " \
                    "cannot continue.".format(e)
        LOG.error(error_msg)
        raise ValueError(error_msg)
    for project in projects_to_analyze:
        LOG.info("Building workflow command lines for project {}".format(project))
        ## For NGI, all projects will go through the same workflows;
        ## later, we'll want to let some database values determine this.

        ## Once the coverage is high enough (check database), we'll also
        ## need to put them through e.g. the GATK

        ## We'll want to make this a generic value in the database ("QC", "DNAAlign", "VariantCalling", etc.)
        ##  and then map to the correct script in the config file. This way we can execute the same pipelines
        ##  for any of the engines

        ## This key will probably exist on the project level, and may have multiple values.
        ## Workflows may imply a number of substeps (e.g. qc, alignment, etc.)
        # workflows_for_project = proj_db.get("workflows") or something like that
        generic_workflow_names_for_project = ["dna_alignonly"]
        try:
            setup_xml_path = project.setup_xml_path
        except AttributeError:
            LOG.warn("Project {} has no setup.xml file. Skipping project "
                     "command-line generation.".format(project))
            continue
        for workflow_name in generic_workflow_names_for_project:
            cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                                  qscripts_dir_path=path_to_piper_qscripts,
                                                  setup_xml_path=setup_xml_path,
                                                  global_config_path=piper_globalconfig_path)
            project.command_lines.append(cl)


def build_setup_xml(projects_to_analyze, config):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param list projects_to_analyze: A list of Project objects to analyze.
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with setup.xml paths as attributes.
    :rtype: list
    """
    for project in projects_to_analyze:
        LOG.info("Building Piper setup.xml file for project {}".format(project))
        project_top_level_dir = os.path.join(project.base_path, project.dirname)
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
            ## TODO Put some useful thing (code??) here
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
            continue

        try:
            cl_args["sfc_binary"] = config['piper']['path_to_setupfilecreator']
        except KeyError:
            # Assume setupFileCreator is on path
            cl_args["sfc_binary"] = "setupFileCreator"

        output_xml_filepath = os.path.join( project_top_level_dir,
                                            "{}_setup.xml".format(project))
        cl_args["output_xml_filepath"] = output_xml_filepath
        cl_args["sequencing_tech"] = "Illumina"

        setupfilecreator_cl = ("{sfc_binary} "
                               "--output {output_xml_filepath} "
                               "--project_name {project} "
                               "--sequencing_platform {sequencing_tech} "
                               "--sequencing_center {sequencing_center} "
                               "--uppnex_project_id {uppmax_proj} "
                               "--reference {reference_path}".format(**cl_args))
        for sample in project.samples.values():
            ## TODO temporary fix before the directory format switch
            #sample_directory = os.path.join(project_top_level_dir, sample.dirname)
            for fcid in sample:
                sample_directory = os.path.join(project_top_level_dir, fcid.dirname)
                setupfilecreator_cl += " --input_sample {}".format(sample_directory)

        try:
            LOG.info("Executing command line: {}".format(setupfilecreator_cl))
            subprocess.check_call(shlex.split(setupfilecreator_cl))
            project.setup_xml_path = output_xml_filepath
        except (subprocess.CalledProcessError, OSError, ValueError) as e:
            error_msg = ("Unable to produce setup XML file for project {}; "
                         "skipping project analysis. "
                         "Error is: \"{}\". .".format(project, e))
            LOG.error(error_msg)
            # There can be multiple projects else we would just raise an Exception
            continue
    return projects_to_analyze


def create_report_tsv(projects_to_analyze):
    """Generate a tsv-formatted file as input for Piper and write to top level of project,
    unless a report.xml file exists already (as it will for Uppsala projects).
    Produces one report.tsv for each project, if the report.xml does not exist.

    This file has the format:

        #SampleName     Lane    ReadLibrary     FlowcellID
        P567_102        1       A               AH0JYUADXX
        P567_102        2       B               AH0JYUADXY

    :param list projects_to_analyze: The list of flowcell directories
    """
    report_header = ("#SampleName", "Lane", "ReadLibrary", "FlowcellID")

    report_paths = []
    for project in projects_to_analyze:
        report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
        report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
        ## TODO I think we might need to replace this file if the project changes
        ##      -- might be cheapest to just generate a new one every time
        if os.path.exists(report_xml_path):
            report_paths.append(report_xml_path)
            LOG.info("Found preexisting report.xml file for project {project}: " \
                     "{report_xml}".format(project, report_xml_path))
            continue

        ## TODO Activate this check/move thing later
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
                    # How to determine lanes, readlibraries?
                    fcid_path = os.path.join(project.base_path,
                                             project.dirname,
                                             sample.dirname,
                                             fcid.dirname)
                    for fq_pairname in find_fastq_read_pairs(directory=fcid_path).keys():
                        try:
                            lane = parse_lane_from_filename(fq_pairname)
                        except ValueError as e:
                            LOG.error("Could not get lane from filename for file {} -- skipping ({})".format(fq_pairname, e))
                            continue
                        read_library = "<NotImplemented>"
                        print("\t".join([sample.name, lane, read_library, fcid.name]), file=rtsv_fh)
