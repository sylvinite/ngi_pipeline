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

from scilifelab_pipeline.log import minimal_logger
from scilifelab_pipeline.utils.config import load_xml_config, load_yaml_config
from scilifelab_pipeline.common import parse_lane_from_filename, \
                                       find_fastq_read_pairs, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)

def main(projects_to_analyze, config_file_path):
    """The main method.

    :param list flowcell_dirs_to_analyze: A lst of flowcell directories containing fastq files to analyze.
    :param str config_file_path: The path to the configuration file.
    """
    config = load_yaml_config(config_file_path)
    create_report_tsv(projects_to_analyze)
    build_setup_xml(projects_to_analyze, config)
    build_piper_cl(projects_to_analyze, config)
    ## Run Johan's converter script if needed
    ## Decide how to track jobs that are running -- write status files? A flat file?
    launch_piper_jobs(projects_to_analyze)


def launch_piper_jobs(projects_to_analyze):
    for project in projects_to_analyze:
        for command_line in projects.command_lines:
            parsed_cl = shlex.split(command_line)
            p_handle = subprocess.Popen(parsed_cl, stdin = subprocess.PIPE,
                                                  stdout = subprocess.PIPE)
            p_stdin, p_stdout = p_handle.communicate()
            ## TODO more stuff


def build_piper_cl(projects_to_analyze, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param list projects_to_analyze: A list of Project objects to analyze.
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    try:
        path_to_piper_rootdir = config['piper']['path_to_piper_rootdir']
        path_to_piper_globalconfig = config['piper']['path_to_piper_globalconfig']
        path_to_piper_qscripts = config['piper']['path_to_piper_qscripts']
    except KeyError as e:
        error_msg = "Could not load key \"{}\" from config file; " \
                    "cannot continue.".format(e)
        LOG.error(error_msg)
        raise ValueError(error_msg)

    # Default is the file globalConfig.xml in the piper root dir
    piper_globalconfig_path = config.get("piper", {}).get("path_to_piper_globalconfig") \
                                 or os.path.join(path_to_piper_rootdir, "globalConfig.xml")
    #if not os.path.isfile(piper_globalconfig_path):
    #    raise IOError("\"{}\" is not a file (need global configuration file).".format(piper_globalconfig_path))
    #else:
    ## Change this to read XML configs (after changing config to XML)
    piper_globalconfig = load_xml_config(piper_globalconfig_path)

    for project in projects_to_analyze:
        ## For NGI, all projects will go through the same workflows;
        ## later, we'll want to let some database values determine this.

        ## Once the coverage is high enough (check database), we'll also
        ## need to put them through e.g. the GATK

        ## We'll want to make this a generic value in the database ("QC", "DNAAlign", "VariantCalling", etc.)
        ##  and then map to the correct script in the config file. This way we can execute the same pipelines
        ##  for any of the engines
        # workflows_for_project = proj_db.get("workflows") or something like that
        generic_workflow_names_for_project = ("dna_alignonly")

        setup_xml_path = project.setup_xml_path
        for workflow_name in generic_workflow_names_for_project:
            LOG.info("Building command line for project {}, " \
                     "workflow {}".format(project, workflow_name))
            workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                             path_to_qscripts=path_to_piper_qscripts,
                                             setup_xml_path=setup_xml_path,
                                             global_config=piper_globalconfig)
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
            reference_genome = 'hg19'
            # sequencing_center = proj_db.get('Sequencing Center')
            cl_args["sequencing_center"] = "NGI"
        except:
            ## TODO Put some useful thing (code??) here
            pass

        # Load needed data from configuration file
        try:
            cl_args["reference_path"] = config['supported_genomes'][reference_genome]
            cl_args["uppmax_proj"] = config['environment']['project_id']
            cl_args["path_to_sfc"] = config['environment']['path_to_setupfilecreator']
        except KeyError as e:
            error_msg = "Could not load required information from" \
                        " configuration file and cannot continue with project {}:" \
                        " value \"{}\" missing".format(project, e.message)
            LOG.error(error_msg)
            continue

        output_xml_filepath = os.path.join( project_top_level_dir,
                                            "{}_setup.xml".format(project))
        cl_args["output_xml_filepath"] = output_xml_filepath
        cl_args["sequencing_tech"] = "Illumina"

        setupfilecreator_cl = "{path_to_sfc} " \
                              "--output {output_xml_filepath} " \
                              "--project_name {project} " \
                              "--sequencing_platform {sequencing_tech} " \
                              "--sequencing_center {sequencing_center} " \
                              "--uppnex_project_id {uppmax_proj} " \
                              "--reference {reference_path}".format(**cl_args)
        for sample in project.samples.values():
            sample_directory = os.path.join(project_top_level_dir, sample.dirname)
            setupfilecreator_cl += " --input_sample {}".format(sample_directory)

        try:
            subprocess.check_call(shlex.split(setupfilecreator_cl))
            project.setup_xml_path = output_xml_filepath
        except (subprocess.CalledProcessError, OSError, ValueError) as e:
            error_msg = "Unable to produce setup XML file for project {}: \"{}\". Skipping project analysis.".format(project, e.message)
            LOG.error(error_msg)
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
                        lane = parse_lane_from_filename(fq_pairname)
                        read_library = "Get this from the database somehow"
                        print("\t".join([sample.name, lane, read_library, fcid.name]), file=rtsv_fh)
