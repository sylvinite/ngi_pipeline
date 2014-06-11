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
import subprocess
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
    report_paths = create_report_tsv(projects_to_analyze)
    #setup_xml_paths = build_setup_xml(projects_to_analyze, config=config)
    # This is actually an unnecessary namespace assignment as the original
    # list will be edited inside the function, but it's clearer this way
    projects_to_analyze = build_setup_xml(projects_to_analyze, config)
    projects_to_analyze = build_piper_cl(projects_to_analyze, config)

    # Run Johan's converter script if needed
    # Build command line
    # Write sbatch file including command line
    # Queue sbatch file

    # Decide how to track jobs that are running -- write status files? A flat file?


def piper_launch(cl_list):
    ### Need to load modules:
    ## module load java/sun_jdk1.7.0_25
    ## module load R/2.15.0
    ### Also need to source globalConfig.sh
    pass


def build_piper_cl(projects_to_analyze, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param list projects_to_analyze: A list of Project objects to analyze.
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises RuntimeError: If a fatal error occurs (missing config value, unreadable files)
    """
    try:
        path_to_qscripts = config['environment']['path_to_qscripts']
        path_to_piper_topdir = config['environment']['path_to_piper_topdir']
    except KeyError as e:
        error_msg = "Could not load key \"{}\" from config file; " \
                    "cannot continue.".format(e)
        LOG.error(error_msg)
        raise RuntimeError(error_msg)

    # Default is the file globalConfig.sh in the piper root dir
    path_to_piper_globalconfig = config.get("environment", {}).get("path_to_piper_globalconfig") \
                                 or os.path.join(path_to_piper_topdir, "globalConfig.sh")
    if not os.path.isfile(path_to_piper_globalconfig):
        raise IOError("\"{}\" is not a file (need global configuration file).".format(path_to_piper_globalconfig))

    for project in projects_to_analyze:
        ## For NGI, all projects will go through the same workflows;
        ## later, we'll want to let some database values determine this.

        ## Once the coverage is high enough (check database), we'll also
        ## need to put them through e.g. the GATK
        
        ## Need QC Pipeline
        ## We'll want to make this a generic value in the database ("QC", "DNAAlign", "VariantCalling", etc.)
        ##  and then map to the correct script in the config file. This way we can execute the same pipelines
        ##  for any of the engines

        ## --> Later we'll probably want to move this step farther up in the command flow and the
        ##      workflow chosen will decide which engine to use!!!
        # workflows_for_project = proj_db.get("workflows") or something like that
        workflow_generic_names_for_project = ("dna_alignonly")
        proj_workflow_qscripts = {}
        for workflow_name in workflow_generic_names_for_project:
            try:
                # Translate generic workflow names to Piper-specific qscript paths
                qscript_path = config["piper_qscripts"][workflow]
                workflow_qscripts
                project.add_workflow(workflow_name, qscript_path)
            except KeyError:
                error_msg = "Could not get QScript path for workflow {} " \
                            "in project {}; skipping.".format(workflow_name, project)
                LOG.error(error_msg)
                continue


        for workflow_name, qscript_path in project.workflows.items():
            cl = "source {


def build_setup_xml(projects_to_analyze, config):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

      -x | --interactive
            This is a optional argument.
      -o Output xml file. | --output Output xml file.
            This is a required argument.
      -p The name of this project. | --project_name The name of this project.
            This is a required argument if you are not using interactive mode.
      -s The technology used for sequencing, e.g. Illumina | --sequencing_platform The technology used for sequencing, e.g. Illumina
            This is a required argument if you are not using interactive mode.
      -c Where the sequencing was carried out, e.g. NGI | --sequencing_center Where the sequencing was carried out, e.g. NGI
            This is a required argument if you are not using interactive mode.
      -a The uppnex project id to charge the core hours to. | --uppnex_project_id The uppnex project id to charge the core hours to.
            This is a required argument if you are not using interactive mode.
      -i Input path to sample directory. | --input_sample Input path to sample directory.
            his is a required argument if you are not using interactive mode. Can be specified multiple times.
      -r Reference fasta file to use. | --reference Reference fasta file to use.
            This is a required argument if you are not using interactive mode.

    :param list projects_to_analyze: A list of Project objects to analyze.
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with setup.xml paths as attributes.
    :rtype: list
    """
    #setup_xml_files = []
    for project in projects_to_analyze:
        project_top_level_dir = os.path.join(project.base_path, project.dirname)
        cl_args = {}

        # Load needed data from database
        ## Maybe write a separate function for this
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

            ###
            # TO THE LAUNCH FUNCTION
            # prep_method = proj_db.get('Library Preparation Method')
            prep_method = 'Standard DNA'
            ###

        except:
            ## TODO Put some useful thing (code??) here
            pass

        # Load needed data from configuration file
        ## maybe write a separate function for this
        try:
            cl_args["reference_path"] = config['supported_genomes'][reference_genome]
            cl_args["uppmax_proj"] = config['environment']['project_id']
            cl_args["path_to_sfc"] = config['environment']['path_to_setupfilecreator']

            ##
            # ALL THIS GOES IN THE LAUNCH FUNCTION
            workflows = config['method_to_workflow_mappings'][prep_method]
            workflow_templates = []
            for workflow in workflows:
                try:
                    # Map the workflow name to the location of the workflow sbatch file
                    workflow_templates.append(config['workflow_templates'][workflow])
                except KeyError:
                    # This will automatically continue to the next workflow in the list after printing
                    error_msg = "No workflow template available for workflow \"{}\"; " \
                                " skipping.".format(workflow)
                    LOG.error(error_msg)
            ##

        except KeyError as e:
            error_msg = "Could not load required information from " \
                        " configuration file and cannot continue with project {}:" \
                        " value \"{}\" missing".format(project, e.message)
            LOG.error(error_msg)
            continue

        cl_args["output_xml_filepath"] = os.path.join( project_top_level_dir,
                                                       "{}_setup.xml".format(project))
        cl_args["sequencing_tech"] = "Illumina"

        # Needs java on path? Load java 1.7 module
        setupfilecreator_cl = "{path_to_sfc} " \
                              "-o {output_xml_filepath} " \
                              "-p {project} " \
                              "-s {sequencing_tech} " \
                              "-c {sequencing_center} " \
                              "-a {uppmax_proj} " \
                              "-r {reference_path}".format(**cl_args)
        for sample in projects.samples.values():
            sample_directory = os.path.join(project_top_level_dir, sample.dirname)
            setupfilecreator_cl += " -s {}".format(sample_directory)

        try:
            subprocess.check_call(shlex.split(setupfilecreator_cl))
            project.setup_xml = output_xml_filepath
            #setup_xml_files.append(output_xml_filepath)
        except (subprocess.CalledProcessError, OSError, ValueError) as e:
            error_msg = "Unable to produce setup XML file for project {}: \"{}\"".format(project, e.message)
            LOG.error(error_msg)
            continue
    #return setup_xml_files
    return projects_to_analyze


def create_report_tsv(projects_to_analyze):
    """Generate a tsv-formatted file as input for Piper and write to top level of project,
    unless a report.xml file exists already (as it will for Uppsala projects).
    Produces one report.tsv for each project, if the report.xml does not exist.

    This file has the format:

        #SampleName     Lane    ReadLibrary     FlowcellID
        P567_102        1       WGS     AH0JYUADXX
        P567_102        2       WGS     AH0JYUADXX

    :param list projects_to_analyze: The list of flowcell directories
    :returns: The path to the report.tsv files.
    :rtype: list
    """
    report_header = ("#SampleName", "Lane", "ReadLibrary", "FlowcellID")

    report_paths = []
    for project in projects_to_analyze:
        report_tsv_path = os.path.join(project.base_path, project.name, "report.tsv")
        report_xml_path = os.path.join(project.base_path, project.name, "report.xml")
        if os.path.exists(report_xml_path):
            report_paths.append(report_xml_path)
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
    return report_paths
