"""Piper workflow-specific code."""

import os
import sys

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.slurm import slurm_time_to_seconds

LOG = minimal_logger(__name__)


PIPER_CL_TEMPLATE = ("piper -S {workflow_qscript_path}"
                     " --xml_input {setup_xml_path}"
                     " --global_config {global_config_path}"
                     " --number_of_threads {num_threads}"
                     " --scatter_gather {scatter_gather}"
                     " -jobRunner {job_runner}"
                     " --job_walltime {job_walltime}"
                     " --disableJobReport"
                     " -run")


def get_subtasks_for_level(level):
    """For a given level (e.g. "sample"), get all the associated
    subtasks that should be run (e.g. "qc", "merge_process_variantcall")

    :param str level: The level (e.g. "sample")
    :returns: The names (strings) of the workflows that should be run at that level
    :rtype: tuple
    """
    if level == "sample":
        return ("merge_process_variantcall",)
    elif level == "genotype":
        return ("genotype_concordance",)
    else:
        LOG.error('The level "{}" has no associated subtasks.')
        return []


@with_ngi_config
def return_cl_for_workflow(workflow_name, qscripts_dir_path, setup_xml_path, global_config_path,
                           output_dir=None, exec_mode="local", genotype_file=None,
                           config=None, config_file_path=None):
    """Return an executable-ready Piper command line.

    :param str workflow_name: The name of the Piper workflow to be run.
    :param str qscripts_dir_path: The path to the directory containing the qscripts
    :param str setup_xml_path: The path to the project-level setup XML file
    :param dict global_config_path: The parsed Piper-specific globalConfig file.
    :param str output_dir: The directory to which to write output files
    :param str exec_mode: "local" or "sbatch"
    :param str genotype_file: The path to the genotype file (only relevant for genotype workflow)

    :returns: The Piper command line to be executed.
    :rtype: str
    :raises NotImplementedError: If the workflow requested has not been implemented.
    """
    workflow_fn_name = "workflow_{}".format(workflow_name)
    # Get the local function if it exists
    try:
        workflow_function = getattr(sys.modules[__name__], workflow_fn_name)
    except AttributeError as e:
        error_msg = "Workflow \"{}\" has no associated function.".format(workflow_fn_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
    LOG.info('Building command line for workflow "{}"'.format(workflow_name))
    return workflow_function(qscripts_dir_path=qscripts_dir_path,
                             setup_xml_path=setup_xml_path,
                             global_config_path=global_config_path,
                             config=config, exec_mode=exec_mode,
                             genotype_file=genotype_file,
                             output_dir=output_dir) 

#def workflow_dna_alignonly(*args, **kwargs):
#    """Return the command line for basic DNA Alignment.
#
#    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
#    :param str setup_xml_path: The path to the setup.xml file.
#    :param dict global_config_path: The path to the Piper-specific globalConfig file.
#
#    :returns: The Piper command to be executed.
#    :rtype: str
#    """
#    # Same command line but with one additional option
#    return workflow_dna_variantcalling(*args, **kwargs) + " --alignment_and_qc" + " --retry_failed 1"


def workflow_merge_process_variantcall(*args, **kwargs):
    """Return the command line for best practice analysis: merging, procesing and variant calling.

    :param str qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param str global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    # Same command line but with some additional options
    return workflow_dna_variantcalling(*args, **kwargs) +  \
            (" --variant_calling "
             "--analyze_separately --retry_failed 1")


def workflow_dna_variantcalling(qscripts_dir_path, setup_xml_path, global_config_path,
                                config, exec_mode, output_dir=None, *args, **kwargs):
    """Return the command line for DNA Variant Calling.

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param str global_config_path: The path to the Piper-specific globalConfig file.
    :param dict config: The parsed ngi_pipeline config file
    :param str output_dir: The path to the desired output directory

    :returns: The Piper command to be executed.
    :rtype: str
    """
    cl_string = PIPER_CL_TEMPLATE
    workflow_qscript_path = os.path.join(qscripts_dir_path, "DNABestPracticeVariantCalling.scala")
    job_walltime = slurm_time_to_seconds(config.get("slurm", {}).get("time") or "4-00:00:00")
    if output_dir:
        cl_string += " --output_directory {output_dir}"
    job_native_args = config.get("piper", {}).get("jobNative")
    if job_native_args:
        # This should be a list
        if type(job_native_args) is not list:
            LOG.warn('jobNative arguments in config file specified in invalid '
                     'format; should be list. Ignoring these parameters.')
        else:
            cl_string += " -jobNative {}".format(" ".join(job_native_args))
    if exec_mode == "sbatch":
        # Execute from within an sbatch file (run jobs on the local node)
        num_threads = int(config.get("piper", {}).get("threads") or 16)
        job_runner = config.get("piper", {}).get("shell_jobrunner") or \
                     "ParallelShell --super_charge --ways_to_split 4 --job_scatter_gather_directory $SNIC_TMP/scatter_gather"
        scatter_gather = 1
    else: # exec_mode == "local"
        # Start a local process that sbatches jobs
        job_runner = "Drmaa"
        scatter_gather = 23
        num_threads = 1
    return cl_string.format(**locals())


def workflow_genotype_concordance(qscripts_dir_path, setup_xml_path,
                                  global_config_path, genotype_file,
                                  config, output_dir=None, *args, **kwargs):
    """Return the command line for genotype concordance checking.

    :param str qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file
    :param str global_config_path: The path to the Piper-specific globalConfig file.
    :param str genotype_file: The path to the genotype VCF file
    :param dict config: The parsed ngi_pipeline config file
    :param str output_dir: The path to the desired output directory
    """
    cl_string = PIPER_CL_TEMPLATE
    workflow_qscript_path = os.path.join(qscripts_dir_path, "DNABestPracticeVariantCalling.scala")
    job_walltime = slurm_time_to_seconds(config.get("slurm", {}).get("time") or "4-00:00:00")
    num_threads = int(config.get("piper", {}).get("threads") or 8)
    job_runner = "Shell"
    scatter_gather = 1
    if output_dir:
        cl_string += " --output_directory {output_dir}"
    cl_string += " --alignment_and_qc"
    cl_string += " --retry_failed 2"
    cl_string += " --genotypes {}".format(genotype_file)
    return cl_string.format(**locals())
