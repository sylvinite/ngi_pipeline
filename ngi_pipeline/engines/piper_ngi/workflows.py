"""Piper workflow-specific code."""

import os
import sys

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.slurm import slurm_time_to_seconds

LOG = minimal_logger(__name__)


def get_subtasks_for_level(level):
    """For a given level (e.g. "sample"), get all the associated
    subtasks that should be run (e.g. "qc", "merge_process_variantcall")

    :param str level: The level (e.g. "sample")
    :returns: The names (strings) of the workflows that should be run at that level
    :rtype: tuple

    :raises NotImplementedError: If the level has no associated subtasks
    """
    if level == "sample":
        return ("merge_process_variantcall",)
    else:
        raise NotImplementedError('The level "{}" has no associated subtasks.')


@with_ngi_config
def return_cl_for_workflow(workflow_name, qscripts_dir_path, setup_xml_path, global_config_path,
                           output_dir=None, exec_mode="local", config=None, config_file_path=None):
    """Return an executable-ready Piper command line.

    :param str workflow_name: The name of the Piper workflow to be run.
    :param str qscripts_dir_path: The path to the directory containing the qscripts
    :param str setup_xml_path: The path to the project-level setup XML file
    :param dict global_config_path: The parsed Piper-specific globalConfig file.
    :param str output_dir: The directory to which to write output files
    :param str exec_mode: "local" or "sbatch"

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
    return workflow_function(qscripts_dir_path, setup_xml_path, global_config_path,
                             config, exec_mode, output_dir) 


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

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    # Same command line but with one additional option
    return workflow_dna_variantcalling(*args, **kwargs) +  " --merge_alignments --data_processing --variant_calling --analyze_separately --retry_failed 1"


def workflow_dna_variantcalling(qscripts_dir_path, setup_xml_path, global_config_path,
                                config, exec_mode, output_dir=None):
    """Return the command line for DNA Variant Calling.

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    ## TODO need to add -jobNative arguments (--qos=seqver)
    workflow_qscript_path = os.path.join(qscripts_dir_path, "DNABestPracticeVariantCalling.scala")
    job_walltime = slurm_time_to_seconds(config.get("slurm", {}).get("time") or "4-00:00:00")
    cl_string = ("piper -S {workflow_qscript_path}"
                 " --xml_input {setup_xml_path}"
                 " --global_config {global_config_path}"
                 " --number_of_threads {num_threads}"
                 " --scatter_gather {scatter_gather}"
                 " -jobRunner {job_runner}"
                 " --job_walltime {job_walltime}"
                 " --disableJobReport"
                 " -run")
    if output_dir:
        cl_string += " --output_directory {output_dir}"
    if exec_mode == "sbatch":
        # Execute from within an sbatch file (run jobs on the local node)
        num_threads = int(config.get("piper", {}).get("threads") or 8)
        job_runner = "Shell"
        scatter_gather = 1
    else: # exec_mode == "local"
        # Start a local process that sbatches jobs
        job_runner = "Drmaa"
        scatter_gather = 23
        num_threads = 1
    return cl_string.format(**locals())
