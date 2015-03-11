"""QC workflow-specific code."""

import os
import shlex
import subprocess
import sys

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import load_modules
from ngi_pipeline.utils.pyutils import flatten

LOG = minimal_logger(__name__)


@with_ngi_config
def return_cl_for_workflow(workflow_name, input_files, output_dir, config=None, config_file_path=None):
    """Return an executable-ready bash command line.

    :param str workflow_name: The name of the workflow to be run.
    :param list input_files: A flat list of input fastq files or a list of lists of paired files
    :param str output_dir: The directory to which to write output files

    :returns: A 2D list of the bash command line(s) to be executed.
    :rtype: list of lists
    :raises NotImplementedError: If the workflow requested has not been implemented.
    """
    workflow_fn_name = "workflow_{}".format(workflow_name)
    # Get the local function if it exists
    try:
        workflow_function = getattr(sys.modules[__name__], workflow_fn_name)
    except AttributeError as e:
        error_msg = 'Workflow "{}" has no associated function.'.format(workflow_fn_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
    LOG.info('Building command line for workflow "{}"'.format(workflow_name))
    cl_list = workflow_function(input_files, output_dir, config)
    if type(cl_list[0]) is not list:
        # I know this is stupid. Shut up.
        # I don't want to talk about it.
        cl_list = [cl_list]
    return cl_list


def workflow_qc(input_files, output_dir, config):
    """Generic qc (includes multiple specific qc utilities)."""
    cl_list = []
    for workflow_name in "fastqc", "fastqscreen":
        workflow_fn_name = "workflow_{}".format(workflow_name)
        try:
            workflow_function = getattr(sys.modules[__name__], workflow_fn_name)
            output_subdir = os.path.join(output_dir, workflow_name)
            cl_list.append(workflow_function(input_files, output_subdir, config))
        except ValueError as e:
            LOG.error('Could not create command line for workflow '
                      '"{}" ({})'.format(workflow_name, e))
    return cl_list


def workflow_fastqc(input_files, output_dir, config):
    """The constructor of the FastQC command line.

    :param list input_files: The list of fastq files to analyze (may be 2D for read pairs)
    :param str output_dir: The path to the desired output directory (will be created)
    :param dict config: The parsed system/pipeline configuration file

    :returns: A list of command lines to be executed in the order given
    :rtype: list
    :raises ValueError: If the FastQC path is not given or is not on PATH
    """
    # Get the path to the fastqc command
    fastqc_path = config.get("paths", {}).get("fastqc")
    if not fastqc_path:
        if find_on_path("fastqc", config):
            fastqc_path = "fastqc"
        else:
            raise ValueError('Path to FastQC could not be found and it is not '
                             'available on PATH; cannot proceed with FastQC '
                             'workflow.')
    num_threads = config.get("qc", {}).get("fastqc", {}).get("threads") or 1
    fastq_files = flatten(input_files) # FastQC cares not for your "read pairs"
    # Construct the command lines
    cl_list = []
    # Module loading
    modules_to_load = get_all_modules_for_workflow("fastqc", config)
    for module in modules_to_load:
        cl_list.append("module load {}".format(module))
    # Create the output directory
    cl_list.append('mkdir -p {output_dir}'.format(output_dir=output_dir))
    # Execute fastqc
    cl_list.append('{fastqc_path} -t {num_threads} -o {output_dir} '
                   '{fastq_files}'.format(output_dir=output_dir,
                                          fastqc_path=fastqc_path,
                                          num_threads=num_threads,
                                          fastq_files=" ".join(fastq_files)))
    return cl_list


def workflow_fastq_screen(input_files, output_dir, config):
    # Get the path to the fastq_screen command
    fastq_screen_path = config.get("paths", {}).get("fastq_screen")
    if not fastq_screen_path:
        if find_on_path("fastq_screen", config):
            fastq_screen_path = "fastq_screen"
        else:
            raise ValueError('Path to fastq_screen could not be found and it is not '
                             'available on PATH; cannot proceed with fastq_screen '
                             'workflow.')
    fastq_screen_config_path = config.get("qc", {}).get("fastq_screen", {}).get("config_path")
    # We probably should have the path to the fastq_screen config file written down somewhere
    if not fastq_screen_config_path:
        LOG.warn('Path to fastq_screen config file not specified; assuming '
                 'it is in the same directory as the fastq_screen binary, '
                 'even though I think this is probably a fairly bad '
                 'assumption to make. You\'re in charge, whatever.')
    else:
        try:
            open(fastq_screen_config_path, 'r').close()
        except IOError as e:
            raise ValueError('Error when accessing fastq_screen configuration '
                             'file as specified in pipeline config: "{}" (path '
                             'given was {})'.format(e, fastq_screen_config_path))

    num_threads = config.get("qc", {}).get("fastq_screen", {}).get("threads") or 1
    subsample_reads = config.get("qc", {}).get("fastq_screen", {}).get("subsample_reads")

    # Construct the command lines
    cl_list = []
    # Module loading
    modules_to_load = get_all_modules_for_workflow("fastqc", config)
    for module in modules_to_load:
        cl_list.append("module load {}".format(module))
    # Make output directory
    cl_list.append('mkdir -p {output_dir}'.format(output_dir=output_dir))
    # fastq_screen stuff here
    for elt in input_files:
        cl = fastq_screen_path
        cl += " --outdir {}".format(output_dir)
        if subsample_reads: cl += " --subset {}".format(subsample_reads)
        if num_threads: cl += " --threads {}".format(num_threads)
        if fastq_screen_config_path: cl += " --conf {}".format(fastq_screen_config_path)
        if type(elt) is list:
            if len(list) == 2:
                # Read pair; run fastq_screen on these together
                cl += (" --paired {}".format(" ".join(elt)))
            else:
                LOG.error('Files passed as list but more than two elements; '
                          'not a read pair? Skipping. ({})'.format(" ".join(elt)))
        elif type(elt) is str or type(elt) is unicode:
            cl += " " + elt
        else:
            LOG.error("Whatchyoo tryin' to get crazy with ese, "
                      "don'tchyoo know I'm loco? "
                      "Ignoring your weird input (not a string, not a list).")
        cl_list.append(cl)
    if not cl_list:
        raise ValueError("No valid input files passed in; skipping fastq_screen analysis.")
    else:
        return cl_list


def get_all_modules_for_workflow(binary_name, config):
    general_modules = config.get("qc", {}).get("load_modules")
    specific_modules = config.get("qc", {}).get(binary_name, {}).get("load_modules")
    modules_to_load = []
    if general_modules:
        modules_to_load.extend(general_modules)
    if specific_modules:
        modules_to_load.extend(specific_modules)
    return modules_to_load


def find_on_path(binary_name, config=None):
    """Determines if the binary in question is on the PATH, loading modules
    as specified in the qc section of the config file.

    :param str binary_name: The name of the binary (e.g. "bowtie2")
    :param dict config: The parsed pipeline/system config (optional)

    :returns: True if the binary is on the PATH; False if not
    :rtype: boolean
    """
    if not config: config = {}
    LOG.info('Path to {} not specified in config file; '
             'checking if it is on PATH'.format(binary_name))
    modules_to_load = get_all_modules_for_workflow(binary_name, config)
    if modules_to_load:
        load_modules(modules_to_load)
    try:
        # If we get no error, fastq_screen is on the PATH
        with open(os.devnull, 'w') as DEVNULL:
            subprocess.check_call(shlex.split("{} --version".format(binary_name)),
                                  stdout=DEVNULL, stderr=DEVNULL)
    except (OSError, subprocess.CalledProcessError) as e:
        return False
    else:
        return True
