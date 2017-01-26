"""QC workflow-specific code."""

import os
import re
import shlex
import subprocess
import sys

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import load_modules, safe_makedir
from ngi_pipeline.utils.pyutils import flatten

LOG = minimal_logger(__name__)


@with_ngi_config
def return_cls_for_workflow(workflow_name, input_files, output_dir, config=None, config_file_path=None):
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
    for workflow_name in "fastqc", "fastq_screen":
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
            LOG.info("fastqc found on PATH")
            fastqc_path = "fastqc"
        else:
            raise ValueError('Path to FastQC could not be found and it is not '
                             'available on PATH; cannot proceed with FastQC '
                             'workflow.')

    fastq_files = flatten(input_files) # FastQC cares not for your "read pairs"
    # Verify that we in fact need to run this on these files
    fastqc_output_file_tmpls = ("{}_fastqc.zip", "{}_fastqc.html")
    fastq_to_analyze = set()
    for fastq_file in fastq_files:
        m = re.match(r'([\w-]+).fastq', os.path.basename(fastq_file))
        #fetch the FCid
        fc_id = re.search('\d{4,6}_(?P<fcid>[A-Z0-9]{10})', fastq_file).groups()[0]
        if not m:
            # fastq file name doesn't match expected pattern -- just process it
            fastq_to_analyze.add(fastq_file)
            continue
        else:
            fastq_file_base = '{}_{}'.format(m.groups()[0], fc_id)
        for fastqc_output_file_tmpl in fastqc_output_file_tmpls:
            fastqc_output_file = \
                    os.path.join(output_dir, fastqc_output_file_tmpl.format(fastq_file_base))
            if not os.path.exists(fastqc_output_file):
                # Output file doesn't exist
                fastq_to_analyze.add(fastq_file)
            elif os.path.getctime(fastq_file) > os.path.getctime(fastqc_output_file):
                # Input file modified more recently than output file
                fastq_to_analyze.add(fastq_file)


    num_threads = config.get("qc", {}).get("fastqc", {}).get("threads") or 1
    # Construct the command lines
    cl_list = []
    # fastqc commands
    for fastq_file in fastq_to_analyze:
        #when building the fastqc command soflink in the qc_ngi folder the fastq file processed being sure to avoid name collision (i.e., same sample run in two different FC but on the same lane number). Run fastqc on the softlink and delete the soflink straight away. In case the fastq file does not match the regex force the exectuion of fastqc... this is what we have been always doing.
        fc_id = re.search('\d{4,6}_(?P<fcid>[A-Z0-9]{10})', fastq_file).groups()[0]
        m = re.match(r'([\w-]+).(fastq.*)', os.path.basename(fastq_file))
        if not m:
            # fastq file name doesn't match expected pattern -- just process it
            linked_fastq_file_basename =  os.path.basename(fastq_file)
        else:
            linked_fastq_file_basename = "{}_{}.{}".format( m.groups()[0], fc_id, m.groups()[1])
        linked_fastq_file = os.path.join(output_dir, linked_fastq_file_basename)
            #add the command
        cl_list.append('ln -s {original_file} {renamed_fastq_file}'.format(original_file=fastq_file,
                                                                               renamed_fastq_file=linked_fastq_file))
        #now the fastq command (one per file)
        cl_list.append('{fastqc_path} -t {num_threads} -o {output_dir} '
                       '{fastq_files}'.format(output_dir=output_dir,
                                              fastqc_path=fastqc_path,
                                              num_threads=num_threads,
                                              fastq_files=linked_fastq_file))
        #remove the link to the fastq file
        cl_list.append('rm {renamed_fastq_file}'.format(renamed_fastq_file=linked_fastq_file))
    if cl_list:
        safe_makedir(output_dir) #create the fastqc folder as fastqc wants it and I have to create soflinks
        # Module loading
        modules_to_load = get_all_modules_for_workflow("fastqc", config)
        mod_list = [ "module load {}".format(module) for module in modules_to_load ]
        if mod_list:
            cl_list = mod_list + cl_list
    if not cl_list:
        LOG.info("FastQC analysis not needed or input files were invalid.")
    return cl_list


def workflow_fastq_screen(input_files, output_dir, config):
    # Get the path to the fastq_screen command
    fastq_screen_path = config.get("paths", {}).get("fastq_screen")
    if not fastq_screen_path:
        if find_on_path("fastq_screen", config):
            LOG.info("fastq_screen found on PATH")
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

    # Determine which files need processing
    fastq_files = flatten(input_files) # Fastq_screen cares not for your "read pairs" anymore from version 1.5
    # Verify that we in fact need to run this on these files
    fastq_screen_output_file_tmpls = ("{}_screen.png", "{}_screen.txt")
    fastq_to_analyze = set()
    for fastq_file in fastq_files:
        m = re.match(r'([\w-]+).fastq', os.path.basename(fastq_file))
        #fetch the FCid
        fc_id = re.search('\d{4,6}_(?P<fcid>[A-Z0-9]{10})', fastq_file).groups()[0]
        if not m:
            # fastq file name doesn't match expected pattern -- just process it
            fastq_to_analyze.add(fastq_file)
            continue
        else:
            fastq_file_base = '{}_{}'.format(m.groups()[0], fc_id)
        for fastq_screen_output_file_tmpl in fastq_screen_output_file_tmpls:
            fastq_screen_output_file = \
                    os.path.join(output_dir, fastq_screen_output_file_tmpl.format(fastq_file_base))
            if not os.path.exists(fastq_screen_output_file):
                # Output file doesn't exist
                fastq_to_analyze.add(fastq_file)
            elif os.path.getctime(fastq_file) > os.path.getctime(fastq_screen_output_file):
                # Input file modified more recently than output file
                fastq_to_analyze.add(fastq_file)

    # Construct the command lines
    cl_list = []
    # fastq_screen commands
    for fastq_file in fastq_to_analyze:
        #when building the fastq_screen command soflink in the qc_ngi folder the fastq file processed being sure to avoid name collision (i.e., same sample run in two different FC but on the same lane number). Run fastq_screen on the softlink and delete the soflink straight away. In case the fastq file does not match the regex force the exectuion of fastq_screen... this is what we have been always doing.
        fc_id = re.search('\d{4,6}_(?P<fcid>[A-Z0-9]{10})', fastq_file).groups()[0]
        m = re.match(r'([\w-]+).(fastq.*)', os.path.basename(fastq_file))
        if not m:
            # fastq file name doesn't match expected pattern -- just process it
            linked_fastq_file_basename =  os.path.basename(fastq_file)
        else:
            linked_fastq_file_basename = "{}_{}.{}".format( m.groups()[0], fc_id, m.groups()[1])
        linked_fastq_file = os.path.join(output_dir, linked_fastq_file_basename)
            #add the command
        cl_list.append('ln -s {original_file} {renamed_fastq_file}'.format(original_file=fastq_file,
                                                                               renamed_fastq_file=linked_fastq_file))
        #now the fastq_screen command (one per file)
        cl = fastq_screen_path
        cl += " --aligner bowtie2"
        cl += " --outdir {}".format(output_dir)
        if subsample_reads: cl += " --subset {}".format(subsample_reads)
        if num_threads: cl += " --threads {}".format(num_threads)
        if fastq_screen_config_path: cl += " --conf {}".format(fastq_screen_config_path)
        cl_list.append(linked_fastq_file)
        cl_list.append(cl)
        #remove the link to the fastq file
        cl_list.append('rm {renamed_fastq_file}'.format(renamed_fastq_file=linked_fastq_file))
    
    if cl_list:
        safe_makedir(output_dir)
        # Module loading
        modules_to_load = get_all_modules_for_workflow("fastq_screen", config)
        mod_list = [ "module load {}".format(module) for module in modules_to_load ]
        if mod_list:
            cl_list = mod_list + cl_list
    else:
        LOG.info("fastq_screen analysis not needed or input files were invalid.")
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
        LOG.debug("Loading modules {}".format(", ".join(modules_to_load)))
        load_modules(modules_to_load)
    try:
        with open(os.devnull, 'w') as DEVNULL:
            subprocess.check_call(shlex.split("{} --version".format(binary_name)),
                                  stdout=DEVNULL, stderr=DEVNULL)
    except (OSError, subprocess.CalledProcessError) as e:
        return False
    else:
        return True
