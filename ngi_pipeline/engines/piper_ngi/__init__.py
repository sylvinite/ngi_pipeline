"""The Piper automated launcher script."""
from __future__ import print_function

import collections
import glob
import os
import re
import shlex
import shutil
import subprocess
import time

from ngi_pipeline.engines.piper_ngi import workflows
from ngi_pipeline.engines.piper_ngi.local_process_tracking import is_sample_analysis_running_local, \
                                                                  record_process_sample
from ngi_pipeline.engines.piper_ngi.utils import create_exit_code_file_path, \
                                                 create_log_file_path, \
                                                 create_sbatch_header
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, \
                                          rotate_file, safe_makedir, \
                                          match_files_under_dir
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree, get_slurm_job_status

LOG = minimal_logger(__name__)

@with_ngi_config
def analyze(project, sample, exec_mode="sbatch", config=None, config_file_path=None):
    """Analyze data at the sample level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param str exec_mode: "sbatch" or "local"
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)

    :raises ValueError: If exec_mode is an unsupported value
    """
    if exec_mode.lower() not in ("sbatch", "local"):
        raise ValueError(('"exec_mode" param must be one of "sbatch" or "local" ')
                         ('value was "{}"'.format(exec_mode)))
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    LOG.info('Sample "{}" in project "{}" is ready for processing.'.format(sample, project))
    for workflow_subtask in get_subtasks_for_level(level="sample"):
        if not is_sample_analysis_running_local(workflow_subtask=workflow_subtask,
                                                project_id=project.project_id,
                                                sample_id=sample.name):
            try:
                log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                     project_base_path=project.base_path,
                                                     project_name=project.dirname,
                                                     sample_id=sample.name)
                rotate_file(log_file_path)
                exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                            project_base_path=project.base_path,
                                                            project_name=project.dirname,
                                                            sample_id=sample.name)
                setup_xml_cl, setup_xml_path = build_setup_xml(project=project,
                                                               sample=sample,
                                                               local_scratch_mode=(exec_mode == "sbatch"),
                                                               config=config)
                piper_cl = build_piper_cl(project=project,
                                          workflow_name=workflow_subtask,
                                          setup_xml_path=setup_xml_path,
                                          exit_code_path=exit_code_path,
                                          config=config,
                                          exec_mode=exec_mode)
                if exec_mode == "sbatch":
                    slurm_job_id = sbatch_piper_sample([setup_xml_cl, piper_cl],
                                                       workflow_subtask,
                                                       project, sample)
                    for x in xrange(10): # Time delay to let sbatch get its act together (takes a few seconds to be visible with sacct)
                        try:
                            get_slurm_job_status(slurm_job_id)
                            break
                        except ValueError:
                            time.sleep(2)
                    else:
                        LOG.error('sbatch file for sample {}/{} did not '
                                  'queue properly! Job ID {} cannot be '
                                  'found.'.format(project, sample, slurm_job_id))

                    process_id = None
                else:
                    launch_piper_job(setup_xml_cl, project)
                    process_handle = launch_piper_job(piper_cl, project)
                    process_id = process_handle.pid
                    slurm_job_id = None
                try:
                    record_process_sample(project=project,
                                          sample=sample,
                                          analysis_module_name="piper_ngi",
                                          slurm_job_id=slurm_job_id,
                                          process_id=process_id,
                                          workflow_subtask=workflow_subtask)
                except RuntimeError as e:
                    LOG.error('Could not record process for project/sample '
                              '{}/{}, workflow {}'.format(project, sample,
                                                          workflow_subtask))
                    ## Question: should we just kill the run in this case or let it go?
                    continue
            except (NotImplementedError, RuntimeError) as e:
                error_msg = ('Processing project "{}" / sample "{}" failed: '
                             '{}'.format(project, sample, e.__repr__()))
                LOG.error(error_msg)


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
def sbatch_piper_sample(command_line_list, workflow_name, project, sample, libprep=None,
                        config=None, config_file_path=None):
    """sbatch a piper sample-level workflow.

    :param list command_line_list: The list of command lines to execute (in order)
    :param str workflow_name: The name of the workflow to execute
    :param NGIProject project: The NGIProject
    :param NGISample sample: The NGISample
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    job_identifier = "{}-{}-{}".format(project, sample, workflow_name)
    # Paths to the various data directories
    project_dirname = project.dirname
    sample_dirname = sample.dirname
    perm_data_dir = os.path.join(project.base_path, "DATA", os.path.join(project_dirname, sample_dirname))
    perm_data_topdir = os.path.join(project.base_path, "DATA", project_dirname)
    perm_analysis_dir = os.path.join(project.base_path, "ANALYSIS", project_dirname)
    perm_aln_dir = os.path.join(perm_analysis_dir, "01_raw_alignments")
    perm_qc_dir = os.path.join(perm_analysis_dir, "02_preliminary_alignment_qc")
    scratch_data_dir = os.path.join("$SNIC_TMP/DATA/", os.path.join(project_dirname, sample_dirname))
    scratch_analysis_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project_dirname)
    scratch_aln_dir = os.path.join(scratch_analysis_dir, "01_raw_alignments")
    scratch_qc_dir = os.path.join(scratch_analysis_dir, "02_preliminary_alignment_qc")
    try:
        slurm_project_id = config["environment"]["project_id"]
    except KeyError:
        raise RuntimeError('No SLURM project id specified in configuration file '
                           'for job "{}"'.format(job_identifier))
    slurm_queue = config.get("slurm", {}).get("queue") or "node"
    num_cores = config.get("slurm", {}).get("cores") or 16
    slurm_time = config.get("piper", {}).get("job_walltime", {}).get("workflow_name") or "4-00:00:00"
    slurm_out_log = os.path.join(perm_analysis_dir, "logs", "{}_sbatch.out".format(job_identifier))
    slurm_err_log = os.path.join(perm_analysis_dir, "logs", "{}_sbatch.err".format(job_identifier))
    sbatch_text = create_sbatch_header(slurm_project_id=slurm_project_id,
                                       slurm_queue=slurm_queue,
                                       num_cores=num_cores,
                                       slurm_time=slurm_time,
                                       job_name="piper_{}".format(job_identifier),
                                       slurm_out_log=slurm_out_log,
                                       slurm_err_log=slurm_err_log)
    sbatch_text_list = sbatch_text.split("\n")
    sbatch_extra_params = config.get("slurm", {}).get("extra_params", {})
    for param, value in sbatch_extra_params.iteritems():
        sbatch_text_list.append("#SBATCH {} {}\n\n".format(param, value))
    modules_to_load = config.get("piper", {}).get("load_modules", [])
    if modules_to_load:
        sbatch_text_list.append("\n# Load requires modules for Piper")
        for module_name in modules_to_load:
            sbatch_text_list.append("module load {}".format(module_name))
    # Fastq files to copy
    sample_fq_file_pattern = "^{}.*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$".format(sample.name)
    fq_files_to_copy = match_files_under_dir(dirname=perm_data_dir,
                                             pattern=sample_fq_file_pattern)
    sample_analysis_file_pattern = "{sample_name}.*.{sample_name}.*".format(sample_name=sample.name)
    # BAM files
    aln_files_to_copy = glob.glob(os.path.join(perm_aln_dir, sample_analysis_file_pattern))
    # Alignment QC files
    qc_files_to_copy = glob.glob(os.path.join(perm_qc_dir, sample_analysis_file_pattern))
    input_files_list = [ fq_files_to_copy, aln_files_to_copy, qc_files_to_copy ]
    output_dirs_list = [ scratch_data_dir, scratch_aln_dir, scratch_qc_dir ]
    comment_txt_list = ["\n# Copy fastq files for sample",
                        "\n# Copy any pre-existing alignment files",
                        "\n# Copy any pre-existing alignment qc files"]
    for comment_text, input_files, output_dir in zip(comment_txt_list, input_files_list, output_dirs_list):
        if input_files:
            sbatch_text_list.append(comment_text)
            sbatch_text_list.append("mkdir -p {}".format(output_dir))
            sbatch_text_list.append(("rsync -rlptoDv {input_files} "
                                     "{output_directory}/").format(input_files=" ".join(input_files),
                                                                  output_directory=output_dir))
    sbatch_text_list.append("\n# Run the actual commands")
    for command_line in command_line_list:
        sbatch_text_list.append(command_line)
    sbatch_text_list.append("\n#Copy back the resulting analysis files")
    sbatch_text_list.append("mkdir -p {}".format(perm_analysis_dir))
    sbatch_text_list.append("rsync -rlptoDv {}/ {}/\n".format(scratch_analysis_dir, perm_analysis_dir))

    # Write the sbatch file
    sbatch_dir = os.path.join(perm_analysis_dir, "sbatch")
    safe_makedir(sbatch_dir)
    sbatch_outfile = os.path.join(sbatch_dir, "{}.sbatch".format(job_identifier))
    if os.path.exists(sbatch_outfile):
        rotate_file(sbatch_outfile)
    with open(sbatch_outfile, 'w') as f:
        f.write("\n".join(sbatch_text_list))
    LOG.info("Queueing sbatch file {} for job {}".format(sbatch_outfile, job_identifier))
    # Queue the sbatch file
    p_handle = execute_command_line("sbatch {}".format(sbatch_outfile),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
    p_out, p_err = p_handle.communicate()
    try:
        slurm_job_id = re.match(r'Submitted batch job (\d+)', p_out).groups()[0]
    except AttributeError:
        raise RuntimeError('Could not submit sbatch job for workflow "{}": '
                           '{}'.format(job_identifier, p_err))
    return int(slurm_job_id)


def launch_piper_job(command_line, project, log_file_path=None):
    """Launch the Piper command line.

    :param str command_line: The command line to execute
    :param Project project: The Project object (needed to set the CWD)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    working_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    file_handle=None
    if log_file_path:
        try:
            file_handle = open(log_file_path, 'w')
        except Exception as e:
            LOG.error('Could not open log file "{}"; reverting to standard '
                      'logger (error: {})'.format(log_file_path, e))
            log_file_path = None
    popen_object = execute_command_line(command_line, cwd=working_dir, shell=True,
                                        stdout=(file_handle or subprocess.PIPE),
                                        stderr=(file_handle or subprocess.PIPE))
    if not log_file_path:
        log_process_non_blocking(popen_object.stdout, LOG.info)
        log_process_non_blocking(popen_object.stderr, LOG.warn)
    return popen_object


## TODO change this to use local_scratch_mode boolean instead of exec_mode
def build_piper_cl(project, workflow_name, setup_xml_path, exit_code_path,
                   config, exec_mode="local"):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param NGIProject project: The project object to analyze.
    :param str workflow_name: The name of the workflow to execute (e.g. "dna_alignonly")
    :param str exit_code_path: The path to the file to which the exit code for this cl will be written
    :param dict config: The (parsed) configuration file for this machine/environment.
    :param str exec_mode: "local" or "sbatch"

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    if exec_mode == "sbatch":
        output_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project.dirname)
        # Can't create these directories ahead of time of course
    elif exec_mode == "local":
        output_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
        safe_makedir(analysis_dir, 0770)
    else:
        raise ValueError('"exec_mode" must be one of "local", "sbatch" (value '
                         'was "{}"'.format(exec_mode))

    # Global Piper configuration
    piper_rootdir = config.get("piper", {}).get("path_to_piper_rootdir")
    piper_global_config_path = \
                    (os.environ.get("PIPER_GLOB_CONF_XML") or
                     config.get("piper", {}).get("path_to_piper_globalconfig") or
                     (os.path.join(piper_rootdir, "globalConfig.xml") if
                     piper_rootdir else None))
    if not piper_global_config_path:
        raise ValueError('Could not find Piper global configuration file in config '
                         'file, as environmental variable ("PIPER_GLOB_CONF_XML"), '
                         'or in Piper root directory.')

    # QScripts directory
    try:
        piper_qscripts_dir = (os.environ.get("PIPER_QSCRIPTS_DIR") or
                              config['piper']['path_to_piper_qscripts'])
    except KeyError:
        raise Valueerror('Could not find Piper QScripts directory in config file or '
                         'as environmental variable ("PIPER_QSCRIPTS_DIR").')

    # Build Piper cl
    LOG.info('Building workflow command line(s) for project "{}" / workflow '
             '"{}"'.format(project, workflow_name))
    cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                          qscripts_dir_path=piper_qscripts_dir,
                                          setup_xml_path=setup_xml_path,
                                          global_config_path=piper_global_config_path,
                                          output_dir=output_dir,
                                          exec_mode=exec_mode)
    # Blank out the file if it already exists
    safe_makedir(os.path.dirname(exit_code_path))
    open(exit_code_path, 'w').close()
    return add_exit_code_recording(cl, exit_code_path)


def add_exit_code_recording(cl, exit_code_path):
    """Takes a command line and returns it with increased pizzaz"""
    record_exit_code = "; echo $? > {}".format(exit_code_path)
    if type(cl) is list:
        # This should work, right? Right
        cl = " ".join(cl)
    return cl + record_exit_code


def build_setup_xml(project, sample, local_scratch_mode, config):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param NGIProject project: The project to be converted.
    :param NGISample sample: the sample object
    :param bool local_scratch_mode: Whether the job will be run in scratch or permanent storage
    :param dict config: The (parsed) configuration file for this machine/environment.

    :raises ValueError: If a required configuration file value is missing
    :raises RuntimeError: If the setupFileCreator returns non-zero
    """
    LOG.info('Building Piper setup.xml file for project "{}" '
             'sample "{}"'.format(project, sample.name))

    if local_scratch_mode:
        project_top_level_dir = os.path.join("$SNIC_TMP/DATA/", project.dirname)
        analysis_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project.dirname)
        # Can't create these directories ahead of time of course
    else:
        project_top_level_dir = os.path.join(project.base_path, "DATA", project.dirname)
        analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
        safe_makedir(analysis_dir, 0770)
    ## TODO handle this elsewhere
    #safe_makedir(os.path.join(analysis_dir, "logs"))

    cl_args = {'project': project.dirname}
    cl_args["sequencing_center"] = "NGI"
    cl_args["sequencing_tech"] = "Illumina"
    ## TODO load these from (ngi_pipeline) config file
    cl_args["qos"] = "seqver"
    
    # Eventually this will be loaded from e.g. Charon
    reference_genome = 'GRCh37'
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
        cl_args["sfc_binary"] = "setupFileCreator" # Assume setupFileCreator is on path

    # setup XML file is always stored in permanent analysis directory
    output_xml_filepath = os.path.join(project.base_path, "ANALYSIS",
                                       project.dirname, "setup_xml_files",
                                       "{}-{}-setup.xml".format(project, sample))
    safe_makedir(os.path.dirname(output_xml_filepath))
    cl_args["output_xml_filepath"] = output_xml_filepath
    
    setupfilecreator_cl = ("{sfc_binary} "
                           "--output {output_xml_filepath} "
                           "--project_name {project} "
                           "--sequencing_platform {sequencing_tech} "
                           "--sequencing_center {sequencing_center} "
                           "--uppnex_project_id {uppmax_proj} "
                           "--reference {reference_path} "
                           "--qos {qos}").format(**cl_args)
    for libprep in sample:
        for seqrun in libprep:
            sample_run_directory = os.path.join(project_top_level_dir, sample.dirname, libprep.name, seqrun.name )
            for fastq_file_name in seqrun.fastq_files:
                fastq_file = os.path.join(sample_run_directory, fastq_file_name)
                setupfilecreator_cl += " --input_fastq {}".format(fastq_file)
    return (setupfilecreator_cl, output_xml_filepath)
