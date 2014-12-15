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

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.engines.piper_ngi import workflows
from ngi_pipelines.engines.piper_ngi.command_creation import build_piper_cl, \
                                                             build_setup_xml
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
    for workflow_subtask in workflows.get_subtasks_for_level(level="sample"):
        if not is_sample_analysis_running_local(workflow_subtask=workflow_subtask,
                                                project_id=project.project_id,
                                                sample_id=sample.name):
            try:
                log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                     project_base_path=project.base_path,
                                                     project_name=project.dirname,
                                                     project_id=project.project_id,
                                                     sample_id=sample.name)
                rotate_file(log_file_path)
                exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                            project_base_path=project.base_path,
                                                            project_name=project.dirname,
                                                            project_id=project.project_id,
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
            except (NotImplementedError, RuntimeError, ValueError) as e:
                error_msg = ('Processing project "{}" / sample "{}" failed: '
                             '{}'.format(project, sample, e.__repr__()))
                LOG.error(error_msg)


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
    perm_data_dir = os.path.join(project.base_path, "DATA", project_dirname, sample_dirname)
    perm_data_topdir = os.path.join(project.base_path, "DATA", project_dirname)
    perm_analysis_dir = os.path.join(project.base_path, "ANALYSIS", project_dirname)
    perm_aln_dir = os.path.join(perm_analysis_dir, "01_raw_alignments")
    perm_qc_dir = os.path.join(perm_analysis_dir, "02_preliminary_alignment_qc")
    scratch_data_dir = os.path.join("$SNIC_TMP/DATA/", project_dirname)
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
    for log_file in slurm_out_log, slurm_err_log:
        rotate_file(log_file)
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
    try:
        charon_session = CharonSession()
    except CharonError as e:
        LOG.warn("Unable to connecto Charon and cannot verify library preps; "
                 "using all fastq files for analysis ({}).format(e)")
        charon_session = None
    fastq_src_dst_list = []
    for libprep in sample:
        if charon_session:
            try:
                libprep_valid = charon_session.libprep_get(projectid=project.project_id,
                                                           sampleid=sample.name,
                                                           libprepid=libprep.name).get("qc") != "FAILED"
            except CharonError as e:
                LOG.warn('Cannot verify library prep {} due to Charon error; proceeding '
                         'with analysis for this libprep (error: {})'.format(libprep, e))
                libprep_valid = True
        else: # Cannot connect to Charon; use all libpreps
            libprep_valid = True
        if libprep_valid:
            for seqrun in libprep:
                try:
                    aln_status =  charon_session.seqrun_get(projectid=project.project_id,
                                                            sampleid=sample.name,
                                                            libprepid=libprep.name,
                                                            seqrunid=seqrun.name)['alignment_status'] 
                except KeyError:
                    LOG.warn('Seqrun {} does not have a value for key "{}"; '
                             'proceeding as though it were "DONE"'.format(seqrun, e.args[0]))
                    aln_status = None
                if aln_status != None:
                    for fastq in seqrun:
                        src_file = os.path.join(project.base_path, "DATA", project.dirname,
                                                sample.dirname, libprep.dirname,
                                                seqrun.dirname, fastq)
                        dst_file = os.path.join(scratch_data_dir, sample.dirname,
                                                libprep.dirname, seqrun.dirname,
                                                fastq)
                        fastq_src_dst_list.append([src_file, dst_file])
                else:
                    LOG.info(('Skipping analysis of project/sample/libprep/seqrun '
                              '{}/{}/{}/{} because alignment status is '
                              '"DONE"').format(project, sample, libprep, seqrun))
        else:
            LOG.info('Library prep "{}" failed QC, excluding from analysis.'.format(libprep))
    sbatch_text_list.append("date")
    sbatch_text_list.append("\necho -e '\\n\\nCopying fastq files'")
    if fastq_src_dst_list:
        for src_file, dst_file in fastq_src_dst_list:
            sbatch_text_list.append("mkdir -p {}".format(os.path.dirname(dst_file)))
            sbatch_text_list.append("rsync -rptoDLv {} {}".format(src_file, dst_file))
    else:
        raise ValueError(('No valid fastq files available to process for '
                          'project/sample {}/{}'.format(project, sample)))

    # BAM files / Alignment QC files
    sample_analysis_file_pattern = "{sample_name}.*.{sample_name}.*".format(sample_name=sample.name)
    aln_files_to_copy = glob.glob(os.path.join(perm_aln_dir, sample_analysis_file_pattern))
    qc_files_to_copy = glob.glob(os.path.join(perm_qc_dir, sample_analysis_file_pattern))
    input_files_list = [ aln_files_to_copy, qc_files_to_copy ]
    output_dirs_list = [ scratch_aln_dir, scratch_qc_dir ]
    echo_text_list = ["Copying any pre-existing alignment files",
                      "Copying any pre-existing alignment qc files"]
    for echo_text, input_files, output_dir in zip(echo_text_list, input_files_list, output_dirs_list):
        if input_files:
            sbatch_text_list.append("date")
            sbatch_text_list.append("\necho -e '\\n\\n{}'".format(echo_text))
            sbatch_text_list.append("mkdir -p {}".format(output_dir))
            sbatch_text_list.append(("rsync -rptoDLv {input_files} "
                                     "{output_directory}/").format(input_files=" ".join(input_files),
                                                                  output_directory=output_dir))
    sbatch_text_list.append("\n# Run the actual commands")
    for command_line in command_line_list:
        sbatch_text_list.append(command_line)
    sbatch_text_list.append("date")
    sbatch_text_list.append("\necho -e '\\n\\nCopying back the resulting analysis files'")
    sbatch_text_list.append("mkdir -p {}".format(perm_analysis_dir))
    sbatch_text_list.append("rsync -rptoDLv {}/ {}/\n".format(scratch_analysis_dir, perm_analysis_dir))

    # Write the sbatch file
    sbatch_dir = os.path.join(perm_analysis_dir, "sbatch")
    safe_makedir(sbatch_dir)
    sbatch_outfile = os.path.join(sbatch_dir, "{}.sbatch".format(job_identifier))
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
