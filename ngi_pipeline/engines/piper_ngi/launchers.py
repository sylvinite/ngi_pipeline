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
import datetime

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.engines.piper_ngi import workflows
from ngi_pipeline.engines.piper_ngi.command_creation import build_piper_cl, \
                                                            build_setup_xml
from ngi_pipeline.engines.piper_ngi.local_process_tracking import is_sample_analysis_running_local, \
                                                                  record_process_sample
from ngi_pipeline.engines.piper_ngi.utils import check_for_preexisting_sample_runs, \
                                                 create_exit_code_file_path, \
                                                 create_log_file_path, \
                                                 create_sbatch_header, \
                                                 get_valid_seqruns_for_sample, \
                                                 record_analysis_details
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, \
                                          rotate_file, safe_makedir, \
                                          match_files_under_dir
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import fastq_files_under_dir
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree, get_slurm_job_status

LOG = minimal_logger(__name__)

@with_ngi_config
def analyze(project, sample, exec_mode="sbatch", restart_finished_jobs=False, 
            restart_running_jobs=False, config=None, config_file_path=None):
    """Analyze data at the sample level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param str exec_mode: "sbatch" or "local"
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)

    :raises ValueError: If exec_mode is an unsupported value
    """
    try:
        check_for_preexisting_sample_runs(project, sample, restart_running_jobs, restart_finished_jobs)
    except RuntimeError as e:
        # may want to process anyway.
        raise RuntimeError('Aborting processing of project/sample "{}/{}": '
                               '{}'.format(project, sample, e))
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
                remove_previous_analysis(project)

                if exec_mode == "sbatch":
                    process_id = None
                    slurm_job_id = sbatch_piper_sample([setup_xml_cl, piper_cl],
                                                       workflow_subtask,
                                                       project, sample,
                                                       restart_finished_jobs=restart_finished_jobs)
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
                else:
                    ## FIXME Now this is broken again
                    raise NotImplementedError("Sorry dude it's a no-go")
                    slurm_job_id = None
                    launch_piper_job(setup_xml_cl, project)
                    process_handle = launch_piper_job(piper_cl, project)
                    process_id = process_handle.pid
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


def collect_files_for_sample_analysis(project_obj, sample_obj, 
                                      restart_finished_jobs=False):
    """This function finds all data files relating to a sample and 
    follows a preset decision path to decide which of them to include in
    a sample-level analysis. This can include fastq files, bam files, and
    alignment-qc-level files.
    """
    ### FASTQ
    # Access the filesystem to determine what fastq files are available
    # For each file, validate it.

    # This funtion goes into Charon and finds all valid libpreps and seqruns,
    # dvs libpreps for which               'qc' != "FAILED"
    # and seqruns  for which 'alignment_status' != "DONE"
    valid_libprep_seqruns = get_valid_seqruns_for_sample(project_id=project_obj.project_id,
                                                         sample_id=sample_obj.name,
                                                         include_failed_libpreps=False,
                                                         include_done_seqruns=restart_finished_jobs)
    if not valid_libprep_seqruns: LOG.error("Notify user or whatever. I don't know.")

    # Now we find all fastq files that are available and validate them against
    # the group compiled in the previous step (get_valid_seqruns_for_sample)
    # We're going to recreate NGIProject/NGISample/NGILibraryPrep/NGISeqrun objects here
    sample_data_directory = os.path.join(project_obj.base_path, "DATA",
                                         project_obj.dirname, sample_obj.dirname)
    fastq_files_on_filesystem = fastq_files_under_dir(sample_data_directory, realpath=False)
    if not fastq_files_on_filesystem: LOG.error("TODO raise an error or something")

    fastq_files_to_analyze = []
    # Create a new NGIProject object (the old one could still be in use elsewhere)
    # Fix this later I've been coding for too long
    proj_obj = NGIProject(project_obj.name, project_obj.dirname,
                          project_obj.project_id, project_obj.base_path)
    sample_obj = proj_obj.add_sample(sample_obj.name, sample_obj.dirname)
    for fastq_path in fastq_files_on_filesystem:
        base_path, fastq = os.path.split(fastq_path)
        if not fastq:
            base_path, fastq = os.path.split(base_path) # Handles trailing slash
        base_path, fs_seqrun_name = os.path.split(base_path)
        base_path, fs_libprep_name = os.path.split(base_path)
        if fs_libprep_name not in valid_libprep_seqruns.keys():
            # Invalid library prep, skip this fastq file
            continue
        elif fs_seqrun_name not in valid_libprep_seqruns.get(fs_libprep_name, []):
            continue
        else:
            libprep_obj = sample_obj.add_libprep(name=fs_libprep_name, dirname=fs_libprep_name)
            seqrun_obj = libprep_obj.add_seqrun(name=fs_seqrun_name, dirname=fs_seqrun_name)
            seqrun_obj.add_fastq_files(fastq)

    ### BAM / ALIGNMENT QC
    # Access the filesystem to determine which alignment (bam) files are available.
    # If there are any, add them to the list of files to include in the new analysis.
    # Include alignment qc files.
    project_analysis_dir = os.path.join(project_obj.base_path, "ANALYSIS", project_obj.dirname)
    project_aln_dir = os.path.join(project_analysis_dir, "01_raw_alignments")
    project_alnqc_dir = os.path.join(project_analysis_dir, "02_preliminary_alignment_qc")
    sample_analysis_file_pattern = "{sample_name}.*.{sample_name}.*".format(sample_name=sample_obj.name)
    aln_files_to_copy = glob.glob(os.path.join(project_aln_dir, sample_analysis_file_pattern))
    qc_files_to_copy = glob.glob(os.path.join(project_alnqc_dir, sample_analysis_file_pattern))

    return (proj_obj, aln_files_to_copy, qc_files_to_copy)

@with_ngi_config
def sbatch_piper_sample(command_line_list, workflow_name, project, sample,
                        libprep=None, restart_finished_jobs=False, 
                        config=None, config_file_path=None):
    """sbatch a piper sample-level workflow.

    :param list command_line_list: The list of command lines to execute (in order)
    :param str workflow_name: The name of the workflow to execute
    :param NGIProject project: The NGIProject
    :param NGISample sample: The NGISample
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    job_identifier = "{}-{}-{}".format(project.project_id, sample, workflow_name)
    # Paths to the various data directories
    project_dirname = project.dirname
    sample_dirname = sample.dirname
    perm_analysis_dir = os.path.join(project.base_path, "ANALYSIS", project_dirname, "piper_ngi")
    scratch_analysis_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project_dirname, "piper_ngi")
    scratch_aln_dir = os.path.join(scratch_analysis_dir, "01_raw_alignments")
    scratch_qc_dir = os.path.join(scratch_analysis_dir, "02_preliminary_alignment_qc")
    #ensure that the analysis dir exists
    safe_makedir(perm_analysis_dir)
    try:
        slurm_project_id = config["environment"]["project_id"]
    except KeyError:
        raise RuntimeError('No SLURM project id specified in configuration file '
                           'for job "{}"'.format(job_identifier))
    slurm_queue = config.get("slurm", {}).get("queue") or "core"
    num_cores = config.get("slurm", {}).get("cores") or 8
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
        sbatch_text_list.append("\n# Load required modules for Piper")
        for module_name in modules_to_load:
            sbatch_text_list.append("module load {}".format(module_name))

    project, src_aln_files, src_alnqc_files = \
            collect_files_for_sample_analysis(project, sample, 
                                                restart_finished_jobs)

    # Fastq files to copy
    fastq_src_dst_list = []
    directories_to_create = set()
    for sample in project:
        for libprep in sample:
            for seqrun in libprep:
                project_specific_path = os.path.join(project.dirname,
                                                     sample.dirname,
                                                     libprep.dirname,
                                                     seqrun.dirname)
                directories_to_create.add(os.path.join("$SNIC_TMP/DATA/", project_specific_path))
                for fastq in seqrun.fastq_files:
                    src_file = os.path.join(project.base_path, "DATA", project_specific_path, fastq)
                    dst_file = os.path.join("$SNIC_TMP/DATA/", project_specific_path, fastq)
                    fastq_src_dst_list.append([src_file, dst_file])

    sbatch_text_list.append("echo -ne '\\n\\nCopying fastq files at '")
    sbatch_text_list.append("date")
    if fastq_src_dst_list:
        for directory in directories_to_create:
            sbatch_text_list.append("mkdir -p {}".format(directory))
        for src_file, dst_file in fastq_src_dst_list:
            sbatch_text_list.append("rsync -rptoDLv {} {}".format(src_file, dst_file))
    else:
        raise ValueError(('No valid fastq files available to process for '
                          'project/sample {}/{}'.format(project, sample)))

    # BAM files / Alignment QC files
    input_files_list = [ src_aln_files, src_alnqc_files ]
    output_dirs_list = [ scratch_aln_dir, scratch_qc_dir ]
    echo_text_list = ["Copying any pre-existing alignment files",
                      "Copying any pre-existing alignment qc files"]
    for echo_text, input_files, output_dir in zip(echo_text_list, input_files_list, output_dirs_list):
        if input_files:
            sbatch_text_list.append("echo -ne '\\n\\n{}' at ".format(echo_text))
            sbatch_text_list.append("date")
            sbatch_text_list.append("mkdir -p {}".format(output_dir))
            sbatch_text_list.append(("rsync -rptoDLv {input_files} "
                                     "{output_directory}/").format(input_files=" ".join(input_files),
                                                                  output_directory=output_dir))
    sbatch_text_list.append("echo -ne '\\n\\nExecuting command lines at '")
    sbatch_text_list.append("date")
    sbatch_text_list.append("# Run the actual commands")
    for command_line in command_line_list:
        sbatch_text_list.append(command_line)


    piper_status_file=create_exit_code_file_path(workflow_subtask=workflow_name,
                                                project_base_path=project.base_path,
                                                project_name=project.dirname,
                                                project_id=project.project_id,
                                                sample_id=sample.name)
    sbatch_text_list.append("\nPIPER_RETURN_CODE=$?")
    #sbatch_text_list.append("if [[ $PIPER_RETURN_CODE == 0 ]]")
    #sbatch_text_list.append("then")
    sbatch_text_list.append("echo -ne '\\n\\nCopying back the resulting analysis files at '")
    sbatch_text_list.append("date")
    sbatch_text_list.append("mkdir -p {}".format(perm_analysis_dir))
    sbatch_text_list.append("rsync -rptoDLv {}/ {}/".format(scratch_analysis_dir, perm_analysis_dir))
    #sbatch_text_list.append("else")
    #sbatch_text_list.append("  echo -e '\\n\\nPiper job failed'")
    #sbatch_text_list.append("fi")

    # Record job completion status
    sbatch_text_list.append("echo $PIPER_RETURN_CODE > {}".format(piper_status_file))

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
    # Detail which seqruns we've started analyzing so we can update statuses later
    record_analysis_details(project, job_identifier)
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

def remove_previous_analysis(project_obj):
    project_dir_path = os.path.join(project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi")
    if glob.glob(project_dir_path):
        LOG.info('deleting previous analysis in {}'.format(project_dir_path))
        shutil.rmtree(project_dir_path)
        safe_makedir(project_dir_path)
def rotate_previous_analysis(project_obj):
    """Rotates the files from the existing analysis starting at 03_merged_aligments"""
    project_dir_path = os.path.join(project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi")
    #analysis_move = glob.glob(os.path.join(project_dir_path, '0[3-9]_*'))
    for sample in project_obj:
        # P123_456 is renamed by Piper to P123-456
        piper_sample_name = sample.name.replace("_", "-", 1)
        sample_files = glob.glob(os.path.join(project_dir_path, "0[3-9]_*", "{}.*".format(piper_sample_name)))
    if sample_files:
        LOG.info('Rotating files for sample {} under {} to '
                 '"previous_analyses" folder'.format(sample, project_dir_path))
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S:%f")
        for sample_file in sample_files:
            # This will be the project_dir_path, so I guess I'm just being paranoid
            common_prefix = os.path.commonprefix([os.path.abspath(project_dir_path),
                                                  os.path.abspath(sample_file)])
            # This part of the directory tree we need to recreate under previous_analyses
            # So e.g. with
            #       /proj/a2015001/Y.Mom_15_01/01_raw_alignments/P123_456.bam
            # we'd get
            #       01_raw_alignments/P123_456.bam
            # and we'd then create
            #       /proj/a2015001/Y.Mom_15_01/previous_analyses/2015-02-19_16:24:12:640314/01_raw_alignments/
            # and move the file to this directory.
            leaf_path = os.path.relpath(sample_file, common_prefix)
            leaf_base, filename = os.path.split(leaf_path)
            previous_analysis_dirpath = os.path.join(common_prefix, "previous_analyses", current_datetime, leaf_base)
            safe_makedir(previous_analysis_dirpath, mode=0o2770)
            LOG.debug("Moving file {} to directory {}".format(sample_file, previous_analysis_dirpath))
            shutil.move(sample_file, previous_analysis_dirpath)
