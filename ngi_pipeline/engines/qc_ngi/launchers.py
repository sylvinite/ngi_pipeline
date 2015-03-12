"""Run the qc pipeline on fastq samples; currently consists of FastQC and
fastq_screen."""

import os
import subprocess

from ngi_pipeline.engines.qc_ngi.workflows import return_cls_for_workflow
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import rotate_file, safe_makedir
from ngi_pipeline.utils.parsers import find_fastq_read_pairs

LOG = minimal_logger(__name__)


@with_ngi_config
def analyze(project, sample, config=None, config_file_path=None):
    """The main entry point for the qc pipeline."""
    LOG.info("Processing project/sample {}/{}".format(project, sample))

    # Two paths diverged in a yellow wood
    project_analysis_path = os.path.join(project.base_path,
                                         "ANALYSIS",
                                         project.project_id,
                                         "qc_ngi")
    # and sorry I could not travel both
    sample_analysis_path = os.path.join(project_analysis_path, sample.name)
    # and be one traveler, long I stood
    log_dir_path = os.path.join(project_analysis_path, "logs")
    # and looked down one as far as I could
    safe_makedir(sample_analysis_path)
    # To where it bent in the undergrowth
    safe_makedir(log_dir_path)
    # I need to go to sleep

    fastq_files_to_process = []
    # I suppose I -should- have quoted the other one
    src_fastq_base = os.path.join(project.base_path, "DATA",
                                  project.project_id, sample.name)
    # Whose woods these are I think I know
    for libprep in sample:
        # His house is in the village though
        for seqrun in libprep:
            # He will not see mt stopping here
            for fastq_file in seqrun:
                # To watch
                path_to_src_fastq = os.path.join(src_fastq_base,
                                                 libprep.name,
                                                 seqrun.name,
                                                 fastq_file)
                # his woods
                fastq_files_to_process.append(path_to_src_fastq)
    # fill up
    paired_fastq_files = find_fastq_read_pairs(fastq_files_to_process).values()
    # with snow
    qc_cl_list = return_cls_for_workflow("qc", paired_fastq_files, sample_analysis_path)

    sbatch_file_path = create_sbatch_file(qc_cl_list, project, sample, config)
    try:
        slurm_job_id = queue_sbatch_file(sbatch_file_path) 
    except RuntimeError as e:
        LOG.error('Failed to queue qc sbatch file for project/sample '
                  '"{}"/"{}"!'.format(project, sample))
    else:
        LOG.info('Queued qc sbatch file for project/sample '
                 '"{}"/"{}": slurm job id {}'.format(project, sample, slurm_job_id))



def queue_sbatch_file(sbatch_file_path):
    LOG.info("Queueing sbatch file {}".format(sbatch_file_path))
    p_handle = execute_command_line("sbatch {}".format(sbatch_file_path),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
    p_out, p_err = p_handle.communicate()
    try:
        slurm_job_id = re.match(r'Submitted batch job (\d+)', p_out).groups()[0]
    except AttributeError:
        raise RuntimeError('Could not submit sbatch file "{}": '
                           '{}'.format(sbatch_file_path, p_err))
    return int(slurm_job_id)


SBATCH_HEADER = """#!/bin/bash -l

#SBATCH -A {slurm_project_id}
#SBATCH -p {slurm_queue}
#SBATCH -n {num_cores}
#SBATCH -t {slurm_time}
#SBATCH -J {job_name}
#SBATCH -o {slurm_out_log}
#SBATCH -e {slurm_err_log}
"""

def create_sbatch_file(cl_list, project, sample, config):
    project_analysis_path = os.path.join(project.base_path,
                                         "ANALYSIS",
                                         project.project_id,
                                         "qc_ngi")
    log_dir_path = os.path.join(project_analysis_path, "logs")
    sbatch_dir_path = os.path.join(project_analysis_path, "sbatch")
    job_label = "{}-{}".format(project.project_id, sample)
    sbatch_file_path = os.path.join(sbatch_dir_path, job_label)
    safe_makedir(log_dir_path)
    safe_makedir(sbatch_dir_path)
    # sbatch parameters
    try:
        slurm_project_id = config["environment"]["project_id"]
    except KeyError:
        raise RuntimeError('No SLURM project id specified in configuration file '
                           'for job "{}"'.format(job_identifier))
    slurm_queue = config.get("slurm", {}).get("queue") or "core"
    num_cores = config.get("slurm", {}).get("cores") or 16
    slurm_time = config.get("qc", {}).get("job_walltime", {}) or "1-00:00:00"
    slurm_out_log = os.path.join(sbatch_dir_path, "{}_sbatch.out".format(job_label))
    slurm_err_log = os.path.join(log_dir_path, "{}_sbatch.err".format(job_label))
    for log_file in slurm_out_log, slurm_err_log:
        rotate_file(log_file)
    sbatch_text = SBATCH_HEADER.format(slurm_project_id=slurm_project_id,
                                       slurm_queue=slurm_queue,
                                       num_cores=num_cores,
                                       slurm_time=slurm_time,
                                       job_name="qc_{}".format(job_label),
                                       slurm_out_log=slurm_out_log,
                                       slurm_err_log=slurm_err_log)
    sbatch_text_list = sbatch_text.split("\n")
    sbatch_extra_params = config.get("slurm", {}).get("extra_params", {})
    for param, value in sbatch_extra_params.iteritems():
        sbatch_text_list.append("#SBATCH {} {}\n\n".format(param, value))
    sbatch_text_list.append("echo -ne '\\n\\nExecuting command lines at '")
    sbatch_text_list.append("date")
    # Note that because these programs have such small output,
    # we're writing results directly to permanent storage and thus
    # it is not necessary to copy results back from anywhere
    sbatch_text_list.append("# Run the actual commands")
    for command_line_sublist in cl_list:
        for command_line in command_line_sublist:
            sbatch_text_list.append(command_line)
    rotate_file(sbatch_file_path)
    LOG.info("Writing sbatch file to {}".format(sbatch_file_path))
    with open(sbatch_file_path, 'w') as f:
        f.write("\n".join(sbatch_text_list))
    return sbatch_file_path
