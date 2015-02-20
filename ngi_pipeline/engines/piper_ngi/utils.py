import collections
import os
import yaml

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.filesystem import rotate_file, safe_makedir

LOG = minimal_logger(__name__)


def get_finished_seqruns_for_sample(project_id, sample_id,
                                    include_failed_libpreps=False):
    """Find all the finished seqruns for a particular sample.

    :param str project_id: The id of the project
    :param str sample_id: The id of the sample

    :returns: A dict of {libprep_01: [seqrun_01, ..., seqrun_nn], ...}
    :rtype: dict
    """
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(projectid=project_id,
                                                         sampleid=sample_id)
    libpreps = collections.defaultdict(list)
    for libprep in sample_libpreps['libpreps']:
        if libprep.get('qc') != "FAILED" or include_failed_libpreps:
            libprep_id = libprep['libprepid']
            for seqrun in charon_session.libprep_get_seqruns(projectid=project_id,
                                                             sampleid=sample_id,
                                                             libprepid=libprep_id)['seqruns']:
                seqrun_id = seqrun['seqrunid']
                aln_status = charon_session.seqrun_get(projectid=project_id,
                                                       sampleid=sample_id,
                                                       libprepid=libprep_id,
                                                       seqrunid=seqrun_id).get('alignment_status')
                if aln_status == "DONE":
                    libpreps[libprep_id].append(seqrun_id)
                else:
                    LOG.info('Skipping seqrun "{}" due to alignment_status '
                             '"{}"'.format(seqrun_id, aln_status))
        else:
            LOG.info('Skipping libprep "{}" due to qc status '
                     '"{}"'.format(libprep, libprep.get("qc")))
    return dict(libpreps)

def get_valid_seqruns_for_sample(project_id, sample_id,
                                 include_failed_libpreps=False,
                                 include_done_seqruns=False):
    """Find all the valid seqruns for a particular sample.

    :param str project_id: The id of the project
    :param str sample_id: The id of the sample
    :param bool include_failed_libpreps: Include seqruns for libreps that have failed QC
    :param bool include_done_seqruns: Include seqruns that are already marked DONE

    :returns: A dict of {libprep_01: [seqrun_01, ..., seqrun_nn], ...}
    :rtype: dict
    """
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(projectid=project_id,
                                                         sampleid=sample_id)
    libpreps = collections.defaultdict(list)
    for libprep in sample_libpreps['libpreps']:
        if libprep.get('qc') != "FAILED" or include_failed_libpreps:
            libprep_id = libprep['libprepid']
            for seqrun in charon_session.libprep_get_seqruns(projectid=project_id,
                                                             sampleid=sample_id,
                                                             libprepid=libprep_id)['seqruns']:
                seqrun_id = seqrun['seqrunid']
                aln_status = charon_session.seqrun_get(projectid=project_id,
                                                       sampleid=sample_id,
                                                       libprepid=libprep_id,
                                                       seqrunid=seqrun_id).get('alignment_status')
                if aln_status != "DONE" or include_done_seqruns:
                    libpreps[libprep_id].append(seqrun_id)
                else:
                    LOG.info('Skipping seqrun "{}" due to alignment_status '
                             '"{}"'.format(seqrun_id, aln_status))
        else:
            LOG.info('Skipping libprep "{}" due to qc status '
                     '"{}"'.format(libprep, libprep.get("qc")))
    return dict(libpreps)


def record_analysis_details(project, job_identifier):
    """Write a yaml file enumerating exactly which fastq files we've started
    analyzing.
    """
    output_file_path = os.path.join(project.base_path, "ANALYSIS",
                                    project.dirname, "logs",
                                    "{}.files".format(job_identifier))
    analysis_dict = {}
    proj_dict = analysis_dict[project.dirname] = {}
    for sample in project:
        samp_dict = proj_dict[sample.name] = {}
        for libprep in sample:
            lib_dict = samp_dict[libprep.name] = {}
            for seqrun in libprep:
                lib_dict[seqrun.name] = seqrun.fastq_files
    rotate_file(output_file_path)
    safe_makedir(os.path.dirname(output_file_path))
    with open(output_file_path, 'w') as f:
        f.write(yaml.dump(analysis_dict))


def create_project_obj_from_analysis_log(project_name, project_id,
                                         project_base_path, sample_id, workflow):
    """Using the log of seqruns used for a sample analysis, recreate a project
    object with relevant sample, libprep, and seqrun objects.
    """
    analysis_log_filename = "{}-{}-{}.files".format(project_id, sample_id, workflow)
    analysis_log_path = os.path.join(project_base_path, "ANALYSIS",
                                     project_id, "logs", analysis_log_filename)
    with open(analysis_log_path, 'r') as f:
        analysis_dict = yaml.load(f)
    project_obj = NGIProject(name=project_name, dirname=project_id,
                             project_id=project_id, base_path=project_base_path)
    sample_obj = project_obj.add_sample(sample_id, sample_id)
    for libprep_name, seqrun_dict in analysis_dict[project_id][sample_id].items():
        libprep_obj = sample_obj.add_libprep(libprep_name, libprep_name)
        for seqrun_name in seqrun_dict.keys():
            libprep_obj.add_seqrun(seqrun_name, seqrun_name)
    return project_obj


def check_for_preexisting_sample_runs(project_obj, sample_obj, restart_running_jobs, restart_finished_jobs):
    """If any analysis is undergoing or has completed for this sample's
    seqruns, raise a RuntimeError.

    :param NGIProject project_obj: The project object
    :param NGISample sample_obj: The sample object
    :param boolean restart_running_jobs: command line parameter
    :param boolean restart_finished_jobs: command line parameter

    :raise RuntimeError if the status is RUNNING or DONE and the flags do not allow to continue
    """
    project_id = project_obj.project_id
    sample_id = sample_obj.name
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(projectid=project_id,
                                                         sampleid=sample_id)
    for libprep in sample_libpreps['libpreps']:
        libprep_id = libprep['libprepid']
        for seqrun in charon_session.libprep_get_seqruns(projectid=project_id,
                                                         sampleid=sample_id,
                                                         libprepid=libprep_id)['seqruns']:
            seqrun_id = seqrun['seqrunid']
            aln_status = charon_session.seqrun_get(projectid=project_id,
                                                   sampleid=sample_id,
                                                   libprepid=libprep_id,
                                                   seqrunid=seqrun_id).get('alignment_status')
            if (aln_status == "RUNNING" and not restart_running_jobs) or \
                (aln_status == "DONE" and not restart_finished_jobs):
                    raise RuntimeError('Project/Sample "{}/{}" has a preexisting '
                          'seqrun "{}" with status "{}"'.format(project_obj,
                          sample_obj, seqrun_id, aln_status))


SBATCH_HEADER = """#!/bin/bash -l

#SBATCH -A {slurm_project_id}
#SBATCH -p {slurm_queue}
#SBATCH -n {num_cores}
#SBATCH -t {slurm_time}
#SBATCH -J {job_name}
#SBATCH -o {slurm_out_log}
#SBATCH -e {slurm_err_log}
"""

def create_sbatch_header(slurm_project_id, slurm_queue, num_cores, slurm_time,
                         job_name, slurm_out_log, slurm_err_log):
    """
    :param str slurm_project_id: The project ID to use for accounting (e.g. "b2013064")
    :param str slurm_queue: "node" or "core"
    :param int num_cores: How many cores to use (max 16 at the moment)
    :param str slurm_time: The time for to schedule (e.g. "0-12:34:56")
    :param str job_name: The name to use for the job (e.g. "Your Mom")
    :param str slurm_out_log: The path to use for the slurm stdout log
    :param str slurm_err_log: The path to use for the slurm stderr log
    """
    ## TODO check how many cores are available for a given slurm queue
    if num_cores > 16: num_cores = 16
    return SBATCH_HEADER.format(slurm_project_id=slurm_project_id,
                                slurm_queue=slurm_queue,
                                num_cores=num_cores,
                                slurm_time=slurm_time,
                                job_name=job_name,
                                slurm_out_log=slurm_out_log,
                                slurm_err_log=slurm_err_log)


def add_exit_code_recording(cl, exit_code_path):
    """Takes a command line and returns it with increased pizzaz"""
    record_exit_code = "; echo $? > {}".format(exit_code_path)
    if type(cl) is list:
        # This should work, right? Right
        cl = " ".join(cl)
    return cl + record_exit_code


def create_log_file_path(workflow_subtask, project_base_path, project_name,
                         project_id=None,sample_id=None, libprep_id=None, seqrun_id=None):
    file_base_pathname = _create_generic_output_file_path(workflow_subtask,
                                                          project_base_path,
                                                          project_name,
                                                          project_id,
                                                          sample_id,
                                                          libprep_id,
                                                          seqrun_id)
    return file_base_pathname + ".log"


def create_exit_code_file_path(workflow_subtask, project_base_path, project_name, project_id,
                               sample_id=None, libprep_id=None, seqrun_id=None):
    file_base_pathname = _create_generic_output_file_path(workflow_subtask,
                                                          project_base_path,
                                                          project_name,
                                                          project_id,
                                                          sample_id,
                                                          libprep_id,
                                                          seqrun_id)
    return file_base_pathname + ".exit"


def _create_generic_output_file_path(workflow_subtask, project_base_path, project_name, project_id,
                                     sample_id=None, libprep_id=None, seqrun_id=None):
    base_path = os.path.join(project_base_path, "ANALYSIS", project_id, "logs")
    file_name = project_id
    if sample_id:
        file_name += "-{}".format(sample_id)
        if libprep_id:
            file_name += "-{}".format(libprep_id)
            if seqrun_id:
                file_name += "-{}".format(seqrun_id)
    file_name += "-{}".format(workflow_subtask)
    return os.path.join(base_path, file_name)
