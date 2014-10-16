import os


SBATCH_HEADER = """
#!/bin/bash -l

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
    return SBATCH_HEADER.format(slurm_project_id=project_id,
                                slurm_queue=slurm_queue,
                                num_cores=num_cores,
                                slurm_time=slurm_time,
                                job_name=job_name,
                                slurm_out_log=slurm_out_log,
                                slurm_err_log=slurm_err_log)


def create_log_file_path(workflow_subtask, project_base_path, project_name,
                         sample_id=None, libprep_id=None, seqrun_id=None):
    file_base_pathname = _create_generic_output_file_path(workflow_subtask,
                                                          project_base_path,
                                                          project_name,
                                                          sample_id,
                                                          libprep_id,
                                                          seqrun_id)
    return file_base_pathname + ".log"


def create_exit_code_file_path(workflow_subtask, project_base_path, project_name,
                               sample_id=None, libprep_id=None, seqrun_id=None):
    file_base_pathname = _create_generic_output_file_path(workflow_subtask,
                                                          project_base_path,
                                                          project_name,
                                                          sample_id,
                                                          libprep_id,
                                                          seqrun_id)
    return file_base_pathname + ".exit"


def _create_generic_output_file_path(workflow_subtask, project_base_path, project_name,
                                     sample_id=None, libprep_id=None, seqrun_id=None):
    base_path = os.path.join(project_base_path, "ANALYSIS", project_name, "logs")
    file_name = project_name
    if sample_id:
        file_name += "-{}".format(sample_id)
        if libprep_id:
            file_name += "-{}".format(libprep_id)
            if seqrun_id:
                file_name += "-{}".format(seqrun_id)
    file_name += "-{}".format(workflow_subtask)
    return os.path.join(base_path, file_name)
