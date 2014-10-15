import os


SBATCH_HEADER = """
#!/bin/bash -l

#SBATCH -A {project_id}
#SBATCH -p {slurm_queue}
#SBATCH -n {num_cores}
#SBATCH -t {slurm_time}
#SBATCH -J {job_name}
#SBATCH -o {slurm_out_log}
#SBATCH -e {slurm_err_log}

"""

def create_sbatch_file(project_id, slurm_queue, num_cores, slurm_time,
                       job_name, slurm_out_log, slurm_err_log,
                       command_line_list, path_to_sbatch_file):
    """
    Note that the path to the sbatch file must be the full path including
    the desired filename.
    """
    sbatch_text = SBATCH_HEADER.format(project_id=project_id,
                                       slurm_queue=slurm_queue,
                                       num_cores=num_cores,
                                       slurm_time=slurm_time,
                                       job_name=job_name,
                                       slurm_out_log=slurm_out_log,
                                       slurm_err_log=slurm_err_log)
    for command_line in command_line_list:
        sbatch_text += "\n{}".format(command_line)
    with open(path_to_sbatch_file, 'w') as f:
        f.write(sbatch_text)
        f.write("\n")

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
