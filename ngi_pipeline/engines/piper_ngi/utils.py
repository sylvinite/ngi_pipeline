import os

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
