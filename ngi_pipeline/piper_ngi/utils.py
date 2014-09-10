import os

def create_log_file_path(workflow_name, project, sample=None, libprep=None, seqrun=None):
    base_path = os.path.join(project.base_path, "ANALYSIS", project.name, "logs")
    file_name = project.name
    if sample:
        file_name += "-{}".format(sample.name)
        if libprep:
            file_name += "-{}".format(libprep.name)
            if seqrun:
                file_name += "-{}".format(seqrun.name)
    file_name += "-{}".format(workflow_name)
    return os.path.join(base_path, file_name) + ".log"

def create_exit_code_file_path(workflow_name, project, sample=None, libprep=None, seqrun=None):
    base_path = os.path.join(project.base_path, "ANALYSIS", project.name, "logs")
    file_name = project.name
    if sample:
        file_name += "-{}".format(sample.name)
        if libprep:
            file_name += "-{}".format(libprep.name)
            if seqrun:
                file_name += "-{}".format(seqrun.name)
    file_name += "-{}".format(workflow_name)
    return os.path.join(base_path, file_name) + ".exit"
