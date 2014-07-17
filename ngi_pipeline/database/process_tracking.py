"""Keeps track of running workflow processes"""

import shelve

from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config

## TODO not sure what to use as parameters here, haven't decided when/where to call the function yet
def get_workflow_returncode(pid, config):
    """Checks to see if a workflow process is finished and if so
    gives the return code.

    :param int pid: The PID of the process to query

    :returns: The return code if the process is finished, None otherwise
    :rtype: int or None
    :raises KeyError: If the pid does not have an entry in the database
    """
    db = get_shelve_database(config)
    process_dict = db[str(pid)]
    p_handle = process_dict["p_handle"]
    # Check if the process is finished
    return_code = p_handle.poll()
    ## How to check if the process is finished and get its return code?
    ## If we keep the subprocess.Popen object, we can call Popen.poll()
    ## If it is finished, it gives the return code; otherwise, it returns None
    return return_code


def record_pid_for_workflow(p_handle, workflow, project, analysis_module, config=None):
    """Track the PID for running workflow analysis processes.

    :param subprocess.Popen p_handle: The subprocess.Popen object which executed the command
    :param str workflow: The name of the workflow that is running
    :param Project project: The Project object for which the workflow is running
    :param analysis_module: The analysis module used to execute the workflow
    :para dict config: The parsed configuration file

    :raises ValueError: if the PID already has a record in the database
    :raises RuntimeError: if the configuration file cannot be found
    :raises KeyError: If the database portion of the configuration file is missing
    """
    ## NOTE I'm not super sure about using shelve for this but I do want to save
    ##      the subprocess.Popen objects. I'm open to suggestions
    db = get_shelve_database(config)
    if p_handle.pid in db:
        ## Not decided yet how to handle this. Shouldn't happen much but might happen I -think-
        ## See http://superuser.com/a/135011/244420
        error_msg = "Record already stored for PID {}; will not overwrite".format(p_handle.pid)
        raise ValueError(error_msg)
    db[str(p_handle.pid)] = {
                        "p_handle": p_handle,
                        "workflow": workflow,
                        "project": project.name,
                        "analysis_module": analysis_module.__name__
                        }
    db.close()


def get_shelve_database(config):
    if not config:
        try:
            config_file_path = locate_ngi_config()
            config = load_yaml_config(config_file_path)
        except RuntimeError:
            error_msg = ("No configuration passed and could not find file "
                         "in default locations.")
            raise RuntimeError(error_msg)
    try:
        database_path = config["database"]["record_tracking_db_path"]
    except KeyError as e:
        error_msg = ("Could not get path to process tracking database "
                     "from provided configuration: key missing: {}".format(e))
        raise KeyError(error_msg)
    return shelve.open(database_path)
