"""Keeps track of running workflow processes"""
import shelve

from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config

LOG = minimal_logger(__name__)


def get_all_tracked_processes(config=None):
    """Returns all the processes that are being tracked locally,
    which is to say all the processes that have a record in our local
    process_tracking database.

    :param dict config: The parsed configuration file (optional)

    :returns: The dict of the entire database
    :rtype: dict
    """
    # This function doesn't do a whole lot
    db = get_shelve_database(config)
    return db


def remove_record_from_local_tracking(project, config=None):
    """Remove a record from the local tracking database.

    :param NGIProject project: The NGIProject object
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the record could not be deleted
    """
    LOG.info('Attempting to remove local process record for '
             'project "{}"'.format(project))
    db = get_shelve_database(config)
    try:
        db.pop(project.name)
    except KeyError:
        error_msg = ('Project "{}" not found in local process '
                     'tracking database.'.format(project))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    db.close()


def write_status_to_charon(project, return_code):
    """Update the status of a workflow for a project in the Charon database.

    :param NGIProject project: The NGIProject object
    :param int return_code: The return code of the workflow process

    :raises RuntimeError: If the Charon database could not be updated
    """
    charon_session = get_charon_session()
    status = "Completed" if return_code is 0 else "Failed"
    project_url = construct_charon_url("project", project.name)
    try:
        project_dict = charon_session.get(get_url)
        project_dict["status"] = status
        charon_session.put(project_dict)
    except HTTPError as e:
        error_msg = ('Failed to update project status for "{}" '
                     'in Charon database: {}'.format(project, e))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)


def record_workflow_process_local(p_handle, workflow, project, analysis_module, config=None):
    """Track the PID for running workflow analysis processes.

    :param subprocess.Popen p_handle: The subprocess.Popen object which executed the command
    :param str workflow: The name of the workflow that is running
    :param Project project: The Project object for which the workflow is running
    :param analysis_module: The analysis module used to execute the workflow
    :param dict config: The parsed configuration file (optional)

      Stored dict resembles {"J.Doe_14_01":
                                {"workflow": "NGI",
                                 "p_handle": p_handle,
                                 "analysis_module": analysis_module.__name__
                                }
                             "J.Johansson_14_02":
                                 ...
                            }

    :raises KeyError: If the database portion of the configuration file is missing
    :raises RuntimeError: If the configuration file cannot be found
    :raises ValueError: If the project already has an entry in the database.
    """
    ## Probably better to use an actual SQL database for this so we can
    ## filter by whatever -- project name, analysis module name, pid, etc.
    ## For the prototyping we can use shelve but later move to sqlite3 or sqlalchemy+Postgres/MySQL/whatever
    LOG.info("Recording process id {} for project {}, " 
             "workflow {}".format(p_handle.pid, project, workflow))
    project_dict = { "workflow": workflow,
                     "p_handle": p_handle,
                     "analysis_module": analysis_module.__name__
                   }
    db = get_shelve_database(config)
    # I don't see how this would ever happen but it makes me nervous to not
    # even check for this.
    if project.name in db:
        error_msg = ("Project {} already has an entry in the local process "
                     "tracking database -- this will not do. Call the schoolmaster!")
        LOG.error(error_msg)
        raise ValueError(error_msg)
    db[project.name] = project_dict
    db.close()
    LOG.info("Successfully recorded process id {} for project {}, " 
             "workflow {}".format(p_handle.pid, project, workflow))


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
