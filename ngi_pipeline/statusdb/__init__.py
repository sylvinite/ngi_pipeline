from ngi_pipeine.utils.classes import memoized

@memoized
## TODO change to use new database API
## TODO How to deal with Uppsala project naming?
def get_project_data_for_id(project_id, proj_db):
    """Pulls all the data about a project from the StatusDB
    given the project's id (e.g. "P602") and a couchdb view object.

    :param str project_id: The project ID
    :param proj_db: The project_db object

    :returns: A dict of the project data
    :rtype: dict
    :raises ValueError: If the project could not be found in the database
    """
    db_view = proj_db.view('project/project_id')
    try:
        return proj_db.get([proj.id for proj in db_view if proj.key == project_id][0])
    except IndexError:
        error_msg = "Warning: project ID '{}' not found in Status DB".format(project_id)
        LOG.error(error_msg)
        raise ValueError(error_msg)
