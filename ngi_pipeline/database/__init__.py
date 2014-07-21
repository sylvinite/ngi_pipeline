from ngi_pipeline.database.session import get_charon_session, construct_charon_url

def get_project_id_from_name(project_name):
    """Given the project name ("Y.Mom_14_01") return the project ID ("P123")

    :param str project_name: The human-friendly name of the project (e.g. "J.Doe_14_01")

    :returns: The alphanumeric database-friendly name of the project (e.g. "P123")
    :rtype: str

    :raises RuntimeError: If there is some problem relating to the GET (HTTP Return code != 200)
    :raises ValueError: If the project id is missing from the project database entry
    """
    charon_session = get_charon_session()
    url = construct_charon_url("project", project_name)
    project_response = charon_session.get(url)
    if project_response.status_code != 200:
        raise RuntimeError("Error when accessing Charon DB: {}".format(project_response.reason))
    try:
        return project_response.json()['projectid']
    except KeyError:
        raise ValueError('Couldn\'t retrive project id for project "{}"; '
                         'value not found in database.'.format(project))
