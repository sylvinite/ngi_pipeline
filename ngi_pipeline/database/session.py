def get_charon_session(append_url=""):
    try:
        API_TOKEN = os.environ['CHARON_API_TOKEN']
        BASE_URL = os.environ['CHARON_BASE_URL']
    except KeyError as e:
        raise ValueError("Could not get required environmental variable "
                         "\"{}\"; cannot connect to database.".format(e))
    # Double trailing slash shouldn't hurt, right?
    base_url = base_url + "/" + append_url
    api_token = api_token = {'X-Charon-API-token': API_TOKEN}
    # Preload the session with the api_token and return it
    return CharonSession(api_token=api_token, base_url=base_url)

def get_charon_session_for_project(project_name):
    return get_charon_session(append_url="api/v1/project/{}".format(project_name))
