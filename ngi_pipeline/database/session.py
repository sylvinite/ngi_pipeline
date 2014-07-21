import os

from ngi_pipeline.database.classes import CharonSession

try:
    CHARON_API_TOKEN = os.environ['CHARON_API_TOKEN']
    CHARON_BASE_URL = os.environ['CHARON_BASE_URL']
except KeyError as e:
    raise ValueError("Could not get required environmental variable "
                     "\"{}\"; cannot connect to database.".format(e))

def construct_charon_url(*args):
    """Build a Charon URL, appending any *args passed."""
    return "{}api/v1/{}".format(CHARON_BASE_URL,'/'.join([str(a) for a in args]))

def get_charon_session():
    """Return a requests.Session preloaded with the api_token and base_url"""
    # Double trailing slash shouldn't hurt, right?
    base_url = construct_charon_url()
    api_token = api_token = {'X-Charon-API-token': CHARON_API_TOKEN}
    # Preload the session with the api_token and the base url
    return CharonSession(api_token=api_token, base_url=base_url)


def get_charon_session_for_project(project_name):
    return get_charon_session(append_url="api/v1/project/{}".format(project_name))
