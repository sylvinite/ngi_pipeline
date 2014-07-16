def return_session():
    try:
        API_TOKEN = os.environ['CHARON_API_TOKEN']
        BASE_URL = os.environ['CHARON_BASE_URL']
    except KeyError as e:
        raise ValueError("Could not get required environmental variable "
                         "\"{}\"; cannot connect to database.".format(e))
    api_token = api_token = {'X-Charon-API-token': API_TOKEN}
    # Preload the session with the api_token and return it
    return CharonSession(api_token=api_token)
