import json
import requests

from functools import partial

class CharonSession(requests.Session):
    def __init__(self, api_token, base_url):
        super(CharonSession, self).__init__()
        self.get = partial(self.get, headers=api_token)
        self.post = partial(self.post, headers=api_token)
        self.put = partial(self.put, headers=api_token)
        self.delete = partial(self.delete, headers=api_token)
        self.base_url = base_url
