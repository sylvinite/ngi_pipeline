import requests
from functools import partial

class CharonSession(requests.Session):
    def __init__(self, api_token):
        super(requests.Session, self).__init__(self)
        self.get = partial(self.get, headers=api_token)
        self.post = partial(self.get, headers=api_token)
        self.put = partial(self.put, headers=api_token)
