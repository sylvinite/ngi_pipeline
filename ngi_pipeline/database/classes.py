import json
import requests

from functools import partial

class CharonSession(requests.Session):
    def __init__(self, api_token, base_url):
        super(CharonSession, self).__init__()
        self.get = partial(self.get, headers=api_token)
        self.post = partial(self.get, headers=api_token)
        self.put = partial(self.put, headers=api_token)
        self.base_url = base_url

    def request(self, *args, **kwargs):
        response = super(CharonSession, self).request(*args, **kwargs)
        response.text_as_dict = self._text_as_dict(response)
        return response

    def _text_as_dict(self, response):
        try:
            return json.loads(response.text)
        except ValueError:
            # Could not be decoded (not JSON)
            return None
