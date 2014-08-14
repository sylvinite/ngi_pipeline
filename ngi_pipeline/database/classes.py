from __future__ import print_function

import functools
import json
import os
import re
import requests


try:
    CHARON_API_TOKEN = os.environ['CHARON_API_TOKEN']
    CHARON_BASE_URL = os.environ['CHARON_BASE_URL']
    # Remove trailing slashes
    m = re.match(r'(?P<url>.*\w+)/*', CHARON_BASE_URL)
    if m:
        CHARON_BASE_URL = m.groups()[0]
except KeyError as e:
    raise ValueError("Could not get required environmental variable "
                     "\"{}\"; cannot connect to database.".format(e))


# Could split this into CharonProjectSession, CharonSampleSession, etc.
class CharonSession(requests.Session):
    def __init__(self, api_token=None, base_url=None):
        super(CharonSession, self).__init__()

        self._api_token = api_token or CHARON_API_TOKEN
        self._api_token_dict = {'X-Charon-API-token': self._api_token}
        self._base_url = base_url or CHARON_BASE_URL

    #def get(url_args, *args, **kwargs):
    #    url = self.construct_charon_url(url_args)
    #    return validate_response(super(CharonSession, self).get(url,
    #                                   headers=self._api_token_dict,
    #                                   *args, **kwargs))

        self.get = validate_response(functools.partial(self.get, headers=self._api_token_dict))
        self.post = validate_response(functools.partial(self.post, headers=self._api_token_dict))
        self.put = validate_response(functools.partial(self.put, headers=self._api_token_dict))
        self.delete = validate_response(functools.partial(self.delete, headers=self._api_token_dict))

    ## Another option is to build this into the get/post/put/delete requests
    ## --> Do we ever need to call this (or those) separately?
    def construct_charon_url(self, *args):
        """Build a Charon URL, appending any *args passed."""
        return "{}/api/v1/{}".format(self._base_url,'/'.join([str(a) for a in args]))

    def project_create(self, proj_id, name=None, status=None, pipeline=None, bpa=None):
        data = dict(projectid=proj_id,
                    name=name,
                    status=status,
                    pipeline=pipeline,
                    best_practice_analysis=bpa)
        return self.post(self.construct_charon_url('project'),
                         data=json.dumps(data))

    def project_get(self, proj_id):
        return self.get(self.construct_charon_url('project', proj_id))

    def project_update(self, proj_id, name=None, status=None, pipeline=None, bpa=None):
        data = dict(
                    #projectid=proj_id,
                    name=name,
                    status=status,
                    pipeline=pipeline,
                    best_practice_analysis=bpa)
        return self.put(self.construct_charon_url('project', proj_id),
                        data=json.dumps(data))

    def projects_get_all(self):
        return self.get(self.construct_charon_url('project'))

    def project_delete(self, proj_id):
        return self.delete(self.construct_charon_url('project', proj_id))

    def sample_create(self, proj_id, samp_id, status=None, received=None,
                      qc_status=None, genotyping_status=None,
                      genotyping_concordance=None, lims_initial_qc=None,
                      total_autosomal_coverage=None):
        pass


class validate_response(object):
    """
    Validate or raise an appropriate exception for a Charon API query.
    """
    def __init__(self, f):
        self.f = f
        ## Should these be class attributes? I don't really know
        self.SUCCESS_CODES = (200, 201, 204)
        # There are certainly more failure codes I need to add here
        self.FAILURE_CODES = {
                400: (ValueError, ("Could not create {obj_type}: invalid input "
                                   "data (code {response.status_code})"
                                   "(url {response.url})")),
                404: (ValueError, ("Could not get {obj_type}: no such "
                                   "{obj_type} in database (code {response.status_code}) "
                                   "(url {response.url})")), # when else can we get this? malformed URL?
                405: (RuntimeError, ("Could not create {obj_type}: method not "
                                     "allowed (code {response.status_code})"
                                     "(url {response.url})")),
                409: (ValueError, ("Could not create {obj_type}: document "
                                   "revision conflict (code {response.status_code})"
                                   "(url {response.url})")),}

    def __call__(self, *args, **kwargs):
        # Testing - either remove or resolve
        obj_type = "object"
        response = self.f(*args, **kwargs)
        if response.status_code not in self.SUCCESS_CODES:
            try:
                err_type, err_msg = self.FAILURE_CODES[response.status_code]
            except KeyError:
                # Error code undefined, used generic text
                err_type = RuntimeError
                err_msg = ("Could not create {obj_type}: {response.reason} "
                           "(code {response.status_code})(url {response.url})")
            raise err_type(err_msg.format(**locals()))
        return response
