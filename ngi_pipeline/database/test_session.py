import os
import requests
import unittest

from ngi_pipeline.database.classes import CharonSession
from ngi_pipeline.database.session import CHARON_BASE_URL, construct_charon_url, get_charon_session


class TestSession(unittest.TestCase):

    # This doesn't work -- changes here to os.environ don't affect os.environ when loading the other file
    #def test_missing_environmental_variable_throws_KeyError(self):
    #    if 'CHARON_API_TOKEN' in os.environ: charon_api_token = os.environ.pop('CHARON_API_TOKEN')
    #    with self.assertRaises(ValueError):
    #        import ngi_pipeline.database.session
    #    if charon_api_token: os.environ['CHARON_API_TOKEN'] = charon_api_token

    def test_construct_charon_url(self):
        append_list = ["road","to","nowhere"]
        # This is a weird test because it's the same code as I'm testing but it also seems weird to code it worse
        finished_url = "{}/api/v1/{}".format(CHARON_BASE_URL,'/'.join([str(a) for a in append_list]))
        # The method expects not a list but individual args
        self.assertEqual(finished_url, construct_charon_url(*append_list))

    def test_get_charon_session(self):
        # Not sure how to test this except to test for the type and attributes
        charon_session = get_charon_session()
        self.assertEqual(type(charon_session), CharonSession)
