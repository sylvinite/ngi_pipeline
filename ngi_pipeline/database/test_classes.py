import requests
import unittest

from ngi_pipeline.database.classes import CharonSession, CHARON_BASE_URL

class TestCharonFunctions(unittest.TestCase):

    def setUp(self):
        self.session = CharonSession()

    def test_validate_response(self):
        response = requests.Response()
        with self.assertRaises(ValueError):
            response.status_code = 400
            self.session.validate_response(response, "test_type")
        with self.assertRaises(ValueError):
            response.status_code = 404
            self.session.validate_response(response, "test_type")
        with self.assertRaises(RuntimeError):
            response.status_code = 405
            self.session.validate_response(response, "test_type")
        with self.assertRaises(ValueError):
            response.status_code = 409
            self.session.validate_response(response, "test_type")

    def test_construct_charon_url(self):
        append_list = ["road","to","nowhere"]
        # This is a weird test because it's the same code as I'm testing but it also seems weird to code it worse
        finished_url = "{}/api/v1/{}".format(CHARON_BASE_URL,'/'.join([str(a) for a in append_list]))
        # The method expects not a list but individual args
        self.assertEqual(finished_url, CharonSession().construct_charon_url(*append_list))
