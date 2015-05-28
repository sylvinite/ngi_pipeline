import json
import unittest

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession
from ngi_pipeline.database.communicate import get_project_id_from_name

class TestCommunicate(unittest.TestCase):

    def setUp(self):
        # Create the test project
        self.project_id = "P100000"
        self.project_name = "P.Mayhem_14_01"
        self.project_data = dict(projectid=self.project_id, name=self.project_name, status=None)
        self.session = CharonSession()
        response = self.session.post(self.session.construct_charon_url('project'),
                                     data=json.dumps(self.project_data))
        assert response.status_code == 201, "Could not create test project in Charon: {}".format(response.reason)
        project = response.json()
        assert project['projectid'] == self.project_id, "Test project ID is incorrect"


    def tearDown(self):
        # Remove the test project
        response = self.session.delete(self.session.construct_charon_url('project', self.project_id))
        assert response.status_code == 204, "Could not delete test project from Charon: {}".format(response.reason)


    def test_get_project_id_from_name(self):
        # Check that it matches
        self.assertEqual(self.project_id, get_project_id_from_name(self.project_name))

    def test_rebuild_project_obj_from_charon(self):
        # Create fake project / sample / libprep / seqrun
        pass
