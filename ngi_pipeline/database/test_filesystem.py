import tempfile
import unittest

from ngi_pipeline.conductor.classes import NGIProject, NGISample, NGILibraryPrep, NGISeqRun
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.tests.generate_test_data import generate_run_id

class TestCharonFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.session = CharonSession()
        # Project
        cls.p_id = "P100000"
        cls.p_name = "Y.Mom_14_01"
        cls.p_bp = tempfile.mkdtemp()
        # Sample
        cls.s_id = "{}_101".format(cls.p_id)
        # Libprep
        cls.l_id = "A"
        # Seqrun
        cls.sr_id = generate_run_id()
        # How to do this? You decide!
        #cls.sr_total_reads = 0
        #cls.sr_mac = 0

    def test_create_charon_entries_from_project(self):
        # Create the NGIObjects
        project_obj = NGIProject(name=self.p_name,
                                 dirname=self.p_name,
                                 project_id=self.p_id,
                                 base_path=self.p_bp)
        sample_obj = project_obj.add_sample(name=self.s_id,
                                            dirname=self.s_id)
        libprep_obj = sample_obj.add_libprep(name=self.l_id,
                                             dirname=self.l_id)
        seqrun_obj = libprep_obj.add_seqrun(name=self.sr_id,
                                            dirname=self.sr_id)

        try:
        # Create them in the db
            create_charon_entries_from_project(project_obj)
        finally:
            charon_session = CharonSession()
            charon_session.project_delete(project_obj.project_id)
