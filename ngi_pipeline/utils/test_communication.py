
import unittest
from communication import mail, mail_sample_analysis



class TestCommunication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_name = "Y.Mom_15_01"
        cls.sample_name = "P1155_101"
        cls.workflow_name = "variant_calling"
        
    def test_mail_sample_analysis(self):
        mail_sample_analysis(self.project_name, self.sample_name, self.workflow_name)

