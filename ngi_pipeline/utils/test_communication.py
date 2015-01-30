import unittest
from communication import mail_analysis

class TestCommunication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_name = "Y.Mom_15_01"
        cls.sample_name = "P1155_101"
        cls.engine_name = "piper_ngi"
        cls.err_text = "Your mom goes to college."

    def test_mail_analysis(self):
        mail_analysis(project_name=self.project_name,
                      sample_name=self.sample_name,
                      engine_name=self.engine_name,
                      info_text=self.err_text)

