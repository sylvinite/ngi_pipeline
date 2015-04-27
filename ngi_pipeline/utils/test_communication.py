import unittest
from communication import mail_analysis

class TestCommunication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_name = "Y.Mom_15_01"
        cls.sample_name = "P1155_101"
        cls.engine_name = "piper_ngi"
        cls.workflow = "Workin' it workin' it"

    def test_mail_analysis(self):
        # INFO
        mail_analysis(project_name=self.project_name,
                      sample_name=self.sample_name,
                      engine_name=self.engine_name,
                      level="INFO",
                      info_text="Your mom goes to college.",
                      workflow=self.workflow)
        # WARN
        mail_analysis(project_name=self.project_name,
                      sample_name=self.sample_name,
                      engine_name=self.engine_name,
                      level="WARN",
                      info_text="Your mom: she goes to college!",
                      workflow=self.workflow)

        # ERROR
        mail_analysis(project_name=self.project_name,
                      sample_name=self.sample_name,
                      engine_name=self.engine_name,
                      level="ERROR",
                      info_text="News about your mom -- she goes to college!!",
                      workflow=self.workflow)

