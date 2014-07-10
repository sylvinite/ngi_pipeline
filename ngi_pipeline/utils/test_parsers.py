import datetime
import tempfile
import unittest

import tests
#import tests.generate_test_data as gtd
#from ngi_pipeline import common
from ngi_pipeline.common import get_flowcell_id_from_dirtree

# Does this need to be a class? What are the advantages? RTD
class TestCommon(unittest.TestCase):

    def test_get_flowcell_id_from_dirtree(self):
        date = datetime.date.today().strftime("%y%m%d")
        flowcell_id = gtd.generate_flowcell_id()
        run_id = gtd.generate_identifier(date=date, flowcell_id=flowcell_id)
        sthlm_project_path = "/proj/a2010002/analysis/{project_name}/{sample_name}/{identifier}".format(
                project_name = gtd.generate_project_name(),
                sample_name = gtd.generate_sample_name(),
                identifier = run_id)
        uppsala_project_path = "/proj/a2010002/analysis/{run_id}/Sample_{sample_name}".format(
                run_id = run_id,
                sample_name = gtd.generate_sample_name())
        # Test Sthlm format
        assertEqual(ngi_pipeline.common.get_flowcell_id_from_dirtree(sthlm_project_path),
                    "{}_{}".format(date, flowcell_id))
        # Test Uppsala format
        assertEqual(ngi_pipeline.common.get_flowcell_id_from_dirtree(uppsala_project_path),
                    "{}_{}".format(date, flowcell_id))
