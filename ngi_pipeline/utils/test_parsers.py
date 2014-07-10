import datetime
import random
import tempfile
import unittest

from .parsers import get_flowcell_id_from_dirtree, parse_lane_from_filename
from ..tests import generate_test_data as gtd

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
        self.assertEqual(get_flowcell_id_from_dirtree(sthlm_project_path), flowcell_id)
        # Test Uppsala format
        self.assertEqual(get_flowcell_id_from_dirtree(uppsala_project_path), flowcell_id)

    def test_parse_lane_from_filename(self):
        lane = random.randint(1,8)
        file_name = gtd.generate_sample_file_name(lane=lane)
        self.assertEqual(lane, parse_lane_from_filename(file_name))
