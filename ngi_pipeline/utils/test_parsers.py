import datetime
import os
import random
import tempfile
import unittest

from .parsers import get_flowcell_id_from_dirtree, parse_lane_from_filename, \
                                       find_fastq_read_pairs, find_fastq_read_pairs_from_dir
from ngi_pipeline.tests import generate_test_data as gtd

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

    def test_find_fastq_read_pairs(self):
        # Test list functionality
        file_list = [ "P123_456_AAAAAA_L001_R1_001.fastq.gz",
                      "P123_456_AAAAAA_L001_R2_001.fastq.gz",]
        expected_output = {"P123_456_AAAAAA_L001": file_list }
        self.assertEqual(expected_output, find_fastq_read_pairs(file_list))

    def test_find_fastq_read_pairs_from_dir(self):
        tmp_dir = tempfile.mkdtemp()
        file_list = map(lambda x: os.path.join(tmp_dir, x),
                    [ "P123_456_AAAAAA_L001_R1_001.fastq.gz",
                      "P123_456_AAAAAA_L001_R2_001.fastq.gz",])
        for file_name in file_list:
            # Touch the file
            open(os.path.join(tmp_dir, file_name), 'w').close()
        expected_output = {"P123_456_AAAAAA_L001": file_list }
        self.assertEqual(expected_output, find_fastq_read_pairs_from_dir(tmp_dir))
