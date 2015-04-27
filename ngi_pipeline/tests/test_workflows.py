import collections
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest

from ngi_pipeline.log.loggers import minimal_logger
#from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config


LOG = minimal_logger(__name__)

class AutomatedAnalysisTest(unittest.TestCase):
    def _install_test_files(self, workflow_dict):
        """Download required sequence and reference files."""
        url = workflow_dict.get("remote_url")
        local_parent_dir = workflow_dict.get("local_parent_dir")
        if url and local_parent_dir:
            dest_files = os.path.join(local_parent_dir,
                                      os.path.basename(url).replace(".tar.gz", ""))
            if not os.path.exists(glob.glob(dest_files)):
                self._download_to_dir(url, dest_files)

    def _download_to_dir(self, url, dest_dir):
        tmp_dir = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_dir, os.path.basename(url))
        cl = "wget -P {} {}".format(tmp_dir, url)
        subprocess.check_call(shlex.split(cl))
        if tmp_file.endswith(".tar.gz") or tmp_file.endswith(".tar.gzip"):
            cl = ["tar", "-xzvf", tmp_file]
            subprocess.check_call(cl)
            tmp_file = tmp_file.replace(".tar.gz", "")
            tmp_file = tmp_file.replace(".tar.gzip", "")
        os.makedirs(os.path.dirname(dest_dir))
        shutil.move(tmp_file, dest_dir)
        os.remove(tmp_dir)

    def test_workflows(self):
        config_file_path = locate_ngi_config()
        config = load_yaml_config(config_file_path)
        for workflow_name, workflow_dict in config.get("test_data", {}).get("workflows", {}).iteritems():
            self._install_test_files(workflow_dict)
            LOG.info('Starting test analysis pipeline for workflow "{}"'.format(workflow_name))
