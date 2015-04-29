import collections
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest

from ngi_pipeline.conductor import flowcell, launchers
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.utils.filesystem import locate_project, recreate_project_from_filesystem

LOG = minimal_logger(__name__)

class AutomatedAnalysisTest(unittest.TestCase):
    def _install_test_files(self, workflow_dict):
        """Download required sequence and reference files."""
        url = workflow_dict.get("remote_url")
        local_parent_dir = workflow_dict.get("local_parent_dir")
        if url and local_parent_dir:
            dest_dir = os.path.join(local_parent_dir,
                                    os.path.basename(url).replace(".tar.gz", ""))
            if not os.path.exists(dest_dir):
                self._download_to_dir(url, dest_dir)

    def _download_to_dir(self, url, dest_dir):
        tmp_dir = tempfile.mkdtemp()
        output_dir = os.path.join(tmp_dir, os.path.basename(url))
        cl = "wget -P {} {}".format(tmp_dir, url)
        subprocess.check_call(shlex.split(cl))
        if not os.path.exists(os.path.dirname(dest_dir)):
            os.makedirs(os.path.dirname(dest_dir))
        if output_dir.endswith(".tar.gz") or output_dir.endswith(".tar.gzip"):
            cl = "tar -C {} -xzvf {}".format(os.path.dirname(dest_dir), output_dir)
            subprocess.check_call(shlex.split(cl))
        else:
            shutil.move(output_dir, dest_dir)
        shutil.rmtree(tmp_dir)

    def test_workflows(self):
        config_file_path = locate_ngi_config()
        config = load_yaml_config(config_file_path)
        for workflow_name, workflow_dict in config.get("test_data", {}).get("workflows", {}).iteritems():
            self._install_test_files(workflow_dict)
            LOG.info('Starting test analysis pipeline for workflow "{}"'.format(workflow_name))
            workflow_dict.get("local_files")
            project_path = workflow_dict.get("project")
            flowcell_path = workflow_dict.get("flowcell")
            if project_path:
                try:
                    project_dir = locate_project(project_path)
                except ValueError as e:
                    LOG.error('Test of workflow "{}" failed: {}'.format(workflow_name, e))
                    continue
                project = recreate_project_from_filesystem(project_dir=project_dir)
                if project and os.path.split(project.base_path)[1] == "DATA":
                    project.base_path = os.path.split(project.base_path)[0]
                launchers.launch_analysis([project])
            elif flowcell_path:
                # This is problematic because it will create a permanent record in Charon
                flowcell.process_demultiplexed_flowcell([flowcell_path])
