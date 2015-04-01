import unittest
import tempfile
import os
from command_creation_config import build_piper_cl, build_setup_xml
from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config

class TestCommandCreation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p_name = "Y.Mom_15_01"
        cls.p_id = "P1155"
        cls.sample_name = "P1155_101"
        cls.engine_name = "piper_ngi"
        cls.p_bp = tempfile.mkdtemp()
        cls.workflow_name = "merge_process_variantcall"
        cls.xml_path = os.path.join(cls.p_bp, "some_config.xml")
        cls.exit_file = os.path.join(cls.p_bp, "some_file.exit")
        cls.config = load_yaml_config(locate_ngi_config())

    def test_command_creation(self):
        project_obj = NGIProject(name=self.p_name,
                                 dirname=self.p_name,
                                 project_id=self.p_id,
                                 base_path=self.p_bp)

        cl = build_piper_cl(project=project_obj, workflow_name=self.workflow_name,
                            setup_xml_path=self.xml_path, exit_code_path=self.exit_file,
                            config=self.config, exec_mode='sbatch')

        tcl = cl.split(" ")

        assert tcl[0] == 'piper'
        assert '--xml_input' in tcl
        assert tcl[tcl.index('--xml_input')+1] == self.xml_path
        assert '--output_directory' in tcl
        assert tcl[tcl.index('--output_directory')+1] == \
                os.path.join('$SNIC_TMP', 'ANALYSIS', self.p_name, 'piper_ngi')
        assert '--merge_alignments' in tcl
        assert '--data_processing' in tcl
        assert '--variant_calling' in tcl
