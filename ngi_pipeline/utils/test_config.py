import json
import os
import tempfile
import unittest
import yaml

from .config import load_json_config, load_xml_config, load_yaml_config
from ..tests import generate_test_data as gtd
from dicttoxml import dicttoxml


class TestCommon(unittest.TestCase):

    def setUp(self):
        self.config_dict = {"base_key":
                             {"string_key": "string_value",
                              "list_key": ["list_value_1", "list_value_2"],
                              }
                            }

        self.tmp_dir = tempfile.mkdtemp()

    def test_load_json_config(self):
        config_file_path = os.path.join(self.tmp_dir, "config.json")
        with open(config_file_path, 'w') as config_file:
            config_file.write(json.dumps(self.config_dict))
        self.assertEqual(self.config_dict, load_json_config(config_file_path))

    def test_load_yaml_config(self):
        config_file_path = os.path.join(self.tmp_dir, "config.yaml")
        with open(config_file_path, 'w') as config_file:
            config_file.write(yaml.dump(self.config_dict, default_flow_style=False))
        self.assertEqual(self.config_dict, load_yaml_config(config_file_path))

    # Goddamn XML parsers return OrderedDicts
    def test_load_xml_config(self):
        config_file_path = os.path.join(self.tmp_dir, "config.xml")
        with open(config_file_path, 'w') as config_file:
            config_file.write(dicttoxml(self.config_dict))
        xml_config_dict = i_hate_xml(load_xml_config(config_file_path, xml_attribs=False))['root']
        self.assertEqual(self.config_dict, xml_config_dict)


def i_hate_xml(xml_dict):
    # Convert OrderedDict objects to normal dict objects
    try:
        xml_dict = dict(xml_dict)
    except ValueError:
        try:
            import ipdb; ipdb.set_trace()
            str(xml_dict)
        except:
            return xml_dict
    for k, v in xml_dict.items():
        xml_dict[k] = i_hate_xml(v)
    return xml_dict
