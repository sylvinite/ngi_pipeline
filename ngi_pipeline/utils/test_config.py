import json
import os
import tempfile
import unittest
import yaml

from .config import load_json_config, load_xml_config, load_yaml_config, \
                    _expand_paths, expand_path, lowercase_keys
from ..tests import generate_test_data as gtd
from dicttoxml import dicttoxml


class TestConfigParsers(unittest.TestCase):

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

    #def test_load_xml_config(self):
    #    config_file_path = os.path.join(self.tmp_dir, "config.xml")
    #    with open(config_file_path, 'w') as config_file:
    #        config_file.write(dicttoxml(self.config_dict))
    #    xml_config_dict = i_hate_xml(load_xml_config(config_file_path, xml_attribs=False))['root']
    #    import ipdb; ipdb.set_trace()
    #    self.assertEqual(self.config_dict, xml_config_dict)

    def test_expand_path(self):
        self.assertEqual(os.environ['HOME'], expand_path("~"))

    def test_expand_paths(self):
        unexpanded_dict = {'home': '$HOME',
                              'home_dict': {'home': '~'}}
        home_path = os.environ['HOME']
        expanded_dict = {'home': home_path,
                              'home_dict': {'home': home_path}}
        self.assertEqual(expanded_dict, _expand_paths(unexpanded_dict))

    def test_lowercase_keys(self):
        uppercase_dict = {"Key": "Value",
                          "DICT": {"KEY": "Value"}}
        lowercase_dict = {"key": "value",
                          "dict": {"key": "value"}}

#def i_hate_xml(xml_object):
#    # Convert OrderedDict objects to normal dict objects and unicode to str
#    try:
#        # Convert OrderedDict to dict
#        xml_object = dict(xml_object)
#        # The dict may have an OrderedDict as a value
#        str_dict = {}
#        for key, value in xml_object.iteritems():
#            str_dict[str(key)] = i_hate_xml(value)
#        xml_object = str_dict
#    except ValueError:
#        # Don't be a dict
#        if type(xml_object) is unicode:
#            xml_object = str(xml_object)
#        elif type(xml_object) is list:
#            xml_object = map(str, xml_object)
#    return xml_object
