import json
import os
import tempfile
import unittest
import yaml

from .config import locate_ngi_config, load_json_config, load_xml_config, \
                    load_yaml_config, load_generic_config, _expand_paths, \
                    expand_path, lowercase_keys
from ..tests import generate_test_data as gtd
#from dicttoxml import dicttoxml


class TestConfigLoaders(unittest.TestCase):
    def setUp(self):
        self.config_dict = {"base_key":
                             {"string_key": "string_value",
                              "list_key": ["list_value_1", "list_value_2"],
                              }
                            }
        self.tmp_dir = tempfile.mkdtemp()

    def test_locate_ngi_config_environ(self):
        environ_var_holder = os.environ.get('NGI_CONFIG')
        temp_ngi_config = os.path.join(self.tmp_dir, "ngi_config.yaml")
        open(temp_ngi_config, 'w').close()
        try:
            os.environ['NGI_CONFIG'] = temp_ngi_config
            assert(locate_ngi_config())
        finally:
            if environ_var_holder:
                os.environ['NGI_CONFIG'] = environ_var_holder
            else:
                os.environ.pop('NGI_CONFIG')

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

    # XML parsers all introduce all these extra goddamn keys I don't need goddammit
    #def test_load_xml_config(self):
    #    config_file_path = os.path.join(self.tmp_dir, "config.xml")
    #    with open(config_file_path, 'w') as config_file:
    #        config_file.write(dicttoxml(self.config_dict))
    #    xml_config_dict = i_hate_xml(load_xml_config(config_file_path, xml_attribs=False))
    #    self.assertEqual(self.config_dict, xml_config_dict)

    def test_load_generic_config_IOError(self):
        config_file_path = "/no/such/file"
        with self.assertRaises(IOError):
            load_generic_config(config_file_path)

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
        uppercase_dict = {"Key": "value",
                          "DICT": {"KEY": "value"}}
        lowercase_dict = {"key": "value",
                          "dict": {"key": "value"}}
        self.assertEqual(lowercase_dict, lowercase_keys(uppercase_dict))

#def i_hate_xml(xml_object):
#   # Convert OrderedDict objects to normal dict objects and unicode to str
#    try:
#        # Convert OrderedDict to dict
#        xml_object = dict(xml_object)
#        # The dict may have an OrderedDict as a value
#        str_dict = {}
#        for key, value in xml_object.iteritems():
#            str_dict[str(key)] = i_hate_xml(value)
#            xml_object = str_dict
#    except ValueError:
#        # Don't be a dict
#        if type(xml_object) is unicode:
#            xml_object = str(xml_object)
#        elif type(xml_object) is list:
#            xml_object = map(str, xml_object)
#    return xml_object
