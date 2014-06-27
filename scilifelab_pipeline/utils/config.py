import ConfigParser
import json
import os
import xmltodict
import yaml

def load_pm_config(config_file=None):
    """Loads a configuration file.

    By default it assumes ~/.pm/pm.conf
    """
    try:
        if not config_file:
            config_file = os.path.join(os.environ.get('HOME'), '.pm', 'pm.conf')
        config = ConfigParser.SafeConfigParser()
        with open(config_file) as f:
            config.readfp(f)
        return config
    except IOError:
        raise IOError('There was a problem loading the configuration file. \
                Please make sure that ~/.pm/pm.conf exists and that you have \
                read permissions')

def load_xml_config(config_file):
    """Load XML config file, expanding environmental variables."""
    return load_generic_config(config_file, config_format="xml")


def load_yaml_config(config_file):
    """Load YAML config file, expanding environmental variables."""
    #try:
    #    with open(config_file) as in_handle:
    #        config = yaml.load(in_handle)
    #    config = _expand_paths(config)
    #    return config
    #except IOError as e:
    #    raise IOError("Could not open configuration file \"{}\".".format(config_file))
    return load_generic_config(config_file, config_format="yaml")

## TODO verify that this works as expected, drinking too much coffee to test code
def load_generic_config(config_file_path, config_format="yaml"):
    """Parse a configuration file, returning a dict. Supports yaml, xml, and json.

    :param str config_file_path: The path to the configuration file.
    :param str config_format: The format of the config file; default yaml.
    :returns: A dict of the configuration file with environment variables expanded.
    :rtype: dict
    """
    parsers_dict = {"json": json.load,
                    "xml": xmltodict.parse,
                    "yaml": yaml.load,}
    try:
        file_ext = os.path.splitext(config_file_path)[1].replace(".", "")
    except (IndexError, AttributeError):
        file_ext = None
    ## TODO Does this work? Coffee coffee
    try:
        parser_fn = parsers_dict[config_format.lower()]
    except KeyError:
        try:
            # If the user-supplied format fails, try parsing using the format
            # specified by the file extension
            parser_fn = parsers_dict[file_ext.lower()]
        except:
            raise IOError("Cannot parse config files in format specified "
                          "(not supported): \"{}\"".format(config_format))
    try:
        with open(config_file_path) as in_handle:
            config = parser_fn(in_handle)
        config = _expand_paths(config)
        return config
    except IOError as e:
        raise IOError("Could not open configuration file \"{}\".".format(config_file_path))


def _expand_paths(config):
    for field, setting in config.items():
        if isinstance(config[field], dict):
            config[field] = _expand_paths(config[field])
        else:
            config[field] = expand_path(setting)
    return config

def expand_path(path):
    """Combines os.path.expandvars with replacing ~ with $HOME."""
    try:
        return os.path.expandvars(path.replace("~", "$HOME"))
    except AttributeError:
        return path

def lowercase_keys(dict, deepcopy=False):
    """Return a (default shallow) copy of the dict passed in with all the keys in lowercase."""
    ## TODO implement deep copy
    dict_copy = {}
    for key, value in dict.iteritems():
        key = key.lower()
        try:
            dict_copy[key] = lowercase_keys(value)
        except AttributeError:
            dict_copy[key] = value
    return dict_copy
