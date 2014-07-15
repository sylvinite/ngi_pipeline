import json
import os
import xmltodict
import yaml


def load_json_config(config_file_path):
    """Load XML config file, expanding environmental variables.

    :param str config_file_path: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    raises IOError: If the config file cannot be opened.
    """
    return load_generic_config(config_file_path, config_format="json")


def load_xml_config(config_file_path, xml_attribs=None):
    """Load XML config file, expanding environmental variables.

    :param str config_file_path: The path to the configuration file.
    :param bool xml_attribs: Include/ignore XML attributes when constructing the dict.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    return load_generic_config(config_file_path, config_format="xml", xml_attribs=xml_attribs)


def load_yaml_config(config_file_path):
    """Load YAML config file, expanding environmental variables.

    :param str config_file_path: The path to the configuration file.

    :returns: A dict of the parsed config file.
    :rtype: dict
    :raises IOError: If the config file cannot be opened.
    """
    return load_generic_config(config_file_path, config_format="yaml")


## TODO verify that this works as expected, drinking too much coffee to test code
def load_generic_config(config_file_path, config_format="yaml", **kwargs):
    """Parse a configuration file, returning a dict. Supports yaml, xml, and json.

    :param str config_file_path: The path to the configuration file.
    :param str config_format: The format of the config file; default yaml.

    :returns: A dict of the configuration file with environment variables expanded.
    :rtype: dict
    :raises IOError: If config file cannot be opened.
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
            try:
                config = parser_fn(in_handle, **kwargs)
            except:
                # User-supplied kwargs may be bad
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
