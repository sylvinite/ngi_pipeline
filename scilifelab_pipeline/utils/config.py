import ConfigParser
import os
import yaml

# Why do we need pm.conf again?
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

## TODO implement for XML
def load_yaml_config(config_file):
    """Load YAML config file, replacing environmental variables."""
    with open(config_file) as in_handle:
        config = yaml.load(in_handle)
    config = _expand_paths(config)
    return config

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
