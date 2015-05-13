"""General Python-specific utility functions, in most cases probably skimmed off StackOverflow."""
import collections
import copy

def flatten(nested_list):
    """All I ever need to know about flattening irregular lists of lists I learned from
    http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python/2158532#2158532"""
    for elt in nested_list:
        if isinstance(elt, collections.Iterable) and not isinstance(elt, basestring):
            for sub in flatten(elt):
                yield sub
        else:
            yield elt

def update_dict(original_dict, updater_dict):
    """Updates a dictionary in a nested fashion, replacing and adding individual
    keys/values but not overwriting entire nested dicts.

    :param dict original_dict: The dict to update
    :param dict updater_dict: The dict with the keys/values to update the original

    :return: A copy of the original dict updated with the new keys/values
    :rtype: dict
    """

    updated_dict = copy.deepcopy(original_dict)
    for key, value in updater_dict.iteritems():
        if updated_dict.get(key) and type(updated_dict[key]) is dict and type(value) is dict:
            # If the value itself is a dict, we want to update, not overwrite
            updated_dict[key] = update_dict(updated_dict[key], value)
        else:
            updated_dict[key] = value
    return updated_dict
