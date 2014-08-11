import collections
import functools

from ngi_pipeline.utils.config import locate_ngi_config

class with_ngi_config(object):
    """
    Try to load the configuration file from a list of default locations.
    """
    def __init__(self, f):
        self.f = f
        # The idea is that ths will allow wrapped functions to keep their
        # original names, but it doesn't seem to be working as I expect
        functools.update_wrapper(self, f)

    def __call__(self, *args, **kwargs):
        if "config_file_path" not in kwargs:
            kwargs["config_file_path"] = locate_ngi_config()
        return self.f(*args, **kwargs)


class memoized(object):
    """
    Decorator, caches results of function calls.
    """
    def __init__(self, func):
        self.func   = func
        self.cached = {}
        functools.update_wrapper(self, func)
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args in self.cached:
            return self.cached[args]
        else:
            return_val = self.func(*args)
            self.cached[args] = return_val
            return return_val
    def __repr__(self):
        return self.func.__doc__
    # This ensures that attribute access (e.g. obj.attr)
    # goes through the __call__ function defined above
    def __get__(self, obj, objtype):
        return functools.partial(self.__call__, obj)

