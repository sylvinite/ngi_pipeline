import functools

class memoized(object):
    """
    Decorator, caches results of function calls.
    """
    def __init__(self, func):
        self.func   = func
        self.cached = {}
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

