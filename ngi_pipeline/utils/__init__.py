from __future__ import print_function

import collections
import contextlib
import functools
import os
import shlex
import shutil
import stat
import subprocess
import tempfile

def load_modules(modules_list):
    """
    Takes a list of environment modules to load (in order) and
    loads them using modulecmd python load

    :param list modules_list: The list of modules to load

    :raises RuntimeError: If there is a problem loading the modules
    """
    # Module loading is normally controlled by a bash function
    # As well as the modulecmd bash which is used in .bashrc, there's also
    # a modulecmd python which allows us to use modules from within python
    # UPPMAX support staff didn't seem to know this existed, so use with caution
    error_msgs = []
    for module in modules_list:
        # Yuck
        lmod_location = "/usr/lib/lmod/lmod/libexec/lmod"
        cl = "{lmod} python load {module}".format(lmod=lmod_location,
                                                  module=module)
        p = subprocess.Popen(shlex.split(cl), stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
        stdout,stderr = p.communicate()
        try:
            assert(stdout), stderr
            exec stdout
        except Exception as e:
            error_msg = "Error loading module {}: {}".format(module, e)
            error_msgs.append(error_msg)
    if error_msgs:
        raise RuntimeError("".join(error_msgs))

def safe_makedir(dname):
    """Make a directory if it doesn't exist, handling concurrent race conditions.
    """
    if not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname)
        except OSError:
            if not os.path.isdir(dname):
                raise
    return dname


def execute_command_line(cl, stdout=None, stderr=None, cwd=None):
    """Execute a command line and return the PID.

    :param cl: Can be either a list or a string, if string, gets shlex.splitted
    :param file stdout: The filehandle destination for STDOUT (can be None)
    :param file stderr: The filehandle destination for STDERR (can be None)
    :param str cwd: The directory to be used as CWD for the process launched

    :returns: Process ID of launched process
    :rtype: str

    :raises RuntimeError: If the OS command-line execution failed.
    """
    if cwd and not os.path.isdir(cwd):
        LOG.warn("CWD specified, \"{}\", is not a valid directory for "
                 "command \"{}\". Setting to None.".format(cwd, cl))
        cwd = None
    if type(cl) is str:
        cl = shlex.split(cl)
    LOG.info("Executing command line: {}".format(" ".join(cl)))
    try:
        p_handle = subprocess.Popen(cl, stdout = stdout,
                                        stderr = stderr,
                                        cwd = cwd)
        error_msg = None
    except OSError:
        error_msg = ("Cannot execute command; missing executable on the path? "
                     "(Command \"{}\")".format(command_line))
    except ValueError:
        error_msg = ("Cannot execute command; command malformed. "
                     "(Command \"{}\")".format(command_line))
    except subprocess.CalledProcessError as e:
        error_msg = ("Error when executing command: \"{}\" "
                     "(Command \"{}\")".format(e, command_line))
    if error_msg:
        raise RuntimeError(error_msg)
    return p_handle.pid


@contextlib.contextmanager
def curdir_tmpdir(remove=True):
    """Context manager to create and remove a temporary directory.
    """
    tmp_dir_base = os.path.join(os.getcwd(), "tmp")
    safe_makedir(tmp_dir_base)
    tmp_dir = tempfile.mkdtemp(dir=tmp_dir_base)
    safe_makedir(tmp_dir)
    # Explicitly change the permissions on the temp directory to make it writable by group
    os.chmod(tmp_dir, stat.S_IRWXU | stat.S_IRWXG)
    try:
        yield tmp_dir
    finally:
        if remove:
            shutil.rmtree(tmp_dir)


@contextlib.contextmanager
def chdir(new_dir):
    """Context manager to temporarily change to a new directory.
    """
    cur_dir = os.getcwd()
    safe_makedir(new_dir)
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)


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

