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
