from __future__ import print_function

import collections
import contextlib
import datetime
import functools
import os
import shlex
import shutil
import stat
import subprocess
import tempfile

from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)

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


def execute_command_line(cl, stdout=None, shell=False, stderr=None, cwd=None):
    """Execute a command line and return the subprocess.Popen object.

    :param cl: Can be either a list or a string; if string, gets shlex.splitted
    :param bool shell: value of shell to pass to subprocess
    :param file stdout: The filehandle destination for STDOUT (can be None)
    :param file stderr: The filehandle destination for STDERR (can be None)
    :param str cwd: The directory to be used as CWD for the process launched

    :returns: The subprocess.Popen object
    :rtype: subprocess.Popen

    :raises RuntimeError: If the OS command-line execution failed.
    """
    if cwd and not os.path.isdir(cwd):
        LOG.warn("CWD specified, \"{}\", is not a valid directory for "
                 "command \"{}\". Setting to None.".format(cwd, cl))
        ## FIXME Better to just raise an exception
        cwd = None
    if type(cl) is str and shell == False:
        LOG.info("Executing command line: {}".format(cl))
        cl = shlex.split(cl)
    if type(cl) is list and shell == True:
        cl = " ".join(cl)
        LOG.info("Executing command line: {}".format(cl))
    try:
        p_handle = subprocess.Popen(cl, stdout=stdout,
                                        stderr=stderr,
                                        cwd=cwd,
                                        shell=shell)
        error_msg = None
    except OSError:
        error_msg = ("Cannot execute command; missing executable on the path? "
                     "(Command \"{}\")".format(cl))
    except ValueError:
        error_msg = ("Cannot execute command; command malformed. "
                     "(Command \"{}\")".format(cl))
    except subprocess.CalledProcessError as e:
        error_msg = ("Error when executing command: \"{}\" "
                     "(Command \"{}\")".format(e, cl))
    if error_msg:
        raise RuntimeError(error_msg)
    return p_handle


def do_rsync(src_files, dst_dir):
    ## TODO I changed this -c because it takes for goddamn ever but I'll set it back once in Production
    #cl = ["rsync", "-car"]
    cl = ["rsync", "-aPv"]
    cl.extend(src_files)
    cl.append(dst_dir)
    cl = map(str, cl)
    # Use for testing: just touch the files rather than copy them
    #for f in src_files:
    #    open(os.path.join(dst_dir,os.path.basename(f)),"w").close()
    subprocess.check_call(cl)
    #execute_command_line(cl)
    return [ os.path.join(dst_dir,os.path.basename(f)) for f in src_files ]


def safe_makedir(dname, mode=0777):
    """Make a directory (tree) if it doesn't exist, handling concurrent race
    conditions.
    """
    if not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname, mode=mode)
        except OSError:
            if not os.path.isdir(dname):
                raise
    return dname

def rotate_log(log_file_path, new_subdirectory="rotated_logs"):
    if os.path.exists(log_file_path) and os.path.isfile(log_file_path):
        file_name, extension = os.path.splitext(log_file_path)
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S:%f")
        if new_subdirectory:
            rotated_file_basepath = os.path.join(os.path.dirname(log_file_path),
                                                 new_subdirectory)
            safe_makedir(rotated_file_basepath)
        else:
            rotated_file_basepath = os.path.dirname(log_file_path)
        rotate_file_path = os.path.join(rotated_file_basepath,
                                        "{}-{}.rotated{}".format(file_name,
                                                                 current_datetime,
                                                                 extension))
        ## TODO what exceptions can we get here? OSError, else?
        try:
            LOG.info('Attempting to rotate log file "{}" to '
                     '"{}"...'.format(log_file_path, rotate_file_path))
            ## FIXME check if the log file is currently open!!
            ## This whole thing is a temporary fix anyway so I guess it's not important.
            ## we won't be using files as logs in any event once we get ELK working.
            shutil.move(log_file_path, rotate_file_path)
        except OSError as e:
            raise OSError('Could not rotate log file "{}" to "{}": '
                          '{}'.format(log_file_path, rotate_file_path, e))

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
    # This is weird behavior. I'm removing and and we'll see if anything breaks.
    #safe_makedir(new_dir)
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)
