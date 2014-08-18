import os
import random
import shlex
import socket
import subprocess
import tempfile
import unittest

from .filesystem import chdir, curdir_tmpdir, do_rsync, execute_command_line, \
                        load_modules, safe_makedir


class TestFilesystemUtils(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def test_load_modules(self):
        modules_to_load = ['R/3.1.0', 'java/sun_jdk1.7.0_25']
        load_modules(modules_to_load)
        assert(subprocess.check_output(shlex.split("R --version")).split()[2] == "3.1.0")

    def test_execute_command_line(self):
        cl = "hostname"
        popen_object = execute_command_line(cl, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        reported_hostname = popen_object.communicate()[0].strip()
        assert(reported_hostname == socket.gethostname())

    def test_execute_command_line_RuntimeError(self):
        cl = "nosuchcommand"
        with self.assertRaises(RuntimeError):
            execute_command_line(cl)

    def test_do_rsync(self):
        src_dir = tempfile.mkdtemp()
        dst_dir = tempfile.mkdtemp()
        file_names = ["file1.txt", "file2.txt"]
        src_file_paths = [os.path.join(src_dir, file_name) for file_name in file_names]
        dst_file_paths = [os.path.join(dst_dir, file_name) for file_name in file_names]
        for src_file_path in src_file_paths:
            open(src_file_path, 'w').close()
        do_rsync(src_file_paths, dst_dir)
        for dst_file_path in dst_file_paths:
            assert(os.path.exists(dst_file_path))

    def test_safe_makedir_singledir(self):
        # Should test that this doesn't overwrite an existing dir as well
        single_dir = os.path.join(self.tmp_dir, "single_directory")
        safe_makedir(single_dir)
        assert(os.path.exists(single_dir))

    def test_safe_makedir_dirtree(self):
        dir_tree = os.path.join(self.tmp_dir, "first", "second", "third")
        safe_makedir(dir_tree)
        assert(os.path.exists(dir_tree))

    def test_curdir_tmpdir(self):
        with curdir_tmpdir() as new_tmp_dir:
            assert(os.path.exists(new_tmp_dir))
        assert(not os.path.exists(new_tmp_dir)), "Directory is not properly removed after creation"

    def test_chdir(self):
        original_dir = os.getcwd()
        new_directory = os.path.join(original_dir, self.tmp_dir)
        with chdir(self.tmp_dir):
            assert(os.getcwd() == new_directory), "New directory does not match intended one"
        assert(os.getcwd() == original_dir), "Original directory is not returned to after context manager is closed"
