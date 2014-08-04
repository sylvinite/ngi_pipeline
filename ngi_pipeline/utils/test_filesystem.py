import os
import random
import tempfile

from .filesystem import chdir, curdir_tmpdir, do_rsync, safe_makedir

def test_do_rsync():
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

def test_safe_makedir_singledir():
    tmp_dir = tempfile.mkdtemp()
    # Should test that this doesn't overwrite an existing dir as well
    single_dir = os.path.join(tmp_dir, "single_directory")
    safe_makedir(single_dir)
    assert(os.path.exists(single_dir))

def test_safe_makedir_dirtree():
    tmp_dir = tempfile.mkdtemp()
    dir_tree = os.path.join(tmp_dir, "first", "second", "third")
    safe_makedir(dir_tree)
    assert(os.path.exists(dir_tree))

def test_curdir_tmpdir():
    with curdir_tmpdir() as new_tmp_dir:
        assert(os.path.exists(new_tmp_dir))
    assert(not os.path.exists(new_tmp_dir)), "Directory is not properly removed after creation"

def test_chdir():
    original_dir = os.getcwd()
    tmp_dir = tempfile.mkdtemp()
    new_directory = os.path.join(original_dir, tmp_dir)
    with chdir(tmp_dir):
        assert(os.getcwd() == new_directory), "New directory does not match intended one"
    assert(os.getcwd() == original_dir), "Original directory is not returned to after context manager is closed"
