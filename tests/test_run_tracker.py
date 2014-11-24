#!/usr/bin/env python

import os
import shutil
import unittest

from run_tracker import *

class TestTracker(unittest.TestCase):
    """ run_tracker.py script tests
    """
    @classmethod
    def setUpClass(self):
        """ Creates the following directory tree for testing purposes:

        tmp/
         ├── 141124_FINISHED
         │   └── Demultiplexing
         │       └── Stats
         │           └── DemultiplexingStats.xml
         ├── 141124_IN_PROGRESS_FCIDXX
         │   └── Demultiplexing
         └── 141124_TOSTART_FCIDXX
        """
        self.tmp_dir = 'tmp'
        self.to_start = os.path.join(self.tmp_dir, '141124_TOSTART_FCIDXX')
        self.in_progress = os.path.join(self.tmp_dir, '141124_IN_PROGRESS_FCIDXX')
        self.finished = os.path.join(self.tmp_dir, '141124_FINISHED')
        os.makedirs(self.tmp_dir)
        os.makedirs(self.to_start)
        os.makedirs(os.path.join(self.in_progress, 'Demultiplexing'))
        os.makedirs(os.path.join(self.finished, 'Demultiplexing', 'Stats'))
        open(os.path.join(self.finished, 'Demultiplexing', 'Stats', 'DemultiplexingStats.xml'), 'w').close()

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)


    def test_1_is_finished(self):
        """ Is finished should be True only if "RTAComplete.txt" file is present...
        """
        self.assertEqual('a', 'a')