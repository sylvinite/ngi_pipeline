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
        |__ 141124_FINISHED_FCIDXX
        |   |__ Demultiplexing
        |   |   |__ Stats
        |   |       |__ DemultiplexingStats.xml
        |   |__ RTAComplete.txt
        |__ 141124_IN_PROGRESS_FCIDXX
        |   |__ Demultiplexing
        |   |__ RTAComplete.txt
        |__ 141124_RUNNING_FCIDXX
        |__ 141124_TOSTART_FCIDXX
            |__ RTAComplete.txt
        """
        self.tmp_dir = 'tmp'
        self.running = os.path.join(self.tmp_dir, '141124_RUNNING_FCIDXX')
        self.to_start = os.path.join(self.tmp_dir, '141124_TOSTART_FCIDXX')
        self.in_progress = os.path.join(self.tmp_dir, '141124_IN_PROGRESS_FCIDXX')
        self.finished = os.path.join(self.tmp_dir, '141124_FINISHED_FCIDXX')
        self.finished_runs = [self.to_start, self.in_progress, self.finished]
        # Create runs directory structure
        os.makedirs(self.tmp_dir)
        os.makedirs(self.running)
        os.makedirs(self.to_start)
        os.makedirs(os.path.join(self.in_progress, 'Demultiplexing'))
        os.makedirs(os.path.join(self.finished, 'Demultiplexing', 'Stats'))
        # Create files indicating that the run is finished
        for run in self.finished_runs:
            open(os.path.join(run, 'RTAComplete.txt'), 'w').close()
        # Create files indicating that the preprocessing is done
        open(os.path.join(self.finished, 'Demultiplexing', 'Stats', 'DemultiplexingStats.xml'), 'w').close()

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    def test_1_is_finished(self):
        """ Is finished should be True only if "RTAComplete.txt" file is present...
        """
        self.assertFalse(is_finished(self.running))
        self.assertTrue(all(map(is_finished, self.finished_runs)))
