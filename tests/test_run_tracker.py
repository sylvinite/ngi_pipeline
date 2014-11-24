#!/usr/bin/env python

import unittest

from run_tracker import *

class TestTracker(unittest.TestCase):
    """ run_tracker.py script tests
    """
    def __init__(self):
        pass

    @classmethod
    def setUpClass(self):
        print 'yuhhh'

    @classmethod
    def tearDownClass(self):
        print 'oh...'
