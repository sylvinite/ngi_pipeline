import os
import shelve
import tempfile
import unittest

from ngi_pipeline.database.process_tracking import get_shelve_database, \
                                                   remove_record_from_local_tracking

class TestProcessTracking(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.fake_db_file = os.path.join(self.tmp_dir, "temp_db")
        self.config = {'database': {'record_tracking_db_path': self.fake_db_file}}
        self.fake_job_dict = {"job_name": "job_object"}

        self.fake_db = shelve.open(self.fake_db_file)
        self.fake_db.update(self.fake_job_dict)
        self.fake_db.close()

#    def test_get_all_tracked_processes(self):
#        self.assertEqual(self.fake_job_dict,
#                         get_all_tracked_processes(config=self.config))

    def test_get_shelve_database(self):
        with get_shelve_database(config=self.config) as db:
            assert(db)

    def test_remove_record_from_local_tracking(self):
        try:
            # Remove
            remove_record_from_local_tracking(self.fake_job_dict.keys()[0],
                                              config=self.config)
            dict_copy = self.fake_job_dict.copy()
            dict_copy.pop("job_name")
            # Can I use this in a test? I guess I'm double-testing
            with get_shelve_database(config=self.config) as db:
                self.assertEqual(dict_copy, db)
        finally:
            with get_shelve_database(config=self.config) as db:
                db = {}
                db.update(self.fake_job_dict)
