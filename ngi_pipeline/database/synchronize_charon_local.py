"""Synchronize the local database with Charon."""

from ngi_pipeline.database.local_process_tracking import get_all_tracked_processes

## Temporary until we switch to SQL
def synchronize_localdb_with_charon():
    """Goes through all the entries in the local job-tracking db and syncs up with Charon."""
    charon_session = CharonSession()
    db_dict = get_all_tracked_processes()
    for key, value in db.iteritems():
        seqrun_match = re.match(r'(?P<project_name>\w\.\w+_\d+_\d+|\w{2}-\d+)_(?P<sample_id>[\w-]+)_(?P<libprep_id>\w|\w{2}\d{3}_\2)_(?P<seqrun_id>\d{6}_\w+_\d{4}_.{10})', key)
        sample_match = re.match(r'(?P<project_name>\w\.\w+_\d+_\d+|\w{2}-\d+)_(?P<sample_id>[\w-]+)', key)
        if seqrun_match:
            import ipdb; ipdb.set_trace()
        elif sample_match:
            import ipdb; ipdb.set_trace()


from ngi_pipeline.database.sqlalchemy_db import get_db_session, Base

def sql_synchronize_localdb_with_charon():
    # List all tables
    all_tables_names = Base.metadata.tables.keys()
    for name in all_tables_names:
        print name

