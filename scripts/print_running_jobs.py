from __future__ import print_function


import argparse
import importlib

from ngi_pipeline.engines.piper_ngi.local_process_tracking import update_charon_with_local_jobs_status
from ngi_pipeline.engines.piper_ngi.database import SeqrunAnalysis, SampleAnalysis, get_db_session

if __name__=="__main__":
    parser = argparse.ArgumentParser("Show all the jobs currently running (currently just for Piper).")

    update_charon_with_local_jobs_status()

    with get_db_session() as session:
        seqrun_jobs = session.query(SeqrunAnalysis).all()
        print("\nSeqrun-level analysis jobs:")
        if seqrun_jobs:
            for seqrun_job in seqrun_jobs:
                print("\t{}".format(seqrun_job))
        else:
            print("\tNone")

        sample_jobs = session.query(SampleAnalysis).all()
        print("\nSample-level analysis jobs:")
        if sample_jobs:
            for sample_job in sample_jobs:
                print("\t{}".format(sample_job))
        else:
            print("\tNone")
        print()
