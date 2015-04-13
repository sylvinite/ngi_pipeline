#!/bin/env python

from __future__ import print_function


import argparse
import importlib

from ngi_pipeline.engines.piper_ngi.local_process_tracking import update_charon_with_local_jobs_status
from ngi_pipeline.engines.piper_ngi.database import SampleAnalysis, get_db_session

if __name__=="__main__":
    parser = argparse.ArgumentParser("Show all the jobs currently running (currently just for Piper).")
    parser.add_argument("-q", "--quiet", action="store_true",
            help="Don't send notification emails on status changes.")
    args = parser.parse_args()

    update_charon_with_local_jobs_status(quiet=args.quiet)

    with get_db_session() as session:
        sample_jobs = session.query(SampleAnalysis).all()
        print("\nSample-level analysis jobs:")
        if sample_jobs:
            for sample_job in sample_jobs:
                print("\t{}".format(sample_job))
        else:
            print("\tNone")
        print()
