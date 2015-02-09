#!/bin/env python

"""
Use this script to launch the pipeline from a project that has already been
organized into Project/Sample/Libprep/Seqrun (as opposed to starting from
a delivered Illumina flowcell).
Note that this does not create entries in the Charon database; use
create_charon_project_from_filesystem.py for that with the "-a" flag.
"""
import argparse
import os
import sys

from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem
from ngi_pipeline.conductor.launchers import launch_analysis


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
            help=("Restart jobs marked as FAILED in Charon."))
    parser.add_argument("project_dir", nargs=1, action="store",
            help=("The path to the project to be processed."))
    #parser.add_argument("-m", "--execution-mode", choices=("local", "sbatch"),
    #                    default="sbatch", dest="exec_mode",
    #        help=("How to execute the jobs (via sbatch or locally); default 'sbatch'"))
    parser.add_argument("-d", "--restart-done", dest="restart_finished_jobs", action="store_true",
            help=("Restart jobs marked as DONE in Charon."))
    parser.add_argument("-r", "--restart-running", dest="restart_running_jobs", action="store_true",
            help=("Restart jobs marked as UNDER_ANALYSIS in Charon. Use with care."))
    parser.add_argument("-a", "--restart-all", dest="restart_all_jobs", action="store_true",
            help=("Just start any kind of job you can get your hands on regardless of status."))
    args_dict = vars(parser.parse_args())
    project = recreate_project_from_filesystem(project_dir=args_dict['project_dir'].pop(),
                                               restrict_to_samples=args_dict['restrict_to_samples'])
    if project and os.path.split(project.base_path)[1] == "DATA":
        project.base_path = os.path.split(project.base_path)[0]
    if args_dict["restart_all_jobs"]:
        args_dict["restart_failed_jobs"] = True
        args_dict["restart_finished_jobs"] = True
        args_dict["restart_running_jobs"] = True
    launch_analysis([project],
                    restart_failed_jobs=args_dict["restart_failed_jobs"],
                    restart_finished_jobs=args_dict["restart_finished_jobs"],
                    restart_running_jobs=args_dict["restart_running_jobs"],
                    exec_mode=args_dict["exec_mode"])
