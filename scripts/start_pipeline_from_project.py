"""
Use this script to launch the pipeline from a project that has already been
organized into Project/Sample/Libprep/Seqrun (as opposed to starting from
a delivered Illumina flowcell).
Note that this does not create entries in the Charon database; use
create_charon_project_from_filesystem.py for that with the "-a" flag.
"""
import argparse
import os

from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem
from ngi_pipeline.conductor.launchers import launch_analysis_for_seqruns, \
                                             launch_analysis_for_samples


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("project_dir", nargs="?", action="store",
            help=("The path to the project to be processed."))
    parser.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
            help=("Restart jobs marked as FAILED in Charon."))
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--seqrun_only", action="store_true",
            help=("Only process at the seqrun level."))
    g.add_argument("--sample_only", action="store_true",
            help=("Only process at the sample level."))

    args_dict = vars(parser.parse_args())
    project = recreate_project_from_filesystem(args_dict['project_dir'],
                                               args_dict['restrict_to_samples'])
    if os.path.split(project.base_path)[1] == "DATA":
        project.base_path = os.path.split(project.base_path)[0]
    if not args_dict['sample_only']:
        launch_analysis_for_seqruns([project], args_dict["restart_failed_jobs"])
    if not args_dict['seqrun_only']:
        launch_analysis_for_samples([project], args_dict["restart_failed_jobs"])
