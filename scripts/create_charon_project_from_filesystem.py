#!/usr/bin/env python
"""Kludgy hack. Use at your own risk"""
from __future__ import print_function

import argparse
import sys

from ngi_pipeline.conductor.flowcell import setup_analysis_directory_structure
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem

LOG = minimal_logger(__name__)

@with_ngi_config
def main(demux_fcid_dirs, restrict_to_projects=None, restrict_to_samples=None,
         best_practice_analysis=None, sequencing_facility=None,
         already_parsed=False,
         force_update=False, delete_existing=False,
         force_create_project=False,
         config=None, config_file_path=None):
    if force_update: force_update = validate_force_update()
    if delete_existing: delete_existing = validate_delete_existing()
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    projects_to_analyze = dict()
    if already_parsed: # Starting from Project/Sample/Libprep/Seqrun tree format
        for demux_fcid_dir in demux_fcid_dirs_set:
            p = recreate_project_from_filesystem(demux_fcid_dir,
                                                 force_create_project=force_create_project)
            projects_to_analyze[p.name] = p
    else: # Raw illumina flowcell
        for demux_fcid_dir in demux_fcid_dirs_set:
            projects_to_analyze = setup_analysis_directory_structure(demux_fcid_dir,
                                                                     projects_to_analyze,
                                                                     restrict_to_projects,
                                                                     restrict_to_samples,
                                                                     create_files=False,
                                                                     config=config)
    if not projects_to_analyze:
        sys.exit("Quitting: no projects found to process in flowcells {}"
                 "or there was an error gathering required "
                 "information.".format(",".join(demux_fcid_dirs_set)))
    else:
        projects_to_analyze = projects_to_analyze.values()
        for project in projects_to_analyze:
            try:
                create_charon_entries_from_project(project, best_practice_analysis=best_practice_analysis,
                                                   sequencing_facility=sequencing_facility,
                                                   force_overwrite=force_update,
                                                   delete_existing=delete_existing)
            except Exception as e:
                print(e, file=sys.stderr)

def validate_force_update():
    return _validate_dangerous_user_thing("OVERWRITE EXISTING DATA in CHARON")

def validate_delete_existing():
    return _validate_dangerous_user_thing("DELETE EXISTING DATA in CHARON")


def _validate_dangerous_user_thing(action="do SOMETHING that Mario thinks you should BE WARNED about"):
    print("DANGER WILL ROBINSON you have told this script to {action}!! Do you in fact want do to this?!?".format(action=action), file=sys.stderr)
    attempts = 0
    while True:
        if attempts < 3:
            attempts += 1
            user_input = raw_input("Confirm overwrite by typing 'yes' or 'no' ({}): ".format(attempts)).lower()
            if user_input not in ('yes', 'no'):
                continue
            elif user_input == 'yes':
                return True
            elif user_input == 'no':
                return False
        else:
            print("No confirmation received; setting force_update to False", file=sys.stderr)
            return False

if __name__=="__main__":
    parser = argparse.ArgumentParser("Populate a Charon project with a flowcell directory")
    parser.add_argument("demux_fcid_dirs", nargs="*",
            help="The path to the fcid containing the project of interest.")
    parser.add_argument("-a", "--already-parsed", action="store_true",
			help="Set this flag if the input path is an already-parsed Project/Sample/Libprep/Seqrun tree, as opposed to a raw flowcell.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
			help="Restrict processing to these projects. Use flag multiple times for multiple projects.")
    parser.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
			help="Restrict processing to these samples. Use flag multiple times for multiple samples.")
    parser.add_argument("-b", "--best_practice_analysis", default="whole_genome_reseq",
			help="The best practice analysis to run for this project.")
    parser.add_argument("--sequencing-facility", default="NGI-S", choices=('NGI-S', 'NGI-U'),
            help="The facility where sequencing was performed.")
    parser.add_argument("-f", "--force", dest="force_update", action="store_true",
			help="Force updating Charon projects. Danger danger danger. This will overwrite things.")
    parser.add_argument("-d", "--delete", dest="delete_existing", action="store_true",
			help="Delete existing projects in Charon. Similarly dangerous.")
    parser.add_argument("-c", "--force-create-project", action="store_true",
			help="Create the project if it does not exist in Charon using the project name as the project id. Generally used only in testing.")

    args_dict = vars(parser.parse_args())
    main(**args_dict)
