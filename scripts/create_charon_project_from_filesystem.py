"""Kludgy hack. Use at your own risk"""

import argparse

from ngi_pipeline.conductor.flowcell import setup_analysis_directory_structure
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.utils.classes import with_ngi_config

## NOTE because setup_analysis_directory_structure uses Charon to determine the library prep,
##      this fails unless you go and modify that code to catch the exception and do something else with it
##      Uppsala keeps their library prep id in their SampleSheet.csv, I don't think Sthlm does but we could start?
@with_ngi_config
def main(demux_fcid_dirs, restrict_to_projects=None, restrict_to_samples=None, force_update=False, config=None, config_file_path=None):
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    # Sort/copy each raw demux FC into project/sample/fcid format -- "analysis-ready"
    projects_to_analyze = dict()
    for demux_fcid_dir in demux_fcid_dirs_set:
        # These will be a bunch of Project objects each containing Samples, FCIDs, lists of fastq files
        projects_to_analyze = setup_analysis_directory_structure(demux_fcid_dir,
                                                                 projects_to_analyze,
                                                                 restrict_to_projects,
                                                                 restrict_to_samples,
                                                                 create_files=False,
                                                                 config=config)
    if not projects_to_analyze:
        error_message = ("No projects found to process in flowcells {}"
                         "or there was an error gathering required "
                         "information.".format(",".join(demux_fcid_dirs_set)))
        LOG.info(error_message)
        sys.exit("Quitting: " + error_message)
    else:
        # Don't need the dict functionality anymore; revert to list
        projects_to_analyze = projects_to_analyze.values()
        for project in projects_to_analyze:
            create_charon_entries_from_project(project)
    

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("demux_fcid_dirs", nargs="*", help="The path to the fcid containing the project of interest.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append", help="Restrict processing to these projects. Use flag multiple times for multiple projects.")
    parser.add_argument("-s", "--sample", dest="restrict_to_samples", action="append", help="Restrict processing to these samples. Use flag multiple times for multiple samples.")

    args_dict = vars(parser.parse_args())
    main(**args_dict)
