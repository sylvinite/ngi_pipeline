"""
Use this script to launch the pipeline from a project that has already been
organized into Project/Sample/Libprep/Seqrun (as opposed to starting from
a delivered Illumina flowcell).
Note that this does not create entries in the Charon database; use
create_charon_project_from_filesystem.py for that with the "-a" flag.
"""
import argparse
from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem
from ngi_pipeline.conductor.launchers import launch_analysis_for_flowcells

def main(demux_fcid_dir, restrict_to_projects=None, restrict_to_samples=None):
    project = recreate_project_from_filesystem(demux_fcid_dir)
    launch_analysis_for_flowcells([project])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("demux_fcid_dir", nargs="?", action="store",
            default="/proj/a2010002/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    args_dict = vars(parser.parse_args())
    main(**args_dict)
