import argparse

from ngi_pipeline import conductor


def main(demux_fcid_dirs, restrict_to_projects=None, restrict_to_samples=None):
    conductor.process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects, restrict_to_samples)

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Quick launcher for testing purposes.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            default=["G.Grigelioniene_14_01"],
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("demux_fcid_dirs", nargs="*", action="store",
            default=["/proj/a2010002/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/"],
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    args_dict = vars(parser.parse_args())
    main(**args_dict)
