import argparse

from ngi_pipeline.conductor.flowcell import process_demultiplexed_flowcell

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Launch seqrun-level analysis.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("demux_fcid_dir", nargs="?", action="store",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    args_ns = parser.parse_args()
    process_demultiplexed_flowcell(args_ns.demux_fcid_dir,
                                   args_ns.restrict_to_projects,
                                   args_ns.restrict_to_samples)
