#!/usr/bin/env python

""" Main entry point for the ngi_pipeline.

It can either start the Tornado server that will trigger analysis on the processing
cluster (UPPMAX for NGI), or trigger analysis itself. 
"""
import argparse

from ngi_pipeline.conductor import flowcell
#from ngi_pipeline.server import main as server_main


############### TESTING STUFF ################
def test():
    import time
    time.sleep(60)

##############################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch NGI pipeline")
    subparsers = parser.add_subparsers(help="Choose the mode to run")

    # Add subparser for the server
    parser_server = subparsers.add_parser('server', help="Start ngi_pipeline server")
    parser_server.add_argument('-p', '--port', help="Port in where to run the application")


    # Add subparser for the process
    parser_process = subparsers.add_parser('process', help="Start some analysis process")
    subparsers_process = parser_process.add_subparsers(help='Choose unit to process')

    # Another subparser for flowcell processing
    process_fc = subparsers_process.add_parser('flowcell', help='Start analysis of raw flowcells')
    process_fc.add_argument("demux_fcid_dir", nargs="?", action="store",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    process_fc.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    process_fc.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    process_fc.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
            help=("Restart jobs marked as 'FAILED' in Charon"))

    # Aubpparser for sample processing
    sample_group = subparsers_process.add_parser('sample', help='Start the analysis of a particular sample')
    sample_group.add_argument('sample_dir', nargs="?", action="store",
        help="The path to the Illumina sample directory")
    args = parser.parse_args()