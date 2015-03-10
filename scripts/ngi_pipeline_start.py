#!/bin/env python

""" Main entry point for the ngi_pipeline.

It can either start the Tornado server that will trigger analysis on the processing
cluster (UPPMAX for NGI), or trigger analysis itself.
"""
import argparse
import os

from ngi_pipeline.conductor import flowcell
from ngi_pipeline.conductor import launchers
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.server import main as server_main
from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem

LOG = minimal_logger("ngi_pipeline_start")


class ArgumentParserWithTheFlagsThatIWant(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ArgumentParserWithTheFlagsThatIWant, self).__init__(*args, **kwargs)
        self.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
                help=("Restart jobs marked as 'FAILED' in Charon"))
        self.add_argument("-d", "--restart-done", dest="restart_finished_jobs", action="store_true",
                help=("Restart jobs marked as DONE in Charon."))
        self.add_argument("-r", "--restart-running", dest="restart_running_jobs", action="store_true",
                help=("Restart jobs marked as UNDER_ANALYSIS in Charon. Use with care."))
        self.add_argument("-a", "--restart-all", dest="restart_all_jobs", action="store_true",
                help=("Just start any kind of job you can get your hands on regardless of status."))
        self.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
                help=("Restrict analysis to these samples. "
                      "Use flag multiple times for multiple samples."))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Launch NGI pipeline")
    subparsers = parser.add_subparsers(help="Choose the mode to run")

    # Add subparser for the server
    parser_server = subparsers.add_parser('server', help="Start ngi_pipeline server")
    parser_server.add_argument('-p', '--port', type=int, help="Port in where to run the application")

    # Add subparser for organizing a flowcell
    parser_organize = subparsers.add_parser('organize',
        help="Organize a flowcell into project/sample/libprep/seqrun format.")
    subparsers_organize = parser_organize.add_subparsers()

    # Add subparser for analysis
    parser_analyze = subparsers.add_parser('analyze', help="Launch analysis.")
    subparsers_analyze = parser_analyze.add_subparsers(parser_class=ArgumentParserWithTheFlagsThatIWant,
            help='Choose unit to analyze')

    # Another subparser for flowcell analysis
    analyze_flowcell = subparsers_analyze.add_parser('flowcell', help='Start analysis of raw flowcells')
    analyze_flowcell.add_argument("demux_fcid_dir", action="store",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process and analyze."))
    analyze_flowcell.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict analysis to these projects. "
                  "Use flag multiple times for multiple projects."))

    # Another subparser for project analysis
    project_group = subparsers_analyze.add_parser('project', help='Start the analysis of a pre-parsed project.')
    project_group.add_argument('project_dir', action='store', help='The path to the project folder to be analyzed.')

    args = parser.parse_args()

    # The following option will be available only if the script has been called with the 'analyze' option
    if args.__dict__.get('restart_all_jobs'):
        args.restart_failed_jobs = True
        args.restart_finished_jobs = True
        args.restart_running_jobs = True

    # Finally execute corresponding functions
    if 'demux_fcid_dir' in args:
        LOG.info("Starting flowcell analysis in directory {}".format(args.demux_fcid_dir))
        flowcell.process_demultiplexed_flowcell(args.demux_fcid_dir,
                                                args.restrict_to_projects,
                                                args.restrict_to_samples,
                                                args.restart_failed_jobs,
                                                args.restart_finished_jobs,
                                                args.restart_running_jobs)
    elif 'project_dir' in args:
        project = recreate_project_from_filesystem(project_dir=args.project_dir,
                                                   restrict_to_samples=args.restrict_to_samples)
        if project and os.path.split(project.base_path)[1] == "DATA":
            project.base_path = os.path.split(project.base_path)[0]
        launchers.launch_analysis([project],
                                  restart_failed_jobs=args.restart_failed_jobs,
                                  restart_finished_jobs=args.restart_finished_jobs,
                                  restart_running_jobs=args.restart_running_jobs)
    elif 'port' in args:
        LOG.info('Starting ngi_pipeline server at port {}'.format(args.port))
        server_main.start(args.port)
