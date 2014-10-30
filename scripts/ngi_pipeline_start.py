""" Main entry point for the ngi_pipeline.

It can either start the Tornado server that will trigger analysis on the processing
cluster (UPPMAX for NGI), or trigger analysis itself. 
"""
import argparse

from ngi_pipeline.conductor import flowcell
#from ngi_pipeline.server import main as server_main

def _check_args(args):
    """ Ensure dependent arguments are correctly specified
    """
    # Arguments for process
    args_for_process = [args.demux_fcid_dir]
    if args.mode == 'process' and not all(args_for_process):
        return ("You specified 'process' mode but no one of the following "
            "arguments: {}".format(', '.join(args_for_process)))
    # Arguments for server
    args_for_server = [args.port]
    if args.mode == 'server' and not all(args_for_server):
        return ''
    pass


############### TESTING STUFF ################
def test():
    import time
    time.sleep(60)

##############################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch NGI pipeline")
    subparsers = parser.add_subparsers(help="Choose the mode to run")

    # Add subparser for the server mode
    parser_server = subparsers.add_parser('server', help="Start ngi_pipeline server")
    parser_server.add_argument('-p', '--port', help="Port in where to run the application")

    # Add subparser for the process mode
    parser_process = subparsers.add_parser('process', help="Start some analysis process")
    parser_process.add_argument('unit', help="Unit to process", coiches=['flowcell'])
    #XXX MAYBE I CAN USE A GROUP OR SOMETHING TO ENSURE THAT IF UNIT == FLOWCELL THEN A FC ID NEEDS TO BE PRESENT?


    # Add subparser for the test mode


    parser.add_argument('mode', help=("Choose whether to launch an analysis or "
        "start the server to listen to HTTP requests"), choices=['process', 'server', 'test'])
    parser.add_argument('--unit', help="Unit to process", choices=['flowcell'])
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple samples."))
    parser.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
            help=("Restart jobs marked as 'FAILED' in Charon"))
    parser.add_argument("--demux-fcid-dir", nargs="?", action="store",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    args_ns = parser.parse_args()
    error_msg = _check_args(args_ns)
    if error_msg:
        parser.error(error_msg)
    if args_ns.mode == 'process':
        process_demultiplexed_flowcell(args_ns.demux_fcid_dir,
                                       args_ns.restrict_to_projects,
                                       args_ns.restrict_to_samples,
                                       args_ns.restart_failed_jobs)
    elif args_ns.mode == 'server':
        server_main.start()
    elif args_ns.mode == 'test':
        test()