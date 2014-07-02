#!/usr/bin/env python
"""Start a nextgen analysis server that handles processing from a distributed task queue.

This reads configuration details and then starts a Celery (http://celeryproject.org>
server that will handle requests that are passed via an external message queue. This
allows distributed processing of analysis sections with less assumptions about the
system architecture.

Usage:
  ngi_server.py <post_process.yaml>
   [--queues=list,of,queues: can specify specific queues to listen for jobs
                             on. No argument runs the default queue, which
                             handles processing alignments. 'toplevel' handles
                             managing the full work process.]
   [--tasks=task.module.import: Specify the module of tasks to make available.
                                Defaults to bcbio.distributed.tasks if not specified.]
   [--basedir=<dirname>: Base directory to work in. Defaults to current directory.]
"""
import argparse
from celery import Celery

from ngi_pipeline.utils.config import load_yaml_config


def main(config_file_path):
    """ Loads configuration and launches the server
    """
    config = load_yaml_config(config_file_path)

    # Find Celery configurations
    broker = config.get('Celery', {}).get('broker', None)
    if not broker:
        raise RuntimeError("Celery config options not found in the configuration file.")

    app = Celery('ngi_tasks', broker=broker)

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True,
            help="The path to the configuration file.")
    parser.add_argument("-q", "--queues", dest="queues", action="store", required=True,
                      default=None)
    parser.add:argument("-t", "--tasks", dest="task_module", action="store", required=True,
                      default=None)
    args = parser.parse_args()
    main(args.config, args.queues, args.tasks)
