#!/usr/bin/env python
"""Start a nextgen analysis server that handles processing from a distributed task queue.

This reads configuration details and then starts a Celery (http://celeryproject.org>
server that will handle requests that are passed via an external message queue. This
allows distributed processing of analysis sections with fewer assumptions about the
system architecture.

Usage:
  ngi_server.py <post_process.yaml>
   [--queues=list,of,queues: can specify specific queues to listen for jobs
                             on. No argument runs the default queue, which
                             handles processing alignments. 'toplevel' handles
                             managing the full work process.]
   [--tasks=task.module.import: Specify the module of tasks to make available.
                                Defaults to bcbio.distributed.tasks if not specified.]
"""
import argparse
import os

from subprocess import check_call

from celery import Celery

from ngi_pipeline import utils
from ngi_pipeline.distributed import create_celery_config
from ngi_pipeline.log import minimal_logger
from ngi_pipeline.utils.config import load_yaml_config


LOG = minimal_logger(__name__)

def main(config_file, queues=None, task_module=None):
    """ Loads configuration and launches the server
    """
    config = load_yaml_config(config_file)

    # Prepare working directory to save logs and config files
    base_dir = os.getcwd()
    if task_module is None:
        task_module = "ngi_pipeline.distributed.tasks"
    LOG.info("Starting distributed worker process: {0}".format(queues if queues else ""))
    with utils.chdir(base_dir):
        with utils.curdir_tmpdir() as work_dir:
            dirs = {"work": work_dir, "config": os.path.dirname(config_file)}
            with create_celery_config(task_module, dirs, config.get('celery', {})):
                run_celeryd(work_dir, queues)

def run_celeryd(work_dir, queues):
    with utils.chdir(work_dir):
        cl = ["celeryd"]
        if queues:
            cl += ["-Q", queues]
        cl += ["-n", "ngi_server"]
        check_call(cl)


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True,
            help="The path to the configuration file.")
    parser.add_argument("-q", "--queues", dest="queues", action="store",
                      default=None, help="Queues the server will listen to")
    parser.add_argument("-t", "--tasks", dest="task_module", action="store",
                      default=None, help="Task module to import")
    args = parser.parse_args()
    main(args.config, args.queues, args.task_module)
