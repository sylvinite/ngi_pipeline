"""
log module
"""
import logging
import os
import sys

from Queue import Queue
from subprocess import Popen, PIPE
from threading import Thread

from ngi_pipeline.utils.classes import with_ngi_config

def log_process_non_blocking(output_buffer, logging_fn):
    """Non-blocking redirection of a buffer to a logging function.
    A useful example:

    LOG = minimal_logger(__name__)
    p = Popen("y", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log_non_blocking(p.stdout, LOG.info)
    log_non_blocking(p.stderr, LOG.warn)
    """
    q = Queue()
    t = Thread(target=_enqueue_output, args=(output_buffer, q, logging_fn))
    t.daemon = True
    t.start()

def _enqueue_output(output_buffer, queue, logging_fn):
    for line in iter(output_buffer.readline, b''):
        logging_fn(line + "\n")
    output_buffer.close()


@with_ngi_config
def minimal_logger(namespace, to_file=True, debug=False,
                   config=None, config_file_path=None):
    """Make and return a minimal console logger. Optionally write to a file as well.

    :param namespace: String - namespace of logger
    :param to_file: Boolean - Log to a file (location in configuration file)
    :param debug: Boolean - Log in DEBUG level or not

    :returns: A logging.Logger object
    :rtype: logging.Logger
    """
    log_level = logging.DEBUG if debug else logging.INFO
    log = logging.getLogger(namespace)
    log.setLevel(log_level)

    # Console logger
    s_h = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    s_h.setFormatter(formatter)
    s_h.setLevel(log_level)
    log.addHandler(s_h)

    # File logger
    if to_file:
        config_file_dir = config.get("logging", {}).get("log_dir") or \
                          os.environ.get("NGI_CONFIG") or os.getcwd()
        log_path = os.path.join(config_file_dir, "ngi_pipeline.log")
        fh = logging.FileHandler(log_path)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        log.addHandler(fh)

    return log
