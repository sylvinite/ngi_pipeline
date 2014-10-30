"""
log module
"""
import logging
import sys

#from ngi_pipeline.utils.config import load_yaml_config
from Queue import Queue
from subprocess import Popen, PIPE
from threading import Thread


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
        # the fastest hack FIXME
        #logging_fn(line)
        logging_fn(line + "\n")
    output_buffer.close()


def minimal_logger(namespace, debug=False):
    """Make and return a minimal console logger.

    :param namespace: String - namespace of logger
    :param debug: Boolean - Log in DEBUG level or not

    :returns: A logging.Logger object
    :rtype: logging.Logger
    """
    log_level = logging.DEBUG if debug else logging.INFO
    log = logging.getLogger(namespace)
    log.setLevel(log_level)
    s_h = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    s_h.setFormatter(formatter)
    s_h.setLevel(log_level)
    log.addHandler(s_h)

    return log


# Uh yeah I don't think this works
def file_logger(namespace, config_file , log_file, log_path_key = None):
    CONFIG = cl.load_config(config_file)
    if not log_path_key:
        log_path = CONFIG['log_dir'] + '/' + log_file
    else:
        log_path = CONFIG[log_path_key] + '/' + log_file

    logger = logging.getLogger(namespace)
    logger.setLevel(logging.DEBUG)

    # file handler:
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.INFO)

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("%(asctime)s (%(levelname)s) : %(message)s")
    fh.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger

