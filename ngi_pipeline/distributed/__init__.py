""" Distributed module for NGI-pipeline
"""
import contextlib
import multiprocessing
import os

########################
#    Useful methods    #
########################

_celeryconfig_tmpl = """
CELERY_IMPORTS = ("{task_import}", )

BROKER_HOST = "{host}"
BROKER_PORT = "{port}"
BROKER_USER = "{userid}"
BROKER_PASSWORD = "{password}"
BROKER_VHOST = "{rabbitmq_vhost}"
CELERY_RESULT_BACKEND= "amqp"
CELERY_TASK_SERIALIZER = "json"
CELERYD_CONCURRENCY = {cores}
CELERY_QUEUE_HA_POLICY = 'all'
CELERY_ACKS_LATE = False
CELERYD_PREFETCH_MULTIPLIER = 1
BROKER_CONNECTION_MAX_RETRIES = 200
"""

@contextlib.contextmanager
def create_celery_config(task_module, dirs, config):
    """ Creates a temporal configuration file for Celery

    :param task_module: String representing the tasks module to load
    :param dirs: Dictionary with temporal directories
    :param config: Dictionary with Celery configurations
    """
    try:
        celery_config = _celeryconfig_tmpl.format(task_import = task_module,
                                                 host = config['host'],
                                                 port = config['port'],
                                                 userid = config['userid'],
                                                 password = config['password'],
                                                 rabbitmq_vhost = config['rabbitmq_vhost'],
                                                 cores = config.get('cores', multiprocessing.cpu_count()))
    except KeyError:
        raise RuntimeError("Could not build configuration file for Celery, missing parameters!")

    out_file = os.path.join(dirs["work"], "celeryconfig.py")
    with open(out_file, "w") as out_handle:
        out_handle.write(celery_config)
    try:
        yield out_file
    finally:
        pyc_file = "{}.pyc".format(os.path.splitext(out_file)[0])
        for fname in [pyc_file, out_file]:
            if os.path.exists(fname):
                os.remove(fname)
