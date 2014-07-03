""" Distributed module for NGI-pipeline
"""
import base64
import contextlib
import multiprocessing
import os
import sys
import uuid

import pika

from pika.credentials import PlainCredentials

########################
#    Useful methods    #
########################

_celeryconfig_tmpl = """
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
def create_celery_config(dirs, config):
    """ Creates a temporal configuration file for Celery

    :param task_module: String representing the tasks module to load
    :param dirs: Dictionary with temporal directories
    :param config: Dictionary with Celery configurations
    """
    try:
        celery_config = _celeryconfig_tmpl.format(host = config['host'],
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


class CeleryMessenger(object):
    """ Helper class to send formatted messages to a Celery quque
    """
    def __init__(self, config, queue, exchange=''):
        try:
            host = config['host']
            port = config['port']
            username = config['userid']
            password = config['password']
            virtual_host = config['rabbitmq_vhost']
        except KeyError:
            raise RuntimeError("Could not build configuration file for Celery, missing parameters!")

        p = pika.ConnectionParameters(host=host, port=port, virtual_host=virtual_host,
                                      credentials=PlainCredentials(username=username,
                                                                   password=password))
        self._connection = pika.BlockingConnection(p)
        self.queue = queue
        self.exchange = exchange
        self.channel = self._connection.channel()

    def send_message(self, task, args):
        """ Sends a message to the instance specified queue

        :param task: String - task to be executed
        :param args: List - Arguments of the task
        """
        exchange = self.exchange
        # Generate a uniqie ID for the task (required by Celery)
        task_id = base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)
        body = {'task': task, 'id': task_id, 'args': args}
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.queue, body=body,
                                  properties=pika.BasicProperties(content_type='application/json'))

