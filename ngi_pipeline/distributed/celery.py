""" Celery module for NGI-pipeline
"""
from __future__ import absolute_import

from celery import Celery

app = Celery('ngi_celery_server',
             broker='redis://',
             backend='redis://',
             include='ngi_pipeline.distributed.tasks')
