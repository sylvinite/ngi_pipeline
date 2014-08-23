""" Module to define Celery tasks for NGI pipeline
"""

from ngi_pipeline.distributed.celery import app

from ngi_pipeline import conductor

@app.task(name='test_task')
def sum(a, b):
    """ Sum a and b
    """
    return a+b

@app.task(name='test_task_that_fails')
def divide_by_0():
    return 1/0.

