from celery.utils.log import get_task_logger


def get_climatoology_logger(name):
    return get_task_logger(name)
