import logging
from typing import Literal

from celery._state import get_current_task
from celery.signals import after_setup_logger
from celery.utils.log import ColorFormatter

from climatoology.app.settings import CABaseSettings


def get_climatoology_logger(name: str) -> logging.Logger:
    """
    Get a logger adapted to the needs of climatoology.

    :param name: the name of the logger, potentially `__name__`
    :return: a standard logger
    """
    return logging.getLogger(name)


class ClimatoologyFormatter(logging.Formatter):
    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style: Literal['%'] = '%',
        validate=True,
        *,
        defaults=None,
    ):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate, defaults=defaults)
        self.worker_formatter = ColorFormatter(fmt=f'Worker: {self._fmt}')
        self.task_formatter = ColorFormatter(fmt=f'Task %(task_name)s[%(task_id)s]: {self._fmt}')
        self.other_formatter = ColorFormatter(fmt=f'Unknown: {self._fmt}')

    def format(self, record: logging.LogRecord) -> str:
        task = get_current_task()
        if task and task.request:
            record.__dict__.update(task_id=task.request.id, task_name=task.name)
            return self.task_formatter.format(record)
        elif record.threadName == 'MainThread':
            return self.worker_formatter.format(record)
        return self.other_formatter.format(record)


@after_setup_logger.connect
def setup_celery_logging(logger: logging.Logger, *args, **kwargs):
    # noinspection argument-list
    settings = CABaseSettings()
    custom_formatter = ClimatoologyFormatter(fmt=settings.log_format)
    for handler in logger.handlers:
        handler.setFormatter(custom_formatter)
