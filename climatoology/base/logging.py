import logging

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


def filter_trace_level_logs():
    logging.getLogger('rasterio._base').addFilter(
        lambda record: not record.levelno == logging.DEBUG and record.getMessage().startswith('Color: (')
    )


class ClimatoologyFormatter(ColorFormatter):
    def format(self, record: logging.LogRecord) -> str:
        context = 'Unknown'
        task = get_current_task()
        if task and task.request:
            context = f'Task {task.name}[{task.request.id}]'
        elif record.threadName == 'MainThread':
            context = 'Worker'
        record.__dict__.update(context=context)
        return super().format(record)


@after_setup_logger.connect
def setup_celery_logging(logger: logging.Logger, *args, **kwargs):
    # noinspection argument-list
    settings = CABaseSettings()
    custom_formatter = ClimatoologyFormatter(fmt=f'%(context)s: {settings.log_format}')
    for handler in logger.handlers:
        handler.setFormatter(custom_formatter)
