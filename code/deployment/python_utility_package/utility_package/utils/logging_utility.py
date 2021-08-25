import logging

from opencensus.ext.azure.log_exporter import AzureLogHandler

LOGGING_FORMAT = logging.Formatter(
    '%(relativeCreated)6d | %(asctime)s | %(levelname)s '
    + ' | %(threadName)s | %(name)s: %(lineno)s  %(funcName)s(): '
    + '%(message)s'
)


def get_logger(logger_name):
    """
    :param str logger_name: Name of the logger to instantiate
    :returns: Logger with a stream handler added
    :rtype: logging.Logger
    """
    if not logger_name.startswith('edm.'):
        logger_name = f'edm.{logger_name}'
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(LOGGING_FORMAT)
    logger.addHandler(console_handler)
    return logger


def add_azure_handler_to_all_loggers(instr_key):
    """
    :param str instr_key: Instrumentation key
    When the instrumentation key has been retrieved from Azure Keyvault,
    this function can be called to add the azure handler to all
    instantiated loggers
    :returns: None
    """
    azure_handler = AzureLogHandler(
        connection_string=f'InstrumentationKey={instr_key}')
    azure_handler.setLevel(logging.DEBUG)
    azure_handler.setFormatter(LOGGING_FORMAT)
    for logger_name, logger in logging.root.manager.loggerDict.items():
        is_logger = isinstance(logger, logging.Logger)
        if logger_name.startswith('edm.') and is_logger:
            logger.addHandler(azure_handler)
