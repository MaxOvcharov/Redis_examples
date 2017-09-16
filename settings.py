import asyncio
import os
import logging

__all__ = ['logger', 'BASE_DIR']

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEBUG = False

# Redis settings

REDIS_RECONNECT_DELAY = 1  # sec
REDIS_RECONNECT_RETRIES = 10000

# Logger settings

BASE_LOGGER = 'test_redis_methods'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'base': {
            'format': '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'base',
        },
    },
    'loggers': {
        BASE_LOGGER: {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False,
        },
        'asyncio': {
            'level': 'INFO',
            'handlers': ['console'],
            'propagate': False,
        },
        'aiohttp': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False,
        },
    },
    'root': {
        'level': 'ERROR',
        'handlers': ['console'],
    }
}

LOGGING_LEVEL_ENV = os.environ.get('PYTHONLOGGINLEVEL')

LOGGING_LEVEL = int(LOGGING_LEVEL_ENV) if LOGGING_LEVEL_ENV else logging.DEBUG


def setup_logging(base_severity=LOGGING_LEVEL):
    """Logging setup"""
    logging.config.dictConfig(LOGGING)
    log = logging.getLogger(BASE_LOGGER)
    log.setLevel(base_severity)

    # Setup asyncio logging
    if os.environ.get('PYTHONASYNCIODEBUG'):
        asyncio.get_event_loop().set_debug(True)
        logging.captureWarnings(True)

    return log

logger = setup_logging()


