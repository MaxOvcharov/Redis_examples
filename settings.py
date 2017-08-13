import os
import logging

__all__ = ['logger', 'BASE_DIR']

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEBUG = False


logger = logging.getLogger('app')
logger.setLevel(logging.DEBUG)

f = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(f)
logger.addHandler(ch)


