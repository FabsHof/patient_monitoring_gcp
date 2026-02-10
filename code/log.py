"""Unified logging with Python's logging module.

Writes to both console and logs/<script_name>.log automatically.
"""

import logging
import os
import sys

LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

_script = os.path.splitext(os.path.basename(sys.argv[0]))[0]
_log_file = os.path.join(LOG_DIR, f'{_script}.log')

_fmt = logging.Formatter(f'[%(asctime)s] [{_script}] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

_logger = logging.getLogger('pipeline')
_logger.setLevel(logging.INFO)

_console = logging.StreamHandler()
_console.setFormatter(_fmt)
_logger.addHandler(_console)

_file = logging.FileHandler(_log_file, mode='w')
_file.setFormatter(_fmt)
_logger.addHandler(_file)


def log(msg: str):
    """Log a timestamped, filename-prefixed message."""
    _logger.info(msg)


def substep(n: int, msg: str):
    """Log a timestamped sub-step (indented, enumerated)."""
    _logger.info('\t> %d) %s', n, msg)
