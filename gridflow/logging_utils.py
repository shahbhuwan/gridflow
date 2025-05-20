# Copyright (c) 2025 Bhuwan Shah
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
import sys
from pathlib import Path
from datetime import datetime

class MinimalFilter(logging.Filter):
    def filter(self, record):
        # Allow CRITICAL and ERROR messages to pass through unfiltered
        if record.levelno >= logging.ERROR:
            return True
        # Filter INFO messages to specific patterns
        return record.levelno == logging.INFO and (
            'Progress:' in record.getMessage() or
            'Completed:' in record.getMessage() or
            'Downloaded' in record.getMessage() or
            'Task started' in record.getMessage() or
            'Task completed' in record.getMessage() or

            record.getMessage().startswith('Found ') or
            record.getMessage().startswith('Starting ') or
            record.getMessage().startswith('Progress:') or
            record.getMessage().startswith('Executing ') or
            record.getMessage().startswith('Connected to') or 
            record.getMessage().startswith('Catalog saved to') or
            record.getMessage().startswith('Trying to connect to') or
            record.getMessage().startswith('Cropped file created:') or
            record.getMessage().startswith('Clipped file created:') 
            
        )

def setup_logging(log_dir: str, log_level: str, prefix: str = "") -> None:
    try:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"{prefix}log_{timestamp}.log"

        log_levels = {
            'minimal': logging.INFO,
            'normal': logging.INFO,
            'verbose': logging.DEBUG,
            'debug': logging.DEBUG
        }
        numeric_level = log_levels.get(log_level.lower(), logging.INFO)

        logger = logging.getLogger()
        logger.setLevel(numeric_level)

        # Clear existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        handlers = []
        try:
            handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
        except Exception as e:
            print(f"Failed to set up file logging: {e}, falling back to console-only logging", file=sys.stderr)
        handlers.append(logging.StreamHandler(sys.stdout))

        minimal_format = '[%(levelname)s] %(message)s'
        standard_format = '%(asctime)s - %(levelname)s - %(message)s'

        for handler in handlers:
            if log_level.lower() == 'minimal':
                handler.setFormatter(logging.Formatter(minimal_format))
                handler.addFilter(MinimalFilter())
            else:
                handler.setFormatter(logging.Formatter(standard_format))
            logger.addHandler(handler)

        logging.debug(f"Logging initialized for {log_file}")
    except Exception as e:
        print(f"Failed to initialize logging: {e}, using console-only logging", file=sys.stderr)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(handler)