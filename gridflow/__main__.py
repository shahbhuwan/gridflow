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

import sys
import logging
import argparse
from .downloader import parse_args, run_download

def print_intro():
    intro = """
============================================================
  ____ ____  ___ ____  _____ _     _____        __
 / ___|  _ \|_ _|  _ \|  ___| |   / _ \ \      / /
| |  _| |_) || || | | | |_  | |  | | | \ \ /\ / /
| |_| |  _ < | || |_| |  _| | |__| |_| |\ V  V /
 \____|_| \_\___|____/|_|   |_____\___/  \_/\_/
============================================================
Welcome to GridFlow v0.2.3! Copyright (c) 2025 Bhuwan Shah
Effortlessly download and process CMIP6 climate data from ESGF
nodes concurrently.
============================================================
"""
    print(intro)

def main():
    # Always print intro
    print_intro()

    # Initialize basic logging configuration
    parser = argparse.ArgumentParser()
    parser.add_argument('--demo', action='store_true', help="Run in demo mode")
    parser.add_argument('-L', '--log-level', default='minimal', help="Logging level")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.2.3')
    args, unknown = parser.parse_known_args()

    # Set up early logging for minimal mode
    logging.basicConfig(
        level=logging.CRITICAL,
        format='%(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.addLevelName(logging.CRITICAL, "MINIMAL")

    # Handle demo mode
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and args.demo):
        sys.argv = [sys.argv[0], '--demo']
    elif args.demo and '--demo' not in unknown:
        unknown.append('--demo')

    args = parse_args()
    run_download(args)

if __name__ == "__main__":
    main()