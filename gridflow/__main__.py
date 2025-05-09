import sys
import argparse
import logging
from gridflow.downloader import parse_args as download_parse_args, run_download, setup_logging
from gridflow.crop_netcdf import main as crop_main
from gridflow.clip_netcdf import main as clip_main
from gridflow.database_generator import main as db_main

def print_intro():
    banner = """
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
    print(banner)

def main():
    print_intro()

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--demo', action='store_true', help="Run in demo mode")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('--log-dir', default='logs', help="Directory for log files")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.2.3')
    args, unknown = parser.parse_known_args()

    if len(sys.argv) == 1 or (len(sys.argv) == 2 and args.demo):
        sys.argv = [sys.argv[0], '--demo']
    elif args.demo and '--demo' not in unknown:
        unknown.append('--demo')

    setup_logging(args.log_dir, args.log_level)

    if 'gridflow-db' in sys.argv[0]:
        db_main()
    elif 'gridflow-crop' in sys.argv[0]:
        crop_main()
    elif 'gridflow-clip' in sys.argv[0]:
        clip_main()
    else:
        args = download_parse_args()
        run_download(args)

if __name__ == "__main__":
    main()