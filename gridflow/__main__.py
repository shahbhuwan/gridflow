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
import argparse
import logging
from pathlib import Path
from gridflow.downloader import run_download, setup_logging
from gridflow.crop_netcdf import crop_netcdf_file
from gridflow.clip_netcdf import clip_netcdf_file
from gridflow.database_generator import generate_database
from gridflow import __version__

def print_intro():
    banner = """
============================================================
  ____ ____  ___ ____  _____ _     _____        __
 / ___|  _ \|_ _|  _ \|  ___| |   / _ \ \      / /
| |  _| |_) || || | | | |_  | |  | | | \ \ /\ / /
| |_| |  _ < | || |_| |  _| | |__| |_| |\ V  V /
 \____|_| \_\___|____/|_|   |_____\___/  \_/\_/
============================================================
Welcome to GridFlow v{}! Copyright (c) 2025 Bhuwan Shah
Effortlessly download and process CMIP6 climate data.
============================================================
""".format(__version__)
    print(banner)

def download_command(args):
    """Handle the download subcommand."""
    setup_logging(args.log_dir, args.log_level, prefix="download_")
    run_download(args)

def crop_command(args):
    """Handle the crop subcommand."""
    setup_logging(args.log_dir, args.log_level, prefix="crop_")
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        logging.error(f"Input directory {input_dir} does not exist")
        sys.exit(1)

    nc_files = list(input_dir.glob("*.nc"))
    if not nc_files:
        logging.error(f"No NetCDF files found in {input_dir}")
        sys.exit(1)

    if args.min_lat >= args.max_lat or args.min_lon >= args.max_lon:
        logging.error("Invalid bounds: min_lat < max_lat and min_lon < max_lon required")
        sys.exit(1)
    if not (-90 <= args.min_lat <= 90 and -90 <= args.max_lat <= 90):
        logging.error("Latitude must be between -90 and 90")
        sys.exit(1)
    if not (-180 <= args.min_lon <= 360 and -180 <= args.max_lon <= 360):
        logging.error("Longitude must be between -180 and 360")
        sys.exit(1)

    logging.info(f"Found {len(nc_files)} NetCDF files to crop")
    success_count = 0
    for nc_file in nc_files:
        output_file = output_dir / nc_file.name.replace('.nc', '_cropped.nc')
        if crop_netcdf_file(nc_file, output_file, args.min_lat, args.max_lat, args.min_lon, args.max_lon, args.buffer_km):
            success_count += 1

    logging.critical(f"Completed: {success_count}/{len(nc_files)} files cropped successfully")
    logging.info(f"Completed: {success_count}/{len(nc_files)} files cropped successfully")

def clip_command(args):
    """Handle the clip subcommand."""
    setup_logging(args.log_dir, args.log_level, prefix="clip_")
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    shapefile_path = Path(args.shapefile)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        logging.error(f"Input directory {input_dir} does not exist")
        sys.exit(1)
    if not shapefile_path.exists():
        logging.error(f"Shapefile {shapefile_path} does not exist")
        sys.exit(1)

    nc_files = list(input_dir.glob("*.nc"))
    if not nc_files:
        logging.error(f"No NetCDF files found in {input_dir}")
        sys.exit(1)

    logging.info(f"Found {len(nc_files)} NetCDF files to clip")
    success_count = 0
    for nc_file in nc_files:
        output_file = output_dir / nc_file.name.replace('.nc', '_clipped.nc')
        if clip_netcdf_file(nc_file, shapefile_path, args.buffer_km, output_file):
            success_count += 1

    logging.critical(f"Completed: {success_count}/{len(nc_files)} files clipped successfully")
    logging.info(f"Completed: {success_count}/{len(nc_files)} files clipped successfully")

def database_command(args):
    """Handle the database subcommand."""
    setup_logging(args.log_dir, args.log_level, prefix="db_")
    database = generate_database(args.input_dir, args.output_dir, demo_mode=args.demo, workers=args.workers)
    if not database:
        logging.error("No valid NetCDF files found or database generation failed")
        sys.exit(1)
    logging.critical(f"Generated database with {len(database)} groups")

def main():
    print_intro()
    parser = argparse.ArgumentParser(
        description="GridFlow: A tool for downloading and processing CMIP6 climate data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--version', action='version', version=f'GridFlow {__version__}')
    subparsers = parser.add_subparsers(dest='command', help="Available commands")

    # Download subcommand
    download_parser = subparsers.add_parser(
        'download', help="Download CMIP6 data from ESGF nodes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    download_parser.add_argument('-p', '--project', default='CMIP6', help="Project name")
    download_parser.add_argument('-e', '--experiment', help="Experiment ID")
    download_parser.add_argument('-var', '--variable', help="Variable name")
    download_parser.add_argument('-f', '--frequency', help="Time frequency")
    download_parser.add_argument('-m', '--model', help="Source ID/Model")
    download_parser.add_argument('-r', '--resolution', help="Nominal resolution")
    download_parser.add_argument('-en', '--ensemble', help="Ensemble member")
    download_parser.add_argument('-a', '--activity', help="Activity ID (e.g., CMIP, ScenarioMIP)")
    download_parser.add_argument('-i', '--institution', help="Institution ID (e.g., NCAR)")
    download_parser.add_argument('-s', '--source-type', help="Source type (e.g., AOGCM, BGC)")
    download_parser.add_argument('-g', '--grid-label', help="Grid label (e.g., gn, gr)")
    download_parser.add_argument('-x', '--extra-params', help="Additional query parameters as JSON")
    download_parser.add_argument('--latest', action='store_true', help="Retrieve only the latest version")
    download_parser.add_argument('-out', '--output-dir', default='./cmip6_data', help="Download directory")
    download_parser.add_argument('-log', '--log-dir', default='./logs', help="Log directory")
    download_parser.add_argument('-meta', '--metadata-dir', default='./metadata', help="Metadata directory")
    download_parser.add_argument('-w', '--workers', type=int, default=4, help="Number of parallel threads")
    download_parser.add_argument('-retries', '--retries', type=int, default=5, help="Number of retries")
    download_parser.add_argument('-t', '--timeout', type=int, default=30, help="HTTP timeout (seconds)")
    download_parser.add_argument('-n', '--max-downloads', type=int, help="Max files to download")
    download_parser.add_argument('-S', '--save-mode', choices=['flat', 'structured'], default='flat', help="Save mode")
    download_parser.add_argument('-id', '--id', help="ESGF username")
    download_parser.add_argument('-pass', '--password', help="ESGF password")
    download_parser.add_argument('-c', '--config', help="JSON config file")
    download_parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    download_parser.add_argument('-d', '--dry-run', action='store_true', help="Simulate download")
    download_parser.add_argument('-T', '--test', action='store_true', help="Run test dataset")
    download_parser.add_argument('--demo', action='store_true', help="Run in demo mode with default settings")
    download_parser.add_argument('--no-verify-ssl', action='store_true', help="Disable SSL verification")
    download_parser.add_argument('--retry-failed', help="Path to failed_downloads.json to retry downloading those files")

    # Crop subcommand
    crop_parser = subparsers.add_parser(
        'crop', help="Crop NetCDF files to a geographic region",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    crop_parser.add_argument('-i', '--input-dir', default='./cmip6_data', help="Directory containing input NetCDF files")
    crop_parser.add_argument('-o', '--output-dir', default='./cmip6_data_cropped', help="Directory for cropped NetCDF files")
    crop_parser.add_argument('--min-lat', type=float, required=True, help="Minimum latitude (-90 to 90)")
    crop_parser.add_argument('--max-lat', type=float, required=True, help="Maximum latitude (-90 to 90)")
    crop_parser.add_argument('--min-lon', type=float, required=True, help="Minimum longitude (-180 to 360)")
    crop_parser.add_argument('--max-lon', type=float, required=True, help="Maximum longitude (-180 to 360)")
    crop_parser.add_argument('--buffer-km', type=float, default=0.0, help="Buffer distance in kilometers")
    crop_parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    crop_parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    crop_parser.add_argument('-w', '--workers', type=int, default=4, help="Number of parallel workers")
    crop_parser.add_argument('--demo', action='store_true', help="Run in demo mode with default settings")

    # Clip subcommand
    clip_parser = subparsers.add_parser(
        'clip', help="Clip NetCDF files using a shapefile",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    clip_parser.add_argument('-i', '--input-dir', default='./cmip6_data', help="Directory containing input NetCDF files")
    clip_parser.add_argument('-o', '--output-dir', default='./cmip6_data_clipped', help="Directory for clipped NetCDF files")
    clip_parser.add_argument('--shapefile', required=True, help="Path to shapefile defining clipping region")
    clip_parser.add_argument('--buffer-km', type=float, default=0.0, help="Buffer distance in kilometers")
    clip_parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    clip_parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    clip_parser.add_argument('-w', '--workers', type=int, default=4, help="Number of parallel workers")
    clip_parser.add_argument('--demo', action='store_true', help="Run in demo mode with test files")

    # Database subcommand
    db_parser = subparsers.add_parser(
        'database', help="Generate a database of NetCDF files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    db_parser.add_argument('-i', '--input-dir', default='./cmip6_data', help="Directory containing NetCDF files")
    db_parser.add_argument('-o', '--output-dir', default='./output', help="Directory to save the database JSON file")
    db_parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    db_parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    db_parser.add_argument('-w', '--workers', type=int, help="Number of parallel workers (default: CPU count)")
    db_parser.add_argument('--demo', action='store_true', help="Run in demo mode with test files")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'download':
        download_command(args)
    elif args.command == 'crop':
        crop_command(args)
    elif args.command == 'clip':
        clip_command(args)
    elif args.command == 'database':
        database_command(args)

if __name__ == "__main__":
    main()