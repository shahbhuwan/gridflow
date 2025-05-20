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
import signal
from pathlib import Path
from .logging_utils import setup_logging
from .cmip6_downloader import run_download as cmip6_run_download
from .cmip5_downloader import run_download as cmip5_run_download
from .prism_downloader import download_prism
from .crop_netcdf import crop_netcdf
from .clip_netcdf import clip_netcdf
from .catalog_generator import generate_catalog

def setup_backend_logging(args, project_prefix: str) -> None:
    if not any(isinstance(h, logging.FileHandler) for h in logging.getLogger().handlers):
        log_dir = Path(args.metadata_dir if hasattr(args, 'metadata_dir') else 'logs')
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create log directory {log_dir}: {e}")
            sys.exit(1)
        setup_logging(log_dir, args.log_level, prefix=f"gridflow_{project_prefix}_")

class StopFlag:
    def __init__(self, stop_flag=None):
        self._stopped = False
        self._external_stop_flag = stop_flag

    def __call__(self) -> bool:
        if self._external_stop_flag and self._external_stop_flag():
            return True
        return self._stopped

    def stop(self):
        self._stopped = True

def download_command(args):
    setup_backend_logging(args, project_prefix="cmip6")
    args.stop_flag = StopFlag()
    cmip6_run_download(args)

def download_cmip5_command(args):
    setup_backend_logging(args, project_prefix="cmip5")
    args.stop_flag = StopFlag()
    if hasattr(args, 'time_frequency'):
        args.frequency = args.time_frequency
    cmip5_run_download(args)

def download_prism_command(args):
    setup_backend_logging(args, project_prefix="prism")
    args.stop_flag = StopFlag()
    if args.demo:
        args.variable = "tmean"
        args.resolution = "4km"
        args.time_step = "monthly"
        args.start_date = "2020-01"
        args.end_date = "2020-03"
        args.workers = 4
    download_prism(
        variable=args.variable,
        resolution=args.resolution,
        time_step=args.time_step,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        metadata_dir=args.metadata_dir,
        log_level=args.log_level,
        retries=args.retries,
        timeout=args.timeout,
        demo=args.demo,
        workers=args.workers,
        stop_flag=args.stop_flag
    )

def crop_command(args):
    setup_backend_logging(args, project_prefix="crop")
    args.stop_flag = StopFlag()
    if not args.demo and any(arg is None for arg in [args.min_lat, args.max_lat, args.min_lon, args.max_lon]):
        logging.error("All spatial bounds (--min-lat, --max-lat, --min-lon, --max-lon) must be provided unless --demo")
        sys.exit(1)
    crop_netcdf(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        min_lat=args.min_lat,
        max_lat=args.max_lat,
        min_lon=args.min_lon,
        max_lon=args.max_lon,
        buffer_km=args.buffer_km,
        stop_flag=args.stop_flag,
        workers=args.workers,
        demo=args.demo
    )

def clip_command(args):
    setup_backend_logging(args, project_prefix="clip")
    args.stop_flag = StopFlag()
    shapefile_path = getattr(args, "shapefile_path", None)
    clip_netcdf(
        input_dir=args.input_dir,
        shapefile_path=shapefile_path,
        buffer_km=args.buffer_km,
        output_dir=args.output_dir,
        stop_flag=args.stop_flag,
        workers=args.workers
    )

def catalog_command(args):
    setup_backend_logging(args, project_prefix="catalog")
    args.stop_flag = StopFlag()
    result = generate_catalog(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        demo_mode=args.demo,
        workers=args.workers,
        stop_flag=args.stop_flag
    )
    if not result and args.demo:
        logging.info("Catalog generation failed in demo mode, exiting")
        sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="GridFlow Command Line Interface")
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Download CMIP6
    download_parser = subparsers.add_parser('download', help='Download CMIP6 data')
    download_parser.add_argument('--project', default='CMIP6')
    download_parser.add_argument('--activity')
    download_parser.add_argument('--experiment')
    download_parser.add_argument('--variable')
    download_parser.add_argument('--frequency')
    download_parser.add_argument('--model')
    download_parser.add_argument('--resolution')
    download_parser.add_argument('--ensemble')
    download_parser.add_argument('--institution')
    download_parser.add_argument('--source-type')
    download_parser.add_argument('--grid-label')
    download_parser.add_argument('--start-date')
    download_parser.add_argument('--end-date')
    download_parser.add_argument('--latest', action='store_true')
    download_parser.add_argument('--extra-params')
    download_parser.add_argument('--output-dir', default='./cmip6_data')
    download_parser.add_argument('--metadata-dir', default='./metadata')
    download_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    download_parser.add_argument('--save-mode', default='flat', choices=['flat', 'structured'])
    download_parser.add_argument('--workers', type=int)
    download_parser.add_argument('--retries', type=int, default=3)
    download_parser.add_argument('--timeout', type=int, default=30)
    download_parser.add_argument('--max-downloads', type=int)
    download_parser.add_argument('--username', help="ESGF username")
    download_parser.add_argument('--password', help="ESGF password")
    download_parser.add_argument('--no-verify-ssl', action='store_true')

    # Download CMIP5
    cmip5_parser = subparsers.add_parser('download-cmip5', help='Download CMIP5 data')
    cmip5_parser.add_argument('--model')
    cmip5_parser.add_argument('--experiment')
    cmip5_parser.add_argument('--frequency')
    cmip5_parser.add_argument('--variable')
    cmip5_parser.add_argument('--ensemble')
    cmip5_parser.add_argument('--institute')
    cmip5_parser.add_argument('--start-date')
    cmip5_parser.add_argument('--end-date')
    cmip5_parser.add_argument('--latest', action='store_true')
    cmip5_parser.add_argument('--extra-params')
    cmip5_parser.add_argument('--output-dir', default='./cmip5_data')
    cmip5_parser.add_argument('--metadata-dir', default='./metadata')
    cmip5_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    cmip5_parser.add_argument('--save-mode', default='flat', choices=['flat', 'structured'])
    cmip5_parser.add_argument('--workers', type=int)
    cmip5_parser.add_argument('--retries', type=int, default=3)
    cmip5_parser.add_argument('--timeout', type=int, default=30)
    cmip5_parser.add_argument('--max-downloads', type=int)
    cmip5_parser.add_argument('--username', help="ESGF username")
    cmip5_parser.add_argument('--password', help="ESGF password")
    cmip5_parser.add_argument('--no-verify-ssl', action='store_true')

    # Download PRISM
    prism_parser = subparsers.add_parser('download-prism', help='Download PRISM data')
    prism_parser.add_argument('--variable', required=True, choices=['ppt', 'tmax', 'tmin', 'tmean', 'tdmean', 'vpdmin', 'vpdmax'])
    prism_parser.add_argument('--resolution', required=True, choices=['4km', '800m'])
    prism_parser.add_argument('--time-step', required=True, choices=['daily', 'monthly'])
    prism_parser.add_argument('--start-date', required=True)
    prism_parser.add_argument('--end-date', required=True)
    prism_parser.add_argument('--output-dir', default='./prism_data')
    prism_parser.add_argument('--metadata-dir', default='./metadata')
    prism_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    prism_parser.add_argument('--retries', type=int, default=3)
    prism_parser.add_argument('--timeout', type=int, default=30)
    prism_parser.add_argument('--demo', action='store_true')
    prism_parser.add_argument('--workers', type=int)

    # Crop
    crop_parser = subparsers.add_parser('crop', help='Crop NetCDF files by spatial bounds')
    crop_parser.add_argument('--input-dir', default='./cmip6_data', help='Input directory with NetCDF files')
    crop_parser.add_argument('--output-dir', default='./cropped_data', help='Output directory for cropped files')
    crop_parser.add_argument('--min-lat', type=float, default=None, help='Minimum latitude bound')
    crop_parser.add_argument('--max-lat', type=float, default=None, help='Maximum latitude bound')
    crop_parser.add_argument('--min-lon', type=float, default=None, help='Minimum longitude bound')
    crop_parser.add_argument('--max-lon', type=float, default=None, help='Maximum longitude bound')
    crop_parser.add_argument('--buffer-km', type=float, default=0.0, help='Buffer distance in kilometers')
    crop_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    crop_parser.add_argument('--workers', type=int)
    crop_parser.add_argument('--demo', action='store_true', help='Run in demo mode with sample spatial bounds')

    # Clip
    clip_parser = subparsers.add_parser('clip', help='Clip NetCDF files by shapefile')
    clip_parser.add_argument('--input-dir', default='./cmip6_data', help='Input directory with NetCDF files')
    clip_parser.add_argument('--shapefile-path', default='../iowa_border/iowa_border.shp', help='Path to shapefile for clipping')
    clip_parser.add_argument('--buffer-km', type=float, default=20, help='Buffer distance in kilometers')
    clip_parser.add_argument('--output-dir', default='./clipped_data', help='Output directory for clipped files')
    clip_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    clip_parser.add_argument('--workers', type=int)

    # Catalog
    catalog_parser = subparsers.add_parser('catalog', help='Generate catalog from NetCDF files')
    catalog_parser.add_argument('--input-dir', default='./cmip6_data', help='Input directory with NetCDF files')
    catalog_parser.add_argument('--output-dir', default='./catalog', help='Output directory for catalog')
    catalog_parser.add_argument('--workers', type=int)
    catalog_parser.add_argument('--log-level', default='minimal', choices=['minimal', 'normal', 'verbose', 'debug'])
    catalog_parser.add_argument('--demo', action='store_true', help='Run in demo mode with sample catalog output')

    args = parser.parse_args()

    command_map = {
        'download': download_command,
        'download-cmip5': download_cmip5_command,
        'download-prism': download_prism_command,
        'crop': crop_command,
        'clip': clip_command,
        'catalog': catalog_command
    }

    try:
        command_map[args.command](args)
    except Exception as e:
        logging.error(f"Command {args.command} failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()