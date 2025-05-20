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

import argparse
import os
from gridflow import __version__
from gridflow.commands import (
    download_command,
    download_cmip5_command,
    download_prism_command,
    crop_command,
    clip_command,
    catalog_command,
)

def print_intro():
    banner = """
==============================================================================================
     ____      _     _ _____ _                
    / ___|_ __(_) __| |  ___| | _____      __ 
   | |  _| '__| |/ _` | |_  | |/ _ \ \ /\ / / 
   | |_| | |  | | (_| |  _| | | (_) \ V  V /  
    \____|_|  |_|\__,_|_|   |_|\___/ \_/\_/   

==============================================================================================
Welcome to GridFlow v{}! Copyright (c) 2025 Bhuwan Shah
Effortlessly download and process CMIP5, CMIP6, and PRISM climate data.
Run `gridflow -h` for help or `gridflow download --demo` to try a sample CMIP6 download.
==============================================================================================
""".format(__version__)
    print(banner)

class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom formatter for consistent CLI help formatting."""
    def _format_args(self, action, default_metavar):
        get_metavar = self._metavar_formatter(action, default_metavar)
        return '%s' % get_metavar(1) if action.nargs is None else super()._format_args(action, default_metavar)

    def _format_action_invocation(self, action):
        if not action.option_strings:
            return super()._format_action_invocation(action)
        option_strings = ', '.join(action.option_strings)
        return f'{option_strings} {self._format_args(action, action.dest.upper())}'

def main():
    """Main entry point for the GridFlow CLI."""
    print_intro()
    parser = argparse.ArgumentParser(
        description=(
            "GridFlow: A tool for downloading and processing CMIP5, CMIP6, and PRISM climate data.\n"
            "Download CMIP5, CMIP6, or PRISM datasets, crop or clip NetCDF files to specific regions,\n"
            "or generate metadata catalogues."
        ),
        epilog=(
            "Examples:\n"
            "  gridflow download --demo                 # Download 10 sample CMIP6 files\n"
            "  gridflow download-cmip5 --demo           # Download 10 sample CMIP5 files\n"
            "  gridflow download-prism --demo           # Download PRISM ppt (4km, 2020-01-01)\n"
            "  gridflow crop --demo                     # Crop files to a sample spatial bound\n"
            "  gridflow clip --demo                     # Clip files using Iowa shapefile\n"
            "  gridflow catalog --demo                  # Generate a sample catalog\n"
            "  gridflow download -h                     # Show CMIP6 download options\n"
            "  gridflow download-cmip5 -h               # Show CMIP5 download options\n"
            "  gridflow crop -h                         # Show crop options\n"
            "  gridflow clip -h                         # Show clip options\n"
            "  gridflow catalog -h                      # Show catalog options\n"
            "  gridflow download-prism -h               # Show PRISM download options\n"
            "\nRun 'gridflow <command> -h' for detailed help."
        ),
        formatter_class=CustomHelpFormatter
    )
    parser.add_argument('-v', '--version', action='version', version=f'GridFlow {__version__}')
    subparsers = parser.add_subparsers(dest='command', help="Available commands", required=True)

    # CMIP6 Download
    download_parser = subparsers.add_parser(
        'download',
        help="Download CMIP6 data from ESGF nodes",
        epilog="Example: gridflow download --demo\nUse --demo for a quick test or specify parameters.",
        formatter_class=CustomHelpFormatter
    )
    download_parser.add_argument('-p', '--project', default='CMIP6', help="Project name (default: CMIP6)")
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
    download_parser.add_argument('--start-date', help="Start date (YYYY-MM-DD or YYYYMM)", type=str, default=None)
    download_parser.add_argument('--end-date', help="End date (YYYY-MM-DD or YYYYMM)", type=str, default=None)
    download_parser.add_argument('-x', '--extra-params', help="Additional query parameters as JSON")
    download_parser.add_argument('--latest', action='store_true', help="Retrieve only the latest version")
    download_parser.add_argument('-out', '--output-dir', default='./cmip6_data', help="Download directory")
    download_parser.add_argument('-log', '--log-dir', default='./logs', help="Log directory")
    download_parser.add_argument('-meta', '--metadata-dir', default='./metadata', help="Metadata directory")
    download_parser.add_argument('-w', '--workers', type=int, default=min(os.cpu_count() or 4, 4), help="Number of parallel threads")
    download_parser.add_argument('-retries', '--retries', type=int, default=5, help="Number of retries")
    download_parser.add_argument('-t', '--timeout', type=int, default=30, help="HTTP timeout (seconds)")
    download_parser.add_argument('-n', '--max-downloads', type=int, help="Max files to download")
    download_parser.add_argument('-S', '--save-mode', choices=['flat', 'structured'], default='flat', help="Save mode")
    download_parser.add_argument('-id', '--id', help="ESGF username")
    download_parser.add_argument('-pass', '--password', help="ESGF password")
    download_parser.add_argument('-c', '--config', help="JSON config file")
    download_parser.add_argument(
        '-L', '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    download_parser.add_argument('-d', '--dry-run', action='store_true', help="Simulate download")
    download_parser.add_argument('-T', '--test', action='store_true', help="Run test dataset")
    download_parser.add_argument('--demo', action='store_true', help="Run in demo mode")
    download_parser.add_argument('--no-verify-ssl', action='store_true', help="Disable SSL verification")
    download_parser.add_argument('--retry-failed', help="Path to failed_downloads.json to retry")

    # CMIP5 Download
    cmip5_parser = subparsers.add_parser(
        'download-cmip5',
        help="Download CMIP5 data from ESGF nodes",
        epilog="Example: gridflow download-cmip5 --demo\nUse --demo for a quick test or specify parameters.",
        formatter_class=CustomHelpFormatter
    )
    cmip5_parser.add_argument('-p', '--project', default='CMIP5', help="Project name (default: CMIP5)")
    cmip5_parser.add_argument('-e', '--experiment', help="Experiment ID")
    cmip5_parser.add_argument('-var', '--variable', help="Variable name")
    cmip5_parser.add_argument('-f', '--frequency', help="Time frequency")
    cmip5_parser.add_argument('-m', '--model', help="Model")
    cmip5_parser.add_argument('-r', '--resolution', help="Nominal resolution")
    cmip5_parser.add_argument('-en', '--ensemble', help="Ensemble member")
    cmip5_parser.add_argument('-i', '--institute', help="Institute (e.g., MOHC)")
    cmip5_parser.add_argument('--start-date', help="Start date (YYYY-MM-DD or YYYYMM)", type=str, default=None)
    cmip5_parser.add_argument('--end-date', help="End date (YYYY-MM-DD or YYYYMM)", type=str, default=None)
    cmip5_parser.add_argument('-x', '--extra-params', help="Additional query parameters as JSON")
    cmip5_parser.add_argument('--latest', action='store_true', help="Retrieve only the latest version")
    cmip5_parser.add_argument('-out', '--output-dir', default='./cmip5_data', help="Download directory")
    cmip5_parser.add_argument('-log', '--log-dir', default='./logs', help="Log directory")
    cmip5_parser.add_argument('-meta', '--metadata-dir', default='./metadata', help="Metadata directory")
    cmip5_parser.add_argument('-w', '--workers', type=int, default=min(os.cpu_count() or 4, 4), help="Number of parallel threads")
    cmip5_parser.add_argument('-retries', '--retries', type=int, default=5, help="Number of retries")
    cmip5_parser.add_argument('-t', '--timeout', type=int, default=30, help="HTTP timeout (seconds)")
    cmip5_parser.add_argument('-n', '--max-downloads', type=int, help="Max files to download")
    cmip5_parser.add_argument('-S', '--save-mode', choices=['flat', 'structured'], default='flat', help="Save mode")
    cmip5_parser.add_argument('--openid', help="ESGF OpenID (e.g., https://esgf-node.llnl.gov/esgf-idp/openid/username)")
    cmip5_parser.add_argument('--username', help="ESGF username")
    cmip5_parser.add_argument('--password', help="ESGF password")
    cmip5_parser.add_argument('-c', '--config', help="JSON config file")
    cmip5_parser.add_argument(
        '-L', '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    cmip5_parser.add_argument('-d', '--dry-run', action='store_true', help="Simulate download")
    cmip5_parser.add_argument('-T', '--test', action='store_true', help="Run test dataset")
    cmip5_parser.add_argument('--demo', action='store_true', help="Run in demo mode")
    cmip5_parser.add_argument('--no-verify-ssl', action='store_true', help="Disable SSL verification")
    cmip5_parser.add_argument('--retry-failed', help="Path to failed_downloads.json to retry")

    # PRISM Download
    prism_parser = subparsers.add_parser(
        'download-prism',
        help="Download PRISM daily or monthly climate data",
        epilog=(
            "Example: gridflow download-prism --demo\n"
            "Downloads three months (tmean, 4km, January-March 2020, monthly) in demo mode.\n"
            "Filenames include resolution (e.g., prism_tmean_us_4km_202001.zip)."
        ),
        formatter_class=CustomHelpFormatter
    )
    prism_parser.add_argument(
        '--variable',
        choices=['ppt', 'tmax', 'tmin', 'tmean', 'tdmean', 'vpdmin', 'vpdmax'],
        required=False,
        help="Variable: ppt, tmax, tmin, tmean, tdmean, vpdmin, vpdmax"
    )
    prism_parser.add_argument(
        '--resolution',
        choices=['4km', '800m'],
        required=False,
        help="Spatial resolution: 4km or 800m"
    )
    prism_parser.add_argument(
        '--time-step',
        choices=['daily', 'monthly'],
        required=False,
        help="Time step: daily or monthly"
    )
    prism_parser.add_argument(
        '--start-date',
        required=False,
        help="Start date (YYYY-MM-DD for daily, YYYY-MM for monthly)"
    )
    prism_parser.add_argument(
        '--end-date',
        required=False,
        help="End date (YYYY-MM-DD for daily, YYYY-MM for monthly)"
    )
    prism_parser.add_argument(
        '--output-dir',
        default='./prism_data',
        help="Output directory"
    )
    prism_parser.add_argument(
        '--metadata-dir',
        default='./metadata',
        help="Metadata directory"
    )
    prism_parser.add_argument(
        '--log-dir',
        default='./logs',
        help="Log directory"
    )
    prism_parser.add_argument(
        '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    prism_parser.add_argument(
        '--retries',
        type=int,
        default=3,
        help="Number of download retries"
    )
    prism_parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help="HTTP timeout (seconds)"
    )
    prism_parser.add_argument(
        '--demo',
        action='store_true',
        help="Download three months (tmean, 4km, January-March 2020, monthly)"
    )
    prism_parser.add_argument(
        '-w', '--workers',
        type=int,
        default=min(os.cpu_count() or 4, 4),
        help="Number of parallel threads"
    )

    # Crop
    crop_parser = subparsers.add_parser(
        'crop',
        help="Crop NetCDF files by spatial bounds",
        epilog="Example: gridflow crop --demo\nUse --demo to crop to sample spatial bounds.",
        formatter_class=CustomHelpFormatter
    )
    crop_parser.add_argument(
        '-i', '--input-dir',
        default='./cmip6_data',
        help="Input directory containing NetCDF files"
    )
    crop_parser.add_argument(
        '-o', '--output-dir',
        default='./cropped_data',
        help="Output directory for cropped files"
    )
    crop_parser.add_argument(
        '--min-lat',
        type=float,
        default=None,
        help="Minimum latitude bound"
    )
    crop_parser.add_argument(
        '--max-lat',
        type=float,
        default=None,
        help="Maximum latitude bound"
    )
    crop_parser.add_argument(
        '--min-lon',
        type=float,
        default=None,
        help="Minimum longitude bound"
    )
    crop_parser.add_argument(
        '--max-lon',
        type=float,
        default=None,
        help="Maximum longitude bound"
    )
    crop_parser.add_argument(
        '--buffer-km',
        type=float,
        default=0.0,
        help="Buffer distance in kilometers"
    )
    crop_parser.add_argument(
        '--log-dir',
        default='./logs',
        help="Log directory"
    )
    crop_parser.add_argument(
        '-L', '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    crop_parser.add_argument(
        '-w', '--workers',
        type=int,
        default=min(os.cpu_count() or 4, 4),
        help="Number of parallel workers"
    )
    crop_parser.add_argument(
        '--demo',
        action='store_true',
        help="Run in demo mode with sample spatial bounds"
    )

    # Clip
    clip_parser = subparsers.add_parser(
        'clip',
        help="Clip NetCDF files in a directory using a shapefile",
        epilog="Example: gridflow clip --demo\nUse --demo with sample Iowa shapefile.",
        formatter_class=CustomHelpFormatter
    )
    clip_parser.add_argument(
        '-i', '--input-dir',
        default='./cmip6_data',
        help="Input criticize containing NetCDF files"
    )
    clip_parser.add_argument(
        '-o', '--output-dir',
        default='./cmip6_data_clipped',
        help="Output directory for clipped files"
    )
    clip_parser.add_argument(
        '--shapefile',
        help="Path to shapefile"
    )
    clip_parser.add_argument(
        '--buffer-km',
        type=float,
        default=0.0,
        help="Buffer distance (km)"
    )
    clip_parser.add_argument(
        '--log-dir',
        default='./logs',
        help="Log directory"
    )
    clip_parser.add_argument(
        '-L', '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    clip_parser.add_argument(
        '-w', '--workers',
        type=int,
        default=min(os.cpu_count() or 4, 4),
        help="Number of parallel workers"
    )
    clip_parser.add_argument(
        '--demo',
        action='store_true',
        help="Run in demo mode"
    )

    # Catalog
    catalog_parser = subparsers.add_parser(
        'catalog',
        help="Generate a catalog of NetCDF files",
        epilog="Example: gridflow catalog --demo\nUse --demo for sample catalog.",
        formatter_class=CustomHelpFormatter
    )
    catalog_parser.add_argument(
        '-i', '--input-dir',
        default='./cmip6_data',
        help="Input NetCDF directory"
    )
    catalog_parser.add_argument(
        '-o', '--output-dir',
        default='./catalog',
        help="Output JSON directory"
    )
    catalog_parser.add_argument(
        '--log-dir',
        default='./logs',
        help="Log directory"
    )
    catalog_parser.add_argument(
        '-L', '--log-level',
        choices=['minimal', 'normal', 'verbose', 'debug'],
        default='minimal',
        help="Logging level"
    )
    catalog_parser.add_argument(
        '-w', '--workers',
        type=int,
        default=min(os.cpu_count() or 4, 4),
        help="Number of parallel workers"
    )
    catalog_parser.add_argument(
        '--demo',
        action='store_true',
        help="Run in demo mode"
    )

    args = parser.parse_args()

    if args.command == 'download':
        download_command(args)
    elif args.command == 'download-cmip5':
        download_cmip5_command(args)
    elif args.command == 'download-prism':
        download_prism_command(args)
    elif args.command == 'crop':
        if not args.demo and any(arg is None for arg in [args.min_lat, args.max_lat, args.min_lon, args.max_lon]):
            parser.error("All spatial bounds (--min-lat, --max-lat, --min-lon, --max-lon) are required unless --demo")
        crop_command(args)
    elif args.command == 'clip':
        if not args.demo and args.shapefile is None:
            parser.error("Argument required unless --demo: --shapefile")
        clip_command(args)
    elif args.command == 'catalog':
        catalog_command(args)

if __name__ == "__main__":
    main()