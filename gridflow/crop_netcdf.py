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
import logging
import sys
import math
import netCDF4
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

__version__ = "0.2.3"

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
Effortlessly crop CMIP6 NetCDF files to specific geographic regions.
============================================================
"""
    print(intro)

def setup_logging(log_dir: str, level: str, prefix: str = "") -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{prefix}crop_netcdf_{timestamp}.log"

    log_levels = {
        'minimal': logging.INFO,
        'normal': logging.INFO,
        'verbose': logging.DEBUG,
        'debug': logging.DEBUG
    }
    numeric_level = log_levels.get(level.lower(), logging.INFO)

    class MinimalFilter(logging.Filter):
        def filter(self, record):
            if level.lower() == 'minimal':
                return record.levelno >= logging.INFO
            return True

    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
    handlers[1].addFilter(MinimalFilter())

    format_str = '%(message)s' if level.lower() == 'minimal' else '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(
        level=numeric_level,
        format=format_str,
        handlers=handlers,
        force=True
    )

def find_coordinate_vars(dataset: netCDF4.Dataset) -> Tuple[Optional[str], Optional[str]]:
    lat_var = lon_var = None
    for var_name, var in dataset.variables.items():
        if 'standard_name' in var.ncattrs():
            if var.getncattr('standard_name') == 'latitude' and len(var.dimensions) == 1:
                lat_var = var_name
            elif var.getncattr('standard_name') == 'longitude' and len(var.dimensions) == 1:
                lon_var = var_name
        elif 'units' in var.ncattrs():
            units = var.getncattr('units').lower()
            if 'degree' in units and len(var.dimensions) == 1:
                if 'lat' in var_name.lower() and lat_var is None:
                    lat_var = var_name
                elif 'lon' in var_name.lower() and lon_var is None:
                    lon_var = var_name
    if lat_var is None or lon_var is None:
        logging.error("Could not find suitable 1D latitude or longitude variables.")
    return lat_var, lon_var

def get_crop_indices(coord_data: np.ndarray, min_val: float, max_val: float, is_longitude: bool = False) -> Tuple[Optional[int], Optional[int]]:
    if is_longitude and min_val > max_val:
        # Handle antimeridian crossing
        indices = np.where((coord_data >= min_val) | (coord_data <= max_val))[0]
    else:
        indices = np.where((coord_data >= min_val) & (coord_data <= max_val))[0]
    if len(indices) == 0:
        return None, None
    return indices[0], indices[-1]

def normalize_longitude(lon: float, target_range: str = '0-360') -> float:
    """Normalize longitude to the target range."""
    lon = lon % 360  # Ensure within 0-360°
    if target_range == '-180-180' and lon > 180:
        lon -= 360
    return lon

def crop_netcdf_file(input_path: Path, output_path: Path, min_lat: float, max_lat: float, min_lon: float, max_lon: float, buffer_km: float = 0.0) -> bool:
    try:
        with netCDF4.Dataset(input_path, 'r') as src:
            lat_var, lon_var = find_coordinate_vars(src)
            if not lat_var or not lon_var:
                logging.error(f"No lat/lon variables found in {input_path.name}")
                return False

            lat_data = src.variables[lat_var][:]
            lon_data = src.variables[lon_var][:]
            if len(lat_data.shape) != 1 or len(lon_data.shape) != 1:
                logging.error(f"Latitude or longitude is not 1D in {input_path.name}")
                return False

            # Determine NetCDF longitude range
            lon_min, lon_max = lon_data.min(), lon_data.max()
            target_range = '0-360' if lon_min >= 0 and lon_max <= 360 else '-180-180'
            logging.debug(f"NetCDF longitude range: {lon_min} to {lon_max}, using {target_range}")

            # Normalize input longitudes to match NetCDF range
            min_lon = normalize_longitude(min_lon, target_range)
            max_lon = normalize_longitude(max_lon, target_range)
            logging.debug(f"Input longitudes normalized: min_lon={min_lon}, max_lon={max_lon}")

            # Calculate buffer in degrees
            if buffer_km > 0:
                lat_buffer_deg = buffer_km / 111.0
                avg_lat = (min_lat + max_lat) / 2.0
                lon_buffer_deg = buffer_km / (111.0 * math.cos(math.radians(avg_lat)))
                min_lat -= lat_buffer_deg
                max_lat += lat_buffer_deg
                min_lon = normalize_longitude(min_lon - lon_buffer_deg, target_range)
                max_lon = normalize_longitude(max_lon + lon_buffer_deg, target_range)
                logging.debug(f"Adjusted bounds with buffer: min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}")

            # Ensure valid bounds
            if min_lat < -90 or max_lat > 90:
                logging.error(f"Latitude bounds out of range: min_lat={min_lat}, max_lat={max_lat}")
                return False
            if target_range == '0-360' and (min_lon < 0 or max_lon > 360):
                logging.error(f"Longitude bounds out of range for 0-360: min_lon={min_lon}, max_lon={max_lon}")
                return False
            if target_range == '-180-180' and (min_lon < -180 or max_lon > 180):
                logging.error(f"Longitude bounds out of range for -180-180: min_lon={min_lon}, max_lon={max_lon}")
                return False

            lat_indices = get_crop_indices(lat_data, min_lat, max_lat)
            lon_indices = get_crop_indices(lon_data, min_lon, max_lon, is_longitude=True)
            if lat_indices[0] is None or lon_indices[0] is None:
                logging.error(f"No data within lat/lon bounds for {input_path.name}")
                return False

            lat_start, lat_end = lat_indices
            lon_start, lon_end = lon_indices
            lat_size = lat_end - lat_start + 1
            lon_size = lon_end - lon_start + 1

            lat_dim = src.variables[lat_var].dimensions[0]
            lon_dim = src.variables[lon_var].dimensions[0]

            with netCDF4.Dataset(output_path, 'w', format=src.file_format) as dst:
                dst.setncatts(src.__dict__)
                for dim in src.dimensions:
                    size = src.dimensions[dim].size
                    if dim == lat_dim:
                        size = lat_size
                    elif dim == lon_dim:
                        size = lon_size
                    dst.createDimension(dim, size)

                for var_name, var in src.variables.items():
                    dims = var.dimensions
                    slices = []
                    for dim in dims:
                        if dim == lat_dim:
                            slices.append(slice(lat_start, lat_end + 1))
                        elif dim == lon_dim:
                            slices.append(slice(lon_start, lon_end + 1))
                        else:
                            slices.append(slice(None))
                    dtype = var.dtype
                    fill_value = var.getncattr('_FillValue') if '_FillValue' in var.ncattrs() else None
                    var_out = dst.createVariable(var_name, dtype, dims, zlib=True, fill_value=fill_value)
                    var_out.setncatts({k: v for k, v in var.__dict__.items() if k != '_FillValue'})
                    data = var[tuple(slices)]
                    logging.debug(f"Copying {var_name}: data shape {data.shape}, var_out shape {var_out.shape}")
                    var_out[:] = data

        logging.info(f"Cropped {input_path.name} to {output_path.name}")
        return True
    except Exception as e:
        logging.error(f"Failed to crop {input_path.name}: {e}")
        return False

def main():
    print_intro()

    parser = argparse.ArgumentParser(
        description="Crop NetCDF files to a specified geographic region with optional buffer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--input-dir', default='./cmip6_data', help="Directory containing input NetCDF files")
    parser.add_argument('--output-dir', default='./cmip6_data_cropped', help="Directory for cropped NetCDF files")
    parser.add_argument('--min-lat', type=float, required=True, help="Minimum latitude (-90 to 90)")
    parser.add_argument('--max-lat', type=float, required=True, help="Maximum latitude (-90 to 90)")
    parser.add_argument('--min-lon', type=float, required=True, help="Minimum longitude (-180 to 360)")
    parser.add_argument('--max-lon', type=float, required=True, help="Maximum longitude (-180 to 360)")
    parser.add_argument('--buffer-km', type=float, default=0.0, help="Buffer distance in kilometers to extend the cropping bounds")
    parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}')
    args = parser.parse_args()

    if args.min_lat >= args.max_lat:
        logging.error("min-lat must be less than max-lat")
        sys.exit(1)
    if args.min_lon >= args.max_lon:
        logging.error("min-lon must be less than max-lon")
        sys.exit(1)
    if not (-90 <= args.min_lat <= 90 and -90 <= args.max_lat <= 90):
        logging.error("Latitude must be between -90 and 90")
        sys.exit(1)
    if not (-180 <= args.min_lon <= 360 and -180 <= args.max_lon <= 360):
        logging.error("Longitude must be between -180 and 360")
        sys.exit(1)

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

    logging.info(f"Found {len(nc_files)} NetCDF files to process")
    success_count = 0
    for nc_file in nc_files:
        output_file = output_dir / nc_file.name.replace('.nc', '_cropped.nc')
        if crop_netcdf_file(nc_file, output_file, args.min_lat, args.max_lat, args.min_lon, args.max_lon, args.buffer_km):
            success_count += 1

    logging.info(f"Completed: {success_count}/{len(nc_files)} files cropped successfully")

if __name__ == "__main__":
    main()