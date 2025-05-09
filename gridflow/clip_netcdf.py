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
import math
import sys
import numpy as np
import netCDF4 as nc
import geopandas as gpd
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from .downloader import setup_logging

__version__ = "0.2.3"

def print_intro():
    intro = r"""
============================================================
  ____ ____  ___ ____  _____ _     _____        __
 / ___|  _ \|_ _|  _ \|  ___| |   / _ \ \      / /
| |  _| |_) || || | | | |_  | |  | | | \ \ /\ / /
| |_| |  _ < | || |_| |  _| | |__| |_| |\ V  V /
 \____|_| \_\___|____/|_|   |_____\___/  \_/\_/
============================================================
Welcome to GridFlow v0.2.3! Copyright (c) 2025 Bhuwan Shah
Effortlessly clip CMIP6 NetCDF files using shape files.
============================================================
"""
    print(intro)

logging_lock = Lock()

def find_coordinate_vars(dataset: nc.Dataset):
    lat_var = lon_var = None
    for var_name, var in dataset.variables.items():
        if 'standard_name' in var.ncattrs():
            if var.getncattr('standard_name') == 'latitude' and len(var.dimensions) == 1:
                lat_var = var_name
            elif var.getncattr('standard_name') == 'longitude' and len(var.dimensions) == 1:
                lon_var = var_name
    if lat_var is None or lon_var is None:
        with logging_lock:
            logging.error("Could not find suitable 1D latitude or longitude variables.")
    return lat_var, lon_var

def reproject_bounds(gdf, target_crs='EPSG:4326'):
    bounds = gdf.to_crs(target_crs).total_bounds
    min_lon, min_lat, max_lon, max_lat = bounds
    return min_lon, min_lat, max_lon, max_lat

def clip_netcdf_file(input_path: Path, shapefile_path: Path, buffer_km: float, output_path: Path) -> bool:
    try:
        gdf = gpd.read_file(shapefile_path)
        min_lon, min_lat, max_lon, max_lat = reproject_bounds(gdf)

        with logging_lock:
            logging.debug(f"Original shapefile CRS: {gdf.crs}")
            logging.debug(
                f"Original shapefile bounds: min_lon={gdf.total_bounds[0]}, min_lat={gdf.total_bounds[1]}, "
                f"max_lon={gdf.total_bounds[2]}, max_lat={gdf.total_bounds[3]}"
            )
            logging.debug(
                f"Reprojected shapefile bounds: min_lon={min_lon}, min_lat={min_lat}, "
                f"max_lon={max_lon}, max_lat={max_lat}"
            )

        if min_lon < 0:
            min_lon = (min_lon + 360) % 360
            max_lon = (max_lon + 360) % 360

        if buffer_km > 0:
            avg_lat = (min_lat + max_lat) / 2.0
            lat_buffer_deg = buffer_km / 111.0
            lon_buffer_deg = buffer_km / (111.0 * math.cos(math.radians(avg_lat)))
            min_lat -= lat_buffer_deg
            max_lat += lat_buffer_deg
            min_lon = (min_lon - lon_buffer_deg) % 360
            max_lon = (max_lon + lon_buffer_deg) % 360
            with logging_lock:
                logging.debug(
                    f"Applied buffer of {buffer_km} km: min_lat={min_lat:.4f}, max_lat={max_lat:.4f}, "
                    f"min_lon={min_lon:.4f}, max_lon={max_lon:.4f}"
                )

        with nc.Dataset(input_path, 'r') as src:
            lat_var, lon_var = find_coordinate_vars(src)
            if not lat_var or not lon_var:
                with logging_lock:
                    logging.error(f"No lat/lon variables found in {input_path.name}")
                return False

            lat_data = src.variables[lat_var][:]
            lon_data = src.variables[lon_var][:]
            if len(lat_data.shape) != 1 or len(lon_data.shape) != 1:
                with logging_lock:
                    logging.error(f"Latitude or longitude is not 1D in {input_path.name}")
                return False

            with logging_lock:
                logging.debug(
                    f"lat_var: {lat_var}, lat_data shape: {lat_data.shape}, "
                    f"min: {lat_data.min()}, max: {lat_data.max()}"
                )
                logging.debug(
                    f"lon_var: {lon_var}, lon_data shape: {lon_data.shape}, "
                    f"min: {lon_data.min()}, max: {lon_data.max()}"
                )

            lat_indices = np.where((lat_data >= min_lat) & (lat_data <= max_lat))[0]
            if min_lon > max_lon:
                lon_indices = np.where((lon_data >= min_lon) | (lon_data <= max_lon))[0]
            else:
                lon_indices = np.where((lon_data >= min_lon) & (lon_data <= max_lon))[0]

            if len(lat_indices) == 0 or len(lon_indices) == 0:
                with logging_lock:
                    logging.error(f"No data within shapefile bounds for {input_path.name}")
                return False

            lat_start, lat_end = lat_indices[0], lat_indices[-1]
            lon_start, lon_end = lon_indices[0], lon_indices[-1]
            lat_size = lat_end - lat_start + 1
            lon_size = lon_end - lon_start + 1
            with logging_lock:
                logging.debug(f"lat_start: {lat_start}, lat_end: {lat_end}, cropped size: {lat_size}")
                logging.debug(f"lon_start: {lon_start}, lon_end: {lon_end}, cropped size: {lon_size}")

            lat_dim = src.variables[lat_var].dimensions[0]
            lon_dim = src.variables[lon_var].dimensions[0]
            with logging_lock:
                logging.debug(f"lat_dim: {lat_dim}, lon_dim: {lon_dim}")

            with nc.Dataset(output_path, 'w', format=src.file_format) as dst:
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
                    with logging_lock:
                        logging.debug(
                            f"Copying {var_name}: data shape {data.shape}, var_out shape {var_out.shape}"
                        )
                    var_out[:] = data

        with logging_lock:
            logging.info(f"Clipped {input_path.name} to {output_path.name}")
        return True
    except Exception as e:
        with logging_lock:
            logging.error(f"Failed to clip {input_path.name}: {e}")
        return False

def process_file(nc_file: Path, shapefile_path: Path, buffer_km: float, output_dir: Path) -> bool:
    output_file = output_dir / nc_file.name.replace('.nc', '_clipped.nc')
    return clip_netcdf_file(nc_file, shapefile_path, buffer_km, output_file)

def main():
    print_intro()
    parser = argparse.ArgumentParser(
        description="Clip NetCDF files using a shapefile with an optional buffer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-i', '--input-dir', default='./cmip6_data', help="Directory containing input NetCDF files")
    parser.add_argument('-o', '--output-dir', default='./cmip6_data_clipped', help="Directory for clipped NetCDF files")
    parser.add_argument('--shapefile', required=True, help="Path to shapefile defining clipping region")
    parser.add_argument('--buffer-km', type=float, default=0.0, help="Buffer distance in kilometers")
    parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('-w', '--workers', type=int, default=4, help="Number of parallel workers")
    parser.add_argument('--demo', action='store_true', help="Run in demo mode with test files")
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}')
    args = parser.parse_args()

    setup_logging(args.log_dir, args.log_level, prefix="clip_")

    if args.demo:
        if args.input_dir == './cmip6_data':  # Only set default if not provided
            args.input_dir = "./demo_cmip6_data"
        if args.output_dir == './cmip6_data_clipped':  # Only set default if not provided
            args.output_dir = "./demo_cmip6_data_clipped"
        if not args.shapefile:
            args.shapefile = "./demo_region.shp"
        args.workers = 2
        args.buffer_km = 10.0
        logging.critical("Clipping CMIP6 NetCDF files in demo mode")

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    shapefile_path = Path(args.shapefile)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        with logging_lock:
            logging.error(f"Input directory {input_dir} does not exist")
        sys.exit(1)
    if not shapefile_path.exists():
        with logging_lock:
            logging.error(f"Shapefile {shapefile_path} does not exist")
        sys.exit(1)

    nc_files = list(input_dir.glob("*.nc"))
    if not nc_files:
        with logging_lock:
            logging.error(f"No NetCDF files found in {input_dir}")
        sys.exit(1)

    with logging_lock:
        logging.info(f"Found {len(nc_files)} NetCDF files to process")

    success_count = 0
    success_lock = Lock()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(process_file, nc_file, shapefile_path, args.buffer_km, output_dir)
            for nc_file in nc_files
        ]
        for future in as_completed(futures):
            if future.result():
                with success_lock:
                    success_count += 1

    with logging_lock:
        logging.critical(f"Completed: {success_count}/{len(nc_files)} files clipped successfully")
        logging.info(f"Completed: {success_count}/{len(nc_files)} files clipped successfully")

if __name__ == "__main__":
    main()