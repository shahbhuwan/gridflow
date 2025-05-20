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
import math
import netCDF4 as nc
import numpy as np
from pathlib import Path
from typing import Optional, Tuple
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

logging_lock = Lock()

def find_coordinate_vars(dataset: nc.Dataset) -> Tuple[Optional[str], Optional[str]]:
    """Find latitude and longitude variables in the NetCDF dataset."""
    lat_var = None
    lon_var = None
    debug_info = ["Available variables and attributes:"]
    for var_name in dataset.variables:
        var = dataset.variables[var_name]
        attrs = {k: str(var.getncattr(k)) for k in var.ncattrs()} if var.ncattrs() else {}
        debug_info.append(f"  {var_name}: shape={var.shape}, attrs={attrs}")
        if len(var.shape) != 1:
            continue
        if hasattr(var, 'standard_name'):
            if var.standard_name == 'latitude':
                lat_var = var_name
            elif var.standard_name == 'longitude':
                lon_var = var_name
        elif var_name.lower() in ['lat', 'latitude', 'y', 'nav_lat']:
            lat_var = var_name
        elif var_name.lower() in ['lon', 'longitude', 'x', 'nav_lon']:
            lon_var = var_name
    if not lat_var or not lon_var:
        with logging_lock:
            logging.error(f"No latitude or longitude variables found in dataset\n" + "\n".join(debug_info))
        return None, None
    with logging_lock:
        logging.debug(f"Found lat_var={lat_var}, lon_var={lon_var}")
    return lat_var, lon_var

def get_crop_indices(coord_data: np.ndarray, min_val: float, max_val: float, is_longitude: bool = False) -> Tuple[Optional[int], Optional[int]]:
    """Find indices for cropping coordinate data within given bounds."""
    if is_longitude and min_val > max_val:
        indices = np.where((coord_data >= min_val) | (coord_data <= max_val))[0]
    else:
        indices = np.where((coord_data >= min_val) & (coord_data <= max_val))[0]
    if len(indices) == 0:
        return None, None
    return indices[0], indices[-1]

def normalize_lon(lon: float, dataset_min: float, dataset_max: float) -> float:
    """Normalize input longitude to match dataset's format (0–360 or -180–180)."""
    if dataset_min >= 0 and dataset_max <= 360:
        if lon < 0:
            return lon + 360
    elif dataset_min >= -180 and dataset_max <= 180:
        if lon > 180:
            return lon - 360
    return lon

def crop_netcdf_file(input_path: Path, output_path: Path, min_lat: float, max_lat: float, min_lon: float, max_lon: float, buffer_km: float = 0.0, stop_flag: callable = None) -> bool:
    """
    Crop a single NetCDF file by spatial bounds (latitude and longitude).

    Args:
        input_path: Path to input NetCDF file.
        output_path: Path to output NetCDF file.
        min_lat: Minimum latitude bound.
        max_lat: Maximum latitude bound.
        min_lon: Minimum longitude bound.
        max_lon: Maximum longitude bound.
        buffer_km: Buffer distance in kilometers to expand bounds.
        stop_flag: Function to check if operation should stop.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        if stop_flag and stop_flag():
            with logging_lock:
                logging.info(f"Cropping stopped for {input_path.name}")
            return False

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

            # Determine longitude range
            lon_min, lon_max = lon_data.min(), lon_data.max()
            target_range = '0-360' if lon_min >= 0 and lon_max <= 360 else '-180-180'
            with logging_lock:
                logging.debug(f"NetCDF longitude range: {lon_min} to {lon_max}, using {target_range}")

            # Normalize input longitudes to match dataset range
            min_lon = normalize_lon(min_lon, lon_min, lon_max)
            max_lon = normalize_lon(max_lon, lon_min, lon_max)
            with logging_lock:
                logging.debug(f"Normalized input lon to match dataset: min_lon={min_lon}, max_lon={max_lon}")

            # Validate normalized bounds
            if target_range == '0-360' and (min_lon < 0 or max_lon > 360):
                with logging_lock:
                    logging.error(f"Invalid longitude bounds for 0-360 after normalization: min_lon={min_lon}, max_lon={max_lon}")
                return False
            if target_range == '-180-180' and (min_lon < -180 or max_lon > 180):
                with logging_lock:
                    logging.error(f"Invalid longitude bounds for -180-180 after normalization: min_lon={min_lon}, max_lon={max_lon}")
                return False
            if min_lat < -90 or max_lat > 90:
                with logging_lock:
                    logging.error(f"Latitude bounds out of range: min_lat={min_lat}, max_lat={max_lat}")
                return False

            # Apply buffer
            if buffer_km > 0:
                lat_buffer_deg = buffer_km / 111.0  # Approx. 111 km per degree of latitude
                avg_lat = (min_lat + max_lat) / 2.0
                lon_buffer_deg = buffer_km / (111.0 * math.cos(math.radians(avg_lat)))  # Adjust for longitude
                min_lat = max(-90, min_lat - lat_buffer_deg)
                max_lat = min(90, max_lat + lat_buffer_deg)
                min_lon = normalize_lon(min_lon - lon_buffer_deg, lon_min, lon_max)
                max_lon = normalize_lon(max_lon + lon_buffer_deg, lon_min, lon_max)
                with logging_lock:
                    logging.debug(f"Adjusted bounds with buffer: min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}")

            # Get cropping indices
            lat_indices = get_crop_indices(lat_data, min_lat, max_lat)
            lon_indices = get_crop_indices(lon_data, min_lon, max_lon, is_longitude=True)
            if lat_indices[0] is None or lon_indices[0] is None:
                with logging_lock:
                    logging.error(f"No data within lat/lon bounds for {input_path.name}")
                return False

            lat_start, lat_end = lat_indices
            lon_start, lon_end = lon_indices
            lat_size = lat_end - lat_start + 1
            lon_size = lon_end - lon_start + 1

            # Identify dimension names
            lat_dim = src.variables[lat_var].dimensions[0]
            lon_dim = src.variables[lon_var].dimensions[0]

            # Create output NetCDF file
            with nc.Dataset(output_path, 'w', format=src.file_format) as dst:
                dst.setncatts(src.__dict__)
                # Copy dimensions, adjusting for cropped lat/lon
                for dim in src.dimensions:
                    size = src.dimensions[dim].size
                    if dim == lat_dim:
                        size = lat_size
                    elif dim == lon_dim:
                        size = lon_size
                    dst.createDimension(dim, size if not src.dimensions[dim].isunlimited() else None)

                # Copy variables
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
                        logging.debug(f"Copying {var_name}: data shape {data.shape}, var_out shape {var_out.shape} in {input_path.name}")
                    var_out[:] = data

        with logging_lock:
            logging.info(f"Cropped {input_path.name} → {output_path.name}")
            logging.info(f"Cropped file created: {output_path}")
        return True

    except Exception as e:
        with logging_lock:
            logging.error(f"Failed to crop {input_path.name}: {e}")
        return False

def crop_netcdf(input_dir: str, output_dir: str, min_lat: float, max_lat: float, min_lon: float, max_lon: float, buffer_km: float = 0.0, stop_flag: callable = None, workers: int = None, demo: bool = False) -> bool:
    """
    Crop all NetCDF files in a directory by spatial bounds in parallel.

    Args:
        input_dir: Path to directory containing input NetCDF files.
        output_dir: Path to directory to save cropped NetCDF files.
        min_lat: Minimum latitude bound.
        max_lat: Maximum latitude bound.
        min_lon: Minimum longitude bound.
        max_lon: Maximum longitude bound.
        buffer_km: Buffer distance in kilometers to expand bounds.
        stop_flag: Function to check if operation should stop.
        workers: Number of parallel workers (defaults to number of CPU cores).
        demo: If True, use demo bounds (35N-45N, 95W-105W).

    Returns:
        bool: True if any files were successfully processed, False otherwise.
    """
    try:
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use demo bounds if specified
        if demo:
            input_dir = Path("./cmip6_data")  # Default to CMIP6 demo output
            output_dir = Path("./cmip6_cropped_data") # Default output for cropped files
            min_lat, max_lat = 35.0, 45.0  # 10-degree box centered around 40N
            min_lon, max_lon = -105.0, -95.0  # Centered around 100W
            buffer_km = 50.0  # 50 km buffer
            with logging_lock:
                logging.info(f"Demo mode: Using bounds min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}, buffer_km={buffer_km}")

        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Validate bounds
        if min_lat >= max_lat or min_lon >= max_lon:
            with logging_lock:
                logging.error(f"Invalid bounds: min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}")
            return False
        if buffer_km < 0:
            with logging_lock:
                logging.error(f"Buffer cannot be negative: buffer_km={buffer_km}")
            return False

        # Find all NetCDF files
        nc_files = list(input_dir.glob("*.nc"))
        if not nc_files:
            with logging_lock:
                logging.critical(f"No NetCDF files found in {input_dir}. Run 'gridflow download --demo' to generate sample files.")
            return False

        total_files = len(nc_files)
        with logging_lock:
            logging.info(f"Found {total_files} NetCDF files to crop")

        # Prepare tasks
        tasks = []
        for nc_file in nc_files:
            output_file = output_dir / f"{nc_file.stem}_cropped{nc_file.suffix}"
            tasks.append((nc_file, output_file))

        # Process files in parallel
        workers = workers or os.cpu_count() or 4
        completed = 0
        success_count = 0
        progress_interval = max(1, total_files // 10)
        next_threshold = progress_interval

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_task = {
                executor.submit(crop_netcdf_file, in_file, out_file, min_lat, max_lat, min_lon, max_lon, buffer_km, stop_flag): (in_file, out_file)
                for in_file, out_file in tasks
            }
            for future in as_completed(future_to_task):
                if stop_flag and stop_flag():
                    with logging_lock:
                        logging.info("Cropping operation stopped by user")
                    executor.shutdown(wait=False)  # Gracefully shut down executor
                    break

                in_file, out_file = future_to_task[future]
                result = future.result()
                completed += 1
                if result:
                    success_count += 1

                with logging_lock:
                    if completed >= next_threshold:
                        logging.info(f"Progress: {completed}/{total_files} files (Successful: {success_count})")
                        next_threshold += progress_interval

        with logging_lock:
            logging.info(f"Final Progress: {completed}/{total_files} files (Successful: {success_count})")
            logging.info(f"Completed: {success_count}/{total_files} files")
        return success_count > 0

    except Exception as e:
        with logging_lock:
            logging.error(f"Failed to crop directory {input_dir}: {e}")
        return False