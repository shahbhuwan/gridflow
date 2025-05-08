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
from shapely.geometry import box
from pathlib import Path
from datetime import datetime

__version__ = "0.2.3"

def setup_logging(log_dir: str, level: str, prefix: str = "") -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{prefix}clip_netcdf_{timestamp}.log"

    log_levels = {
        'minimal': logging.INFO,
        'normal': logging.INFO,
        'verbose': logging.DEBUG,
        'debug': logging.DEBUG
    }
    numeric_level = log_levels.get(level.lower(), logging.INFO)

    class MinimalFilter(logging.Filter):
        def filter(self, record):
            return record.levelno >= logging.INFO if level.lower() == 'minimal' else True

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

def find_coordinate_vars(dataset: nc.Dataset):
    lat_var = lon_var = None
    for var_name, var in dataset.variables.items():
        if 'standard_name' in var.ncattrs():
            if var.getncattr('standard_name') == 'latitude' and len(var.dimensions) == 1:
                lat_var = var_name
            elif var.getncattr('standard_name') == 'longitude' and len(var.dimensions) == 1:
                lon_var = var_name
    if lat_var is None or lon_var is None:
        logging.error("Could not find suitable 1D latitude or longitude variables.")
    return lat_var, lon_var

def reproject_bounds(gdf, target_crs='EPSG:4326'):
    """Reproject shapefile bounds to target CRS (WGS84)."""
    bounds = gdf.to_crs(target_crs).total_bounds
    min_lon, min_lat, max_lon, max_lat = bounds
    return min_lon, min_lat, max_lon, max_lat

def clip_netcdf_file(input_path: Path, shapefile_path: Path, buffer_km: float, output_path: Path) -> bool:
    try:
        # Read shapefile and reproject bounds to WGS84
        gdf = gpd.read_file(shapefile_path)
        min_lon, min_lat, max_lon, max_lat = reproject_bounds(gdf)

        # Log original and reprojected shapefile bounds
        logging.debug(f"Original shapefile CRS: {gdf.crs}")
        logging.debug(f"Original shapefile bounds: min_lon={gdf.total_bounds[0]}, min_lat={gdf.total_bounds[1]}, max_lon={gdf.total_bounds[2]}, max_lat={gdf.total_bounds[3]}")
        logging.debug(f"Reprojected shapefile bounds: min_lon={min_lon}, min_lat={min_lat}, max_lon={max_lon}, max_lat={max_lat}")

        # Convert longitudes to 0-360° if necessary
        if min_lon < 0:
            min_lon = (min_lon + 360) % 360
            max_lon = (max_lon + 360) % 360
        if min_lon > max_lon:
            min_lon, max_lon = max_lon, min_lon

        # Apply buffer in degrees
        if buffer_km > 0:
            avg_lat = (min_lat + max_lat) / 2.0
            lat_buffer_deg = buffer_km / 111.0  # 1 degree ≈ 111 km
            lon_buffer_deg = buffer_km / (111.0 * math.cos(math.radians(avg_lat)))
            min_lat -= lat_buffer_deg
            max_lat += lat_buffer_deg
            min_lon = (min_lon - lon_buffer_deg) % 360
            max_lon = (max_lon + lon_buffer_deg) % 360
            logging.debug(f"Applied buffer of {buffer_km} km: min_lat={min_lat:.4f}, max_lat={max_lat:.4f}, min_lon={min_lon:.4f}, max_lon={max_lon:.4f}")

        with nc.Dataset(input_path, 'r') as src:
            lat_var, lon_var = find_coordinate_vars(src)
            if not lat_var or not lon_var:
                logging.error(f"No lat/lon variables found in {input_path.name}")
                return False

            lat_data = src.variables[lat_var][:]
            lon_data = src.variables[lon_var][:]
            if len(lat_data.shape) != 1 or len(lon_data.shape) != 1:
                logging.error(f"Latitude or longitude is not 1D in {input_path.name}")
                return False

            logging.debug(f"lat_var: {lat_var}, lat_data shape: {lat_data.shape}, min: {lat_data.min()}, max: {lat_data.max()}")
            logging.debug(f"lon_var: {lon_var}, lon_data shape: {lon_data.shape}, min: {lon_data.min()}, max: {lon_data.max()}")

            # Find indices within bounds, handling antimeridian if needed
            lat_indices = np.where((lat_data >= min_lat) & (lat_data <= max_lat))[0]
            if min_lon > max_lon:  # Antimeridian crossing
                lon_indices = np.where((lon_data >= min_lon) | (lon_data <= max_lon))[0]
            else:
                lon_indices = np.where((lon_data >= min_lon) & (lon_data <= max_lon))[0]

            if len(lat_indices) == 0 or len(lon_indices) == 0:
                logging.error(f"No data within shapefile bounds for {input_path.name}")
                return False

            lat_start, lat_end = lat_indices[0], lat_indices[-1]
            lon_start, lon_end = lon_indices[0], lon_indices[-1]
            lat_size = lat_end - lat_start + 1
            lon_size = lon_end - lon_start + 1
            logging.debug(f"lat_start: {lat_start}, lat_end: {lat_end}, cropped size: {lat_size}")
            logging.debug(f"lon_start: {lon_start}, lon_end: {lon_end}, cropped size: {lon_size}")

            lat_dim = src.variables[lat_var].dimensions[0]
            lon_dim = src.variables[lon_var].dimensions[0]
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
                    logging.debug(f"Copying {var_name}: data shape {data.shape}, var_out shape {var_out.shape}")
                    var_out[:] = data

        logging.info(f"Clipped {input_path.name} to {output_path.name}")
        return True
    except Exception as e:
        logging.error(f"Failed to clip {input_path.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Clip NetCDF files using a shapefile with an optional buffer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--input-dir', default='./cmip6_data', help="Directory containing input NetCDF files")
    parser.add_argument('--output-dir', default='./cmip6_data_clipped', help="Directory for clipped NetCDF files")
    parser.add_argument('--shapefile', required=True, help="Path to shapefile defining clipping region")
    parser.add_argument('--buffer-km', type=float, default=0.0, help="Buffer distance in kilometers to extend the clipping bounds")
    parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('-v', '--version', action='version', version=f'clip_netcdf {__version__}')
    args = parser.parse_args()

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

    logging.info(f"Found {len(nc_files)} NetCDF files to process")
    success_count = 0
    for nc_file in nc_files:
        output_file = output_dir / nc_file.name.replace('.nc', '_clipped.nc')
        if clip_netcdf_file(nc_file, shapefile_path, args.buffer_km, output_file):
            success_count += 1

    logging.info(f"Completed: {success_count}/{len(nc_files)} files clipped successfully")

if __name__ == "__main__":
    main()