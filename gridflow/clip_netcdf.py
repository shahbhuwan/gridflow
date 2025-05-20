import logging
import sys
import numpy as np
import netCDF4 as nc
import geopandas as gpd
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from shapely.vectorized import contains
from shapely.prepared import prep
from typing import Union, Optional

logging_lock = Lock()

def reproject_bounds(gdf: gpd.GeoDataFrame, target_crs: str = 'EPSG:4326') -> tuple[float, float, float, float, gpd.GeoDataFrame]:
    """Reproject shapefile to target CRS and return bounds and reprojected GeoDataFrame."""
    original_crs = gdf.crs
    gdf_reproj = gdf.to_crs(target_crs)
    bounds = gdf_reproj.total_bounds
    min_lon, min_lat, max_lon, max_lat = bounds
    with logging_lock:
        logging.debug(f"[REPROJECT] Original CRS: {original_crs}, Target CRS: {target_crs}")
        logging.debug(f"[REPROJECT] Original bounds: min_lon={gdf.total_bounds[0]:.4f}, min_lat={gdf.total_bounds[1]:.4f}, "
                      f"max_lon={gdf.total_bounds[2]:.4f}, max_lat={gdf.total_bounds[3]:.4f}")
        logging.debug(f"[REPROJECT] Reprojected bounds: min_lon={min_lon:.4f}, min_lat={min_lat:.4f}, "
                      f"max_lon={max_lon:.4f}, max_lat={max_lat:.4f}")
        if (max_lon - min_lon) > 100 or (max_lat - min_lat) > 50:
            logging.warning(f"[REPROJECT] Shapefile bounds are large: lon_span={max_lon - min_lon:.2f}, "
                            f"lat_span={max_lat - min_lat:.2f}. Verify shapefile region.")
    return min_lon, min_lat, max_lon, max_lat, gdf_reproj

def add_buffer(gdf: gpd.GeoDataFrame, buffer_km: float = 0) -> gpd.GeoDataFrame:
    """
    Return *gdf* with all geometries buffered outward by *buffer_km* (kilometres).
    Uses an equal-area CRS for uniform buffering.
    """
    if buffer_km <= 0:
        return gdf
    gdf_m = gdf.to_crs("EPSG:6933")  # Metres everywhere
    gdf_m["geometry"] = gdf_m.buffer(buffer_km * 1_000)
    return gdf_m.to_crs("EPSG:4326")

def clip_single_file(
    input_file: Path,
    prep_geom,
    output_file: Path,
    stop_flag: Optional[callable] = None
) -> bool:
    """
    Mask every 2-D (lat, lon) field in *input_file* with *prep_geom* and write
    to *output_file*. Returns True on success, False on failure.
    """
    try:
        if stop_flag and stop_flag():
            with logging_lock:
                logging.info(f"Clipping stopped before {input_file.name}")
            return False

        with nc.Dataset(str(input_file), "r") as src:
            lat = src.variables["lat"][:]  # 1-D
            lon = src.variables["lon"][:]
            if lon.max() > 180:
                lon = np.where(lon > 180, lon - 360, lon)
            lon2d, lat2d = np.meshgrid(lon, lat)
            mask2d = contains(prep_geom, lon2d, lat2d)

            with nc.Dataset(str(output_file), "w", format=src.file_format) as dst:
                for dname, dim in src.dimensions.items():
                    dst.createDimension(dname, len(dim) if not dim.isunlimited() else None)

                for vname, varin in src.variables.items():
                    fill_kw = {}
                    if "_FillValue" in varin.ncattrs():
                        fill_kw["fill_value"] = varin.getncattr("_FillValue")

                    out = dst.createVariable(
                        vname, varin.datatype, varin.dimensions,
                        zlib=True, complevel=5, **fill_kw
                    )
                    out.setncatts({k: varin.getncattr(k)
                                  for k in varin.ncattrs() if k != "_FillValue"})

                    data = varin[:]
                    if ("lat" in varin.dimensions) and ("lon" in varin.dimensions):
                        fill_val = fill_kw.get("fill_value", np.nan)
                        data = np.where(mask2d, data, fill_val)
                    out[:] = data

                dst.setncatts({k: src.getncattr(k) for k in src.ncattrs()})

        with logging_lock:
            logging.info(f"Clipped file created: {output_file}")
        return True

    except Exception as e:
        with logging_lock:
            logging.error(f"Failed to clip {input_file.name}: {e}")
        return False

def clip_netcdf(
    input_dir: str,
    shapefile_path: str,
    output_dir: str,
    stop_flag: Optional[callable] = None,
    workers: Optional[int] = None,
    buffer_km: float = 0,
    demo: bool = False
) -> bool:
    """
    Clip NetCDF files in *input_dir* using *shapefile_path* and save to *output_dir*.
    Returns True on success, False if stopped or failed.
    """
    try:
        if stop_flag and stop_flag():
            with logging_lock:
                logging.info("Clipping operation stopped before starting")
            return False

        # Resolve default shapefile path
        default_shapefile = Path("./gridflow/iowa_border/iowa_border.shp")
        if getattr(sys, 'frozen', False):
            base_path = Path(sys._MEIPASS)
            default_shapefile = base_path / "iowa_border" / "iowa_border.shp"

        # Use provided shapefile_path, fall back to default in demo mode
        shapefile_path = Path(shapefile_path or default_shapefile)
        if demo:
            shapefile_path = default_shapefile
            with logging_lock:
                logging.info(f"Demo mode: Using shapefile {shapefile_path}")

        # Verify shapefile exists
        if not shapefile_path.exists():
            if demo:
                with logging_lock:
                    logging.critical(f"No shapefile found at {shapefile_path}. Ensure the shapefile exists.")
                return False
            if getattr(sys, 'frozen', False):
                alt_path = base_path / shapefile_path
                if alt_path.exists():
                    shapefile_path = alt_path
                else:
                    with logging_lock:
                        logging.error(f"Shapefile does not exist: {shapefile_path}")
                    raise FileNotFoundError(f"Shapefile does not exist: {shapefile_path}")
            else:
                with logging_lock:
                    logging.error(f"Shapefile does not exist: {shapefile_path}")
                raise FileNotFoundError(f"Shapefile does not exist: {shapefile_path}")

        # Resolve input and output directories
        input_dir = Path(input_dir or "./cmip6_data")
        output_dir = Path(output_dir or "./cmip6_clipped_data")
        output_dir.mkdir(parents=True, exist_ok=True)

        if demo:
            with logging_lock:
                logging.info(f"Demo mode: Using input_dir={input_dir}, output_dir={output_dir}, "
                             f"shapefile_path={shapefile_path}")

        # Read, buffer, and prepare geometry
        gdf_raw = gpd.read_file(shapefile_path)
        gdf_buf = add_buffer(gdf_raw, buffer_km=buffer_km)
        gdf_buf = gdf_buf.to_crs("EPSG:4326")
        prep_geom = prep(gdf_buf.unary_union)

        # Gather NetCDF files
        nc_files = sorted(input_dir.glob("*.nc"))
        if not nc_files:
            with logging_lock:
                logging.critical(f"No NetCDF files found in {input_dir}. Run 'gridflow download --demo' to generate sample files.")
            return False

        out_paths = [output_dir / f"{p.stem}_clipped{p.suffix}" for p in nc_files]

        # Threaded clipping
        workers = workers or (os.cpu_count() or 4)
        progress_int = max(1, len(nc_files) // 10)
        next_mark = progress_int
        completed = success = 0

        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_to_nc = {
                ex.submit(
                    clip_single_file,
                    nc_path,
                    prep_geom,
                    out_path,
                    stop_flag
                ): (nc_path, out_path)
                for nc_path, out_path in zip(nc_files, out_paths)
            }

            for fut in as_completed(future_to_nc):
                if stop_flag and stop_flag():
                    with logging_lock:
                        logging.info("Clipping operation stopped by user")
                    ex._threads.clear()  # Clear threads to allow shutdown
                    ex.shutdown(wait=False, cancel_futures=True)
                    return False

                nc_path, _ = future_to_nc[fut]
                if fut.result():
                    success += 1
                else:
                    with logging_lock:
                        logging.error(f"Clipping failed for {nc_path.name}")
                    raise RuntimeError(f"Clipping failed for {nc_path.name}")

                completed += 1
                if completed >= next_mark:
                    with logging_lock:
                        logging.info(f"Progress: {completed}/{len(nc_files)} files "
                                     f"(Successful: {success})")
                    next_mark += progress_int

        with logging_lock:
            logging.info(f"Completed: {success}/{len(nc_files)} files")
        if success == 0:
            with logging_lock:
                logging.error("No files were clipped successfully")
            raise RuntimeError("No files were clipped successfully")
        return True

    except Exception as e:
        with logging_lock:
            logging.error(f"Failed to clip directory {input_dir}: {e}")
        raise

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#     try:
#         clip_netcdf(
#             input_dir="D:/GUI-Test/cmip6",
#             shapefile_path="D:/GUI-Test/conus.shp",
#             output_dir="D:/GUI-Test/cmip6_clipped",
#             buffer_km=100
#         )
#     except Exception as e:
#         logging.error(f"Script failed: {e}")