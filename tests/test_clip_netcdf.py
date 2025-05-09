import logging
import pytest
import netCDF4 as nc
import geopandas as gpd
import numpy as np
from unittest.mock import patch, MagicMock
from pathlib import Path
from gridflow.clip_netcdf import setup_logging, find_coordinate_vars, reproject_bounds, clip_netcdf_file, main
import concurrent.futures as cf
import warnings

# Suppress geopandas shapely.geos deprecation warning
warnings.filterwarnings("ignore", category=DeprecationWarning, module="geopandas._compat")

# Fixture to reset logging before each test
@pytest.fixture(autouse=True)
def reset_logging():
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(logging.NOTSET)

@pytest.fixture
def sample_netcdf(tmp_path):
    nc_path = tmp_path / "test.nc"
    with nc.Dataset(nc_path, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        ds.createDimension("time", 1)
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon = ds.createVariable("lon", "f4", ("lon",))
        tas = ds.createVariable("tas", "f4", ("time", "lat", "lon"), fill_value=-9999)
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.linspace(-45, 45, 10)
        lon[:] = np.linspace(0, 360, 20)
        tas[:] = np.random.rand(1, 10, 20)
    return nc_path

@pytest.fixture
def sample_shapefile(tmp_path):
    shp_path = tmp_path / "shape.shp"
    gdf = gpd.GeoDataFrame({"geometry": [gpd.points_from_xy([10], [10])[0]]}, crs="EPSG:4326")
    gdf.to_file(shp_path)
    return shp_path

def test_setup_logging(tmp_path):
    log_dir = tmp_path / "logs"
    logger = logging.getLogger()
    original_handlers = logger.handlers[:]
    
    with patch("logging.FileHandler") as mock_file_handler, patch("logging.StreamHandler") as mock_stream_handler:
        setup_logging(str(log_dir), "minimal", "test_")
        mock_file_handler.assert_called()
        mock_stream_handler.assert_called()
        assert logger.level == logging.CRITICAL
    
    logger.handlers = original_handlers

    with patch("logging.FileHandler"), patch("logging.StreamHandler"):
        setup_logging(str(log_dir), "debug", "test_")
        assert logger.level == logging.DEBUG
    
    logger.handlers = original_handlers

def test_find_coordinate_vars(sample_netcdf):
    with nc.Dataset(sample_netcdf, "r") as ds:
        lat_var, lon_var = find_coordinate_vars(ds)
        assert lat_var == "lat"
        assert lon_var == "lon"

def test_find_coordinate_vars_no_standard_name(sample_netcdf):
    with nc.Dataset(sample_netcdf, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon = ds.createVariable("lon", "f4", ("lon",))
        lat[:] = np.linspace(-45, 45, 10)
        lon[:] = np.linspace(0, 360, 20)
    with nc.Dataset(sample_netcdf, "r") as ds:
        lat_var, lon_var = find_coordinate_vars(ds)
        assert lat_var is None
        assert lon_var is None

def test_reproject_bounds(sample_shapefile):
    gdf = gpd.read_file(sample_shapefile)
    min_lon, min_lat, max_lon, max_lat = reproject_bounds(gdf, target_crs="EPSG:4326")
    assert min_lon == pytest.approx(10)
    assert min_lat == pytest.approx(10)
    assert max_lon == pytest.approx(10)
    assert max_lat == pytest.approx(10)

def test_clip_netcdf_file(sample_netcdf, sample_shapefile, tmp_path):
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(sample_netcdf, sample_shapefile, buffer_km=1000, output_path=output_path)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert ds.variables["lat"].shape[0] <= 10
        assert ds.variables["lon"].shape[0] <= 20
        assert ds.variables["tas"].getncattr('_FillValue') == -9999

def test_clip_netcdf_file_no_data(sample_netcdf, sample_shapefile, tmp_path):
    output_path = tmp_path / "output.nc"
    with patch("gridflow.clip_netcdf.reproject_bounds", return_value=(400, 400, 410, 410)):
        result = clip_netcdf_file(sample_netcdf, sample_shapefile, buffer_km=0, output_path=output_path)
        assert result is False
        assert not output_path.exists()

def test_clip_netcdf_file_no_fill_value(sample_netcdf, sample_shapefile, tmp_path):
    nc_path = tmp_path / "test_no_fill.nc"
    with nc.Dataset(nc_path, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        ds.createDimension("time", 1)
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon = ds.createVariable("lon", "f4", ("lon",))
        tas = ds.createVariable("tas", "f4", ("time", "lat", "lon"))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.linspace(-45, 45, 10)
        lon[:] = np.linspace(0, 360, 20)
        tas[:] = np.random.rand(1, 10, 20)
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(nc_path, sample_shapefile, buffer_km=1000, output_path=output_path)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert not hasattr(ds.variables["tas"], '_FillValue')

def test_main_no_files(tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_clipped"
    log_dir = tmp_path / "logs"
    shapefile = tmp_path / "shape.shp"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()
    shapefile.touch()

    with patch("sys.argv", [
        "clip_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--shapefile", str(shapefile),
        "--log-dir", str(log_dir),
        "--workers", "2"
    ]), patch("sys.exit") as mock_exit:
        main()
        mock_exit.assert_called_with(1)

def test_main_invalid_shapefile(tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_clipped"
    log_dir = tmp_path / "logs"
    shapefile = tmp_path / "nonexistent.shp"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()

    with patch("sys.argv", [
        "clip_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--shapefile", str(shapefile),
        "--log-dir", str(log_dir),
        "--workers", "2"
    ]), patch("sys.exit") as mock_exit:
        main()
        mock_exit.assert_called_with(1)

def test_main_parallel(sample_netcdf, sample_shapefile, tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_clipped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()
    nc_file = input_dir / "test.nc"
    nc_file.symlink_to(sample_netcdf)

    with patch("sys.argv", [
        "clip_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--shapefile", str(sample_shapefile),
        "--log-dir", str(log_dir),
        "--workers", "2"
    ]), patch("gridflow.clip_netcdf.clip_netcdf_file", return_value=True) as mock_clip, \
         patch("gridflow.clip_netcdf.ThreadPoolExecutor") as mock_executor, \
         patch("gridflow.clip_netcdf.as_completed") as mock_as_completed:
        executor_mock = MagicMock()
        def submit_side_effect(*args, **kwargs):
            func = args[0]
            func_args = args[1:]
            result = func(*func_args)
            future = cf.Future()
            future.set_result(result)
            return future
        executor_mock.submit.side_effect = submit_side_effect
        mock_executor.return_value.__enter__.return_value = executor_mock
        mock_as_completed.return_value = [MagicMock(result=lambda: True)]
        main()
        assert mock_executor.called
        assert mock_executor.call_args == ((), {'max_workers': 2})
        mock_clip.assert_called_with(
            nc_file, sample_shapefile, 0.0, output_dir / "test_clipped.nc"
        )

def test_main_demo_mode(sample_netcdf, sample_shapefile, tmp_path):
    input_dir = tmp_path / "demo_cmip6_data"
    output_dir = tmp_path / "demo_cmip6_data_clipped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()
    nc_file = input_dir / "test.nc"
    nc_file.symlink_to(sample_netcdf)

    with patch("sys.argv", [
        "clip_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--shapefile", str(sample_shapefile),
        "--log-dir", str(log_dir),
        "--demo"
    ]), patch("gridflow.clip_netcdf.clip_netcdf_file", return_value=True) as mock_clip, \
         patch("gridflow.clip_netcdf.ThreadPoolExecutor") as mock_executor, \
         patch("gridflow.clip_netcdf.as_completed") as mock_as_completed:
        executor_mock = MagicMock()
        def submit_side_effect(*args, **kwargs):
            func = args[0]
            func_args = args[1:]
            result = func(*func_args)
            future = cf.Future()
            future.set_result(result)
            return future
        executor_mock.submit.side_effect = submit_side_effect
        mock_executor.return_value.__enter__.return_value = executor_mock
        mock_as_completed.return_value = [MagicMock(result=lambda: True)]
        main()
        assert mock_executor.called
        assert mock_executor.call_args == ((), {'max_workers': 2})
        mock_clip.assert_called_with(
            nc_file, sample_shapefile, 10.0, output_dir / "test_clipped.nc"
        )

def test_clip_netcdf_file_invalid_shapefile(sample_netcdf, tmp_path):
    """Test clip_netcdf_file with invalid shapefile (covers lines 102–104)."""
    invalid_shp = tmp_path / "invalid.shp"
    invalid_shp.write_bytes(b"not a shapefile")
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(sample_netcdf, invalid_shp, buffer_km=0, output_path=output_path)
    assert result is False, "Expected False for invalid shapefile"
    assert not output_path.exists(), "Output file should not be created"

def test_clip_netcdf_file_missing_coordinates(sample_netcdf, sample_shapefile, tmp_path):
    """Test clip_netcdf_file with missing coordinates (covers lines 82–83, 109–111)."""
    invalid_file = tmp_path / "invalid.nc"
    with nc.Dataset(invalid_file, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        tas = ds.createVariable("tas", "f4", ("lat", "lon"), fill_value=-9999)
        tas[:] = np.random.rand(10, 20)
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(invalid_file, sample_shapefile, buffer_km=0, output_path=output_path)
    assert result is False, "Expected False for missing coordinates"
    assert not output_path.exists(), "Output file should not be created"

def test_clip_netcdf_file_edge_bounds(sample_netcdf, sample_shapefile, tmp_path):
    """Test clip_netcdf_file with edge bounds (covers lines 181–184, 226–228)."""
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(sample_netcdf, sample_shapefile, buffer_km=10000, output_path=output_path)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert ds.dimensions["lat"].size == 10
        assert ds.dimensions["lon"].size == 20

def test_clip_netcdf_file_no_intersection(sample_netcdf, tmp_path):
    """Test clip_netcdf_file with shapefile outside data bounds (covers lines 211, 213, 215)."""
    shp_path = tmp_path / "outside.shp"
    gdf = gpd.GeoDataFrame({"geometry": [gpd.points_from_xy([400], [400])[0]]}, crs="EPSG:4326")
    gdf.to_file(shp_path)
    output_path = tmp_path / "output.nc"
    result = clip_netcdf_file(sample_netcdf, shp_path, buffer_km=0, output_path=output_path)
    assert result is False
    assert not output_path.exists()