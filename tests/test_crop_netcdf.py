import logging
import pytest
import netCDF4 as nc
import numpy as np
from unittest.mock import patch, MagicMock
from pathlib import Path
from gridflow.crop_netcdf import setup_logging, find_coordinate_vars, get_crop_indices, normalize_longitude, crop_netcdf_file, main
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
        lon[:] = np.linspace(-180, 180, 20)
        tas[:] = np.random.rand(1, 10, 20)
    return nc_path

@pytest.fixture
def sample_netcdf_0_360(tmp_path):
    nc_path = tmp_path / "test_0_360.nc"
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
        lon[:] = np.linspace(-180, 180, 20)
    with nc.Dataset(sample_netcdf, "r") as ds:
        lat_var, lon_var = find_coordinate_vars(ds)
        assert lat_var is None
        assert lon_var is None

def test_get_crop_indices():
    data = np.array([0, 10, 20, 30, 40])
    start, end = get_crop_indices(data, 10, 30)
    assert start == 1
    assert end == 3

def test_get_crop_indices_antimeridian():
    data = np.array([350, 0, 10, 20, 30])
    start, end = get_crop_indices(data, 350, 10, is_longitude=True)
    assert start == 0
    assert end == 2

def test_get_crop_indices_no_data():
    data = np.array([0, 10, 20])
    start, end = get_crop_indices(data, 50, 60)
    assert start is None
    assert end is None

def test_normalize_longitude():
    assert normalize_longitude(370, '0-360') == pytest.approx(10)
    assert normalize_longitude(370, '-180-180') == pytest.approx(10)
    assert normalize_longitude(-10, '-180-180') == pytest.approx(-10)
    assert normalize_longitude(-10, '0-360') == pytest.approx(350)
    assert normalize_longitude(360, '0-360') == pytest.approx(0)
    assert normalize_longitude(180, '-180-180') == pytest.approx(180)

def test_crop_netcdf_file(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=-20, max_lon=20, buffer_km=0)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert ds.variables["lat"].shape[0] <= 10
        assert ds.variables["lon"].shape[0] <= 20
        assert ds.variables["tas"].getncattr('_FillValue') == -9999

def test_crop_netcdf_file_0_360(sample_netcdf_0_360, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf_0_360, output_path, min_lat=-10, max_lat=10, min_lon=340, max_lon=20, buffer_km=0)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert ds.variables["lat"].shape[0] <= 10
        assert ds.variables["lon"].shape[0] <= 20

def test_crop_netcdf_file_with_buffer(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=-20, max_lon=20, buffer_km=111)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert ds.variables["lat"].shape[0] <= 10
        assert ds.variables["lon"].shape[0] <= 20

def test_crop_netcdf_file_no_data(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=100, max_lat=110, min_lon=0, max_lon=10, buffer_km=0)
    assert result is False
    assert not output_path.exists()

def test_crop_netcdf_file_invalid_lat_bounds(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-100, max_lat=100, min_lon=-20, max_lon=20, buffer_km=0)
    assert result is False
    assert not output_path.exists()

def test_crop_netcdf_file_invalid_lon_bounds_0_360(sample_netcdf_0_360, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf_0_360, output_path, min_lat=-10, max_lat=10, min_lon=-20, max_lon=380, buffer_km=0)
    assert result is False
    assert not output_path.exists()

def test_crop_netcdf_file_invalid_lon_bounds_180(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=-200, max_lon=200, buffer_km=0)
    assert result is False
    assert not output_path.exists()

def test_crop_netcdf_file_edge_lon_bounds(sample_netcdf_0_360, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf_0_360, output_path, min_lat=-10, max_lat=10, min_lon=0, max_lon=360, buffer_km=0)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables

def test_crop_netcdf_file_antimeridian_180(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=170, max_lon=-170, buffer_km=0)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert "lat" in ds.variables
        assert "lon" in ds.variables
        assert ds.variables["lat"].shape[0] <= 10
        assert ds.variables["lon"].shape[0] <= 20

def test_crop_netcdf_file_exception(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    with patch("netCDF4.Dataset", side_effect=Exception("NetCDF error")):
        result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=-20, max_lon=20, buffer_km=0)
        assert result is False
        assert not output_path.exists()

def test_crop_netcdf_file_debug_logging(sample_netcdf, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-10, max_lat=10, min_lon=-20, max_lon=20, buffer_km=0)
    assert result is True
    assert "Copying tas" in caplog.text

def test_main_invalid_args(tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_cropped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()

    with patch("sys.argv", [
        "crop_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--log-dir", str(log_dir),
        "--min-lat", "10",
        "--max-lat", "-10",
        "--min-lon", "-20",
        "--max-lon", "20",
        "--workers", "2"
    ]), patch("sys.exit") as mock_exit:
        main()
        mock_exit.assert_called_with(1)

def test_main_invalid_lon_range(tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_cropped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()

    with patch("sys.argv", [
        "crop_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--log-dir", str(log_dir),
        "--min-lat", "-10",
        "--max-lat", "10",
        "--min-lon", "20",
        "--max-lon", "-20",
        "--workers", "2"
    ]), patch("sys.exit") as mock_exit:
        main()
        mock_exit.assert_called_with(1)

def test_main_no_files(tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_cropped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()

    with patch("sys.argv", [
        "crop_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--log-dir", str(log_dir),
        "--min-lat", "-10",
        "--max-lat", "10",
        "--min-lon", "-20",
        "--max-lon", "20",
        "--workers", "2"
    ]), patch("sys.exit") as mock_exit:
        main()
        mock_exit.assert_called_with(1)

def test_main_parallel(sample_netcdf, tmp_path):
    input_dir = tmp_path / "cmip6_data"
    output_dir = tmp_path / "cmip6_data_cropped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()
    nc_file = input_dir / "test.nc"
    nc_file.symlink_to(sample_netcdf)

    with patch("sys.argv", [
        "crop_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--log-dir", str(log_dir),
        "--min-lat", "-10",
        "--max-lat", "10",
        "--min-lon", "-20",
        "--max-lon", "20",
        "--workers", "8"
    ]), patch("gridflow.crop_netcdf.crop_netcdf_file", return_value=True) as mock_crop, \
         patch("gridflow.crop_netcdf.ThreadPoolExecutor") as mock_executor, \
         patch("gridflow.crop_netcdf.as_completed") as mock_as_completed:
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
        assert mock_executor.call_args == ((), {'max_workers': 8})
        mock_crop.assert_called_with(
            nc_file, output_dir / "test_cropped.nc", -10.0, 10.0, -20.0, 20.0, 0.0
        )

def test_main_demo_mode(sample_netcdf, tmp_path):
    input_dir = tmp_path / "demo_cmip6_data"
    output_dir = tmp_path / "demo_cmip6_data_cropped"
    log_dir = tmp_path / "logs"
    input_dir.mkdir()
    output_dir.mkdir()
    log_dir.mkdir()
    nc_file = input_dir / "test.nc"
    nc_file.symlink_to(sample_netcdf)

    with patch("sys.argv", [
        "crop_netcdf.py",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--log-dir", str(log_dir),
        "--min-lat", "35.0",
        "--max-lat", "70.0",
        "--min-lon", "-10.0",
        "--max-lon", "40.0",
        "--demo"
    ]), patch("gridflow.crop_netcdf.crop_netcdf_file", return_value=True) as mock_crop, \
         patch("gridflow.crop_netcdf.ThreadPoolExecutor") as mock_executor, \
         patch("gridflow.crop_netcdf.as_completed") as mock_as_completed:
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
        mock_crop.assert_called_with(
            nc_file, output_dir / "test_cropped.nc", 35.0, 70.0, -10.0, 40.0, 10.0
        )

# Updated tests to cover additional functionality
def test_get_crop_indices_invalid_bounds(sample_netcdf):
    with nc.Dataset(sample_netcdf, "r") as ds:
        lat_var = ds.variables["lat"]
        lon_var = ds.variables["lon"]
        start, end = get_crop_indices(lat_var[:], 100, -100)
        assert start is None and end is None, "Expected None indices for invalid latitude bounds"
        start, end = get_crop_indices(lon_var[:], 170, -170, is_longitude=True)
        assert start is not None and end is not None, "Expected valid indices for longitude bounds"

def test_crop_netcdf_file_edge_bounds(sample_netcdf, tmp_path):
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(sample_netcdf, output_path, min_lat=-45, max_lat=45, min_lon=-180, max_lon=-170, buffer_km=0)
    assert result is True
    assert output_path.exists()
    with nc.Dataset(output_path, "r") as ds:
        assert ds.dimensions["lat"].size <= 10
        assert ds.dimensions["lon"].size <= 20

def test_crop_netcdf_file_no_data_variable(tmp_path):
    invalid_file = tmp_path / "invalid.nc"
    with nc.Dataset(invalid_file, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon = ds.createVariable("lon", "f4", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.linspace(-45, 45, 10)
        lon[:] = np.linspace(-180, 180, 20)
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(invalid_file, output_path, min_lat=0, max_lat=10, min_lon=0, max_lon=10, buffer_km=0)
    assert result is True
    assert output_path.exists()

def test_crop_netcdf_file_missing_coordinates(sample_netcdf, tmp_path):
    invalid_file = tmp_path / "invalid.nc"
    with nc.Dataset(invalid_file, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        tas = ds.createVariable("tas", "f4", ("lat", "lon"), fill_value=-9999)
        tas[:] = np.random.rand(10, 20)
    output_path = tmp_path / "output.nc"
    result = crop_netcdf_file(invalid_file, output_path, min_lat=0, max_lat=10, min_lon=0, max_lon=10, buffer_km=0)
    assert result is False
    assert not output_path.exists()