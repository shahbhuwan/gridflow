import pytest
import numpy as np
from pathlib import Path
from gridflow.crop_netcdf import find_coordinate_vars, normalize_longitude

def test_find_coordinate_vars(tmp_path):
    import netCDF4
    nc_path = tmp_path / "test.nc"
    with netCDF4.Dataset(nc_path, "w") as ds:
        ds.createDimension("lat", 10)
        ds.createDimension("lon", 20)
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon = ds.createVariable("lon", "f4", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.linspace(-90, 90, 10)
        lon[:] = np.linspace(0, 360, 20)
    with netCDF4.Dataset(nc_path, "r") as ds:
        lat_var, lon_var = find_coordinate_vars(ds)
        assert lat_var == "lat"
        assert lon_var == "lon"

def test_normalize_longitude():
    assert normalize_longitude(-90, "0-360") == 270
    assert normalize_longitude(270, "-180-180") == -90
    assert normalize_longitude(360, "0-360") == 0
    assert normalize_longitude(-180, "-180-180") == -180