import pytest
import geopandas as gpd
from shapely.geometry import box
from gridflow.clip_netcdf import reproject_bounds

def test_reproject_bounds():
    gdf = gpd.GeoDataFrame(geometry=[box(202073, 4470598, 736849, 4822673)], crs="EPSG:26915")
    min_lon, min_lat, max_lon, max_lat = reproject_bounds(gdf)
    assert -96.64 < min_lon < -96.63
    assert 40.37 < min_lat < 40.38
    assert -90.15 < max_lon < -90.14
    assert 43.50 < max_lat < 43.51