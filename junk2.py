import geopandas as gpd
from shapely.geometry import box

# Create the test GeoDataFrame
gdf = gpd.GeoDataFrame(geometry=[box(202073, 4470598, 736849, 4822673)], crs="EPSG:26915")

# Reproject to EPSG:4326 (WGS84)
gdf_wgs84 = gdf.to_crs("EPSG:4326")
min_lon, min_lat, max_lon, max_lat = gdf_wgs84.total_bounds

print(f"min_lon: {min_lon}")
print(f"min_lat: {min_lat}")
print(f"max_lon: {max_lon}")
print(f"max_lat: {max_lat}")