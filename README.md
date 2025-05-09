GridFlow
A Python library to download, crop, clip, and generate databases for CMIP6 data from ESGF nodes with deduplication and error handling.
Installation
pip install gridflow

Usage
Download CMIP6 Data
gridflow --demo

Crop NetCDF Files
gridflow-crop --input-dir ./cmip6_data --output-dir ./cmip6_data_cropped --min-lat 35 --max-lat 70 --min-lon -10 --max-lon 40

Clip NetCDF Files with Shapefile
gridflow-clip --input-dir ./cmip6_data --output-dir ./cmip6_data_clipped --shapefile path/to/region.shp --buffer-km 10

Generate Database of NetCDF Files
gridflow-db --input-dir ./cmip6_data --output-file database.json

Requirements

Python >= 3.6
requests
netCDF4
geopandas

License
AGPL-3.0-or-later
