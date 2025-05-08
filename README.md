GridFlow
 A Python Stuart-friendly library to download, crop, and clip CMIP6 data from ESGF nodes with deduplication and error handling.

 ## Installation
 ```bash
 pip install gridflow
 ```

 ## Usage

 ### Download CMIP6 Data
 ```bash
 gridflow --demo
 ```

 ### Crop NetCDF Files
 ```bash
 gridflow-crop --input-dir ./cmip6_data --output-dir ./cmip6_data_cropped --min-lat 35 --max-lat 70 --min-lon -10 --max-lon 40
 ```

 ### Clip NetCDF Files with Shapefile
 ```bash
 gridflow-clip --input-dir ./cmip6_data --output-dir ./cmip6_data_clipped --shapefile path/to/region.shp --buffer-km 10
 ```

 ## Requirements
 - Python >= 3.6
 - requests
 - netCDF4
 - geopandas

 ## License
 AGPL-3.0-or-later

