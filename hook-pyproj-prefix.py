# hook-pyproj-prefix.py
import os, pyproj
os.environ['PROJ_LIB'] = pyproj.datadir.get_data_dir()
