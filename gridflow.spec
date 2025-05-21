# -*- mode: python ; coding: utf-8 -*-
import os
import glob
import fiona
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, collect_submodules

block_cipher = None

# Collect vocab JSON files
vocab_src_dir = os.path.join('gui', 'vocab')
vocab_datas = [(src, 'gui/vocab') for src in glob.glob(os.path.join(vocab_src_dir, '*.json'))]

# Collect Iowa shapefile files
shapefile_dir = os.path.join('gridflow', 'iowa_border')
shapefile_datas = [(src, 'gridflow/iowa_border') for src in glob.glob(os.path.join(shapefile_dir, '*'))]

# Collect dynamic libraries and data for spatial dependencies
hiddenimports = [
    'gridflow',
    'gridflow.commands',
    'gridflow.cmip5_downloader',
    'gridflow.cmip6_downloader',
    'gridflow.prism_downloader',
    'gridflow.crop_netcdf',
    'gridflow.clip_netcdf',
    'gridflow.catalog_generator',
    'gridflow.logging_utils',
    'gui',
    'gui.main',
    'geopandas',
    'PyQt5',
    'PyQt5.QtWidgets',
    'PyQt5.QtCore',
    'PyQt5.QtGui',
    'requests',
    'netCDF4',
    'numpy',
    'numpy.core',
    'dateutil',  # Fixed typo
    'shapely',
    'shapely.vectorized',
    'shapely.prepared',
    'argparse',
    'fiona',  # Ensure fiona is included
]
hiddenimports += collect_submodules('fiona')
hiddenimports += collect_submodules('geopandas', filter=lambda name: not name.startswith('geopandas.tests'))
hiddenimports += collect_submodules('shapely', filter=lambda name: not name.startswith('shapely.tests'))
hiddenimports += collect_submodules('netCDF4')
hiddenimports += collect_submodules('PyQt5', filter=lambda name: not name.startswith('PyQt5.uic.port_v2'))

# Collect fiona data files
fiona_gdal_data = os.path.join(os.path.dirname(fiona.__file__), 'gdal_data')
fiona_datas = [(os.path.join(fiona_gdal_data, f), 'fiona/gdal_data') for f in os.listdir(fiona_gdal_data) if os.path.isfile(os.path.join(fiona_gdal_data, f))]

datas = [
    ('gridflow_logo.png', '.'),
    ('gridflow_logo.ico', '.'),
] + vocab_datas + shapefile_datas + fiona_datas
datas += collect_data_files('pyproj')
datas += collect_data_files('netCDF4')
datas += collect_data_files('shapely')

binaries = []
binaries += collect_dynamic_libs('shapely')
binaries += collect_dynamic_libs('netCDF4')
binaries += collect_dynamic_libs('geopandas')

# Add GDAL binaries for fiona
gdal_bin_dir = os.path.join(os.path.dirname(fiona.__file__), '..', 'gdal_data')
if os.path.exists(gdal_bin_dir):
    binaries += [(os.path.join(gdal_bin_dir, f), 'gdal_data') for f in os.listdir(gdal_bin_dir) if f.endswith(('.dll', '.so', '.dylib'))]

a = Analysis(
    ['gridflow_entry.py'],
    pathex=['.'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['hook-pyproj-prefix.py'],
    excludes=['matplotlib', 'sip'],  # Exclude sip and matplotlib
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='gridflow',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,  # Console for CLI, GUI mode handled by --gui flag
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='gridflow_logo.ico',
)