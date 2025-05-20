# -*- mode: python ; coding: utf-8 -*-
import os
import glob
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
    'requests',
    'netCDF4',
    'numpy',
    'python_dateutil',
    'shapely',
    'shapely.vectorized',
    'shapely.prepared',
]
hiddenimports += collect_submodules('fiona')
hiddenimports += collect_submodules('geopandas')
hiddenimports += collect_submodules('shapely')
hiddenimports += collect_submodules('netCDF4')

datas = [
    ('gridflow_logo.png', '.'),
    ('gridflow_logo.ico', '.'),
] + vocab_datas + shapefile_datas
datas += collect_data_files('pyproj')
datas += collect_data_files('fiona')

binaries = []
binaries += collect_dynamic_libs('fiona')
binaries += collect_dynamic_libs('shapely')
binaries += collect_dynamic_libs('netCDF4')

a = Analysis(
    ['gridflow/__main__.py'],
    pathex=['.'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['hook-pyproj-prefix.py'],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='gridflow-cli',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='gridflow-cli',
)