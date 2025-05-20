# -*- mode: python ; coding: utf-8 -*-
import os
import glob
import pyproj
from PyInstaller.utils.hooks import (
    collect_data_files,
    collect_dynamic_libs,
    collect_submodules,
)

block_cipher = None

# Dynamically get PROJ data directory
proj_data_dir = pyproj.datadir.get_data_dir()

# Collect vocab JSON files
vocab_src_dir = os.path.join('gui', 'vocab')
vocab_datas = [(src, 'gui/vocab') for src in glob.glob(os.path.join(vocab_src_dir, '*.json'))]

# Collect Iowa shapefile files
shapefile_dir = os.path.join('gridflow', 'shapefiles', 'iowa_border')
shapefile_datas = [(src, 'gridflow/shapefiles/iowa_border') for src in glob.glob(os.path.join(shapefile_dir, '*'))]

# Collect dynamic libraries and data for spatial dependencies
hiddenimports = [
    'gridflow.clip_netcdf',  # Explicitly include clip_netcdf module
    'gridflow.commands',     # Ensure commands module is included
]
hiddenimports += collect_submodules('fiona')
hiddenimports += collect_submodules('pyogrio')
hiddenimports += collect_submodules('geopandas')
hiddenimports += collect_submodules('shapely')
hiddenimports += collect_submodules('netCDF4')

datas = []
datas += vocab_datas
datas += shapefile_datas
datas += collect_data_files('pyproj')
datas += collect_data_files('fiona')
datas += collect_data_files('pyogrio')

# Collect dynamic libs for Fiona, PyOgrio, Shapely, NetCDF4
binaries = []
binaries += collect_dynamic_libs('fiona')
binaries += collect_dynamic_libs('pyogrio')
binaries += collect_dynamic_libs('shapely')
binaries += collect_dynamic_libs('netCDF4')

# Build the Analysis
from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT

a = Analysis(
    ['run_gui.py'],
    pathex=['D:\\GUI\\gridflow', 'D:\\GUI\\test3\\Lib\\site-packages'],
    binaries=binaries,
    datas=[
        ('gridflow_logo.png', '.'),
        ('gridflow_logo.ico', '.'),
    ] + datas,
    hiddenimports=hiddenimports,
    runtime_hooks=['hook-pyproj-prefix.py'],
    hookspath=[],
    hooksconfig={},
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GridFlowGUI',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='gridflow_logo.ico'
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='GridFlowGUI'
)