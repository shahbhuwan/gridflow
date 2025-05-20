# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['gridflow/__main__.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('gridflow/iowa_border/*', 'gridflow/iowa_border'),
        ('gridflow_logo.png', '.'),
        ('gridflow_logo.ico', '.'),
        ('gui/vocab/*.json', 'gui/vocab'),
    ],
    hiddenimports=[
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
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
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