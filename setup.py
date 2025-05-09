from setuptools import setup, find_packages
import os

readme_path = "README.md"
long_description = ""
if os.path.exists(readme_path):
    with open(readme_path, encoding="utf-8") as f:
        long_description = f.read()
else:
    long_description = "A library to download, crop, clip, and generate databases for CMIP6 data from ESGF nodes with deduplication and error handling"

setup(
    name="gridflow",
    version="0.2.3",
    description="A library to download, crop, clip, and generate databases for CMIP6 data from ESGF nodes with deduplication and error handling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Bhuwan Shah",
    author_email="bshah@iastate.edu",
    url="https://github.com/shahbhuwan/GridFlow",
    license="AGPL-3.0-or-later",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=[
        "requests>=2.28.0",
        "netCDF4>=1.6.0",
        "numpy>=1.21.0",
        "geopandas>=0.10.0",
    ],
    extras_require={
        "test": [
            "pytest>=8.3.2",
            "pytest-cov>=5.0.0",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "gridflow = gridflow.__main__:main",
            "gridflow-download = gridflow.downloader:run_download",
            "gridflow-crop = gridflow.crop_netcdf:main",
            "gridflow-clip = gridflow.clip_netcdf:main",
            "gridflow-db = gridflow.database_generator:main",
        ],
    },
)