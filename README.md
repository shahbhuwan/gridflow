GridFlow
GridFlow is a Python library for downloading and processing CMIP6 climate data from ESGF nodes. It supports concurrent downloading of datasets, cropping and clipping NetCDF files to specific geographic regions, and generating metadata databases. Designed for researchers and data scientists, GridFlow offers deduplication, error handling, parallel processing, and a unified command-line interface (CLI).
Features

Download: Retrieve CMIP6 data from ESGF nodes with customizable query parameters (e.g., activity, variable, resolution, frequency).
Crop: Crop NetCDF files to a specified latitude/longitude bounding box with optional buffering.
Clip: Clip NetCDF files using a shapefile for precise geographic regions.
Database: Generate a JSON database grouping NetCDF files by activity, source, and variant.
Unified CLI: Access all functionality through a single gridflow command with subcommands.
Parallel Processing: Utilize multiple threads for efficient downloading and processing.
Error Handling: Automatic retries, checksum verification, and logging for robust operations.
Testing: Comprehensive test suite using pytest to ensure reliability.

Installation
Prerequisites

Python 3.8 or higher
System libraries for geopandas and netCDF4:

# Ubuntu
sudo apt-get install libgeos-dev libgdal-dev libhdf5-dev

# macOS
brew install geos gdal hdf5

# Windows (use a package manager like Chocolatey)
choco install hdf5

Install GridFlow
Using Pre-built Artifacts
If you have the pre-built wheel or source distribution in dist/:
pip install dist/gridflow-0.2.3-py3-none-any.whl

From Source
Clone the repository and install:
git clone https://github.com/shahbhuwan/GridFlow.git
cd GridFlow
python -m venv gridflow-env
source gridflow-env/bin/activate  # On Windows: gridflow-env\Scripts\activate
pip install .

Manual Dependency Installation (if needed)
pip install -r requirements.txt
pip install .

Usage
GridFlow provides a unified CLI with subcommands for each operation. Run gridflow --help to see available options.
Download CMIP6 Data
Download HighResMIP tas (surface air temperature) data at 50 km resolution with daily frequency:
gridflow download \
  --project CMIP6 \
  --activity HighResMIP \
  --variable tas \
  --resolution "50 km" \
  --frequency day \
  --output-dir ./cmip6_data \
  --metadata-dir ./metadata \
  --log-level normal \
  --workers 4 \
  --retries 5 \
  --timeout 30 \
  --max-downloads 10

Outputs: NetCDF files in ./cmip6_data, metadata in ./metadata/query_results.json, logs in ./logs.

Use --demo for a test run with predefined settings.
Use --retry-failed path/to/failed_downloads.json to retry failed downloads.

Crop NetCDF Files
Crop NetCDF files to a geographic region (e.g., latitude 35–70, longitude -10–40):
gridflow crop \
  --input-dir ./cmip6_data \
  --output-dir ./cmip6_data_cropped \
  --min-lat 35 \
  --max-lat 70 \
  --min-lon -10 \
  --max-lon 40 \
  --buffer-km 10 \
  --log-level normal \
  --workers 4

Outputs: Cropped NetCDF files in ./cmip6_data_cropped.

Use --demo for a test run.

Clip NetCDF Files
Clip NetCDF files using a shapefile (e.g., Iowa border shapefile in iowa_border/):
gridflow clip \
  --input-dir ./cmip6_data \
  --output-dir ./cmip6_data_clipped \
  --shapefile ./iowa_border/iowa_border.shp \
  --buffer-km 10 \
  --log-level normal \
  --workers 4

Outputs: Clipped NetCDF files in ./cmip6_data_clipped.

Use --demo for a test run with a sample shapefile.

Generate Database
Create a JSON database of NetCDF files:
gridflow database \
  --input-dir ./cmip6_data \
  --output-dir ./output \
  --log-level normal \
  --workers 4

Outputs: database.json in ./output.

Use --demo for a test run with demo_database.json.

Individual Commands
Standalone commands are also available:

gridflow-download
gridflow-crop
gridflow-clip
gridflow-db

Example:
gridflow-download --project CMIP6 --variable tas --demo

Testing
GridFlow includes a test suite in the tests/ directory, using pytest to verify functionality. Tests cover downloading, cropping, clipping, and database generation.
Running Tests
Install testing dependencies:
pip install pytest pytest-cov

Run tests:
pytest tests/

This runs all tests in tests/ (e.g., test_downloader.py, test_clip_netcdf.py).
Run Tests with Coverage
To generate a coverage report:
pytest --cov=gridflow --cov-report=html tests/

Outputs a coverage report in htmlcov/. View the report by opening htmlcov/index.html in a browser.
Sample Data
The iowa_border/ directory contains shapefiles (e.g., iowa_border.shp) used for testing the clip subcommand. Ensure test NetCDF files are available in cmip6_data/ for full test coverage.
Building the Package
GridFlow includes pre-built artifacts and build configuration files (setup.py, pyproject.toml, MANIFEST.in) for creating distributable packages.
Using Pre-built Artifacts
If you have pre-built files in dist/:
pip install dist/gridflow-0.2.3-py3-none-any.whl

Rebuilding the Package
Install build tools:
pip install --upgrade pip setuptools wheel build twine

Build the package:
python -m build

Generates:

dist/gridflow-0.2.3.tar.gz (source distribution)
dist/gridflow-0.2.3-py3-none-any.whl (wheel)

Verify the build:
pip install dist/gridflow-0.2.3-py3-none-any.whl
gridflow --help

Upload to PyPI (optional):
twine upload dist/*

Requires a PyPI account.
Project Structure
GridFlow/
├── gridflow/
│   ├── __init__.py
│   ├── __main__.py
│   ├── downloader.py
│   ├── clip_netcdf.py
│   ├── crop_netcdf.py
│   ├── database_generator.py
├── tests/
│   ├── __init__.py
│   ├── test_downloader.py
│   ├── test_clip_netcdf.py
│   ├── test_crop_netcdf.py
│   ├── test_database_generator.py
│   ├── test_main.py
├── iowa_border/
│   ├── iowa_border.shp
│   ├── iowa_border.dbf
│   ├── iowa_border.shx
│   ├── ...
├── dist/
│   ├── gridflow-0.2.3.tar.gz
│   ├── gridflow-0.2.3-py3-none-any.whl
├── .github/
│   └── workflows/
│       ├── test.yml
├── setup.py
├── pyproject.toml
├── MANIFEST.in
├── README.md
├── LICENSE.txt
├── requirements.txt
├── .pytest_cache/
├── htmlcov/
├── .coverage
├── coverage.xml

Notes:

iowa_border/ contains sample shapefiles for testing the clip subcommand.
.pytest_cache/, htmlcov/, .coverage, and coverage.xml are generated by pytest and coverage tools.
.github/workflows/test.yml defines a GitHub Actions workflow for automated testing.

Troubleshooting

ESGF Node Errors: If nodes like esgf-node.llnl.gov fail, try --no-verify-ssl or modify ESGF_NODES in downloader.py to prioritize esgf-node.ipsl.upmc.fr.
Dependency Issues: Ensure system libraries for geopandas and netCDF4 are installed. Reinstall dependencies:pip install --force-reinstall -r requirements.txt


No Files Found: Verify query parameters on the ESGF website (https://esgf-node.llnl.gov/search/cmip6). Use --log-level debug.
Shapefile Errors: Ensure iowa_border/iowa_border.shp is valid. Test with QGIS.
Test Failures: Check tests/ for required sample data (e.g., NetCDF files in cmip6_data/). Run pytest --verbose for details.

Check logs in ./logs for detailed error messages.
Contributing
Contributions are welcome! Please:

Fork the repository.
Create a feature branch (git checkout -b feature/your-feature).
Commit changes (git commit -m "Add your feature").
Run tests (pytest tests/).
Push to the branch (git push origin feature/your-feature).
Open a pull request.

License
GridFlow is licensed under the GNU Affero General Public License v3 or later (AGPLv3+). See LICENSE.txt for details.
Contact
For questions or support, contact Bhuwan Shah at bshah@iastate.edu or open an issue on GitHub.
