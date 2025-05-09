# Copyright (c) 2025 Bhuwan Shah
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import netCDF4
import json
import logging
import os
from pathlib import Path
from typing import Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from .downloader import setup_logging

def extract_metadata(file_path: str) -> Dict[str, str]:
    """Extract CMIP6 metadata from a NetCDF file."""
    file_path = Path(file_path)  # Convert string back to Path
    if not file_path.exists():
        return {"file_path": str(file_path), "metadata": {}, "error": f"File {file_path} does not exist"}
    
    try:
        with netCDF4.Dataset(file_path, 'r') as ds:
            metadata = {
                "activity_id": getattr(ds, "activity_id", ""),
                "source_id": getattr(ds, "source_id", ""),
                "variant_label": getattr(ds, "variant_label", ""),
                "variable_id": getattr(ds, "variable_id", ""),
                "institution_id": getattr(ds, "institution_id", "")
            }
        return {"file_path": str(file_path), "metadata": metadata, "error": None}
    except Exception as e:
        return {"file_path": str(file_path), "metadata": {}, "error": f"Failed to extract metadata: {repr(e)}"}

def generate_database(input_dir: str, output_dir: str, demo_mode: bool = False, workers: int = None) -> Dict[str, Dict]:
    """Generate a database grouping NetCDF files by activity_id, source_id, variant_label in parallel."""
    input_dir = Path(input_dir)
    if not input_dir.exists():
        logging.error(f"Input directory {input_dir} does not exist")
        return {}

    filename = "demo_database.json" if demo_mode else "database.json"
    output_dir = Path(output_dir)
    
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to create output directory {output_dir}: {str(e)}")
        return {}

    output_file = output_dir / filename

    workers = workers or os.cpu_count() or 4
    database = {}
    nc_files = list(input_dir.glob("*.nc"))

    if not nc_files:
        logging.warning(f"No NetCDF files found in {input_dir}")
        return {}

    logging.info(f"Processing {len(nc_files)} NetCDF files with {workers} workers")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_file = {executor.submit(extract_metadata, str(nc_file)): nc_file for nc_file in nc_files}
        for future in as_completed(future_to_file):
            result = future.result()
            file_path = result["file_path"]
            metadata = result["metadata"]
            error = result.get("error")
            if error:
                logging.error(error)
                continue
            if not metadata or not all(metadata.get(k) for k in ["activity_id", "source_id", "variant_label"]):
                logging.warning(f"Skipping {file_path}: Incomplete metadata")
                continue

            key = f"{metadata['activity_id']}:{metadata['source_id']}:{metadata['variant_label']}"
            if key not in database:
                database[key] = {
                    "activity_id": metadata["activity_id"],
                    "source_id": metadata["source_id"],
                    "variant_label": metadata["variant_label"],
                    "institution_id": metadata["institution_id"],
                    "files": []
                }
            database[key]["files"].append({
                "variable_id": metadata["variable_id"],
                "path": file_path
            })

    try:
        with open(output_file, 'w') as f:
            json.dump(database, f, indent=2)
        logging.info(f"Database saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save database to {output_file}: {str(e)}")
        return {}

    logging.info(f"Generated database with {len(database)} groups")
    return database

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate a database of CMIP6 NetCDF files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-i' ,'--input-dir', default='./cmip6_data', help="Directory containing NetCDF files")
    parser.add_argument('-o' ,'--output-dir', default='./output', help="Directory to save the database JSON file")
    parser.add_argument('--log-dir', default='./logs', help="Directory for log files")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('-w' ,'--workers', type=int, help="Number of parallel workers (default: CPU count)")
    parser.add_argument('--demo', action='store_true', help="Run in demo mode with test files")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.2.3')
    args = parser.parse_args()

    setup_logging(args.log_dir, args.log_level, prefix="db_")

    if args.demo:
        args.input_dir = "./demo_cmip6_data"
        args.output_dir = "./demo_output"
        args.workers = 2
        logging.critical("Generating CMIP6 database in demo mode")

    database = generate_database(args.input_dir, args.output_dir, demo_mode=args.demo, workers=args.workers)
    if not database:
        logging.error("No valid NetCDF files found or database generation failed")
        sys.exit(1)
    logging.critical(f"Generated database with {len(database)} groups")

if __name__ == "__main__":
    main()