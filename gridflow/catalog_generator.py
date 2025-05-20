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

import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import netCDF4

# Suppress HDF5 error messages
os.environ["HDF5_LOG_LEVEL"] = "0"

def extract_metadata(file_path: str) -> Dict[str, str]:
    """Extract metadata from a NetCDF file.

    Args:
        file_path (str): Path to the NetCDF file.

    Returns:
        Dict[str, str]: Dictionary containing file path, metadata, and error status.
    """
    file_path = Path(file_path)
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

def is_non_prefixed_filename(filename: str) -> bool:
    """Determine if a filename is non-prefixed (starts with a CMIP variable).

    Args:
        filename (str): Name of the file.

    Returns:
        bool: True if the filename starts with a CMIP variable (e.g., 'tas_'), False otherwise.
    """
    cmip_variables = {'tas', 'pr', 'huss', 'psl', 'ts', 'uas', 'vas'}  # Add more as needed
    return any(filename.startswith(f"{var}_") for var in cmip_variables)

def get_base_filename(filename: str) -> str:
    """Extract the base filename by removing known prefixes.

    Args:
        filename (str): Name of the file.

    Returns:
        str: Base filename without prefix, or original filename if no prefix is found.
    """
    prefixes = ['ScenarioMIP_250km_', 'CMIP6_', 'CMIP5_']  # Add more prefixes as needed
    for prefix in prefixes:
        if filename.startswith(prefix):
            return filename[len(prefix):]
    return filename

def generate_catalog(
    input_dir: str,
    output_dir: str,
    demo_mode: bool = False,
    workers: Optional[int] = None,
    stop_flag: Optional[callable] = None
) -> Dict[str, Dict]:
    """Generate a catalog of metadata from NetCDF files in input_dir and its subdirectories.

    Files with non-prefixed filenames (e.g., starting with 'tas_') are preferred for the main catalog
    when both prefixed and non-prefixed versions exist across any folders. If only one version exists
    (prefixed or non-prefixed), it is included. Duplicate filenames are logged in duplicates.json.
    Files with incomplete metadata are skipped. Detailed logging tracks processed, included, skipped,
    and duplicate files.

    Args:
        input_dir (str): Directory containing NetCDF files (searched recursively).
        output_dir (str): Directory to save the catalog and duplicates JSON files.
        demo_mode (bool): If True, use 'cmip6_catalog.json' as output filename.
        workers (Optional[int]): Number of worker threads for parallel processing.
        stop_flag (Optional[callable]): Function to check for stop signal.

    Returns:
        Dict[str, Dict]: Catalog dictionary containing metadata groups, or empty dict if failed.
    """
    input_dir = Path(input_dir)
    if not input_dir.exists():
        logging.critical(f"Input directory {input_dir} does not exist. Ensure the directory is valid.")
        return {}

    filename = "cmip6_catalog.json" if demo_mode else "catalog.json"
    duplicates_filename = "duplicates.json"
    output_dir = Path(output_dir)

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.critical(f"Failed to create output directory {output_dir}: {str(e)}")
        return {}

    output_file = output_dir / filename
    duplicates_file = output_dir / duplicates_filename

    # Recursively find all *.nc files
    nc_files = list(input_dir.rglob("*.nc"))
    if not nc_files:
        if demo_mode:
            logging.critical(f"No NetCDF files found in {input_dir}. Run 'gridflow download --demo' to generate sample files.")
            return {}
        logging.warning(f"No NetCDF files found in {input_dir} or its subdirectories")
        return {}

    # Group files by base filename for duplicate detection
    files_by_base = {}
    for nc_file in nc_files:
        base_name = get_base_filename(nc_file.name)
        if base_name not in files_by_base:
            files_by_base[base_name] = []
        files_by_base[base_name].append(nc_file)

    # Deduplicate files by path and filename
    unique_files = []
    seen_paths = set()
    duplicates = []
    for base_name, files in files_by_base.items():
        if len(files) == 1:
            # Only one file, include it
            if str(files[0]) not in seen_paths:
                unique_files.append(files[0])
                seen_paths.add(str(files[0]))
        else:
            # Multiple files, prefer non-prefixed if available, otherwise take first
            non_prefixed = next((f for f in files if is_non_prefixed_filename(f.name)), files[0])
            if str(non_prefixed) not in seen_paths:
                unique_files.append(non_prefixed)
                seen_paths.add(str(non_prefixed))
            for nc_file in files:
                if nc_file != non_prefixed and str(nc_file) not in seen_paths:
                    duplicates.append({
                        "file_path": str(nc_file),
                        "metadata": {"note": f"Duplicate filename, matches {non_prefixed.name}"},
                        "metadata_key": base_name
                    })
                    logging.warning(f"Duplicate filename detected: {nc_file.name} matches {non_prefixed.name}")
                    seen_paths.add(str(nc_file))

    workers = workers or os.cpu_count() or 4
    catalog = {}
    total_files = len(unique_files)
    processed_count = 0
    skipped_count = 0
    included_count = 0

    logging.info(f"Processing {total_files} unique NetCDF files with {workers} workers")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {executor.submit(extract_metadata, str(nc_file)): nc_file for nc_file in unique_files}
        for future in as_completed(future_to_file):
            if stop_flag and stop_flag():
                logging.info("Catalog generation stopped by user")
                executor._threads.clear()  # Clear threads to allow shutdown
                executor.shutdown(wait=False, cancel_futures=True)
                return catalog
            processed_count += 1
            result = future.result()
            file_path = result["file_path"]
            metadata = result["metadata"]
            error = result.get("error")
            if error:
                logging.error(error)
                skipped_count += 1
                continue
            if not metadata or not all(metadata.get(k) for k in ["activity_id", "source_id", "variant_label", "variable_id"]):
                missing_fields = [k for k in ["activity_id", "source_id", "variant_label", "variable_id"] if not metadata.get(k)]
                logging.warning(f"Skipping {file_path}: Incomplete metadata (missing: {', '.join(missing_fields)})")
                skipped_count += 1
                continue

            key = f"{metadata['activity_id']}:{metadata['source_id']}:{metadata['variant_label']}"
            if key not in catalog:
                catalog[key] = {
                    "activity_id": metadata["activity_id"],
                    "source_id": metadata["source_id"],
                    "variant_label": metadata["variant_label"],
                    "institution_id": metadata["institution_id"],
                    "variables": {}
                }
            variable_id = metadata["variable_id"]
            if variable_id not in catalog[key]["variables"]:
                catalog[key]["variables"][variable_id] = {
                    "file_count": 0,
                    "files": []
                }
            catalog[key]["variables"][variable_id]["files"].append({"path": file_path})
            catalog[key]["variables"][variable_id]["file_count"] += 1
            included_count += 1
            logging.info(f"Progress: {processed_count}/{total_files} files")

    if demo_mode and included_count == 0:
        logging.critical(f"No valid NetCDF files with complete metadata found in {input_dir}. Run 'gridflow download --demo' to generate sample files.")
        return {}

    # Log summary
    logging.info(f"Summary: Processed {processed_count} files, Included {included_count} files, "
                 f"Skipped {skipped_count} files ({len(duplicates)} duplicates, {skipped_count - len(duplicates)} errors/incomplete)")

    # Save main catalog
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=2)
        logging.info(f"Catalog saved to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save catalog to {output_file}: {str(e)}")
        return {}

    # Save duplicates JSON
    if duplicates:
        try:
            with open(duplicates_file, 'w', encoding='utf-8') as f:
                json.dump(duplicates, f, indent=2)
            logging.info(f"Duplicate files saved to {duplicates_file}")
        except Exception as e:
            logging.error(f"Failed to save duplicates to {duplicates_file}: {str(e)}")
    else:
        logging.info("No duplicate files found")

    logging.info(f"Generated catalog with {len(catalog)} groups")
    return catalog