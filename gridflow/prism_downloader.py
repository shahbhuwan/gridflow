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

import logging
import requests
import hashlib
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Dict, Optional, Tuple

logging_lock = Lock()
success_lock = Lock()

class FileManager:
    def __init__(self, download_dir: str, metadata_dir: str, metadata_prefix: str = ""):
        self.download_dir = Path(download_dir)
        self.metadata_dir = Path(metadata_dir)
        self.metadata_prefix = metadata_prefix
        try:
            self.download_dir.mkdir(parents=True, exist_ok=True)
            self.metadata_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            with logging_lock:
                logging.error(f"Failed to create directories {self.download_dir} or {self.metadata_dir}: {e}")
            raise

    def get_output_path(self, variable: str, resolution: str, date_str: str) -> Path:
        output_filename = f"prism_{variable}_us_{resolution}_{date_str}.zip"
        return self.download_dir / output_filename

    def save_metadata(self, files: List[Dict], filename: str) -> None:
        metadata_path = self.metadata_dir / f"{self.metadata_prefix}{filename}"
        try:
            # Convert Path objects to strings for JSON serialization
            serializable_files = [
                {k: str(v) if isinstance(v, Path) else v for k, v in file.items()}
                for file in files
            ]
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(serializable_files, f, indent=2)
            with logging_lock:
                logging.debug(f"Saved metadata to {metadata_path}")
        except Exception as e:
            with logging_lock:
                logging.error(f"Failed to save metadata to {metadata_path}: {e}")
            raise

def compute_sha256(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def check_data_availability(variable: str, resolution: str, time_step: str, year: int, date_str: str) -> Optional[Dict]:
    res_label = '30s' if resolution == '800m' else '25m'  # Mapping: 4km -> 25m, 800m -> 30s
    base_url = f"https://data.prism.oregonstate.edu/time_series/us/an/{resolution}/{variable}/{time_step}/{year}/"
    filename = f"prism_{variable}_us_{res_label}_{date_str}.zip"
    url = f"{base_url}{filename}"

    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        with logging_lock:
            logging.debug(f"HEAD response for {url}: {response.status_code}")
        if response.status_code == 200:
            with logging_lock:
                logging.info(f"Data available for {variable} at {resolution} ({time_step}) on {date_str}")
            return {'url': url, 'filename': filename, 'date': date_str}
    except requests.RequestException as e:
        with logging_lock:
            logging.debug(f"HEAD request failed for {url}: {e}, falling back to GET")

    try:
        response = requests.get(url, stream=True, timeout=10, allow_redirects=True)
        with logging_lock:
            logging.debug(f"GET response for {url}: {response.status_code}")
        if response.status_code == 200:
            with logging_lock:
                logging.info(f"Data available for {variable} at {resolution} ({time_step}) on {date_str}")
            return {'url': url, 'filename': filename, 'date': date_str}
        else:
            with logging_lock:
                logging.warning(f"Data unavailable for {variable} on {date_str} ({time_step}, {resolution}): HTTP {response.status_code}, skipping")
            return None
    except requests.RequestException as e:
        with logging_lock:
            logging.warning(f"Data unavailable for {variable} on {date_str} ({time_step}, {resolution}): {e}, skipping")
        return None

class Downloader:
    def __init__(self, file_manager: FileManager, retries: int, timeout: int, workers: int):
        self.file_manager = file_manager
        self.retries = retries
        self.timeout = timeout
        self.workers = workers
        self.successful_downloads = 0

    def download_file(self, file_info: Dict, attempt: int = 1) -> Optional[str]:
        url = file_info['url']
        output_path = file_info['output_path']
        output_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, self.retries + 1):
            try:
                with logging_lock:
                    logging.debug(f"Attempting to download {url} (Attempt {attempt}/{self.retries})")
                response = requests.get(url, stream=True, timeout=self.timeout)
                with logging_lock:
                    logging.debug(f"HTTP response for {url}: {response.status_code}")
                response.raise_for_status()

                expected_size = int(response.headers.get('Content-Length', 0))
                with logging_lock:
                    logging.debug(f"Expected file size for {url}: {expected_size} bytes")

                downloaded_size = 0
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)

                if expected_size > 0 and downloaded_size != expected_size:
                    with logging_lock:
                        logging.error(f"File size mismatch for {output_path.name}: expected {expected_size} bytes, got {downloaded_size} bytes")
                    return None

                sha256_checksum = compute_sha256(output_path)
                with logging_lock:
                    logging.info(f"SHA256 checksum for {output_path.name}: {sha256_checksum}")
                    logging.warning("Note: PRISM server does not provide checksums for verification. Compare manually if needed.")
                    logging.info(f"Downloaded {output_path.name}")
                with success_lock:
                    self.successful_downloads += 1
                return str(output_path)
            except requests.RequestException as e:
                with logging_lock:
                    logging.warning(f"Download failed for {url} (Attempt {attempt}/{self.retries}): {e}")
                if attempt == self.retries:
                    with logging_lock:
                        logging.error(f"Failed to download {url} after {self.retries} attempts")
                    return None
        return None

    def download_all(self, files: List[Dict], stop_flag: callable = None) -> List[str]:
        downloaded_files = []
        total_files = len(files)
        if total_files == 0:
            with logging_lock:
                logging.info("No files to download")
            return []

        progress_interval = max(1, total_files // 10)
        completed = 0
        next_threshold = progress_interval

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_file = {executor.submit(self.download_file, f): f for f in files}
            for future in as_completed(future_to_file):
                if stop_flag and stop_flag():
                    with logging_lock:
                        logging.info("PRISM download stopped by user")
                    executor.shutdown(wait=False)
                    return downloaded_files
                try:
                    path = future.result()
                    if path:
                        downloaded_files.append(path)
                except Exception as e:
                    with logging_lock:
                        logging.error(f"Unexpected error in download task: {e}")
                completed += 1
                with logging_lock:
                    if completed >= next_threshold:
                        logging.info(f"Progress: {completed}/{total_files} files")
                        next_threshold += progress_interval

        with logging_lock:
            logging.info(f"Final Progress: {completed}/{total_files} files")
        return downloaded_files

def validate_date(date_str: str, time_step: str) -> bool:
    try:
        if time_step == "daily":
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                dt = datetime.strptime(date_str, '%Y%m%d')
        elif time_step == "monthly":
            try:
                dt = datetime.strptime(date_str, '%Y-%m')
            except ValueError:
                dt = datetime.strptime(date_str, '%Y%m')
        else:
            with logging_lock:
                logging.error(f"Invalid time_step: {time_step}. Must be 'daily' or 'monthly'.")
            return False

        if not (1 <= dt.month <= 12):
            with logging_lock:
                logging.error(f"Invalid month in {date_str}: {dt.month}. Must be 1–12.")
            return False

        min_year = 1981 if time_step == "daily" else 1895
        max_year = datetime.now().year
        if dt.year < min_year:
            with logging_lock:
                logging.error(f"Date {date_str} is too old for {time_step} data (starts {min_year}).")
            return False
        if dt.year > max_year:
            with logging_lock:
                logging.error(f"Date {date_str} exceeds current year {max_year} for {time_step} data.")
            return False
        return True
    except ValueError as e:
        with logging_lock:
            expected_format = "YYYY-MM-DD or YYYYMMDD" if time_step == "daily" else "YYYY-MM or YYYYMM"
            logging.error(f"Invalid date format: {date_str}. Expected {expected_format} for {time_step} data. Error: {e}")
        return False

def download_prism(
    variable: str,
    resolution: str,
    time_step: str,
    start_date: str,
    end_date: str,
    output_dir: str = './prism_data',
    metadata_dir: str = './metadata',
    log_level: str = "minimal",
    retries: int = 3,
    timeout: int = 30,
    demo: bool = False,
    workers: int = None,
    stop_flag: callable = None
) -> bool:
    VALID_VARIABLES = ['ppt', 'tmax', 'tmin', 'tmean', 'tdmean', 'vpdmin', 'vpdmax']
    if variable not in VALID_VARIABLES:
        with logging_lock:
            logging.error(f"Invalid variable '{variable}', must be one of {', '.join(VALID_VARIABLES)}")
        raise ValueError(f"Invalid variable: {variable}")

    metadata_prefix = "gridflow_prism_"
    file_manager = FileManager(output_dir, metadata_dir, metadata_prefix)

    if demo:
        variable = "tmean"
        resolution = "4km"
        time_step = "monthly"
        start_date = "2020-01"
        end_date = "2020-03"
        workers = 4
        with logging_lock:
            logging.info("Running in demo mode: downloading tmean for January–March 2020 (monthly, 4km)")

    if not validate_date(start_date, time_step):
        raise ValueError(f"Invalid start date: {start_date}")
    if not validate_date(end_date, time_step):
        raise ValueError(f"Invalid end date: {end_date}")

    start_dt = datetime.strptime(start_date, '%Y-%m-%d' if '-' in start_date else '%Y%m%d') if time_step == 'daily' else \
               datetime.strptime(start_date, '%Y-%m' if '-' in start_date else '%Y%m')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d' if '-' in end_date else '%Y%m%d') if time_step == 'daily' else \
             datetime.strptime(end_date, '%Y-%m' if '-' in end_date else '%Y%m')
    if time_step == 'monthly':
        end_dt = end_dt + relativedelta(months=1) - timedelta(days=1)

    if start_dt > end_dt:
        with logging_lock:
            logging.error("start_date must be before or equal to end_date")
        raise ValueError("Start date must be before or equal to end date")

    # Generate list of dates to check
    dates_to_check = []
    current_dt = start_dt
    while current_dt <= end_dt:
        year = current_dt.year
        date_str = current_dt.strftime('%Y%m%d' if time_step == 'daily' else '%Y%m')
        dates_to_check.append((variable, resolution, time_step, year, date_str))
        current_dt += timedelta(days=1) if time_step == 'daily' else relativedelta(months=1)

    # Initialize downloader and files list
    downloader = Downloader(file_manager, retries, timeout, workers or os.cpu_count() or 4)
    files_to_download = []
    files_to_download_all = []
    chunk_size = downloader.workers  # Chunk size equals number of workers

    # Process dates in chunks
    for i in range(0, len(dates_to_check), chunk_size):
        chunk = dates_to_check[i:i + chunk_size]
        chunk_files = []
        existing_files = []

        # Parallel availability checks for the chunk
        with ThreadPoolExecutor(max_workers=chunk_size) as executor:
            future_to_date = {
                executor.submit(check_data_availability, *args): args
                for args in chunk
            }
            for future in as_completed(future_to_date):
                if stop_flag and stop_flag():
                    with logging_lock:
                        logging.info("PRISM availability check stopped by user")
                    executor.shutdown(wait=False)
                    return False
                try:
                    result = future.result()
                    if result:
                        url = result['url']
                        date_str = result['date']
                        output_path = file_manager.get_output_path(variable, resolution, date_str)
                        file_info = {'url': url, 'output_path': output_path, 'date': date_str}
                        if output_path.exists():
                            with logging_lock:
                                logging.info(f"File {output_path.name} already exists")
                            existing_files.append(file_info)
                            with success_lock:
                                downloader.successful_downloads += 1
                        else:
                            chunk_files.append(file_info)
                        files_to_download_all.append(file_info)
                except Exception as e:
                    with logging_lock:
                        logging.error(f"Error checking availability: {e}")

        if chunk_files or existing_files:
            with logging_lock:
                logging.info(f"Found {len(chunk_files) + len(existing_files)} {time_step} files in chunk {i // chunk_size + 1} "
                             f"({len(existing_files)} existing, {len(chunk_files)} to download)")
            # Download files in parallel for this chunk
            downloaded = downloader.download_all(chunk_files, stop_flag=stop_flag)
            files_to_download.extend(downloaded)

        if stop_flag and stop_flag():
            with logging_lock:
                logging.info("PRISM process stopped by user")
            break

    if not files_to_download_all:
        with logging_lock:
            logging.error("No PRISM files to download")
        raise ValueError("No PRISM files available for download")

    with logging_lock:
        logging.info(f"Total found {len(files_to_download_all)} {time_step} files to download")

    # Save metadata for all files (existing and downloaded)
    file_manager.save_metadata(files_to_download_all, "query_results.json")

    with logging_lock:
        logging.info(f"Completed: {downloader.successful_downloads}/{len(files_to_download_all)} files processed successfully")
    return downloader.successful_downloads > 0