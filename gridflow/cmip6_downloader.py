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

import os
import sys
import json
import time
import logging
import requests
from pathlib import Path
from threading import Lock, Event
from datetime import datetime
from hashlib import md5, sha256
from gridflow import __version__
from urllib.parse import urlencode
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ESGF_NODES = [
    "https://esgf-node.llnl.gov/esg-search/search",
    "https://esgf-node.ipsl.upmc.fr/esg-search/search",
    "https://esgf-data.dkrz.de/esg-search/search",
    "https://esgf-index1.ceda.ac.uk/esg-search/search"
]

class InterruptibleSession(requests.Session):
    def __init__(self, stop_event: Event):
        super().__init__()
        self.stop_event = stop_event
        # Configure retries and timeouts
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.mount("http://", HTTPAdapter(max_retries=retries))
        self.mount("https://", HTTPAdapter(max_retries=retries))

    def get(self, url, **kwargs):
        # Set a short read timeout to allow frequent stop checks
        kwargs.setdefault("timeout", (5, 1))  # (connect_timeout, read_timeout)
        response = super().get(url, **kwargs)
        if self.stop_event.is_set():
            response.close()
            raise requests.exceptions.RequestException("Download interrupted by user")
        return response

class FileManager:
    RESOLUTION_MAPPING = {
        'HiRAM-SIT-HR': '25km',
        'CanESM5': '250km',
        'CESM2': '100km',
    }

    def __init__(self, download_dir: str, metadata_dir: str, save_mode: str, prefix: str = "", metadata_prefix: str = ""):
        self.download_dir = Path(download_dir)
        self.metadata_dir = Path(metadata_dir)
        self.save_mode = save_mode.lower()
        self.prefix = prefix
        self.metadata_prefix = metadata_prefix
        try:
            self.download_dir.mkdir(parents=True, exist_ok=True)
            self.metadata_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create directories {self.download_dir} or {self.metadata_dir}: {e}")
            sys.exit(1)

    def get_output_path(self, file_info: Dict) -> Path:
        filename = file_info.get('title', '')
        activity = file_info.get('activity_id', ['unknown'])[0].replace('/', '_')
        variable = file_info.get('variable_id', ['unknown'])[0].replace('/', '_')
        model = file_info.get('source_id', ['unknown'])[0]

        # Try to get resolution from nominal_resolution in metadata
        nominal_resolution = file_info.get('nominal_resolution', [''])[0]
        if nominal_resolution and nominal_resolution != '':
            # Clean and format nominal_resolution (e.g., '250 km' -> '250km')
            resolution = nominal_resolution.replace(' ', '').replace('km', 'km')
        else:
            # Fallback to RESOLUTION_MAPPING
            resolution = self.RESOLUTION_MAPPING.get(model, 'unknown').replace(' ', '')

        if self.save_mode == 'flat':
            prefixed_filename = f"{self.prefix}{activity}_{resolution}_{filename}"
            return self.download_dir / prefixed_filename
        else:
            subdir = self.download_dir / variable / resolution / activity
            subdir.mkdir(parents=True, exist_ok=True)
            return subdir / filename

    def save_metadata(self, files: List[Dict], filename: str) -> None:
        metadata_path = self.metadata_dir / f"{self.metadata_prefix}{filename}"
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(files, f, indent=2)
            logging.debug(f"Saved metadata to {metadata_path}")
        except Exception as e:
            logging.error(f"Failed to save metadata {filename}: {e}")

class QueryHandler:
    def __init__(self, nodes: List[str] = ESGF_NODES, stop_event: Optional[Event] = None):
        self.nodes = nodes
        self.session = InterruptibleSession(stop_event if stop_event else Event())
        self.stop_event = stop_event

    def build_query(self, base_url: str, params: Dict[str, str]) -> str:
        query_params = {
            'type': 'File',
            'project': 'CMIP6',
            'format': 'application/solr+json',
            'limit': '1000',
            'distrib': 'true',
            **params
        }
        return f"{base_url}?{urlencode(query_params, safe='/')}"

    def fetch_datasets(self, params: Dict[str, str], timeout: int) -> List[Dict]:
        files = []
        seen_ids = set()
        for node in self.nodes:
            if self.stop_event and self.stop_event.is_set():
                logging.info("Stopping query due to stop event")
                return files
            try:
                logging.info(f"Trying to connect to {node}")
                node_files = self._fetch_from_node(node, params, timeout)
                unique_files = []
                for f in node_files:
                    file_id = f.get('id', '')
                    if file_id and file_id not in seen_ids:
                        seen_ids.add(file_id)
                        unique_files.append(f)
                files.extend(unique_files)
                if files:
                    logging.debug(f"Retrieved {len(files)} files from {node}")
                    return files
                logging.warning(f"No files found at {node}, trying next node")
            except requests.RequestException as e:
                logging.error(f"Failed to connect to {node}: {str(e)} (Type: {type(e).__name__})")
                continue
            except Exception as e:
                logging.error(f"Unexpected error while querying {node}: {str(e)} (Type: {type(e).__name__})")
                continue
        logging.error("All nodes failed to respond or no files were found")
        sys.exit(1) 
        return files

    def _fetch_from_node(self, node: str, params: Dict[str, str], timeout: int) -> List[Dict]:
        files = []
        offset = 0
        while True:
            if self.stop_event and self.stop_event.is_set():
                logging.info("Stopping query due to stop event")
                return files
            query_params = {**params, 'offset': str(offset)}
            query_url = self.build_query(node, query_params)
            logging.debug(f"Querying: {query_url}")
            try:
                response = self.session.get(query_url, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                docs = data.get('response', {}).get('docs', [])
                files.extend(docs)
                num_found = int(data.get('response', {}).get('numFound', 0))
                logging.debug(f"Fetched {len(docs)} files, total found: {num_found}")
                if offset + len(docs) >= num_found:
                    break
                offset += len(docs)
            except (requests.RequestException, ValueError) as e:
                logging.error(f"Query failed for {node}: {str(e)} (Type: {type(e).__name__})")
                raise
            except Exception as e:
                logging.error(f"Unexpected error in _fetch_from_node for {node}: {str(e)} (Type: {type(e).__name__})")
                raise
        return files

    def fetch_specific_file(self, file_info: Dict, timeout: int) -> Optional[Dict]:
        params = {
            'project': 'CMIP6',
            'title': file_info.get('title', ''),
            'variable_id': file_info.get('variable_id', [''])[0],
            'source_id': file_info.get('source_id', [''])[0],
            'experiment_id': file_info.get('experiment_id', [''])[0],
            'frequency': file_info.get('frequency', [''])[0],
            'variant_label': file_info.get('variant_label', [''])[0],
            'activity_id': file_info.get('activity_id', [''])[0],
            'limit': '1'
        }
        params = {k: v for k, v in params.items() if v}

        for node in self.nodes:
            if self.stop_event and self.stop_event.is_set():
                logging.info("Stopping file query due to stop event")
                return None
            try:
                logging.info(f"Querying {node} for file {params['title']}")
                files = self._fetch_from_node(node, params, timeout)
                if files:
                    for f in files:
                        if f.get('title') == params['title']:
                            logging.debug(f"Found file {params['title']} at {node}")
                            return f
                logging.warning(f"File {params['title']} not found at {node}, trying next node")
            except requests.RequestException as e:
                logging.error(f"Failed to query {node} for {params['title']}: {e}")
                continue
        logging.error(f"Failed to find file {params['title']} at any node")
        return None

class Downloader:
    def __init__(self, file_manager: FileManager, max_workers: int, retries: int, timeout: int, max_downloads: int, username: Optional[str], password: Optional[str], verify_ssl: bool, openid: Optional[str] = None):
        self.file_manager = file_manager
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.max_downloads = max_downloads
        self.stop_event = Event()
        self.session = InterruptibleSession(self.stop_event)
        self.verify_ssl = verify_ssl
        self.log_lock = Lock()
        self.successful_downloads = 0
        self.query_handler = QueryHandler(stop_event=self.stop_event)
        self.executor = None
        self.pending_futures: List[Future] = []
        if username and password:
            self.session.auth = (username, password)
            with self.log_lock:
                logging.warning("Using basic authentication; some ESGF nodes may require OAuth or other methods. Check ESGF documentation.")
        elif openid:
            with self.log_lock:
                logging.warning("OpenID provided but not implemented in this version. Downloads may fail for restricted data.")
        else:
            with self.log_lock:
                logging.warning("No authentication credentials provided. Downloads may fail for restricted data. Use --username and --password, or --openid for ESGF authentication.")

    def shutdown(self):
        self.stop_event.set()  # Signal all operations to stop
        if self.executor:
            for future in self.pending_futures:
                future.cancel()
            self.executor.shutdown(wait=False)  # Immediate shutdown
            self.executor = None
            self.pending_futures = []
        self.session.close()

    def verify_checksum(self, file_path: Path, file_info: Dict) -> bool:
        checksum = file_info.get('checksum', [''])[0]
        if not checksum:
            logging.warning(f"No checksum provided for {file_path.name}")
            return True
        try:
            checksum_type = file_info.get('checksum_type', ['sha256'])[0].lower()
            with open(file_path, 'rb') as f:
                data = f.read()
                if checksum_type == 'md5':
                    file_hash = md5(data).hexdigest()
                elif checksum_type == 'sha256':
                    file_hash = sha256(data).hexdigest()
                else:
                    logging.warning(f"Unsupported checksum type {checksum_type} for {file_path.name}")
                    return True
            if file_hash == checksum:
                return True
            logging.error(f"Checksum mismatch for {file_path.name}: expected {checksum}, got {file_hash}")
            return False
        except Exception as e:
            logging.error(f"Checksum verification failed for {file_path.name}: {e}")
            return False

    def download_file(self, file_info: Dict, attempt: int = 1) -> Tuple[Optional[str], Optional[Dict]]:
        if self.stop_event.is_set():
            with self.log_lock:
                logging.info(f"Skipping download of {file_info.get('title', '')} due to stop event")
            return None, file_info

        urls = file_info.get('url', [])
        filename = file_info.get('title', '')
        if not urls or not filename:
            with self.log_lock:
                logging.error(f"Invalid file info: missing URLs or title")
            return None, file_info

        output_path = self.file_manager.get_output_path(file_info)
        temp_path = output_path.with_suffix(output_path.suffix + '.tmp')

        if output_path.exists():
            if self.verify_checksum(output_path, file_info):
                with self.log_lock:
                    logging.info(f"Downloaded {filename} (already exists)")
                return str(output_path), None
            else:
                try:
                    output_path.unlink()
                except Exception as e:
                    logging.error(f"Failed to remove existing file {output_path}: {e}")

        for url in urls:
            if isinstance(url, str) and "HTTPServer" in url:
                download_url = url.split('|')[0]
            elif isinstance(url, list) and len(url) > 0 and "HTTPServer" in url[1]:
                download_url = url[0]
            else:
                continue
            try:
                with self.log_lock:
                    logging.info(f"Downloading {filename} from {download_url}")
                response = self.session.get(download_url, stream=True, verify=self.verify_ssl)
                response.raise_for_status()

                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if self.stop_event.is_set():
                            with self.log_lock:
                                logging.info(f"Stopping download of {filename} due to stop event")
                            response.close()
                            return None, file_info
                        if chunk:
                            f.write(chunk)
                if self.verify_checksum(temp_path, file_info):
                    temp_path.rename(output_path)
                    with self.log_lock:
                        logging.info(f"Downloaded {filename} to {output_path}")
                    return str(output_path), None
                else:
                    try:
                        temp_path.unlink()
                    except Exception as e:
                        logging.error(f"Failed to remove temp file {temp_path}: {e}")
                    raise ValueError("Checksum verification failed")

            except (requests.RequestException, ValueError) as e:
                if self.stop_event.is_set():
                    with self.log_lock:
                        logging.info(f"Stopping download of {filename} due to stop event")
                    return None, file_info
                with self.log_lock:
                    logging.warning(f"Attempt {attempt} failed for {filename} from {download_url}: {e}")
                if attempt <= self.retries:
                    time.sleep(2 ** attempt + 5)
                    return self.download_file(file_info, attempt + 1)
                with self.log_lock:
                    logging.error(f"Failed to download {filename} after {self.retries} attempts: {e}")
                return None, file_info

        with self.log_lock:
            logging.error(f"Failed to download {filename} after {self.retries} attempts")
        return None, file_info

    def download_all(self, files: List[Dict], phase: str = "initial") -> Tuple[List[str], List[Dict]]:
        downloaded_files = []
        failed_files = []
        total_files = min(len(files), self.max_downloads) if self.max_downloads else len(files)
        if total_files == 0:
            with self.log_lock:
                logging.info("No files to download")
            return [], []

        progress_interval = max(1, total_files // 10)
        completed = 0
        next_threshold = progress_interval

        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            self.pending_futures = [self.executor.submit(self.download_file, f) for f in files[:total_files]]
            for future in as_completed(self.pending_futures):
                if self.stop_event.is_set():
                    with self.log_lock:
                        logging.info("Download operation stopped by user")
                    break
                try:
                    path, failed_info = future.result()
                    if path:
                        downloaded_files.append(path)
                        if not failed_info:
                            self.successful_downloads += 1
                    if failed_info:
                        failed_files.append(failed_info)
                except Exception as e:
                    with self.log_lock:
                        logging.error(f"Unexpected error in download task: {e}")
                    failed_files.append(files[self.pending_futures.index(future)])
                completed += 1
                with self.log_lock:
                    if completed >= next_threshold:
                        logging.info(f"Progress: {self.successful_downloads}/{total_files} files (Failed: {len(failed_files)})")
                        next_threshold += progress_interval
        finally:
            if self.stop_event.is_set():
                self.shutdown()

        # with self.log_lock:
        #     logging.info(f"Progress: {self.successful_downloads}/{total_files} files (Failed: {len(failed_files)})")
        return downloaded_files, failed_files

    def retry_failed(self, failed_files: List[Dict]) -> Tuple[List[str], List[Dict]]:
        if not failed_files:
            with self.log_lock:
                logging.info("No failed files to retry")
            return [], []

        total_files = len(failed_files)
        downloaded_files = []
        remaining_failed = failed_files.copy()
        retry_round = 0

        while remaining_failed and retry_round < self.retries:
            if self.stop_event.is_set():
                with self.log_lock:
                    logging.info("Retry operation stopped by user")
                return downloaded_files, remaining_failed
            retry_round += 1
            with self.log_lock:
                logging.info(f"Retry round {retry_round} for {len(remaining_failed)} failed files")
            current_failed = []

            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
            try:
                self.pending_futures = []
                for file_info in remaining_failed:
                    if self.stop_event.is_set():
                        with self.log_lock:
                            logging.info("Retry operation stopped by user")
                        break
                    filename = file_info.get('title', 'unknown')
                    with self.log_lock:
                        logging.info(f"Retrying {filename} (Round {retry_round})")

                    updated_file_info = self.query_handler.fetch_specific_file(file_info, self.timeout)
                    if not updated_file_info:
                        with self.log_lock:
                            logging.error(f"Could not find updated metadata for {filename}, skipping retry")
                        current_failed.append(file_info)
                        continue

                    future = self.executor.submit(self.download_file, updated_file_info)
                    self.pending_futures.append(future)

                for future in as_completed(self.pending_futures):
                    if self.stop_event.is_set():
                        with self.log_lock:
                            logging.info("Retry operation stopped by user")
                        break
                    file_info = remaining_failed[self.pending_futures.index(future)]
                    filename = file_info.get('title', 'unknown')
                    try:
                        path, failed_info = future.result()
                        if path:
                            downloaded_files.append(path)
                            self.successful_downloads += 1
                            with self.log_lock:
                                logging.info(f"Successfully downloaded {filename} after retry")
                        if failed_info:
                            current_failed.append(failed_info)
                        with self.log_lock:
                            logging.info(f"Progress: {self.successful_downloads}/{total_files} files (Failed: {len(current_failed)})")
                    except Exception as e:
                        with self.log_lock:
                            logging.error(f"Unexpected error retrying {filename}: {e}")
                        current_failed.append(file_info)
            finally:
                if self.stop_event.is_set():
                    self.shutdown()

            remaining_failed = current_failed

        return downloaded_files, remaining_failed

def load_config(config_path: str) -> Dict:
    if not config_path:
        return {}
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logging.debug(f"Loaded configuration from {config_path}")
        return config
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Failed to load config file {config_path}: {e}")
        sys.exit(1)

def parse_file_time_range(filename: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        time_part = filename.split('_')[-1].replace('.nc', '')
        start_date, end_date = time_part.split('-')
        if len(start_date) == 8:
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        elif len(start_date) == 6:
            start_date = f"{start_date[:4]}-{start_date[4:6]}-01"
            end_date = f"{end_date[:4]}-{end_date[4:6]}-01"
        else:
            raise ValueError(f"Unsupported date format in {time_part}")
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        return start_date, end_date
    except Exception as e:
        logging.debug(f"Failed to parse time range from {filename}: {e}")
        return None, None

def run_download(args) -> None:
    try:
        logging.debug("Starting run_download")
        prefix = ""
        metadata_prefix = f"gridflow_{args.project.lower()}_"

        if args.retry_failed:
            retry_file_path = Path(args.retry_failed)
            if not retry_file_path.exists():
                logging.error(f"Retry file {args.retry_failed} does not exist.")
                sys.exit(1)
            if not retry_file_path.is_file():
                logging.error(f"Retry file {args.retry_failed} is not a file.")
                sys.exit(1)
            try:
                with open(retry_file_path, 'r', encoding='utf-8') as f:
                    files = json.load(f)
            except Exception as e:
                logging.error(f"Failed to read {args.retry_failed}: {e}")
                sys.exit(1)
            if not files:
                logging.info("No failed files to retry")
                sys.exit(0)
        else:
            config = load_config(args.config) if args.config else {}
            params = {
                'project': config.get('project', args.project),
                'activity_id': config.get('activity', args.activity),
                'experiment_id': config.get('experiment', args.experiment),
                'frequency': config.get('frequency', args.frequency),
                'variable_id': config.get('variable', args.variable),
                'source_id': config.get('model', args.model),
                'variant_label': config.get('ensemble', args.ensemble),
                'institution_id': config.get('institution', args.institution),
                'source_type': config.get('source_type', args.source_type),
                'grid_label': config.get('grid_label', args.grid_label),
                'nominal_resolution': config.get('resolution', args.resolution),
            }
            logging.debug(f"Query parameters: {params}")
            if args.latest or config.get('latest', False):
                params['latest'] = 'true'
            if args.extra_params:
                try:
                    params.update(json.loads(args.extra_params))
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid extra-params JSON: {e}")
                    sys.exit(1)
            params = {k: v for k, v in params.items() if v is not None}

            if args.demo or args.test:
                params = {
                    'project': 'CMIP6',
                    'variable_id': 'tas',
                    'source_id': 'CMCC-ESM2',
                    'frequency': 'mon',
                    'variant_label': 'r1i1p1f1',
                    'activity_id': 'ScenarioMIP',
                    'limit': '10'
                }
                if args.demo:
                    args.max_downloads = 10
                    logging.info("Downloading CMIP6 tas files in demo mode")

            if not params:
                logging.error("No valid search parameters provided")
                sys.exit(1)

            query_handler = QueryHandler(stop_event=getattr(args, 'stop_event', None))
            files = query_handler.fetch_datasets(params, args.timeout)
            if not files:
                logging.error("No files found matching the query")
                sys.exit(1)

            files_by_title = {f['title']: f for f in files if 'title' in f}
            removed_count = len(files) - len(files_by_title)
            if removed_count > 0:
                logging.debug(f"Removed {removed_count} files with duplicate titles")
            files = list(files_by_title.values())

            logging.info(f"Found {len(files)} files")

        file_manager = FileManager(args.output_dir, args.metadata_dir, args.save_mode, prefix, metadata_prefix)
        if not args.retry_failed:
            file_manager.save_metadata(files, "query_results.json")
        if args.dry_run:
            logging.info(f"Dry run: Would download {len(files)} files")
            sys.exit(0)

        downloader = Downloader(
            file_manager,
            args.workers,
            args.retries,
            args.timeout,
            args.max_downloads,
            args.id if hasattr(args, 'id') else None,
            args.password if hasattr(args, 'password') else None,
            not args.no_verify_ssl,
            args.openid if hasattr(args, 'openid') else None
        )
        try:
            downloaded, failed = downloader.download_all(files, phase="initial")
            if failed:
                file_manager.save_metadata(failed, "failed_downloads.json")
                logging.info(f"Retrying {len(failed)} failed downloads")
                retry_downloaded, retry_failed = downloader.retry_failed(failed)
                downloaded.extend(retry_downloaded)
                if retry_failed:
                    file_manager.save_metadata(retry_failed, "failed_downloads_final.json")
                    logging.error(f"{len(retry_failed)} downloads failed after retries.")
                else:
                    logging.info("All retries completed successfully")
            logging.info(f"Completed: {downloader.successful_downloads}/{len(files)} files downloaded successfully")
        finally:
            downloader.shutdown()
    except Exception as e:
        logging.critical(f"Unexpected error in run_download: {e}")
        raise