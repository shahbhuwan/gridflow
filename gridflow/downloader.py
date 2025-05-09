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
import math
import json
import time
import logging
import requests
import argparse
from pathlib import Path
from threading import Lock
from datetime import datetime
from hashlib import md5, sha256
from urllib.parse import urlencode
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

__version__ = "0.2.3"

ESGF_NODES = [
    "https://esgf-node.llnl.gov/esg-search/search",
    "https://esgf-node.ipsl.upmc.fr/esg-search/search",
    "https://esgf-data.dkrz.de/esg-search/search",
    "https://esgf-index1.ceda.ac.uk/esg-search/search"
]

def setup_logging(log_dir: str, level: str, prefix: str = "") -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{prefix}cmip6_download_{timestamp}.log"
    failed_log_file = log_dir / f"{prefix}cmip6_failed_downloads_{timestamp}.log"

    log_levels = {
        'minimal': logging.CRITICAL,
        'normal': logging.INFO,
        'verbose': logging.DEBUG,
        'debug': logging.DEBUG
    }
    numeric_level = log_levels.get(level.lower(), logging.CRITICAL)

    class MinimalFilter(logging.Filter):
        def filter(self, record):
            return record.levelno >= logging.CRITICAL if level.lower() == 'minimal' else True

    handlers = [
        logging.FileHandler(log_file),
        logging.FileHandler(failed_log_file),
        logging.StreamHandler(sys.stdout)
    ]
    handlers[2].addFilter(MinimalFilter())

    format_str = '%(message)s' if level.lower() == 'minimal' else '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(
        level=numeric_level,
        format=format_str,
        handlers=handlers,
        force=True
    )
    logging.addLevelName(logging.CRITICAL, "MINIMAL")

class FileManager:
    def __init__(self, download_dir: str, metadata_dir: str, save_mode: str, prefix: str = ""):
        self.download_dir = Path(download_dir)
        self.metadata_dir = Path(metadata_dir)
        self.save_mode = save_mode.lower()
        self.prefix = prefix
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def get_output_path(self, file_info: Dict) -> Path:
        filename = file_info.get('title', '')
        activity_id = file_info.get('activity_id', ['unknown'])[0]
        resolution = file_info.get('nominal_resolution', ['unknown'])[0].replace(' ', '')
        variable = file_info.get('variable_id', ['unknown'])[0]

        if self.save_mode == 'flat':
            prefixed_filename = f"{activity_id}_{resolution}_{filename}"
            return self.download_dir / prefixed_filename
        else:
            subdir = self.download_dir / variable / resolution / activity_id
            subdir.mkdir(parents=True, exist_ok=True)
            return subdir / filename

    def save_metadata(self, files: List[Dict], filename: str) -> None:
        metadata_path = self.metadata_dir / f"{self.prefix}{filename}"
        try:
            with open(metadata_path, 'w') as f:
                json.dump(files, f, indent=2)
            logging.debug(f"Saved metadata to {metadata_path}")
        except Exception as e:
            logging.error(f"Failed to save metadata {filename}: {e}")

class QueryHandler:
    def __init__(self, nodes: List[str] = ESGF_NODES):
        self.nodes = nodes
        self.session = requests.Session()

    def build_query(self, base_url: str, params: Dict[str, str]) -> str:
        query_params = {
            'type': 'File',
            'format': 'application/solr+json',
            'limit': '1000',
            'distrib': 'true',
            **params
        }
        return f"{base_url}?{urlencode(query_params)}"

    def fetch_datasets(self, params: Dict[str, str], timeout: int) -> List[Dict]:
        files = []
        seen_ids = set()
        for node in self.nodes:
            try:
                logging.critical(f"Trying to connect to {node}")
                logging.info(f"Attempting to query node: {node}")
                files = self._fetch_from_node(node, params, timeout)
                unique_files = []
                for f in files:
                    file_id = f.get('id', '')
                    if file_id and file_id not in seen_ids:
                        seen_ids.add(file_id)
                        unique_files.append(f)
                files = unique_files
                if files:
                    logging.critical(f"Connected to {node}")
                    logging.info(f"Successfully queried node: {node}, {len(files)} unique files")
                    return files
                logging.warning(f"No files found at {node}, trying next node")
            except requests.RequestException as e:
                logging.error(f"Failed to connect to {node}: {e}")
                continue
        logging.error("All nodes failed to respond")
        return files

    def _fetch_from_node(self, node: str, params: Dict[str, str], timeout: int) -> List[Dict]:
        files = []
        offset = 0
        while True:
            query_params = {**params, 'offset': str(offset)}
            query_url = self.build_query(node, query_params)
            logging.debug(f"Querying: {query_url}")
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
        return files

class Downloader:
    def __init__(self, file_manager: FileManager, max_workers: int, retries: int, timeout: int, max_downloads: int, id: Optional[str], password: Optional[str], verify_ssl: bool):
        self.file_manager = file_manager
        self.max_workers = max_workers
        self.retries = retries
        self.timeout = timeout
        self.max_downloads = max_downloads
        self.session = requests.Session()
        if id and password:
            self.session.auth = (id, password)
        self.verify_ssl = verify_ssl
        self.log_lock = Lock()

    def verify_checksum(self, file_path: Path, file_info: Dict) -> bool:
        checksum = file_info.get('checksum', [''])[0]
        if not checksum:
            logging.warning(f"No checksum provided for {file_path.name}")
            return True
        try:
            checksum_type = file_info.get('checksum_type', ['md5'])[0].lower()
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
        urls = file_info.get('url', [])
        filename = file_info.get('title', '')
        if not urls or not filename:
            with self.log_lock:
                logging.error(f"Invalid file info: {file_info}")
            return None, file_info

        output_path = self.file_manager.get_output_path(file_info)
        temp_path = output_path.with_suffix(output_path.suffix + '.tmp')

        if output_path.exists():
            if self.verify_checksum(output_path, file_info):
                with self.log_lock:
                    logging.critical(f"Downloaded {filename} (already exists)")
                return str(output_path), None
            else:
                output_path.unlink()

        for url in urls:
            download_url = url.split('|')[0]
            try:
                with self.log_lock:
                    logging.critical(f"Downloading {filename}")
                    logging.info(f"Downloading {filename} from {download_url} (Attempt {attempt}/{self.retries})")
                response = self.session.get(download_url, stream=True, timeout=self.timeout, verify=self.verify_ssl)
                response.raise_for_status()

                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                if self.verify_checksum(temp_path, file_info):
                    temp_path.rename(output_path)
                    with self.log_lock:
                        logging.critical(f"Downloaded {filename}")
                    return str(output_path), None
                else:
                    temp_path.unlink()
                    raise ValueError("Checksum verification failed")

            except (requests.RequestException, ValueError) as e:
                with self.log_lock:
                    logging.warning(f"Download failed for {filename} from {download_url}: {e}")
                if attempt < self.retries:
                    time.sleep(2 ** attempt)
                    return self.download_file(file_info, attempt + 1)
                continue
        with self.log_lock:
            logging.critical(f"Failed to download {filename}")
        return None, file_info

    def download_all(self, files: List[Dict], phase: str = "initial") -> Tuple[List[str], List[Dict]]:
        downloaded_files = []
        failed_files = []
        total_files = min(len(files), self.max_downloads) if self.max_downloads else len(files)
        if total_files == 0:
            return [], []

        progress_interval = max(1, total_files // 10)
        completed = 0
        next_threshold = progress_interval
        actually_downloaded = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self.download_file, f): f for f in files[:total_files]}
            for future in as_completed(future_to_file):
                path, failed_info = future.result()
                if path:
                    downloaded_files.append(path)
                    if not failed_info:
                        actually_downloaded += 1
                if failed_info:
                    failed_files.append(failed_info)
                completed += 1
                with self.log_lock:
                    if completed >= next_threshold:
                        percentage = math.ceil((completed / total_files) * 100)
                        logging.critical(f"Progress: {percentage}% ({completed}/{total_files} files)")
                        logging.info(f"Progress ({phase}): {percentage}% ({completed}/{total_files} files)")
                        next_threshold += progress_interval

        with self.log_lock:
            if completed == total_files and completed <= next_threshold - progress_interval:
                logging.critical(f"Progress: 100% ({completed}/{total_files} files)")
                logging.info(f"Progress ({phase}): 100% ({completed}/{total_files} files)")

        return downloaded_files, failed_files

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download CMIP6 data from ESGF nodes with deduplication and error handling.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('-p', '--project', default='CMIP6', help="Project name")
    parser.add_argument('-e', '--experiment', help="Experiment ID")
    parser.add_argument('-var', '--variable', help="Variable name")
    parser.add_argument('-f', '--frequency', help="Time frequency")
    parser.add_argument('-m', '--model', help="Source ID/Model")
    parser.add_argument('-r', '--resolution', help="Nominal resolution")
    parser.add_argument('-en', '--ensemble', help="Ensemble member")
    parser.add_argument('-a', '--activity', help="Activity ID (e.g., CMIP, ScenarioMIP)")
    parser.add_argument('-i', '--institution', help="Institution ID (e.g., NCAR)")
    parser.add_argument('-s', '--source-type', help="Source type (e.g., AOGCM, BGC)")
    parser.add_argument('-g', '--grid-label', help="Grid label (e.g., gn, gr)")
    parser.add_argument('-x', '--extra-params', help="Additional query parameters as JSON")
    parser.add_argument('--latest', action='store_true', help="Retrieve only the latest version")
    parser.add_argument('-out', '--output-dir', default='./cmip6_data', help="Download directory")
    parser.add_argument('-log', '--log-dir', default='./logs', help="Log directory")
    parser.add_argument('-meta', '--metadata-dir', default='./metadata', help="Metadata directory")
    parser.add_argument('-w', '--workers', type=int, default=4, help="Number of parallel threads")
    parser.add_argument('-retries', '--retries', type=int, default=5, help="Number of retries")
    parser.add_argument('-t', '--timeout', type=int, default=30, help="HTTP timeout (seconds)")
    parser.add_argument('-n', '--max-downloads', type=int, help="Max files to download")
    parser.add_argument('-S', '--save-mode', choices=['flat', 'structured'], default='flat', help="Save mode")
    parser.add_argument('-id', '--id', help="ESGF username")
    parser.add_argument('-pass', '--password', help="ESGF password")
    parser.add_argument('-c', '--config', help="JSON config file")
    parser.add_argument('-L', '--log-level', choices=['minimal', 'normal', 'verbose', 'debug'], default='minimal', help="Logging level")
    parser.add_argument('-d', '--dry-run', action='store_true', help="Simulate download")
    parser.add_argument('-T', '--test', action='store_true', help="Run test dataset")
    parser.add_argument('--demo', action='store_true', help="Run in demo mode with default settings")
    parser.add_argument('--no-verify-ssl', action='store_true', help="Disable SSL verification")
    parser.add_argument('--retry-failed', help="Path to failed_downloads.json to retry downloading those files")
    return parser.parse_args()

def load_config(config_path: str) -> Dict:
    if not config_path:
        return {}
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.debug(f"Loaded configuration from {config_path}")
        return config
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Failed to load config file {config_path}: {e}")
        sys.exit(1)

def run_download(args: argparse.Namespace) -> None:
    if not args.retry_failed:
        activity_id = getattr(args, 'activity', None) or 'unknown'
        resolution = getattr(args, 'resolution', None) or 'unknown'
        if args.demo or args.test:
            activity_id = 'CMIP'
            resolution = '100km'
        resolution = resolution.replace(' ', '') if resolution else 'unknown'
        prefix = f"{activity_id}_{resolution}_"
        setup_logging(args.log_dir, args.log_level, prefix)

    if args.retry_failed:
        try:
            with open(args.retry_failed, 'r') as f:
                files = json.load(f)
        except Exception as e:
            logging.error(f"Failed to read {args.retry_failed}: {e}")
            sys.exit(1)
        if not files:
            logging.info("No failed files to retry")
            sys.exit(0)
        first_file = files[0]
        activity_id = first_file.get('activity_id', ['unknown'])[0]
        resolution = first_file.get('nominal_resolution', ['unknown'])[0].replace(' ', '')
        prefix = f"{activity_id}_{resolution}_"
        setup_logging(args.log_dir, args.log_level, prefix)
    else:
        config = load_config(args.config)
        params = {
            'project': config.get('project', args.project),
            'activity_id': config.get('activity', args.activity),
            'experiment_id': config.get('experiment', args.experiment),
            'variable_id': config.get('variable', args.variable),
            'frequency': config.get('frequency', args.frequency),
            'source_id': config.get('model', args.model),
            'nominal_resolution': config.get('resolution', args.resolution),
            'variant_label': config.get('ensemble', args.ensemble),
            'institution_id': config.get('institution', args.institution),
            'source_type': config.get('source_type', args.source_type),
            'grid_label': config.get('grid_label', args.grid_label),
        }
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
            params = {'project': 'CMIP6', 'variable_id': 'tas', 'source_id': 'CESM2', 'experiment_id': 'historical', 'limit': '10'}
            if args.demo:
                args.workers = max(1, int(os.cpu_count() * 0.75))
                args.max_downloads = 10
                logging.critical("Downloading CMIP6 tas files in demo mode")

        if not params:
            logging.error("No valid search parameters provided")
            sys.exit(1)

        query_handler = QueryHandler()
        files = query_handler.fetch_datasets(params, args.timeout)
        if not files:
            logging.error("No files found matching the query")
            sys.exit(1)

        files_by_title = {f['title']: f for f in files if 'title' in f}
        files = list(files_by_title.values())
        logging.info(f"Deduplicated to {len(files)} unique files")

    file_manager = FileManager(args.output_dir, args.metadata_dir, args.save_mode, prefix)
    if not args.retry_failed:
        file_manager.save_metadata(files, "query_results.json")
    if args.dry_run:
        logging.critical(f"Would download {len(files)} files")
        logging.info(f"Dry run: Would download {len(files)} files")
        sys.exit(0)

    downloader = Downloader(file_manager, args.workers, args.retries, args.timeout, args.max_downloads, args.id, args.password, not args.no_verify_ssl)
    downloaded, failed = downloader.download_all(files, phase="initial")
    actually_downloaded = len([p for p in downloaded if not Path(p).exists() or not any(f.get('title') == Path(p).name for f in failed)])
    if failed:
        file_manager.save_metadata(failed, "failed_downloads.json")
        logging.info(f"Retrying {len(failed)} failed downloads")
        retry_downloaded, retry_failed = downloader.download_all(failed, phase="retry")
        downloaded.extend(retry_downloaded)
        actually_downloaded += len([p for p in retry_downloaded if not Path(p).exists() or not any(f.get('title') == Path(p).name for f in retry_failed)])
        if retry_failed:
            file_manager.save_metadata(retry_failed, "failed_downloads_final.json")
            logging.error(f"{len(retry_failed)} downloads failed after retries")

    logging.critical("Completed")
    logging.info(f"Download complete. {actually_downloaded} files downloaded to {args.output_dir}, {len(failed)} failed")