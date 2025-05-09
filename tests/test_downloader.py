import logging
import json
import pytest
import requests
from pathlib import Path
from unittest.mock import patch, MagicMock
from gridflow.downloader import setup_logging, FileManager, QueryHandler, Downloader, parse_args, load_config, run_download

# Fixture to reset logging before each test
@pytest.fixture(autouse=True)
def reset_logging():
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(logging.NOTSET)

@pytest.fixture
def sample_output_dir(tmp_path):
    output_dir = tmp_path / "cmip6_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

@pytest.fixture
def sample_metadata_dir(tmp_path):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    return metadata_dir

@pytest.fixture
def file_info():
    return {
        "title": "tas.nc",
        "activity_id": ["CMIP"],
        "nominal_resolution": ["100km"],
        "variable_id": ["tas"],
        "url": ["http://example.com/tas.nc|HTTPServer"],
        "checksum": ["abc123"],
        "checksum_type": ["md5"]
    }

def test_setup_logging(tmp_path):
    log_dir = tmp_path / "logs"
    logger = logging.getLogger()
    original_handlers = logger.handlers[:]
    
    with patch("logging.FileHandler") as mock_file_handler, patch("logging.StreamHandler") as mock_stream_handler:
        setup_logging(str(log_dir), "minimal", "test_")
        mock_file_handler.assert_called()
        mock_stream_handler.assert_called()
        assert logger.level == logging.CRITICAL
    
    logger.handlers = original_handlers

    with patch("logging.FileHandler"), patch("logging.StreamHandler"):
        setup_logging(str(log_dir), "debug", "test_")
        assert logger.level == logging.DEBUG
    
    logger.handlers = original_handlers

def test_file_manager_get_output_path_flat(sample_output_dir, sample_metadata_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    output_path = file_manager.get_output_path(file_info)
    expected = sample_output_dir / "CMIP_100km_tas.nc"
    assert output_path == expected

def test_file_manager_get_output_path_structured(sample_output_dir, sample_metadata_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "structured", "test_")
    output_path = file_manager.get_output_path(file_info)
    expected = sample_output_dir / "tas" / "100km" / "CMIP" / "tas.nc"
    assert output_path == expected

def test_file_manager_save_metadata(sample_output_dir, sample_metadata_dir):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    files = [{"title": "tas.nc"}]
    file_manager.save_metadata(files, "test.json")
    metadata_path = sample_metadata_dir / "test_test.json"
    assert metadata_path.exists()
    with open(metadata_path, "r") as f:
        assert json.load(f) == files

def test_query_handler_build_query():
    query_handler = QueryHandler()
    params = {"project": "CMIP6", "variable_id": "tas"}
    query = query_handler.build_query("https://example.com/search", params)
    assert "type=File" in query
    assert "format=application%2Fsolr%2Bjson" in query
    assert "limit=1000" in query
    assert "distrib=true" in query
    assert "project=CMIP6" in query
    assert "variable_id=tas" in query

def test_query_handler_fetch_datasets():
    query_handler = QueryHandler(nodes=["https://example.com/search"])
    params = {"project": "CMIP6"}
    mock_response = {
        "response": {
            "docs": [{"id": "file1", "title": "tas.nc"}, {"id": "file2", "title": "pr.nc"}],
            "numFound": 2
        }
    }
    
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = mock_response
        files = query_handler.fetch_datasets(params, timeout=10)
        assert len(files) == 2
        assert files[0]["id"] == "file1"
        assert files[1]["id"] == "file2"
        mock_get.assert_called()

def test_query_handler_fetch_datasets_pagination():
    query_handler = QueryHandler(nodes=["https://example.com/search"])
    params = {"project": "CMIP6"}
    mock_responses = [
        {
            "response": {
                "docs": [{"id": "file1", "title": "tas.nc"}],
                "numFound": 2
            }
        },
        {
            "response": {
                "docs": [{"id": "file2", "title": "pr.nc"}],
                "numFound": 2
            }
        }
    ]
    
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: mock_responses[0]),
            MagicMock(status_code=200, json=lambda: mock_responses[1])
        ]
        files = query_handler.fetch_datasets(params, timeout=10)
        assert len(files) == 2
        assert files[0]["id"] == "file1"
        assert files[1]["id"] == "file2"
        assert mock_get.call_count == 2

def test_downloader_verify_checksum(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    
    file_info["checksum"] = ["6af8307c2460f2d208ad254f04be4b0d"]
    assert downloader.verify_checksum(file_path, file_info) is True
    
    file_info["checksum"] = ["wrong_checksum"]
    assert downloader.verify_checksum(file_path, file_info) is False

def test_downloader_verify_checksum_sha256(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    
    file_info["checksum"] = ["e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"]
    file_info["checksum_type"] = ["sha256"]
    assert downloader.verify_checksum(file_path, file_info) is True

def test_downloader_verify_checksum_unsupported(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    
    file_info["checksum"] = ["some_checksum"]
    file_info["checksum_type"] = ["unknown"]
    assert downloader.verify_checksum(file_path, file_info) is True

def test_downloader_download_file(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    output_path = sample_output_dir / "CMIP_100km_tas.nc"
    
    with patch.object(downloader.session, "get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.iter_content.return_value = [b"fake_data"]
        mock_get.return_value = mock_response
        
        with patch.object(downloader, "verify_checksum", return_value=True):
            path, failed_info = downloader.download_file(file_info)
            assert path == str(output_path)
            assert failed_info is None
            assert output_path.exists()

def test_downloader_download_file_failure(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    
    with patch.object(downloader.session, "get") as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException("Failed")
        
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info
        assert not (sample_output_dir / "CMIP_100km_tas.nc").exists()

def test_downloader_download_file_empty_urls(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    file_info["url"] = []
    
    path, failed_info = downloader.download_file(file_info)
    assert path is None
    assert failed_info == file_info
    assert not (sample_output_dir / "CMIP_100km_tas.nc").exists()

def test_downloader_download_file_retries(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=2, timeout=10, max_downloads=None,
        id=None, password=None, verify_ssl=True
    )
    output_path = sample_output_dir / "CMIP_100km_tas.nc"
    
    with patch.object(downloader.session, "get") as mock_get, \
         patch("time.sleep") as mock_sleep:
        mock_get.side_effect = [
            requests.exceptions.RequestException("Failed"),
            MagicMock(status_code=200, iter_content=lambda chunk_size: [b"fake_data"])
        ]
        
        with patch.object(downloader, "verify_checksum", return_value=True):
            path, failed_info = downloader.download_file(file_info)
            assert path == str(output_path)
            assert failed_info is None
            assert output_path.exists()
            assert mock_get.call_count == 2
            mock_sleep.assert_called_with(2)

def test_downloader_with_authentication(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=None,
        id="user", password="pass", verify_ssl=True
    )
    assert downloader.session.auth == ("user", "pass")
    
    with patch.object(downloader.session, "get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.iter_content.return_value = [b"fake_data"]
        mock_get.return_value = mock_response
        
        with patch.object(downloader, "verify_checksum", return_value=True):
            path, failed_info = downloader.download_file(file_info)
            assert path == str(sample_output_dir / "CMIP_100km_tas.nc")
            assert failed_info is None
            mock_get.assert_called_with(
                "http://example.com/tas.nc", stream=True, timeout=10, verify=True
            )

def test_downloader_download_all(sample_output_dir, file_info):
    downloader = Downloader(
        FileManager(str(sample_output_dir), str(sample_output_dir), "flat"),
        max_workers=1, retries=1, timeout=10, max_downloads=2,
        id=None, password=None, verify_ssl=True
    )
    files = [file_info, {**file_info, "title": "pr.nc", "url": ["http://example.com/pr.nc|HTTPServer"]}]
    
    with patch.object(downloader, "download_file") as mock_download:
        mock_download.side_effect = [
            (str(sample_output_dir / "CMIP_100km_tas.nc"), None),
            (str(sample_output_dir / "CMIP_100km_pr.nc"), None)
        ]
        downloaded, failed = downloader.download_all(files, phase="test")
        assert len(downloaded) == 2
        assert len(failed) == 0
        assert downloaded == [str(sample_output_dir / "CMIP_100km_tas.nc"), str(sample_output_dir / "CMIP_100km_pr.nc")]

def test_parse_args():
    with patch("sys.argv", ["downloader.py", "--project", "CMIP6", "--variable", "tas", "--output-dir", "./data"]):
        args = parse_args()
        assert args.project == "CMIP6"
        assert args.variable == "tas"
        assert args.output_dir == "./data"
        assert args.log_level == "minimal"
        assert args.save_mode == "flat"

def test_load_config(tmp_path):
    config_path = tmp_path / "config.json"
    config_data = {"project": "CMIP6", "variable": "tas"}
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    
    config = load_config(str(config_path))
    assert config == config_data

def test_load_config_invalid(tmp_path):
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        f.write("invalid json")
    
    with patch("sys.exit") as mock_exit, patch("logging.error") as mock_error:
        load_config(str(config_path))
        mock_exit.assert_called_with(1)
        mock_error.assert_called()

def test_run_download_demo_mode(sample_output_dir, sample_metadata_dir):
    with patch("gridflow.downloader.QueryHandler.fetch_datasets") as mock_fetch, \
         patch("gridflow.downloader.Downloader.download_all") as mock_download, \
         patch("gridflow.downloader.FileManager.save_metadata") as mock_save:
        mock_fetch.return_value = [{"title": "tas.nc", "id": "file1"}]
        mock_download.return_value = ([str(sample_output_dir / "CMIP_100km_tas.nc")], [])
        
        args = MagicMock()
        args.demo = True
        args.output_dir = str(sample_output_dir)
        args.metadata_dir = str(sample_metadata_dir)
        args.log_dir = str(sample_output_dir / "logs")
        args.log_level = "minimal"
        args.save_mode = "flat"
        args.workers = 4
        args.retries = 5
        args.timeout = 30
        args.max_downloads = 10
        args.id = None
        args.password = None
        args.no_verify_ssl = False
        args.retry_failed = None
        args.config = None
        args.dry_run = False
        args.extra_params = None
        
        run_download(args)
        mock_fetch.assert_called()
        mock_download.assert_called()
        mock_save.assert_called()

def test_run_download_dry_run(sample_output_dir, sample_metadata_dir):
    with patch("gridflow.downloader.QueryHandler.fetch_datasets") as mock_fetch, \
         patch("gridflow.downloader.FileManager.save_metadata") as mock_save, \
         patch("sys.exit") as mock_exit:
        mock_fetch.return_value = [{"title": "tas.nc", "id": "file1"}]
        
        args = MagicMock()
        args.dry_run = True
        args.output_dir = str(sample_output_dir)
        args.metadata_dir = str(sample_metadata_dir)
        args.log_dir = str(sample_output_dir / "logs")
        args.log_level = "minimal"
        args.save_mode = "flat"
        args.workers = 4
        args.retries = 5
        args.timeout = 30
        args.max_downloads = None
        args.id = None
        args.password = None
        args.no_verify_ssl = False
        args.retry_failed = None
        args.config = None
        args.demo = False
        args.test = False
        args.project = "CMIP6"
        args.activity = "CMIP"
        args.resolution = "100km"
        args.extra_params = None
        
        run_download(args)
        mock_fetch.assert_called()
        mock_save.assert_called()
        mock_exit.assert_called_with(0)

def test_run_download_retry_failed_success(sample_output_dir, sample_metadata_dir):
    failed_file = sample_metadata_dir / "failed_downloads.json"
    failed_data = [{"title": "tas.nc", "id": "file1", "activity_id": ["CMIP"], "nominal_resolution": ["100km"]}]
    with open(failed_file, "w") as f:
        json.dump(failed_data, f)
    
    with patch("gridflow.downloader.Downloader.download_all") as mock_download, \
         patch("gridflow.downloader.FileManager.save_metadata") as mock_save, \
         patch("sys.exit") as mock_exit:
        mock_download.return_value = ([str(sample_output_dir / "CMIP_100km_tas.nc")], [])
        
        args = MagicMock()
        args.retry_failed = str(failed_file)
        args.output_dir = str(sample_output_dir)
        args.metadata_dir = str(sample_metadata_dir)
        args.log_dir = str(sample_output_dir / "logs")
        args.log_level = "minimal"
        args.save_mode = "flat"
        args.workers = 4
        args.retries = 5
        args.timeout = 30
        args.max_downloads = None
        args.id = None
        args.password = None
        args.no_verify_ssl = False
        args.dry_run = False
        args.demo = False
        args.test = False
        args.config = None
        args.extra_params = None
        
        run_download(args)
        mock_download.assert_called()
        mock_save.assert_not_called()
        mock_exit.assert_not_called()

def test_run_download_retry_failed_with_failures(sample_output_dir, sample_metadata_dir):
    failed_file = sample_metadata_dir / "failed_downloads.json"
    failed_data = [
        {"title": "tas.nc", "id": "file1", "activity_id": ["CMIP"], "nominal_resolution": ["100km"]},
        {"title": "pr.nc", "id": "file2", "activity_id": ["CMIP"], "nominal_resolution": ["100km"]}
    ]
    with open(failed_file, "w") as f:
        json.dump(failed_data, f)
    
    with patch("gridflow.downloader.Downloader.download_all") as mock_download, \
         patch("gridflow.downloader.FileManager.save_metadata") as mock_save, \
         patch("sys.exit") as mock_exit:
        mock_download.side_effect = [
            ([str(sample_output_dir / "CMIP_100km_tas.nc")], [failed_data[1]]),
            ([], [failed_data[1]])
        ]
        
        args = MagicMock()
        args.retry_failed = str(failed_file)
        args.output_dir = str(sample_output_dir)
        args.metadata_dir = str(sample_metadata_dir)
        args.log_dir = str(sample_output_dir / "logs")
        args.log_level = "minimal"
        args.save_mode = "flat"
        args.workers = 4
        args.retries = 5
        args.timeout = 30
        args.max_downloads = None
        args.id = None
        args.password = None
        args.no_verify_ssl = False
        args.dry_run = False
        args.demo = False
        args.test = False
        args.config = None
        args.extra_params = None
        
        run_download(args)
        mock_download.assert_called()
        mock_save.assert_called()
        mock_exit.assert_not_called()

def test_download_file_network_error(sample_output_dir):
    """Test download failure due to network error (covers lines 217–222)."""
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=30, max_downloads=1, id=None, password=None, verify_ssl=True)
    file_info = {"title": "tas.nc", "url": ["http://example.com/tas.nc|HTTPServer"]}
    with patch("requests.get", side_effect=requests.exceptions.RequestException("Network error")):
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info

def test_download_file_retry_exhausted(sample_output_dir):
    """Test retry logic exhaustion (covers lines 217–222)."""
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=2, timeout=30, max_downloads=1, id=None, password=None, verify_ssl=True)
    file_info = {"title": "tas.nc", "url": ["http://example.com/tas.nc|HTTPServer"]}
    with patch("requests.get", side_effect=[requests.exceptions.ConnectionError("Connection error")] * 3), \
         patch("time.sleep") as mock_sleep:
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info
        assert mock_sleep.call_count == 1  # One retry due to immediate failure

def test_fetch_datasets_empty_response(sample_output_dir, sample_metadata_dir):
    """Test QueryHandler.fetch_datasets with empty response (covers lines 388–392)."""
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat")
    query_handler = QueryHandler(nodes=["https://example.com/search"])
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"response": {"docs": []}}
    with patch("requests.get", return_value=mock_response):
        params = {"project": "CMIP6"}
        datasets = query_handler.fetch_datasets(params, timeout=10)
        assert datasets == []

def test_fetch_datasets_auth_error(sample_output_dir, sample_metadata_dir):
    """Test QueryHandler.fetch_datasets with authentication error (covers lines 388–392)."""
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat")
    query_handler = QueryHandler(nodes=["https://example.com/search"])
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401 Client Error: Unauthorized")
    with patch("requests.get", return_value=mock_response):
        params = {"project": "CMIP6"}
        datasets = query_handler.fetch_datasets(params, timeout=10)
        assert datasets == []

def test_download_file_invalid_url(sample_output_dir):
    """Test download with invalid URL (covers lines 201–203)."""
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=30, max_downloads=1, id=None, password=None, verify_ssl=True)
    file_info = {"title": "tas.nc", "url": ["invalid://url|HTTPServer"]}
    with patch("requests.get", side_effect=requests.exceptions.InvalidURL("Invalid URL")):
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info

def test_run_download_no_files(sample_output_dir, sample_metadata_dir):
    """Test run_download with no files found (covers lines 359–361)."""
    args = MagicMock()
    args.project = "CMIP6"
    args.output_dir = str(sample_output_dir)
    args.metadata_dir = str(sample_metadata_dir)
    args.log_dir = str(sample_output_dir / "logs")
    args.log_level = "minimal"
    args.save_mode = "flat"
    args.workers = 4
    args.retries = 5
    args.timeout = 30
    args.max_downloads = 10
    args.id = None
    args.password = None
    args.no_verify_ssl = False
    args.retry_failed = None
    args.config = None
    args.dry_run = False
    args.demo = False
    args.extra_params = None
    with patch("gridflow.downloader.QueryHandler.fetch_datasets", return_value=[]), \
         patch("sys.exit") as mock_exit:
        run_download(args)
        mock_exit.assert_called_with(1)