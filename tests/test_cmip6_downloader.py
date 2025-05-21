import logging
import json
import pytest
import requests
from pathlib import Path
from unittest.mock import patch, MagicMock
from threading import Event
from gridflow.cmip6_downloader import FileManager, QueryHandler, Downloader, load_config, parse_file_time_range, run_download, InterruptibleSession

# Fixture to reset logging before each test
@pytest.fixture(autouse=True)
def reset_logging():
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(logging.NOTSET)
    yield
    logger.handlers = []  # Ensure cleanup after each test

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
        "id": "file1",
        "title": "tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc",
        "activity_id": ["ScenarioMIP"],
        "nominal_resolution": ["100km"],
        "variable_id": ["tas"],
        "source_id": ["CMCC-ESM2"],
        "experiment_id": ["ssp585"],
        "frequency": ["mon"],
        "variant_label": ["r1i1p1f1"],
        "url": ["http://example.com/tas.nc|HTTPServer"],
        "checksum": ["e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"],
        "checksum_type": ["sha256"]
    }

@pytest.fixture
def stop_event():
    return Event()

def test_file_manager_init_directory_creation(sample_output_dir, sample_metadata_dir):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    assert sample_output_dir.exists()
    assert sample_metadata_dir.exists()

def test_file_manager_init_directory_failure(sample_output_dir, sample_metadata_dir, caplog):
    with patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied")):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat")
            assert "Failed to create directories" in caplog.text

def test_file_manager_get_output_path_flat(sample_output_dir, sample_metadata_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    output_path = file_manager.get_output_path(file_info)
    expected = sample_output_dir / "test_ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    assert output_path == expected

def test_file_manager_get_output_path_structured(sample_output_dir, sample_metadata_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "structured", "test_")
    output_path = file_manager.get_output_path(file_info)
    expected = sample_output_dir / "tas" / "100km" / "ScenarioMIP" / "tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    assert output_path == expected

def test_file_manager_get_output_path_resolution_fallback(sample_output_dir, sample_metadata_dir, file_info):
    file_info_modified = file_info.copy()
    file_info_modified["nominal_resolution"] = [""]
    file_info_modified["source_id"] = ["CanESM5"]
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    output_path = file_manager.get_output_path(file_info_modified)
    expected = sample_output_dir / "test_ScenarioMIP_250km_tas_Amon_CanESM5_ssp585_r1i1p1f1_gn_201501-210012.nc"
    assert output_path == expected

def test_file_manager_get_output_path_missing_info(sample_output_dir, sample_metadata_dir):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", "test_")
    file_info = {"title": "tas.nc"}
    output_path = file_manager.get_output_path(file_info)
    expected = sample_output_dir / "test_unknown_unknown_tas.nc"
    assert output_path == expected

def test_file_manager_save_metadata(sample_output_dir, sample_metadata_dir):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat", metadata_prefix="gridflow_cmip6_")
    files = [{"title": "tas.nc"}]
    file_manager.save_metadata(files, "results.json")
    metadata_path = sample_metadata_dir / "gridflow_cmip6_results.json"
    assert metadata_path.exists()
    with open(metadata_path, "r") as f:
        assert json.load(f) == files

def test_file_manager_save_metadata_failure(sample_output_dir, sample_metadata_dir, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat")
    with patch("builtins.open", side_effect=OSError("Write error")):
        with caplog.at_level(logging.ERROR):
            file_manager.save_metadata([{"title": "tas.nc"}], "results.json")
            assert "Failed to save metadata" in caplog.text

def test_query_handler_build_query():
    query_handler = QueryHandler()
    params = {"project": "CMIP6", "variable_id": "tas", "experiment_id": "ssp585"}
    query = query_handler.build_query("https://example.com/search", params)
    assert "type=File" in query
    assert "project=CMIP6" in query
    assert "variable_id=tas" in query
    assert "experiment_id=ssp585" in query
    assert "format=application%2Fsolr%2Bjson" in query
    assert "limit=1000" in query
    assert "distrib=true" in query

def test_query_handler_fetch_datasets_success(file_info, stop_event):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    params = {"project": "CMIP6", "variable_id": "tas"}
    mock_response = {
        "response": {
            "docs": [file_info, {**file_info, "id": "file2", "title": "pr.nc"}],
            "numFound": 2
        }
    }
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        files = query_handler.fetch_datasets(params, timeout=10)
        assert len(files) == 2
        assert files[0]["id"] == "file1"
        assert files[1]["id"] == "file2"
        mock_get.assert_called()

def test_query_handler_fetch_datasets_pagination(file_info, stop_event):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    params = {"project": "CMIP6"}
    mock_responses = [
        {"response": {"docs": [file_info], "numFound": 2}},
        {"response": {"docs": [{**file_info, "id": "file2", "title": "pr.nc"}], "numFound": 2}}
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

def test_query_handler_fetch_datasets_empty_response(stop_event, caplog):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    params = {"project": "CMIP6"}
    mock_response = {"response": {"docs": [], "numFound": 0}}
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                query_handler.fetch_datasets(params, timeout=10)
            assert "All nodes failed to respond or no files were found" in caplog.text

def test_query_handler_fetch_datasets_auth_error(stop_event, caplog):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    params = {"project": "CMIP6"}
    mock_response = MagicMock(status_code=401)
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
    with patch.object(query_handler.session, "get", return_value=mock_response):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                query_handler.fetch_datasets(params, timeout=10)
            assert "Failed to connect to https://example.com/search" in caplog.text

def test_query_handler_fetch_datasets_multiple_nodes(file_info, stop_event):
    query_handler = QueryHandler(nodes=["https://node1.com/search", "https://node2.com/search"], stop_event=stop_event)
    params = {"project": "CMIP6"}
    mock_response = {"response": {"docs": [file_info], "numFound": 1}}
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        files = query_handler.fetch_datasets(params, timeout=10)
        assert len(files) == 1
        assert files[0]["id"] == "file1"
        assert mock_get.call_count == 1  # Should stop after first successful node

def test_query_handler_fetch_specific_file(file_info, stop_event):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    mock_response = {"response": {"docs": [file_info], "numFound": 1}}
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        result = query_handler.fetch_specific_file(file_info, timeout=10)
        assert result == file_info
        mock_get.assert_called()

def test_query_handler_fetch_specific_file_not_found(file_info, stop_event, caplog):
    query_handler = QueryHandler(nodes=["https://example.com/search"], stop_event=stop_event)
    mock_response = {"response": {"docs": [], "numFound": 0}}
    with patch.object(query_handler.session, "get") as mock_get:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: mock_response)
        with caplog.at_level(logging.ERROR):
            result = query_handler.fetch_specific_file(file_info, timeout=10)
            assert result is None
            assert "Failed to find file tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc at any node" in caplog.text

def test_interruptible_session_stop_event(stop_event):
    session = InterruptibleSession(stop_event)
    stop_event.set()
    with pytest.raises(requests.exceptions.RequestException, match="Download interrupted by user"):
        session.get("http://example.com/tas.nc")

def test_downloader_init_authentication(file_info, sample_output_dir, sample_metadata_dir, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_metadata_dir), "flat")
    with caplog.at_level(logging.WARNING):
        downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username="user", password="pass", verify_ssl=True)
        assert downloader.session.auth == ("user", "pass")
        assert "Using basic authentication" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
        assert downloader.session.auth is None
        assert "No authentication credentials provided" in caplog.text

def test_downloader_verify_checksum_sha256(sample_output_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    assert downloader.verify_checksum(file_path, file_info) is True

def test_downloader_verify_checksum_md5(sample_output_dir, file_info):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    file_info["checksum"] = ["6af8307c2460f2d208ad254f04be4b0d"]
    file_info["checksum_type"] = ["md5"]
    assert downloader.verify_checksum(file_path, file_info) is True

def test_downloader_verify_checksum_unsupported(sample_output_dir, file_info, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    file_info["checksum_type"] = ["unknown"]
    with caplog.at_level(logging.WARNING):
        assert downloader.verify_checksum(file_path, file_info) is True
        assert "Unsupported checksum type unknown" in caplog.text

def test_downloader_verify_checksum_failure(sample_output_dir, file_info, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_path = sample_output_dir / "tas.nc"
    with open(file_path, "wb") as f:
        f.write(b"test_data")
    file_info["checksum"] = ["wrong_checksum"]
    with caplog.at_level(logging.ERROR):
        assert downloader.verify_checksum(file_path, file_info) is False
        assert "Checksum mismatch" in caplog.text

def test_downloader_download_file_success(sample_output_dir, file_info, stop_event):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    output_path = sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    with patch.object(downloader.session, "get") as mock_get:
        mock_response = MagicMock(status_code=200)
        mock_response.iter_content.return_value = [b"test_data"]
        mock_get.return_value = mock_response
        path, failed_info = downloader.download_file(file_info)
        assert path == str(output_path)
        assert failed_info is None
        assert output_path.exists()

def test_downloader_download_file_existing_valid(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    output_path = sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    with open(output_path, "wb") as f:
        f.write(b"test_data")
    with caplog.at_level(logging.INFO):
        path, failed_info = downloader.download_file(file_info)
        assert path == str(output_path)
        assert failed_info is None
        assert "already exists" in caplog.text

def test_downloader_download_file_network_error(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    with patch.object(downloader.session, "get", side_effect=requests.exceptions.RequestException("Network error")):
        with caplog.at_level(logging.ERROR):
            path, failed_info = downloader.download_file(file_info)
            assert path is None
            assert failed_info == file_info
            assert "Failed to download tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc after 1 attempts" in caplog.text

def test_downloader_download_file_retries(sample_output_dir, file_info, stop_event):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=2, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    output_path = sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    with patch.object(downloader.session, "get") as mock_get, patch("time.sleep") as mock_sleep:
        mock_get.side_effect = [
            requests.exceptions.RequestException("Failed"),
            MagicMock(status_code=200, iter_content=lambda chunk_size: [b"test_data"])
        ]
        path, failed_info = downloader.download_file(file_info)
        assert path == str(output_path)
        assert failed_info is None
        assert output_path.exists()
        assert mock_get.call_count == 2
        mock_sleep.assert_called_with(7)  # 2^1 + 5

def test_downloader_download_file_invalid_url(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_info["url"] = ["invalid://url|HTTPServer"]
    with patch.object(downloader.session, "get", side_effect=requests.exceptions.InvalidURL("Invalid URL")):
        with caplog.at_level(logging.ERROR):
            path, failed_info = downloader.download_file(file_info)
            assert path is None
            assert failed_info == file_info
            assert "Failed to download tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc after 1 attempts" in caplog.text

def test_downloader_download_file_missing_info(sample_output_dir, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    file_info = {"title": "", "url": []}
    with caplog.at_level(logging.ERROR):
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info
        assert "Invalid file info: missing URLs or title" in caplog.text

def test_downloader_download_file_stop_event(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    stop_event.set()
    with caplog.at_level(logging.INFO):
        path, failed_info = downloader.download_file(file_info)
        assert path is None
        assert failed_info == file_info
        assert "Skipping download of tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc due to stop event" in caplog.text

def test_downloader_download_all(sample_output_dir, file_info, stop_event):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=2, username=None, password=None, verify_ssl=True)
    files = [file_info, {**file_info, "id": "file2", "title": "pr.nc", "url": ["http://example.com/pr.nc|HTTPServer"]}]
    with patch.object(downloader, "download_file") as mock_download:
        mock_download.side_effect = [
            (str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"), None),
            (str(sample_output_dir / "ScenarioMIP_100km_pr.nc"), None)
        ]
        downloaded, failed = downloader.download_all(files, phase="test")
        assert len(downloaded) == 2
        assert len(failed) == 0
        assert downloaded == [
            str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"),
            str(sample_output_dir / "ScenarioMIP_100km_pr.nc")
        ]

def test_downloader_download_all_partial_failure(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=2, username=None, password=None, verify_ssl=True)
    files = [file_info, {**file_info, "id": "file2", "title": "pr.nc", "url": ["http://example.com/pr.nc|HTTPServer"]}]
    with patch.object(downloader, "download_file") as mock_download:
        mock_download.side_effect = [
            (str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"), None),
            (None, files[1])
        ]
        with caplog.at_level(logging.INFO):
            downloaded, failed = downloader.download_all(files, phase="test")
            assert len(downloaded) == 1
            assert len(failed) == 1
            assert downloaded == [str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc")]
            assert failed == [files[1]]
            assert "Progress: 1/2 files (Failed: 1)" in caplog.text

def test_downloader_download_all_stop_event(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    stop_event.set()
    files = [file_info]
    with caplog.at_level(logging.INFO):
        downloaded, failed = downloader.download_all(files, phase="test")
        assert len(downloaded) == 0
        assert len(failed) == 0
        assert "Download operation stopped by user" in caplog.text

def test_downloader_retry_failed_success(sample_output_dir, file_info, stop_event):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    files = [file_info]
    with patch.object(downloader.query_handler, "fetch_specific_file", return_value=file_info), \
         patch.object(downloader, "download_file", return_value=(str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"), None)):
        downloaded, failed = downloader.retry_failed(files)
        assert len(downloaded) == 1
        assert len(failed) == 0
        assert downloaded == [str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc")]

def test_downloader_retry_failed_not_found(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    files = [file_info]
    with patch.object(downloader.query_handler, "fetch_specific_file", return_value=None):
        with caplog.at_level(logging.ERROR):
            downloaded, failed = downloader.retry_failed(files)
            assert len(downloaded) == 0
            assert len(failed) == 1
            assert failed == [file_info]
            assert "Could not find updated metadata for tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc" in caplog.text

def test_downloader_retry_failed_stop_event(sample_output_dir, file_info, stop_event, caplog):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    stop_event.set()
    files = [file_info]
    with caplog.at_level(logging.INFO):
        downloaded, failed = downloader.retry_failed(files)
        assert len(downloaded) == 0
        assert len(failed) == 1
        assert failed == [file_info]
        assert "Retry operation stopped by user" in caplog.text

def test_downloader_shutdown(sample_output_dir, file_info, stop_event):
    file_manager = FileManager(str(sample_output_dir), str(sample_output_dir), "flat")
    downloader = Downloader(file_manager, max_workers=1, retries=1, timeout=10, max_downloads=None, username=None, password=None, verify_ssl=True)
    downloader.executor = MagicMock()
    downloader.pending_futures = [MagicMock()]
    downloader.shutdown()
    assert downloader.stop_event.is_set()
    downloader.executor.shutdown.assert_called_with(wait=False)
    assert downloader.pending_futures == []

def test_load_config_valid(tmp_path):
    config_path = tmp_path / "config.json"
    config_data = {"project": "CMIP6", "variable_id": "tas"}
    with open(config_path, "w") as f:
        json.dump(config_data, f)
    config = load_config(str(config_path))
    assert config == config_data

def test_load_config_invalid(tmp_path, caplog):
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        f.write("invalid json")
    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            load_config(str(config_path))
        assert "Failed to load config file" in caplog.text

def test_parse_file_time_range_valid():
    filename = "tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc"
    start_date, end_date = parse_file_time_range(filename)
    assert start_date == "2015-01-01"
    assert end_date == "2100-12-01"

def test_parse_file_time_range_invalid(caplog):
    filename = "tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_invalid.nc"
    with caplog.at_level(logging.DEBUG):
        start_date, end_date = parse_file_time_range(filename)
        assert start_date is None
        assert end_date is None
        assert "Failed to parse time range" in caplog.text

def test_run_download_demo_mode(sample_output_dir, sample_metadata_dir, file_info):
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
    args.openid = None
    args.project = "CMIP6"
    args.activity = "ScenarioMIP"
    args.experiment = "ssp585"
    args.frequency = "mon"
    args.variable = "tas"
    args.model = "CMCC-ESM2"
    args.ensemble = "r1i1p1f1"
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = "100km"
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with patch("gridflow.cmip6_downloader.QueryHandler.fetch_datasets", return_value=[file_info]), \
         patch("gridflow.cmip6_downloader.Downloader.download_all", return_value=([str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc")], [])), \
         patch("gridflow.cmip6_downloader.FileManager.save_metadata") as mock_save:
        run_download(args)
        mock_save.assert_called_with([file_info], "query_results.json")

def test_run_download_dry_run(sample_output_dir, sample_metadata_dir, file_info, caplog):
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
    args.project = "CMIP6"
    args.activity = "ScenarioMIP"
    args.experiment = "ssp585"
    args.frequency = "mon"
    args.variable = "tas"
    args.model = None
    args.ensemble = None
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = None
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with patch("gridflow.cmip6_downloader.QueryHandler.fetch_datasets", return_value=[file_info]), \
         patch("gridflow.cmip6_downloader.FileManager.save_metadata") as mock_save:
        with caplog.at_level(logging.INFO):
            with pytest.raises(SystemExit):
                run_download(args)
            assert "Dry run: Would download 1 files" in caplog.text
            mock_save.assert_called_with([file_info], "query_results.json")

def test_run_download_retry_failed_success(sample_output_dir, sample_metadata_dir, file_info):
    failed_file = sample_metadata_dir / "failed_downloads.json"
    failed_data = [file_info]
    with open(failed_file, "w") as f:
        json.dump(failed_data, f)

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
    args.config = None
    args.project = None
    args.activity = None
    args.experiment = None
    args.frequency = None
    args.variable = None
    args.model = None
    args.ensemble = None
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = None
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with patch("gridflow.cmip6_downloader.Downloader.download_all", return_value=([str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc")], [])), \
         patch("gridflow.cmip6_downloader.FileManager.save_metadata") as mock_save:
        run_download(args)
        mock_save.assert_not_called()

def test_run_download_retry_failed_with_failures(sample_output_dir, sample_metadata_dir, file_info):
    failed_file = sample_metadata_dir / "failed_downloads.json"
    failed_data = [
        file_info,
        {**file_info, "id": "file2", "title": "pr.nc", "url": ["http://example.com/pr.nc|HTTPServer"]}
    ]
    with open(failed_file, "w") as f:
        json.dump(failed_data, f)

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
    args.config = None
    args.project = None
    args.activity = None
    args.experiment = None
    args.frequency = None
    args.variable = None
    args.model = None
    args.ensemble = None
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = None
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with patch("gridflow.cmip6_downloader.Downloader.download_all", side_effect=[
        ([str(sample_output_dir / "ScenarioMIP_100km_tas_Amon_CMCC-ESM2_ssp585_r1i1p1f1_gn_201501-210012.nc")], [failed_data[1]]),
        ([], [failed_data[1]])
    ]), \
         patch("gridflow.cmip6_downloader.FileManager.save_metadata") as mock_save:
        run_download(args)
        mock_save.assert_called_with([failed_data[1]], "failed_downloads_final.json")

def test_run_download_no_files(sample_output_dir, sample_metadata_dir, caplog):
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
    args.max_downloads = None
    args.id = None
    args.password = None
    args.no_verify_ssl = False
    args.retry_failed = None
    args.config = None
    args.dry_run = False
    args.demo = False
    args.activity = None
    args.experiment = None
    args.frequency = None
    args.variable = None
    args.model = None
    args.ensemble = None
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = None
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with patch("gridflow.cmip6_downloader.QueryHandler.fetch_datasets", return_value=[]):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(SystemExit):
                run_download(args)
            assert "No files found matching the query" in caplog.text

def test_run_download_invalid_retry_file(sample_output_dir, sample_metadata_dir, caplog):
    args = MagicMock()
    args.retry_failed = "nonexistent.json"
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
    args.config = None
    args.project = None
    args.activity = None
    args.experiment = None
    args.frequency = None
    args.variable = None
    args.model = None
    args.ensemble = None
    args.institution = None
    args.source_type = None
    args.grid_label = None
    args.resolution = None
    args.latest = False
    args.extra_params = None
    args.stop_event = None

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            run_download(args)
        assert "Retry file nonexistent.json does not exist" in caplog.text