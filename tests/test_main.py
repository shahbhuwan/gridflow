import pytest
import sys
import logging
from unittest.mock import patch
from gridflow import __main__
import warnings

# Suppress geopandas shapely.geos deprecation warning
warnings.filterwarnings("ignore", category=DeprecationWarning, module="geopandas._compat")

@pytest.fixture
def capsys_no_logging(capsys):
    logger = logging.getLogger()
    original_handlers = logger.handlers[:]
    original_level = logger.level
    logger.handlers = []
    logger.setLevel(logging.NOTSET)
    try:
        yield capsys
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)

def test_main_no_args(capsys_no_logging):
    with patch("sys.argv", ["gridflow"]), \
         patch("gridflow.__main__.download_parse_args") as mock_parse_args, \
         patch("gridflow.__main__.run_download") as mock_run_download, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging, \
         patch("gridflow.downloader.QueryHandler.fetch_datasets", return_value=[{"title": "tas.nc"}]):
        mock_parse_args.return_value = mock_parse_args
        try:
            __main__.main()
        except SystemExit as e:
            print(f"SystemExit occurred with code: {e.code}")
        captured = capsys_no_logging.readouterr()
        assert "Welcome to GridFlow v0.2.3" in captured.out, "Intro message not printed"
        mock_parse_args.assert_called_once()
        mock_run_download.assert_called_once_with(mock_parse_args)
        mock_setup_logging.assert_called_once_with('logs', 'minimal')
        assert sys.argv == ["gridflow", "--demo"], "sys.argv not modified correctly"

def test_main_demo_mode(capsys_no_logging):
    with patch("sys.argv", ["gridflow", "--demo"]), \
         patch("gridflow.__main__.download_parse_args") as mock_parse_args, \
         patch("gridflow.__main__.run_download") as mock_run_download, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging, \
         patch("gridflow.downloader.QueryHandler.fetch_datasets", return_value=[{"title": "tas.nc"}]):
        mock_parse_args.return_value = mock_parse_args
        try:
            __main__.main()
        except SystemExit as e:
            print(f"SystemExit occurred with code: {e.code}")
        captured = capsys_no_logging.readouterr()
        assert "Welcome to GridFlow v0.2.3" in captured.out, "Intro message not printed"
        mock_parse_args.assert_called_once()
        mock_run_download.assert_called_once_with(mock_parse_args)
        mock_setup_logging.assert_called_once_with('logs', 'minimal')
        assert sys.argv == ["gridflow", "--demo"], "sys.argv not modified correctly"

def test_main_version(capsys_no_logging):
    with patch("sys.argv", ["gridflow", "--version"]):
        with pytest.raises(SystemExit) as exc_info:
            __main__.main()
        assert exc_info.value.code == 0, "Expected exit code 0 for --version"
        captured = capsys_no_logging.readouterr()
        assert "0.2.3" in captured.out, "Version number not printed"

def test_main_with_args(capsys_no_logging, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text('{"project": "CMIP6"}')
    args = ["gridflow", "--log-level", "debug", "--config", str(config_file), "--log-dir", "custom_logs"]
    with patch("sys.argv", args), \
         patch("gridflow.__main__.download_parse_args") as mock_parse_args, \
         patch("gridflow.__main__.run_download") as mock_run_download, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging, \
         patch("gridflow.downloader.QueryHandler.fetch_datasets", return_value=[{"title": "tas.nc"}]):
        mock_parse_args.return_value = mock_parse_args
        try:
            __main__.main()
        except SystemExit as e:
            print(f"SystemExit occurred with code: {e.code}")
        captured = capsys_no_logging.readouterr()
        assert "Welcome to GridFlow v0.2.3" in captured.out, "Intro message not printed"
        mock_parse_args.assert_called_once()
        mock_run_download.assert_called_once_with(mock_parse_args)
        mock_setup_logging.assert_called_once_with('custom_logs', 'debug')

def test_main_invalid_args(capsys_no_logging):
    with patch("sys.argv", ["gridflow", "--invalid"]), \
         patch("gridflow.__main__.download_parse_args") as mock_parse_args, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging:
        mock_parse_args.side_effect = SystemExit(2)
        with pytest.raises(SystemExit) as exc_info:
            __main__.main()
        assert exc_info.value.code == 2, "Expected exit code 2 for invalid args"
        captured = capsys_no_logging.readouterr()
        assert "Welcome to GridFlow v0.2.3" in captured.out, "Intro message not printed"
        mock_setup_logging.assert_called_once_with('logs', 'minimal')

@pytest.mark.timeout(5)
def test_main_verbose_logging(capsys_no_logging):
    with patch("sys.argv", ["gridflow", "--log-level", "verbose"]), \
         patch("gridflow.__main__.download_parse_args") as mock_parse_args, \
         patch("gridflow.__main__.run_download") as mock_run_download, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging, \
         patch("gridflow.downloader.QueryHandler.fetch_datasets", return_value=[{"title": "tas.nc"}]):
        mock_parse_args.return_value = mock_parse_args
        try:
            __main__.main()
        except SystemExit as e:
            print(f"SystemExit occurred with code: {e.code}")
        captured = capsys_no_logging.readouterr()
        assert "Welcome to GridFlow v0.2.3" in captured.out, "Intro message not printed"
        mock_parse_args.assert_called_once()
        mock_run_download.assert_called_once_with(mock_parse_args)
        mock_setup_logging.assert_called_once_with('logs', 'verbose')

def test_main_db_mode(capsys_no_logging):
    """Test main with sys.argv[0] = 'gridflow-db' (covers line 38)."""
    with patch("sys.argv", ["gridflow-db"]), \
         patch("gridflow.__main__.db_main") as mock_db_main, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging:
        __main__.main()
    mock_db_main.assert_called_once()
    mock_setup_logging.assert_called_once_with('logs', 'minimal')
    captured = capsys_no_logging.readouterr()
    assert "Welcome to GridFlow v0.2.3" in captured.out

def test_main_crop_mode(capsys_no_logging):
    """Test main with sys.argv[0] = 'gridflow-crop' (covers line 43)."""
    with patch("sys.argv", ["gridflow-crop"]), \
         patch("gridflow.__main__.crop_main") as mock_crop_main, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging:
        __main__.main()
    mock_crop_main.assert_called_once()
    mock_setup_logging.assert_called_once_with('logs', 'minimal')
    captured = capsys_no_logging.readouterr()
    assert "Welcome to GridFlow v0.2.3" in captured.out

def test_main_clip_mode(capsys_no_logging):
    """Test main with sys.argv[0] = 'gridflow-clip' (covers line 45)."""
    with patch("sys.argv", ["gridflow-clip"]), \
         patch("gridflow.__main__.clip_main") as mock_clip_main, \
         patch("gridflow.__main__.setup_logging") as mock_setup_logging:
        __main__.main()
    mock_clip_main.assert_called_once()
    mock_setup_logging.assert_called_once_with('logs', 'minimal')
    captured = capsys_no_logging.readouterr()
    assert "Welcome to GridFlow v0.2.3" in captured.out