import pytest
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from gridflow.database_generator import extract_metadata, generate_database, main
import netCDF4
import logging

@pytest.fixture
def mock_netcdf_file(tmp_path):
    """Create a single mock NetCDF file with valid metadata."""
    file_path = tmp_path / "test.nc"
    with netCDF4.Dataset(file_path, "w") as ds:
        ds.activity_id = "CMIP"
        ds.source_id = "CESM2"
        ds.variant_label = "r1i1p1f1"
        ds.variable_id = "tas"
        ds.institution_id = "NCAR"
    return file_path

@pytest.fixture
def mock_netcdf_files(tmp_path):
    """Create multiple mock NetCDF files with varying metadata."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    files = []
    for i in range(3):
        file_path = input_dir / f"test_{i}.nc"
        with netCDF4.Dataset(file_path, "w") as ds:
            ds.activity_id = "CMIP"
            ds.source_id = f"Model{i}"
            ds.variant_label = "r1i1p1f1"
            ds.variable_id = "tas"
            ds.institution_id = "NCAR"
        files.append(file_path)
    return input_dir, files

@pytest.fixture
def output_dir(tmp_path):
    """Create an output directory for test results."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir

@pytest.fixture
def demo_input_dir(tmp_path):
    """Create a demo input directory with mock NetCDF files."""
    demo_dir = tmp_path / "demo_cmip6_data"
    demo_dir.mkdir()
    file_path = demo_dir / "demo.nc"
    with netCDF4.Dataset(file_path, "w") as ds:
        ds.activity_id = "CMIP"
        ds.source_id = "DemoModel"
        ds.variant_label = "r1i1p1f1"
        ds.variable_id = "tas"
        ds.institution_id = "DemoInst"
    return demo_dir

def test_extract_metadata(mock_netcdf_file):
    """Test extract_metadata with a valid NetCDF file."""
    result = extract_metadata(str(mock_netcdf_file))
    assert result["file_path"] == str(mock_netcdf_file)
    assert result["metadata"] == {
        "activity_id": "CMIP",
        "source_id": "CESM2",
        "variant_label": "r1i1p1f1",
        "variable_id": "tas",
        "institution_id": "NCAR"
    }
    assert result["error"] is None

def test_extract_metadata_invalid(tmp_path):
    """Test extract_metadata with a non-existent file."""
    invalid_file = tmp_path / "invalid.nc"
    result = extract_metadata(str(invalid_file))
    assert result["file_path"] == str(invalid_file)
    assert result["metadata"] == {}
    assert "File" in result["error"]
    assert "does not exist" in result["error"]

def test_extract_metadata_corrupt_file(tmp_path):
    """Test extract_metadata with a corrupt or unreadable file."""
    corrupt_file = tmp_path / "corrupt.nc"
    corrupt_file.write_bytes(b"not a netcdf file")
    result = extract_metadata(str(corrupt_file))
    assert result["file_path"] == str(corrupt_file)
    assert result["metadata"] == {}
    assert "Failed to extract metadata" in result["error"]

def test_generate_database_single(mock_netcdf_file, output_dir):
    """Test generate_database with a single NetCDF file."""
    input_dir = mock_netcdf_file.parent
    result = generate_database(str(input_dir), str(output_dir))
    assert len(result) == 1
    key = "CMIP:CESM2:r1i1p1f1"
    assert key in result
    assert result[key]["activity_id"] == "CMIP"
    assert result[key]["source_id"] == "CESM2"
    assert result[key]["variant_label"] == "r1i1p1f1"
    assert result[key]["institution_id"] == "NCAR"
    assert len(result[key]["files"]) == 1
    assert result[key]["files"][0]["variable_id"] == "tas"
    assert result[key]["files"][0]["path"] == str(mock_netcdf_file)
    assert (output_dir / "database.json").exists()
    with open(output_dir / "database.json") as f:
        saved_data = json.load(f)
    assert saved_data == result

def test_generate_database_multi(mock_netcdf_files, output_dir):
    """Test generate_database with multiple NetCDF files."""
    input_dir, files = mock_netcdf_files
    result = generate_database(str(input_dir), str(output_dir), workers=2)
    assert len(result) == 3
    for i in range(3):
        key = f"CMIP:Model{i}:r1i1p1f1"
        assert key in result
        assert result[key]["activity_id"] == "CMIP"
        assert result[key]["source_id"] == f"Model{i}"
        assert result[key]["variant_label"] == "r1i1p1f1"
        assert result[key]["institution_id"] == "NCAR"
        assert len(result[key]["files"]) == 1
        assert result[key]["files"][0]["variable_id"] == "tas"
        assert result[key]["files"][0]["path"] == str(input_dir / f"test_{i}.nc")
    assert (output_dir / "database.json").exists()
    with open(output_dir / "database.json") as f:
        saved_data = json.load(f)
    assert saved_data == result

def test_generate_database_no_files(tmp_path, output_dir):
    """Test generate_database with an empty input directory."""
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    result = generate_database(str(input_dir), str(output_dir))
    assert result == {}
    assert not (output_dir / "database.json").exists()

def test_generate_database_invalid_input_dir(output_dir):
    """Test generate_database with a non-existent input directory."""
    input_dir = "non_existent_dir"
    result = generate_database(input_dir, str(output_dir))
    assert result == {}
    assert not (output_dir / "database.json").exists()

def test_generate_database_demo_mode(mock_netcdf_file, output_dir):
    """Test generate_database in demo mode."""
    input_dir = mock_netcdf_file.parent
    result = generate_database(str(input_dir), str(output_dir), demo_mode=True)
    assert len(result) == 1
    assert (output_dir / "demo_database.json").exists()
    with open(output_dir / "demo_database.json") as f:
        saved_data = json.load(f)
    assert saved_data == result

def test_generate_database_incomplete_metadata(mock_netcdf_file, output_dir):
    """Test generate_database with a file missing required metadata."""
    file_path = mock_netcdf_file
    with netCDF4.Dataset(file_path, "w") as ds:  # Overwrite with incomplete metadata
        ds.activity_id = "CMIP"
        ds.source_id = ""  # Missing source_id
        ds.variant_label = "r1i1p1f1"
        ds.variable_id = "tas"
    input_dir = file_path.parent
    result = generate_database(str(input_dir), str(output_dir))
    assert result == {}  # File skipped due to incomplete metadata
    assert (output_dir / "database.json").exists()
    with open(output_dir / "database.json") as f:
        saved_data = json.load(f)
    assert saved_data == {}

def test_main(mock_netcdf_file, output_dir):
    """Test the main function with valid arguments."""
    input_dir = mock_netcdf_file.parent
    args = ["gridflow-db", "-i", str(input_dir), "-o", str(output_dir), "-L", "debug"]
    with patch("sys.argv", args), \
         patch("gridflow.database_generator.setup_logging") as mock_setup_logging, \
         patch("logging.critical") as mock_logging_critical:
        main()
    mock_setup_logging.assert_called_once_with("./logs", "debug", prefix="db_")
    mock_logging_critical.assert_called_with("Generated database with 1 groups")
    assert (output_dir / "database.json").exists()

def test_main_demo_mode(tmp_path, monkeypatch):
    """Test the main function in demo mode with a valid demo directory."""
    monkeypatch.chdir(tmp_path)  # Set current working directory to tmp_path
    demo_dir = tmp_path / "demo_cmip6_data"
    demo_dir.mkdir()
    file_path = demo_dir / "demo.nc"
    with netCDF4.Dataset(file_path, "w") as ds:
        ds.activity_id = "CMIP"
        ds.source_id = "DemoModel"
        ds.variant_label = "r1i1p1f1"
        ds.variable_id = "tas"
        ds.institution_id = "DemoInst"
    args = ["gridflow-db", "--demo"]
    with patch("sys.argv", args), \
         patch("gridflow.database_generator.setup_logging") as mock_setup_logging, \
         patch("logging.critical") as mock_logging_critical:
        main()
    mock_setup_logging.assert_called_once_with("./logs", "minimal", prefix="db_")
    mock_logging_critical.assert_any_call("Generating CMIP6 database in demo mode")
    mock_logging_critical.assert_any_call("Generated database with 1 groups")
    demo_output_dir = tmp_path / "demo_output"
    assert demo_output_dir.exists()
    assert (demo_output_dir / "demo_database.json").exists()

def test_main_no_files(tmp_path, output_dir):
    """Test the main function with an empty input directory."""
    input_dir = tmp_path / "empty"
    input_dir.mkdir()
    args = ["gridflow-db", "-i", str(input_dir), "-o", str(output_dir)]
    with patch("sys.argv", args), \
         patch("gridflow.database_generator.setup_logging"), \
         patch("logging.error") as mock_logging_error:
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    mock_logging_error.assert_called_with("No valid NetCDF files found or database generation failed")

def test_main_invalid_args(capsys):
    """Test the main function with invalid arguments."""
    args = ["gridflow-db", "--invalid"]
    with patch("sys.argv", args), \
         patch("gridflow.database_generator.setup_logging"):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "unrecognized arguments: --invalid" in captured.err

def test_main_version(capsys):
    """Test the main function with --version flag."""
    args = ["gridflow-db", "--version"]
    with patch("sys.argv", args), \
         patch("gridflow.database_generator.setup_logging"):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "0.2.3" in captured.out