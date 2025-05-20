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

import pytest
import requests
from unittest.mock import patch
from gridflow.downloader.prism_downloader import download_prism, compute_sha256
from pathlib import Path
import logging
import hashlib

@pytest.fixture
def temp_dir(tmp_path):
    """Create temporary directories for output and logs."""
    output_dir = tmp_path / "prism_data"
    log_dir = tmp_path / "logs"
    output_dir.mkdir()
    log_dir.mkdir()
    return output_dir, log_dir

@patch('requests.head')
@patch('requests.get')
def test_download_prism_demo(mock_get, mock_head, temp_dir, caplog):
    """Test PRISM download in demo mode."""
    output_dir, log_dir = temp_dir
    mock_head.return_value.status_code = 200
    mock_get.return_value.status_code = 200
    mock_get.return_value.headers = {'Content-Length': '1024'}
    mock_get.return_value.iter_content.return_value = [b'dummy_data']

    caplog.set_level(logging.INFO)
    download_prism(
        variable='ppt',
        resolution='4km',
        time_step='daily',
        year=2020,
        output_dir=str(output_dir),
        log_dir=str(log_dir),
        log_level='debug',
        retries=1,
        timeout=10,
        demo=True
    )

    output_file = output_dir / 'prism_ppt_us_4km_20200101.zip'
    assert output_file.exists()
    assert "Downloaded prism_ppt_us_4km_20200101.zip" in caplog.text
    assert "SHA256 checksum for prism_ppt_us_4km_20200101.zip" in caplog.text

@patch('requests.head')
def test_download_prism_skip_existing(mock_head, temp_dir, caplog):
    """Test skipping existing file."""
    output_dir, log_dir = temp_dir
    output_file = output_dir / 'prism_ppt_us_4km_20200101.zip'
    output_file.write_text('dummy_data')

    mock_head.return_value.status_code = 200
    with patch('requests.get') as mock_get:
        caplog.set_level(logging.INFO)
        download_prism(
            variable='ppt',
            resolution='4km',
            time_step='daily',
            year=2020,
            output_dir=str(output_dir),
            log_dir=str(log_dir),
            log_level='debug',
            retries=1,
            timeout=10,
            demo=True
        )
        mock_get.assert_not_called()
        assert "File prism_ppt_us_4km_20200101.zip already exists, skipping" in caplog.text

@patch('requests.head')
def test_download_prism_unavailable(mock_head, temp_dir, caplog):
    """Test handling unavailable data."""
    output_dir, log_dir = temp_dir
    mock_head.return_value.status_code = 404

    caplog.set_level(logging.WARNING)
    download_prism(
        variable='ppt',
        resolution='4km',
        time_step='daily',
        year=2020,
        output_dir=str(output_dir),
        log_dir=str(log_dir),
        log_level='debug',
        retries=1,
        timeout=10,
        demo=True
    )

    assert "Data not available for ppt at 4km (daily) on 20200101: HTTP 404" in caplog.text
    assert not (output_dir / 'prism_ppt_us_4km_20200101.zip').exists()

def test_compute_sha256(temp_dir):
    """Test SHA256 checksum calculation."""
    output_dir, _ = temp_dir
    test_file = output_dir / 'test.txt'
    test_file.write_text('dummy_data')
    expected_checksum = hashlib.sha256(b'dummy_data').hexdigest()
    assert compute_sha256(test_file) == expected_checksum