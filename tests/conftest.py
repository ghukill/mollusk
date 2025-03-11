import os
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def temp_directory():
    with tempfile.TemporaryDirectory() as temp_dir:
        original_dir = Path.cwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(original_dir)
