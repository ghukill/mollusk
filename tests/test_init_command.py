"""Tests for the init command."""


def test_init_creates_repository(cli_runner, temp_directory):
    """Test that init command creates a Mollusk repository in the current directory."""
    from mollusk.cli import init

    result = cli_runner.invoke(init)
    assert result.exit_code == 0
    settings_file = temp_directory / "settings.py"
    assert settings_file.exists()
    settings_content = settings_file.read_text()
    assert "REPOSITORY_NAME" in settings_content
    assert "LOG_LEVEL" in settings_content
    readme_file = temp_directory / "README.md"
    assert readme_file.exists()
    init_file = temp_directory / "__init__.py"
    assert init_file.exists()


def test_init_with_existing_content(cli_runner, temp_directory):
    """Test that init command works with existing content in the directory."""
    existing_file = temp_directory / "existing_file.txt"
    existing_file.write_text("This is an existing file")
    from mollusk.cli import init

    result = cli_runner.invoke(init)
    assert result.exit_code == 0
    settings_file = temp_directory / "settings.py"
    assert settings_file.exists()
    assert existing_file.exists()
    assert existing_file.read_text() == "This is an existing file"
