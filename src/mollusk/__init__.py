import importlib.util
import logging
import types
from pathlib import Path

logger = logging.getLogger("mollusk")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Settings will be populated here
settings = None


def _import_default_settings():
    """Import the default settings module."""
    try:
        return importlib.import_module("mollusk.settings")
    except ImportError as e:
        logger.error(f"Could not import default settings: {e}")
        raise


def _find_repository_settings():
    """Look for settings.py in the current directory or parents."""
    cwd = Path.cwd()

    # Check current directory
    settings_path = cwd / "settings.py"
    if settings_path.exists():
        return _import_settings_from_path(settings_path)

    # Check parent directories
    for parent in cwd.parents:
        settings_path = parent / "settings.py"
        if settings_path.exists():
            return _import_settings_from_path(settings_path)

    return None


def _import_settings_from_path(path):
    """Import settings from a path."""
    try:
        spec = importlib.util.spec_from_file_location("local_settings", path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            logger.debug(f"Loaded repository settings from {path}")
            return module
    except Exception as e:
        logger.warning(f"Error importing settings from {path}: {e}")

    return None


# Load settings
def _load_settings():
    # First load default settings
    default_settings = _import_default_settings()

    # Create a new module to hold merged settings
    merged_settings = types.ModuleType("mollusk.merged_settings")

    # Copy defaults
    for key, value in default_settings.__dict__.items():
        if not key.startswith("_"):
            setattr(merged_settings, key, value)

    # Load and merge repository settings if found
    repo_settings = _find_repository_settings()
    if repo_settings:
        for key, value in repo_settings.__dict__.items():
            if not key.startswith("_"):
                setattr(merged_settings, key, value)

    # Update logging level based on settings
    if hasattr(merged_settings, "LOG_LEVEL"):
        numeric_level = getattr(logging, merged_settings.LOG_LEVEL, logging.INFO)
        logger.setLevel(numeric_level)
        logger.debug(f"Log level set to {merged_settings.LOG_LEVEL}")

    return merged_settings


# Initialize settings
settings = _load_settings()

# Update logger
logger.setLevel(settings.LOG_LEVEL)
