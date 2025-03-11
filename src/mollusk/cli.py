import logging
import shutil
from importlib.resources import as_file, files
from pathlib import Path

import click

from mollusk import logger


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def main(*, verbose: bool) -> None:
    """Mollusk CLI tool."""
    # configure logging for CLI usage
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # remove any existing handlers to avoid duplicate logging
    logger.handlers.clear()
    logger.addHandler(handler)

    # set logging level
    logger.setLevel(logging.INFO)
    if verbose:
        logger.setLevel(logging.DEBUG)


@main.command()
def ping() -> None:
    """Ping/Pong."""
    logger.info("debug pong")
    logger.info("info pong")
    click.echo("pong, from mollusk")


@main.command()
def init() -> None:
    """Initialize a new Mollusk repository."""
    project_path = Path.cwd()
    template_dir = files("mollusk.templates") / "new_project"
    library_dir = files("mollusk")
    with as_file(template_dir) as src, as_file(library_dir) as lib:
        shutil.copytree(src, project_path, dirs_exist_ok=True)
        shutil.copy(lib / "settings.py", project_path)
    logger.info(f"New mollusk repository created at: '{project_path}'")


if __name__ == "__main__":
    main()
