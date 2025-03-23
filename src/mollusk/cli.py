# mollusk/cli.py
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
    if verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")


@main.command()
def ping() -> None:
    """Ping pong."""
    logger.debug("pong, from mollusk")
    logger.info("pong, from mollusk")
    click.echo("pong, from mollusk")


@main.command()
@click.option("--location", "-l", default=".", help="Repository creation location")
def init(*, location: str) -> None:
    """Initialize a new Mollusk repository."""
    repository_path = Path(location).resolve()

    if not repository_path.exists():
        logger.info(f"Creating directory: '{repository_path}'")
        repository_path.mkdir(parents=True, exist_ok=True)

    template_dir = files("mollusk.templates") / "new_repository"
    library_dir = files("mollusk")

    with as_file(template_dir) as src, as_file(library_dir) as lib:
        shutil.copytree(src, repository_path, dirs_exist_ok=True)
        shutil.copy(lib / "settings.py", repository_path)

    logger.info(f"New mollusk repository created at: '{repository_path}'")


if __name__ == "__main__":
    main()
