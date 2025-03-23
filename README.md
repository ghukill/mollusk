# mollusk
Mollusk

## Overview

Mollusk is a CLI and python library (`mollusk`).  The CLI can be used to create new repositories, where then the `mollusk` library is used.  This is similar to other python projects, e.g. Django, where there are commands to initialize and manage applications but it's also a python library.  If there is not a term for this, there should be 😎. 

## Development

### Installation

1- Clone repository
```shell
git clone https://github.com/ghukill/mollusk
cd mollusk
```

2- Create virtual environment and install dependencies:
```shell
uv venv .venv
uv sync --dev
```

3- Run tests:
```shell
uv run pytest -vv
```

3- Run linting:
```shell
uv run ruff check src/
```

## Repository

### Create New Repository

Create a directory for the new repository:
```shell
mkdir /tmp/my-mollusk-repo
cd /tmp/my-mollusk-repo
```

Create a python virtual environment and install the `mollusk` library:
```shell
uv init --bare --python 3.13
uv venv .venv
uv add git+https://github.com/ghukill/mollusk
```

Initialize a new Mollusk repository:
```shell
uv run mollusk init
```

Edit settings file:
```shell
vim settings.py
```