# mollusk/settings.py
"""Mollusk repository settings."""

import os

# -------------------------------------------------------------------
# General
# -------------------------------------------------------------------
REPOSITORY_NAME = "A Mollusk Repository"


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
LOG_LEVEL = os.getenv("MOLLUSK_LOG_LEVEL", "INFO")
