#!/bin/bash
set -e

uv run ruff check
uv run ruff format --check
uv run ty check
uv run pytest -m "not integration"
