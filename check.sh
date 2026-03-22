#!/bin/bash
set -e

uv run ruff check
uv run ruff format --check
uv run ty check nyc_dot_bot/
uv run pytest -m "not integration"
