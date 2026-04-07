#!/bin/bash
export PATH="$HOME/.local/bin:$PATH"
cd /mnt/e/Dev/Filmu/FilmuCore
uv run --extra dev pytest -q \
  tests/test_show_completion.py \
  tests/test_partial_show_requests.py \
  tests/test_show_search_context.py \
  tests/test_scrape_routes.py \
  2>&1
