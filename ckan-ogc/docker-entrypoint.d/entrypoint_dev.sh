#!/bin/bash

set -xeuo pipefail

/wait-for --timeout "$TIMEOUT" "$CKAN_URL" -- pdm run python3 -m ptvsd --host 0.0.0.0 --port 5678 --wait ogc2ckan/ogc2ckan.py

exec "$@"
