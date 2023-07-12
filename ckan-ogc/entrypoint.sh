#!/bin/bash

set -xeuo pipefail

/wait-for --timeout "$TIMEOUT" "$CKAN_URL" -- pdm run python3 ogc2ckan/ogc2ckan.py

exec "$@"
