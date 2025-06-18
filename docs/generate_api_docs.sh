#!/bin/sh
# Script to generate API documentation using pdoc

# Ensure the output directory exists
mkdir -p docs/api

# Run pdoc
# Adjust the module path if your package structure is different
# This assumes your main package 'archivey' is under 'src/'
# Modern versions of pdoc no longer use the --html flag, so we
# simply specify the output directory with -o.
pdoc -o docs/api src/archivey

echo "API documentation generated in docs/api"
