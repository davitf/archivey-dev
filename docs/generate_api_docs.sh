#!/bin/sh
# Script to generate API documentation using pdoc

# Ensure the output directory exists
mkdir -p docs

# Run pdoc
# Adjust the module path if your package structure is different
# This assumes your main package 'archivey' is under 'src/'
pdoc src/archivey -o docs
echo "API documentation generated in docs"
