#!/bin/sh
# Script to generate API documentation using pdoc

# Ensure the output directory exists
mkdir -p docs/api

# Run pdoc using the current Python interpreter
# Modern versions of pdoc no longer use the --html flag, we simply
# specify the output directory with ``-o``.
python -m pdoc -o docs/api src/archivey

# Remove dataclass constructors that would otherwise clutter the docs
sed -i '/ArchiveMember.__init__/d' docs/api/archivey.html

echo "API documentation generated in docs/api"
