#!/bin/sh
# Script to generate API documentation using pdoc

# Ensure the output directory exists
mkdir -p docs/api

# Copy the README so MkDocs can use it as the landing page
cp README.md docs/index.md

# Run pdoc using the current Python interpreter
# Modern versions of pdoc no longer use the --html flag, we simply
# specify the output directory with ``-o``.
python -m pdoc -o docs/api src/archivey

# Remove dataclass constructors that would otherwise clutter the docs
# Remove internal attributes that pdoc does not filter out from dataclasses
sed -i '/ArchiveMember.__init__/d' docs/api/archivey.html
sed -i '/_member_id/d' docs/api/archivey.html
sed -i '/_archive_id/d' docs/api/archivey.html
sed -i '/_edited_by_filter/d' docs/api/archivey.html

echo "API documentation generated in docs/api"

# Build the static site with MkDocs if available
if command -v mkdocs >/dev/null 2>&1; then
    mkdocs build -d site
fi
