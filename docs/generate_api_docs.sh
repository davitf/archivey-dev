#!/bin/sh
# Build the documentation site using MkDocs and mkdocstrings

# Copy the README so MkDocs can use it as the landing page
cp README.md docs/index.md

# Build the site
mkdocs build -d site
