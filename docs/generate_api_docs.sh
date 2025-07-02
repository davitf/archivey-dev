#!/bin/sh
# Build the documentation site using MkDocs and mkdocstrings

# Copy the README so MkDocs can use it as the landing page
# Adjust internal links so they work from the docs directory
sed \
  -e 's#](docs/#](#g' \
  -e 's#](tests/#](../tests/#g' \
  -e 's#](src/#](../src/#g' README.md > docs/index.md

# Build the site
mkdocs build -d site
