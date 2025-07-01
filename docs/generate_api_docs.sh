#!/bin/sh
# Script to generate documentation using portray

OUTPUT_DIR="docs/api"
mkdir -p "$OUTPUT_DIR"

# Generate docs with portray. PYTHONPATH ensures the src layout is found.
if command -v portray >/dev/null 2>&1; then
    CMD=portray
else
    CMD="python3 -m portray"
fi

PYTHONPATH=src $CMD as_html -o "$OUTPUT_DIR" -m archivey

echo "Documentation generated in $OUTPUT_DIR"
