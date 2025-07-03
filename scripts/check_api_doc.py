#!/usr/bin/env python3

import re
import sys
from pathlib import Path

# Path to __init__.py
init_file = Path("src/archivey/__init__.py")
# Path to your reference file
api_md = Path("docs/api.md")

# 1. get __all__ from the init file
all_pattern = re.compile(r"__all__\s*=\s*\[(.*?)\]", re.DOTALL)
init_text = init_file.read_text()
match = all_pattern.search(init_text)
if not match:
    print("Could not find __all__ in __init__.py")
    sys.exit(1)

all_entries = re.findall(r'"([^"]+)"', match.group(1))
all_set = set(all_entries)

# 2. get symbols documented in the markdown
md_text = api_md.read_text()

# New documentation format lists the exported symbols under a single
# ``::: archivey`` block with ``members:`` entries.  Parse all ``-`` lines that
# follow a ``members:`` section and collect the symbol names.
members_blocks = re.findall(r"members:\n((?:\s+-\s+[\w_]+\n)+)", md_text)
md_symbols: set[str] = set()
for block in members_blocks:
    md_symbols.update(re.findall(r"-\s+([\w_]+)", block))

# 3. compare
missing_in_md = all_set - md_symbols
extra_in_md = md_symbols - all_set

ok = True

if missing_in_md:
    print("These symbols are exported but not documented in api.md:")
    for m in sorted(missing_in_md):
        print(f"  - {m}")
    ok = False

if extra_in_md:
    print("These symbols are documented but not in __all__:")
    for e in sorted(extra_in_md):
        print(f"  - {e}")
    ok = False

if not ok:
    sys.exit(1)

print("✅ API documentation looks consistent.")
