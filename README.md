# Archivey

**Archivey** is a Python library that provides a unified interface for reading several archive and compression formats, wrapping built-in Python modules and optional external packages.

👉 **Full documentation is published [here](https://davitf.github.io/archivey/)**.

---

## Quick start

Install with third-party libraries:

```bash
pip install archivey[optional]
```

Or manage dependencies yourself for only the formats you need. RAR support requires `unrar` to be installed separately.

### Usage example

```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    # Extract all files
    archive.extractall("output_dir/")

    # Or process each file inside the archive
    for member, stream in archive.iter_members_with_io():
        print(member.filename, member.type, member.file_size)
        if stream is not None:  # skip directories and links
            data = stream.read()
            print("  ", data[:50])
```

See more details in the [User guide](https://davitf.github.io/archivey/user_guide/).

---

## Why use this?

- Automatic archive format detection
- Consistent interface across multiple archive types
- Optimized for random access and streaming
- Sensible, secure defaults for file extraction

---

## Resources

- [User guide](https://davitf.github.io/archivey/user_guide/)
- [API Reference](https://davitf.github.io/archivey/reference/)
- [GitHub repository](https://github.com/davitf/archivey) (or the [development repository](https://github.com/davitf/archivey-dev) with messier commits and AI-generated pull requests)
- [Developer guide](https://davitf.github.io/archivey/developer_guide/), if you'd like to contribute
