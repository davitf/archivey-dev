# Archivey

**Archivey** is a Python library that provides a unified interface for reading various archive formats:

```python
from archivey import open_archive

with open_archive("example.zip") as archive:  # Automatic format detection
    # Extract all files
    archive.extractall("output_dir/")

    # Or process each file in the archive
    for member, stream in archive.iter_members_with_streams():
        print(member.filename, member.type, member.file_size)
        if stream is not None:  # File-like stream for files, None for dirs and links
            data = stream.read()
            print("  ", data[:50])
```

It wraps built-in modules and optional third-party libraries, adding missing features and fixing limitations in the underlying tools. See the [User guide](https://davitf.github.io/archivey/user_guide/) for more details.


### Installation

```bash
pip install archivey[optional]
```

The [optional] extra includes all recommended third-party libraries for full format support.

**Note:** RAR support requires the `unrar` binary to be available on your system or installed separately.

### Resources

- 📖 [Documentation](https://davitf.github.io/archivey/)
- 📘 [API reference](https://davitf.github.io/archivey/api/)
- 🛠️ [GitHub repository](https://github.com/davitf/archivey)  
  ↳ or the [development repo](https://github.com/davitf/archivey-dev), with in-progress work, rougher commits and AI-generated pull requests
