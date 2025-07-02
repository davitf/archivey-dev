# Contributing to Archivey

Thanks for your interest in improving Archivey!

See the [Developer Guide](docs/developer_guide.md) for details on the package layout and how to implement new readers.

## Getting started

1. Install Python 3.10 or newer and clone the repository.
2. Install optional dependencies and tools:
   ```bash
   pip install uv hatch
   sudo apt-get install -y unrar  # optional, for RAR tests
   ```
3. Run the tests with:
   ```bash
   uv run --extra optional pytest
   ```
   Use `-k` to run a subset of tests.

Pull requests are welcome. Please follow the guidelines in `AGENTS.md` and keep the code simple and well typed.
