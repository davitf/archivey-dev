set shell := ["bash", "-euxo", "pipefail", "-c"]
set positional-arguments

# The default recipe, install all dependencies and run all the local checks
default: qa

# Run all the quality assurance checks
qa: lint type

# Run the linter
lint *args:
	ruff check --fix . && ruff format . && npx pyright . {{args}}

# Run the type checker
type *args:
	uv run mypy {{args}}

# Run the tests with a specific dependency set
test-old-libs *args:
	uv venv --python 3.10 .venv-old-libs && . .venv-old-libs/bin/activate && export PYTHONPATH=`pwd`/src && uv pip install -r requirements-old.txt && uv pip install -e ".[dev]" && pytest tests {{args}}

test-new-libs *args:
	uv venv --python 3.13 .venv-new-libs && . .venv-new-libs/bin/activate && export PYTHONPATH=`pwd`/src && uv pip install -r requirements-new.txt && uv pip install -e ".[dev]" && pytest tests {{args}}
