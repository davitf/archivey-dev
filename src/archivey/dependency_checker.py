import sys
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Optional


@dataclass
class DependencyVersions:
    """Versions of optional dependencies used by archivey."""

    python_version: Optional[str] = None
    rarfile_version: Optional[str] = None
    py7zr_version: Optional[str] = None
    cryptography_version: Optional[str] = None
    lz4_version: Optional[str] = None
    zstandard_version: Optional[str] = None
    pycdlib_version: Optional[str] = None
    backports_strenum_version: Optional[str] = None
    tqdm_version: Optional[str] = None


def get_dependency_versions() -> DependencyVersions:
    """Get versions of all optional dependencies.

    Returns:
        DependencyVersions: A dataclass containing version information for all dependencies.
    """
    versions = DependencyVersions()

    versions.python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )

    # Check optional dependencies
    for package, attr in [
        ("rarfile", "rarfile_version"),
        ("py7zr", "py7zr_version"),
        ("cryptography", "cryptography_version"),
        ("lz4", "lz4_version"),
        ("zstandard", "zstandard_version"),
        ("pycdlib", "pycdlib_version"),
        ("backports.strenum", "backports_strenum_version"),
        ("tqdm", "tqdm_version"),
    ]:
        try:
            setattr(versions, attr, version(package))
        except PackageNotFoundError:
            pass

    return versions


def format_dependency_versions(versions: DependencyVersions) -> str:
    """Format dependency versions as a string.

    Args:
        versions: The DependencyVersions instance to format

    Returns:
        str: A formatted string showing all dependency versions
    """
    lines = ["Dependency Versions:"]
    for field in versions.__dataclass_fields__:
        value = getattr(versions, field)
        if value is not None:
            lines.append(f"  {field}: {value}")
        else:
            lines.append(f"  {field}: not installed")
    return "\n".join(lines)


if __name__ == "__main__":
    versions = get_dependency_versions()
    print(format_dependency_versions(versions))
