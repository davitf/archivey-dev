from archivey.config import (
    ArchiveyConfig,
    default_config,
    get_default_config,
    set_default_config,
)
from archivey.core import open_archive
from typing import Any
from archivey.reader_registry import (
    register_reader,
    unregister_reader,
    get_reader_factory,
)
from archivey.dependency_checker import (
    DependencyVersions,
    format_dependency_versions,
    get_dependency_versions,
)
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError,
)
from archivey.folder_reader import FolderReader
from archivey.formats import detect_archive_format_by_signature
from archivey.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    CreateSystem,
    MemberType,
)

__all__ = [
    "open_archive",
    "FolderReader",
    "ArchiveError",
    "ArchiveFormatError",
    "ArchiveCorruptedError",
    "ArchiveEncryptedError",
    "ArchiveEOFError",
    "ArchiveMemberNotFoundError",
    "ArchiveNotSupportedError",
    "ArchiveMember",
    "ArchiveInfo",
    "ArchiveFormat",
    "detect_archive_format_by_signature",
    "MemberType",
    "CreateSystem",
    "DependencyVersions",
    "get_dependency_versions",
    "format_dependency_versions",
    "ArchiveyConfig",
    "get_default_config",
    "set_default_config",
    "default_config",
    "register_reader",
    "unregister_reader",
    "get_reader_factory",
]


def _register_builtin_readers() -> None:
    """Register builtin readers for the supported formats."""
    from archivey.types import SINGLE_FILE_COMPRESSED_FORMATS, TAR_COMPRESSED_FORMATS

    def _rar_factory(path: str, _format: ArchiveFormat, _streaming_only: bool, **kw: Any):
        from archivey.rar_reader import RarReader
        return RarReader(path, pwd=kw.get("pwd"))

    def _zip_factory(path: str, _format: ArchiveFormat, _streaming_only: bool, **kw: Any):
        from archivey.zip_reader import ZipReader
        return ZipReader(path, pwd=kw.get("pwd"))

    def _sevenzip_factory(path: str, _format: ArchiveFormat, streaming_only: bool, **kw: Any):
        from archivey.sevenzip_reader import SevenZipReader
        return SevenZipReader(path, pwd=kw.get("pwd"), streaming_only=streaming_only)

    def _tar_factory(path: str, fmt: ArchiveFormat, streaming_only: bool, **kw: Any):
        from archivey.tar_reader import TarReader
        return TarReader(path, pwd=kw.get("pwd"), format=fmt, streaming_only=streaming_only)

    def _single_file_factory(path: str, fmt: ArchiveFormat, _streaming_only: bool, **kw: Any):
        from archivey.single_file_reader import SingleFileReader
        return SingleFileReader(path, format=fmt, pwd=kw.get("pwd"))

    def _folder_factory(path: str, _fmt: ArchiveFormat, _streaming_only: bool, **kw: Any):
        return FolderReader(path)

    register_reader(ArchiveFormat.RAR, _rar_factory)
    register_reader(ArchiveFormat.ZIP, _zip_factory)
    register_reader(ArchiveFormat.SEVENZIP, _sevenzip_factory)
    for fmt in TAR_COMPRESSED_FORMATS + [ArchiveFormat.TAR]:
        register_reader(fmt, _tar_factory)
    for fmt in SINGLE_FILE_COMPRESSED_FORMATS:
        register_reader(fmt, _single_file_factory)
    register_reader(ArchiveFormat.FOLDER, _folder_factory)


_register_builtin_readers()
