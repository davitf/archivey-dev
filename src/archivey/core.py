from typing import IO, Union, Optional, Any

from archivey.formats import ArchiveFormat, detect_archive_format_by_filename, detect_archive_format_by_signature
from archivey.base_writer import ArchiveWriter
from archivey.zip_writer import ZipWriter
from archivey.tar_writer import TarWriter
from archivey.exceptions import ArchiveFormatError, UnsupportedArchiveError

# Mapping from ArchiveFormat enum to writer class and default mode args
WRITER_REGISTRY = {
    ArchiveFormat.ZIP: (ZipWriter, {"compression": None}), # zipfile.ZIP_DEFLATED is default in ZipWriter
    ArchiveFormat.TAR: (TarWriter, {}),
    ArchiveFormat.TAR_GZ: (TarWriter, {}), # Mode "w:gz" will be set based on format
    ArchiveFormat.TAR_BZ2: (TarWriter, {}), # Mode "w:bz2"
    ArchiveFormat.TAR_XZ: (TarWriter, {}), # Mode "w:xz"
    # Add other supported TAR formats here if they have distinct writer needs or default modes
}

def open_archive_writer(
    archive: Union[str, IO[bytes]],
    mode: str = "w",
    format: Optional[ArchiveFormat] = None,
    *,
    encoding: Optional[str] = None,
    **kwargs: Any,
) -> ArchiveWriter:
    """Open an archive file for writing.

    This function provides a high-level interface to create an appropriate
    ArchiveWriter instance based on the specified format or detected from
    the filename (if `archive` is a path).

    Args:
        archive: Path to the archive file (str) or a file-like object.
        mode: The mode to open the archive in (e.g., "w", "x", "a").
              For TAR formats, compression can be part of the mode (e.g., "w:gz")
              or determined by the `format` argument.
        format: The explicit format of the archive (ArchiveFormat enum).
                If None, it will be detected from the filename if `archive` is a path.
                If `archive` is a file-like object and format is None, ZIP is often
                assumed unless mode suggests otherwise (e.g. "w:gz" for TarWriter).
        encoding: The encoding for filenames and comments within the archive.
        **kwargs: Additional keyword arguments passed to the specific archive writer
                  (e.g., `compression` for ZipWriter, `compresslevel`).

    Returns:
        An instance of a subclass of ArchiveWriter (e.g., ZipWriter, TarWriter).

    Raises:
        UnsupportedArchiveError: If the format is not supported for writing.
        ArchiveFormatError: If the format cannot be determined or is ambiguous.
    """
    detected_format: Optional[ArchiveFormat] = None

    if format is None:
        if isinstance(archive, str):
            detected_format = detect_archive_format_by_filename(archive)
            if detected_format == ArchiveFormat.UNKNOWN:
                # Try signature if filename detection fails and it's a path that might exist (e.g. mode 'a')
                # For mode 'w' or 'x' to a new file, signature won't help.
                # This part is more relevant for reading; for writing, filename is primary.
                # If mode is 'a' and file exists, could try reading signature, but that's complex.
                # For now, rely on filename for writing if format is not given.
                pass # Keep detected_format as UNKNOWN if filename didn't give a clear format
        else: # It's a file-like object, format MUST be specified or inferred from mode for TAR
            if "gz" in mode or "bz2" in mode or "xz" in mode: # Heuristic for TAR streams
                if "gz" in mode: detected_format = ArchiveFormat.TAR_GZ
                elif "bz2" in mode: detected_format = ArchiveFormat.TAR_BZ2
                elif "xz" in mode: detected_format = ArchiveFormat.TAR_XZ
                else: # Should not happen based on outer if
                    raise ArchiveFormatError(
                        "Cannot determine TAR compression format from mode for file-like object. "
                        "Please specify the 'format' argument."
                    )
            else: # Default to ZIP for generic IO objects if no format/mode hint
                  # Or raise error if format is strictly required for IO objects
                # For now, let's require format for IO objects if not a clear TAR mode
                raise ArchiveFormatError(
                    "Format must be specified when 'archive' is a file-like object "
                    "and mode does not indicate a TAR compression (e.g., 'w:gz')."
                )
    else:
        detected_format = format

    if detected_format is None or detected_format == ArchiveFormat.UNKNOWN:
        # If still unknown, default to ZIP if it's a string path, otherwise error.
        if isinstance(archive, str) and not ("tar" in archive.lower() or ".t" in archive.lower()):
             # Basic heuristic if filename was like "myarchive.dat" -> assume zip
             # This might be too presumptive. Consider raising error if truly unknown.
            detected_format = ArchiveFormat.ZIP
            # print(f"Warning: Archive format is unknown, defaulting to ZIP for path '{archive}'.")
        else:
            raise ArchiveFormatError(
                f"Could not determine archive format for '{archive}'. "
                "Please specify the 'format' argument or use a standard file extension."
            )

    if detected_format not in WRITER_REGISTRY:
        raise UnsupportedArchiveError(f"Writing not supported for format: {detected_format.value}")

    writer_class, default_args = WRITER_REGISTRY[detected_format]
    
    # Combine default args with user-provided kwargs, user kwargs take precedence
    writer_kwargs = {**default_args, **kwargs}

    # Pass encoding to the writer's __init__
    if encoding:
        writer_kwargs['encoding'] = encoding

    # For TarWriter, the 'format' enum itself can be passed to __init__
    # to help it configure the tarfile mode correctly, especially if user mode is just "w".
    if writer_class is TarWriter:
        writer_kwargs['format'] = detected_format # Pass the detected format

    return writer_class(archive, mode=mode, **writer_kwargs)
